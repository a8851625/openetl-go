package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/parquet-go/parquet-go"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSink("s3", func(config map[string]any) (core.Sink, error) {
		return NewS3Sink(config)
	})
	registry.RegisterSink("file_sink", func(config map[string]any) (core.Sink, error) {
		return NewFileSink(config)
	})
}

type FileSinkConfig struct {
	OutputDir string
	Format    string
	Prefix    string
	Endpoint  string
	Region    string
	Bucket    string
	AccessKey string
	SecretKey string
	// MaxRetries configures retry attempts on S3 5xx errors (default 3).
	MaxRetries int
	// RetryBaseMs is the base delay for exponential backoff (default 500ms).
	RetryBaseMs int
}

type FileSink struct {
	name   string
	config FileSinkConfig
	file   *os.File
	buf    *bytes.Buffer
	mu     sync.Mutex
	client *minio.Client
}

func NewS3Sink(config map[string]any) (*FileSink, error) {
	cfg := FileSinkConfig{
		Format:      "json",
		OutputDir:   "/tmp/etl-output",
		MaxRetries:  3,
		RetryBaseMs: 500,
	}
	if v, ok := config["endpoint"]; ok {
		if vs, ok := v.(string); ok {
			cfg.Endpoint = vs
		}
	}
	if v, ok := config["region"]; ok {
		if vs, ok := v.(string); ok {
			cfg.Region = vs
		}
	}
	if v, ok := config["bucket"]; ok {
		if vs, ok := v.(string); ok {
			cfg.Bucket = vs
		}
	}
	if v, ok := config["access_key"]; ok {
		if vs, ok := v.(string); ok {
			cfg.AccessKey = vs
		}
	}
	if v, ok := config["secret_key"]; ok {
		if vs, ok := v.(string); ok {
			cfg.SecretKey = vs
		}
	}
	if v, ok := config["output_dir"]; ok {
		if vs, ok := v.(string); ok {
			cfg.OutputDir = vs
		}
	}
	if v, ok := config["format"]; ok {
		if vs, ok := v.(string); ok {
			cfg.Format = vs
		}
	}
	if v, ok := config["prefix"]; ok {
		if vs, ok := v.(string); ok {
			cfg.Prefix = vs
		}
	}
	if v, ok := config["max_retries"]; ok {
		switch mr := v.(type) {
		case int:
			cfg.MaxRetries = mr
		case float64:
			cfg.MaxRetries = int(mr)
		}
	}
	if v, ok := config["retry_base_ms"]; ok {
		switch rb := v.(type) {
		case int:
			cfg.RetryBaseMs = rb
		case float64:
			cfg.RetryBaseMs = int(rb)
		}
	}
	s := &FileSink{name: "s3", config: cfg, buf: &bytes.Buffer{}}
	return s, nil
}

func NewFileSink(config map[string]any) (*FileSink, error) {
	cfg := FileSinkConfig{
		Format:      "json",
		OutputDir:   "/tmp/etl-output",
		MaxRetries:  3,
		RetryBaseMs: 500,
	}
	if v, ok := config["output_dir"]; ok {
		if vs, ok := v.(string); ok {
			cfg.OutputDir = vs
		}
	}
	if v, ok := config["format"]; ok {
		if vs, ok := v.(string); ok {
			cfg.Format = vs
		}
	}
	if v, ok := config["prefix"]; ok {
		if vs, ok := v.(string); ok {
			cfg.Prefix = vs
		}
	}
	if v, ok := config["path"]; ok {
		if vs, ok := v.(string); ok {
			cfg.OutputDir = filepath.Dir(vs)
		}
	}
	s := &FileSink{name: "file_sink", config: cfg, buf: &bytes.Buffer{}}
	return s, nil
}

func (s *FileSink) Name() string { return s.name }

func (s *FileSink) Open(ctx context.Context) error {
	if s.config.Endpoint != "" && s.config.Bucket != "" {
		endpoint := strings.TrimPrefix(strings.TrimPrefix(s.config.Endpoint, "http://"), "https://")
		client, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(s.config.AccessKey, s.config.SecretKey, ""),
			Secure: strings.HasPrefix(s.config.Endpoint, "https://"),
		})
		if err != nil {
			return fmt.Errorf("create s3 client: %w", err)
		}
		exists, err := client.BucketExists(ctx, s.config.Bucket)
		if err != nil {
			return fmt.Errorf("check bucket: %w", err)
		}
		if !exists {
			if err := client.MakeBucket(ctx, s.config.Bucket, minio.MakeBucketOptions{Region: s.config.Region}); err != nil {
				return fmt.Errorf("create bucket: %w", err)
			}
		}
		s.client = client
	}
	return os.MkdirAll(s.config.OutputDir, 0755)
}

func (s *FileSink) Write(ctx context.Context, records []core.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Filter out DELETE operations. S3/file sinks are append-only and
	// cannot meaningfully represent row deletions. Writing DELETE records
	// as data rows would confuse downstream consumers.
	var writable []core.Record
	for _, rec := range records {
		if rec.Operation == core.OpDelete {
			continue
		}
		writable = append(writable, rec)
	}
	if len(writable) == 0 {
		return nil
	}

	payload, err := s.encode(writable)
	if err != nil {
		return err
	}

	// Deterministic object name based on content hash so that replay of the
	// same batch produces the same object name (overwrite/idempotent) rather
	// than creating duplicate objects. The date prefix preserves ordering.
	sum := sha256.Sum256(payload)
	datePrefix := time.Now().UTC().Format("2006/01/02/")
	objectName := fmt.Sprintf("%s%s%s.%s", s.config.Prefix, datePrefix, hex.EncodeToString(sum[:16]), s.config.Format)

	if s.client != nil {
		return s.uploadWithRetry(ctx, objectName, payload)
	}

	// Atomic file write: write to temp file then rename
	filename := filepath.Join(s.config.OutputDir, objectName)
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	tmpFilename := filename + ".tmp"
	if err := os.WriteFile(tmpFilename, payload, 0644); err != nil {
		os.Remove(tmpFilename)
		return err
	}
	return os.Rename(tmpFilename, filename)
}

func (s *FileSink) uploadWithRetry(ctx context.Context, objectName string, payload []byte) error {
	var lastErr error
	size := int64(len(payload))

	for attempt := 0; attempt <= s.config.MaxRetries; attempt++ {
		if attempt > 0 {
			delay := time.Duration(s.config.RetryBaseMs<<(attempt-1)) * time.Millisecond
			select {
			case <-time.After(delay):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		reader := bytes.NewReader(payload)
		uploadInfo, err := s.client.PutObject(ctx, s.config.Bucket, objectName, reader, size, minio.PutObjectOptions{
			ContentType:    s.contentType(),
			PartSize:       5 * 1024 * 1024,
			SendContentMd5: true,
		})
		if err == nil {
			_ = uploadInfo
			return nil
		}
		lastErr = err
		if !isRetryableS3Error(err) {
			return fmt.Errorf("s3 upload: %w", err)
		}
		_, _ = reader.Seek(0, io.SeekStart)
	}
	return fmt.Errorf("s3 upload failed after %d retries: %w", s.config.MaxRetries+1, lastErr)
}

func isRetryableS3Error(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "500") ||
		strings.Contains(msg, "503") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "temporary failure") ||
		strings.Contains(msg, "slow down")
}

func (s *FileSink) encode(records []core.Record) ([]byte, error) {
	var buf bytes.Buffer

	switch s.config.Format {
	case "json", "jsonl":
		for _, rec := range records {
			data, _ := json.Marshal(rec.Data)
			buf.Write(data)
			buf.WriteByte('\n')
		}

	case "csv":
		w := csv.NewWriter(&buf)
		var cols []string
		if len(records) > 0 {
			for k := range records[0].Data {
				cols = append(cols, k)
			}
			sort.Strings(cols)
			w.Write(cols)
		}
		for _, rec := range records {
			var row []string
			for _, col := range cols {
				v := rec.Data[col]
				row = append(row, fmt.Sprintf("%v", v))
			}
			w.Write(row)
		}
		w.Flush()
		if err := w.Error(); err != nil {
			return nil, err
		}

	case "parquet":
		return s.encodeParquet(records)
	}

	return buf.Bytes(), nil
}

func (s *FileSink) contentType() string {
	switch s.config.Format {
	case "csv":
		return "text/csv"
	default:
		return "application/json"
	}
}

func (s *FileSink) Close() error { return nil }

func (s *FileSink) encodeParquet(records []core.Record) ([]byte, error) {
	if len(records) == 0 {
		return nil, nil
	}

	colSet := map[string]bool{}
	for _, rec := range records {
		for k := range rec.Data {
			colSet[k] = true
		}
	}
	cols := make([]string, 0, len(colSet))
	for k := range colSet {
		cols = append(cols, k)
	}
	sort.Strings(cols)

	node := parquet.Group{}
	for _, col := range cols {
		node[col] = parquet.String()
	}
	pschema := parquet.NewSchema("record", node)

	buf := new(bytes.Buffer)
	pwriter := parquet.NewWriter(buf, pschema)

	for _, rec := range records {
		row := make(map[string]string, len(cols))
		for _, col := range cols {
			if v, ok := rec.Data[col]; ok {
				row[col] = fmt.Sprintf("%v", v)
			}
		}
		if err := pwriter.Write(row); err != nil {
			return nil, fmt.Errorf("parquet write row: %w", err)
		}
	}

	if err := pwriter.Close(); err != nil {
		return nil, fmt.Errorf("parquet close: %w", err)
	}

	return buf.Bytes(), nil
}
