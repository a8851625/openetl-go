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
	sinkCounters // P4-20: per-sink write metrics (SK-4)
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

// SinkMetrics implements core.SinkMetricsProvider (P4-20, SK-4).
func (s *FileSink) SinkMetrics() core.SinkMetrics { return s.metricsFor(s.name) }

func (s *FileSink) Open(ctx context.Context) error {
	if s.config.Endpoint != "" && s.config.Bucket != "" {
		endpoint := strings.TrimPrefix(strings.TrimPrefix(s.config.Endpoint, "http://"), "https://")
		client, err := minio.New(endpoint, &minio.Options{
			Creds:  credentials.NewStaticV4(s.config.AccessKey, s.config.SecretKey, ""),
			Secure: strings.HasPrefix(s.config.Endpoint, "https://"),
		})
		if err != nil {
			return fmt.Errorf("create s3 client (endpoint %s, bucket %s, region %s): %w", s.config.Endpoint, s.config.Bucket, s.config.Region, err) // P5-15: WHERE context
		}
		exists, err := client.BucketExists(ctx, s.config.Bucket)
		if err != nil {
			return fmt.Errorf("check bucket: %w", err)
		}
		if !exists {
			if err := client.MakeBucket(ctx, s.config.Bucket, minio.MakeBucketOptions{Region: s.config.Region}); err != nil {
				return fmt.Errorf("create bucket %s (endpoint %s, region %s): %w", s.config.Bucket, s.config.Endpoint, s.config.Region, err) // P5-15: WHERE context
			}
		}
		s.client = client
	}
	return os.MkdirAll(s.config.OutputDir, 0755)
}

func (s *FileSink) Write(ctx context.Context, records []core.Record) (err error) {
	defer func() { if err != nil { s.recordError() } }() // P5-12: count write failures
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
	start := time.Now()

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
		if err := s.uploadWithRetry(ctx, objectName, payload); err != nil {
			return err
		}
		s.recordMetrics(len(writable), time.Since(start))
		return nil
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
	if err := os.Rename(tmpFilename, filename); err != nil {
		return err
	}
	s.recordMetrics(len(writable), time.Since(start))
	return nil
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

	// Infer each column's type from its first non-nil sample value so the
	// Parquet schema is typed (int/double/bool/timestamp) instead of every
	// column being String (P4-21, SK-5). Columns are Optional so sparse rows
	// (a record missing a column) encode as null rather than failing.
	colKind := make(map[string]parquetTypeKind, len(cols))
	node := parquet.Group{}
	for _, col := range cols {
		k := inferParquetKind(records, col)
		colKind[col] = k
		node[col] = parquet.Optional(parquetNodeFor(k))
	}
	pschema := parquet.NewSchema("record", node)

	buf := new(bytes.Buffer)
	pwriter := parquet.NewWriter(buf, pschema)

	for _, rec := range records {
		row := make(map[string]any, len(cols))
		for _, col := range cols {
			if v, ok := rec.Data[col]; ok {
				row[col] = coerceParquetValue(v, colKind[col])
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

// parquetTypeKind classifies a parquet column's logical type.
type parquetTypeKind int

const (
	pqString parquetTypeKind = iota
	pqInt
	pqDouble
	pqBool
	pqTimestamp
)

// inferParquetKind samples the first non-nil value of a column to pick its
// parquet type. Unknown types fall back to String (lossy, but never wrong).
func inferParquetKind(records []core.Record, col string) parquetTypeKind {
	for _, rec := range records {
		v, ok := rec.Data[col]
		if !ok || v == nil {
			continue
		}
		switch v.(type) {
		case bool:
			return pqBool
		case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
			return pqInt
		case float32, float64:
			return pqDouble
		case time.Time:
			return pqTimestamp
		default:
			return pqString
		}
	}
	return pqString
}

func parquetNodeFor(k parquetTypeKind) parquet.Node {
	switch k {
	case pqBool:
		return parquet.Leaf(parquet.BooleanType)
	case pqInt:
		return parquet.Int(64)
	case pqDouble:
		return parquet.Leaf(parquet.DoubleType)
	case pqTimestamp:
		return parquet.Timestamp(parquet.Millisecond)
	default:
		return parquet.String()
	}
}

// coerceParquetValue converts a record value to the Go type matching the column
// node so parquet-go encodes it under the typed schema (not as a string).
func coerceParquetValue(v any, k parquetTypeKind) any {
	if v == nil {
		return nil
	}
	switch k {
	case pqInt:
		if n, ok := toInt64Value(v); ok {
			return n
		}
	case pqDouble:
		if f, ok := toFloat64Value(v); ok {
			return f
		}
	case pqBool:
		if b, ok := v.(bool); ok {
			return b
		}
	case pqTimestamp:
		if t, ok := v.(time.Time); ok {
			return t
		}
	}
	return fmt.Sprintf("%v", v)
}

func toInt64Value(v any) (int64, bool) {
	switch x := v.(type) {
	case int:
		return int64(x), true
	case int8:
		return int64(x), true
	case int16:
		return int64(x), true
	case int32:
		return int64(x), true
	case int64:
		return x, true
	case uint:
		return int64(x), true
	case uint8:
		return int64(x), true
	case uint16:
		return int64(x), true
	case uint32:
		return int64(x), true
	case uint64:
		return int64(x), true
	}
	return 0, false
}

func toFloat64Value(v any) (float64, bool) {
	switch x := v.(type) {
	case float32:
		return float64(x), true
	case float64:
		return x, true
	}
	return 0, false
}
