package source

import (
	"bufio"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSource("file", func(config map[string]any) (core.Source, error) {
		return NewFileSource(config)
	})
}

type FileSource struct {
	name      string
	path      string
	format    string
	delimiter rune
	hasHeader bool
	columns   []string
	batchSize int
}

func NewFileSource(config map[string]any) (*FileSource, error) {
	s := &FileSource{
		name:      "file",
		format:    "csv",
		delimiter: ',',
		hasHeader: true,
		batchSize: 1000,
	}
	if v, ok := config["name"]; ok {
		if vs, ok := v.(string); ok {
			s.name = vs
		}
	}
	if v, ok := config["path"]; ok {
		if vs, ok := v.(string); ok {
			s.path = vs
		}
	}
	if v, ok := config["format"]; ok {
		if vs, ok := v.(string); ok {
			s.format = vs
		}
	}
	if v, ok := config["delimiter"]; ok {
		if d, ok := v.(string); ok && len(d) > 0 {
			s.delimiter = rune(d[0])
		}
	}
	if v, ok := config["has_header"]; ok {
		if b, ok := v.(bool); ok {
			s.hasHeader = b
		}
	}
	s.batchSize = readInt(config, "batch_size", 1000)
	return s, nil
}

func (s *FileSource) Name() string { return s.name }

func (s *FileSource) Open(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
	switch s.format {
	case "csv":
		return s.openCSV(ctx, cp)
	case "json":
		return s.openJSON(ctx, cp)
	default:
		return nil, fmt.Errorf("unsupported file format: %s", s.format)
	}
}

// countingReader wraps an io.Reader to count bytes consumed for checkpoint tracking.
type countingReader struct {
	r        io.Reader
	consumed int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.consumed += int64(n)
	return n, err
}

func (s *FileSource) openCSV(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	var lastRecordOffset int64
	if cp != nil {
		var pos filePosition
		if json.Unmarshal(cp.Position, &pos) == nil {
			lastRecordOffset = pos.Offset
		}
	}

	csvr := csv.NewReader(f)
	csvr.Comma = s.delimiter
	csvr.LazyQuotes = true

	var headers []string
	if s.hasHeader {
		hdrRec, err := csvr.Read()
		if err != nil {
			f.Close()
			return nil, fmt.Errorf("read csv header: %w", err)
		}
		headers = hdrRec
	}
	for skipped := int64(0); skipped < lastRecordOffset; skipped++ {
		if _, err := csvr.Read(); err != nil {
			f.Close()
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("skip csv record %d: %w", skipped+1, err)
		}
	}

	return &csvReader{
		f:         f,
		csv:       csvr,
		headers:   headers,
		tableName: filepath.Base(s.path[:len(s.path)-len(filepath.Ext(s.path))]),
		offset:    lastRecordOffset,
	}, nil
}

// parseCSVLine parses a single CSV line into fields.
func parseCSVLine(line string, delim rune) ([]string, error) {
	r := csv.NewReader(strings.NewReader(line))
	r.Comma = delim
	r.LazyQuotes = true
	return r.Read()
}

func (s *FileSource) openJSON(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
	f, err := os.Open(s.path)
	if err != nil {
		return nil, fmt.Errorf("open file: %w", err)
	}

	var lastRecordOffset int64
	startByteOffset := int64(0)
	if cp != nil {
		var pos filePosition
		if json.Unmarshal(cp.Position, &pos) == nil {
			startByteOffset = pos.ByteOffset
			lastRecordOffset = pos.Offset
			if startByteOffset > 0 {
				if _, err := f.Seek(startByteOffset, io.SeekStart); err != nil {
					f.Close()
					return nil, fmt.Errorf("seek: %w", err)
				}
			}
		}
	}

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 1<<20), 10<<20)

	return &jsonReader{
		f:              f,
		scanner:        scanner,
		tableName:      filepath.Base(s.path[:len(s.path)-len(filepath.Ext(s.path))]),
		offset:         lastRecordOffset,
		byteOffset:     0,
		byteOffsetBase: startByteOffset,
	}, nil
}

type csvReader struct {
	f         *os.File
	csv       *csv.Reader
	headers   []string
	tableName string
	offset    int64
}

func (r *csvReader) Read(ctx context.Context) (core.Record, error) {
	row, err := r.csv.Read()
	if err != nil {
		if err == io.EOF {
			return core.Record{}, io.EOF
		}
		return core.Record{}, fmt.Errorf("csv read: %w", err)
	}
	return r.record(row), nil
}

func (r *csvReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
	var records []core.Record
	for i := 0; i < n; i++ {
		rec, err := r.Read(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return records, err
		}
		records = append(records, rec)
	}
	return records, nil
}

func (r *csvReader) record(row []string) core.Record {
	r.offset++
	data := make(map[string]any)
	if len(r.headers) > 0 {
		for i, h := range r.headers {
			if i < len(row) {
				data[h] = row[i]
			}
		}
	} else {
		for i, v := range row {
			data[fmt.Sprintf("col_%d", i)] = v
		}
	}
	return core.Record{
		Operation: core.OpInsert,
		Data:      data,
		Metadata: core.Metadata{
			Table:     r.tableName,
			Timestamp: time.Now(),
			Offset:    r.offset,
		},
	}
}

func (r *csvReader) Snapshot(ctx context.Context) (core.Checkpoint, error) {
	pos := filePosition{
		Offset:  r.offset,
		Headers: r.headers,
	}
	data, _ := json.Marshal(pos)
	return core.Checkpoint{Source: "file", Position: data, Timestamp: time.Now()}, nil
}

func (r *csvReader) CheckpointForRecord(ctx context.Context, rec core.Record) (core.Checkpoint, error) {
	pos := filePosition{
		Offset:  rec.Metadata.Offset,
		Headers: r.headers,
	}
	data, _ := json.Marshal(pos)
	return core.Checkpoint{Source: "file", Position: data, Timestamp: time.Now()}, nil
}

func (r *csvReader) Close() error { return r.f.Close() }

type jsonReader struct {
	f              *os.File
	scanner        *bufio.Scanner
	tableName      string
	offset         int64
	byteOffset     int64
	byteOffsetBase int64
}

func (r *jsonReader) Read(ctx context.Context) (core.Record, error) {
	if !r.scanner.Scan() {
		if err := r.scanner.Err(); err != nil {
			return core.Record{}, err
		}
		return core.Record{}, io.EOF
	}
	r.offset++
	r.byteOffset += int64(len(r.scanner.Bytes())) + 1

	var data map[string]any
	if err := json.Unmarshal(r.scanner.Bytes(), &data); err != nil {
		return core.Record{}, fmt.Errorf("json decode: %w", err)
	}

	return core.Record{
		Operation: core.OpInsert,
		Data:      data,
		Metadata: core.Metadata{
			Table:     r.tableName,
			Timestamp: time.Now(),
			Offset:    r.offset,
		},
	}, nil
}

func (r *jsonReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
	var records []core.Record
	for i := 0; i < n; i++ {
		rec, err := r.Read(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return records, err
		}
		records = append(records, rec)
	}
	return records, nil
}

func (r *jsonReader) Snapshot(ctx context.Context) (core.Checkpoint, error) {
	pos := filePosition{Offset: r.offset, ByteOffset: r.byteOffsetBase + r.byteOffset}
	data, _ := json.Marshal(pos)
	return core.Checkpoint{Source: "file", Position: data, Timestamp: time.Now()}, nil
}

func (r *jsonReader) CheckpointForRecord(ctx context.Context, rec core.Record) (core.Checkpoint, error) {
	pos := filePosition{Offset: rec.Metadata.Offset, ByteOffset: r.byteOffsetBase + r.byteOffset}
	data, _ := json.Marshal(pos)
	return core.Checkpoint{Source: "file", Position: data, Timestamp: time.Now()}, nil
}

func (r *jsonReader) Close() error { return r.f.Close() }

type filePosition struct {
	Offset     int64    `json:"offset"`
	ByteOffset int64    `json:"byte_offset,omitempty"`
	Headers    []string `json:"headers,omitempty"`
}
