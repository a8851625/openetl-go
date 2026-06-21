package source

import (
	"context"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSource("demo", func(config map[string]any) (core.Source, error) {
		return NewDemoSource(config)
	})
}

// DemoSource generates synthetic records for testing and demos.
// Config:
//
//	interval_ms: delay between records (default 100)
//	count: total records to generate (0 = infinite)
//	fields: list of {name, type} where type is "counter"|"random"|"now"|"static:value"
type DemoSource struct {
	name       string
	intervalMs int
	count      int
	fields     []demoField
}

type demoField struct {
	name      string
	typ       string
	staticVal string
}

func NewDemoSource(config map[string]any) (*DemoSource, error) {
	s := &DemoSource{
		name:       "demo",
		intervalMs: 100,
		count:      0,
	}
	if v, ok := config["name"]; ok {
		if vs, ok := v.(string); ok {
			s.name = vs
		}
	}
	if v, ok := config["interval_ms"]; ok {
		switch iv := v.(type) {
		case int:
			s.intervalMs = iv
		case float64:
			s.intervalMs = int(iv)
		}
	}
	if v, ok := config["count"]; ok {
		switch cv := v.(type) {
		case int:
			s.count = cv
		case float64:
			s.count = int(cv)
		}
	}
	if v, ok := config["fields"]; ok {
		if fields, ok := v.([]any); ok {
			for _, f := range fields {
				if fm, ok := f.(map[string]any); ok {
					df := demoField{}
					if n, ok := fm["name"].(string); ok {
						df.name = n
					}
					if t, ok := fm["type"].(string); ok {
						df.typ = t
					}
					s.fields = append(s.fields, df)
				}
			}
		}
	}
	return s, nil
}

func (s *DemoSource) Name() string { return s.name }

func (s *DemoSource) Open(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
	var startIdx int64
	if cp != nil && len(cp.Position) > 0 {
		fmt.Sscanf(string(cp.Position), "%d", &startIdx)
	}
	return &demoReader{
		source:  s,
		counter: startIdx,
	}, nil
}

type demoReader struct {
	source  *DemoSource
	counter int64
}

func (r *demoReader) Read(ctx context.Context) (core.Record, error) {
	idx := atomic.AddInt64(&r.counter, 1) - 1
	if r.source.count > 0 && int(idx) >= r.source.count {
		return core.Record{}, io.EOF
	}
	if r.source.intervalMs > 0 {
		select {
		case <-time.After(time.Duration(r.source.intervalMs) * time.Millisecond):
		case <-ctx.Done():
			return core.Record{}, ctx.Err()
		}
	}

	data := make(map[string]any)
	for _, f := range r.source.fields {
		switch f.typ {
		case "counter":
			data[f.name] = idx
		case "random":
			data[f.name] = idx * 7 % 100
		case "now":
			data[f.name] = time.Now().Format(time.RFC3339)
		default:
			if len(f.typ) > 7 && f.typ[:7] == "static:" {
				data[f.name] = f.typ[7:]
			} else {
				data[f.name] = f.typ
			}
		}
	}

	return core.Record{
		Operation: core.OpInsert,
		Data:      data,
		Metadata: core.Metadata{
			Source:    r.source.name,
			Timestamp: time.Now(),
			Offset:    idx + 1,
		},
	}, nil
}

func (r *demoReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
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

func (r *demoReader) Snapshot(ctx context.Context) (core.Checkpoint, error) {
	return core.Checkpoint{
		Source:   r.source.name,
		Position: []byte(fmt.Sprintf("%d", atomic.LoadInt64(&r.counter))),
	}, nil
}

func (r *demoReader) CheckpointForRecord(ctx context.Context, rec core.Record) (core.Checkpoint, error) {
	pos := rec.Metadata.Offset
	if pos <= 0 {
		pos = atomic.LoadInt64(&r.counter)
	}
	return core.Checkpoint{
		Source:   r.source.name,
		Position: []byte(fmt.Sprintf("%d", pos)),
	}, nil
}

func (r *demoReader) Close() error { return nil }
