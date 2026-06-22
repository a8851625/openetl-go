package core

import (
	"context"
	"encoding/json"
	"errors"
	"time"
)

var ErrRecordFiltered = errors.New("record filtered")

type OpType string

const (
	OpInsert OpType = "INSERT"
	OpUpdate OpType = "UPDATE"
	OpDelete OpType = "DELETE"
	OpDDL    OpType = "DDL"
)

type Metadata struct {
	Source     string    `json:"source"`
	Table      string    `json:"table"`
	Key        string    `json:"key,omitempty"`
	Timestamp  time.Time `json:"timestamp"`
	BinlogFile string    `json:"binlog_file,omitempty"`
	BinlogPos  uint32    `json:"binlog_pos,omitempty"`
	Gtid       string    `json:"gtid,omitempty"`
	LSN        string    `json:"lsn,omitempty"`
	Partition  int32     `json:"partition,omitempty"`
	Offset     int64     `json:"offset,omitempty"`
	DDL        string    `json:"ddl,omitempty"`
	// Route is set by the router transform to a downstream route tag (TF-5).
	// Separate from Source so provenance (which pipeline/source produced the
	// record, used by DLQ entries + metrics) is preserved while edges have a
	// dedicated field to match on.
	Route string `json:"route,omitempty"`
}

type Record struct {
	Operation OpType         `json:"operation"`
	Data      map[string]any `json:"data"`
	Before    map[string]any `json:"before"`
	Metadata  Metadata       `json:"metadata"`
}

type Checkpoint struct {
	ID        string          `json:"id"`
	JobName   string          `json:"job_name"`
	Source    string          `json:"source"`
	Position  json.RawMessage `json:"position"`
	Timestamp time.Time       `json:"timestamp"`
}

type CheckpointStore interface {
	Save(ctx context.Context, cp Checkpoint) error
	Load(ctx context.Context, jobName string) (*Checkpoint, error)
	Delete(ctx context.Context, jobName string) error
	List(ctx context.Context) ([]Checkpoint, error)
}

type Source interface {
	Open(ctx context.Context, cp *Checkpoint) (RecordReader, error)
	Name() string
}

type RecordReader interface {
	Read(ctx context.Context) (Record, error)
	ReadBatch(ctx context.Context, n int) ([]Record, error)
	Snapshot(ctx context.Context) (Checkpoint, error)
	Close() error
}

type RecordCheckpointer interface {
	CheckpointForRecord(ctx context.Context, rec Record) (Checkpoint, error)
}

type Sink interface {
	Open(ctx context.Context) error
	Write(ctx context.Context, records []Record) error
	Close() error
	Name() string
}

// SinkMetrics provides per-sink write metrics for Prometheus exposure.
// Sinks that track metrics internally implement this optional interface.
type SinkMetrics struct {
	SinkName     string
	RowsWritten  int64
	BatchesSent  int64
	WriteLatency float64 // milliseconds
	Errors       int64
}

// SinkMetricsProvider is an optional interface that sinks implement to expose
// internal write metrics. The pipeline runner collects these for Prometheus.
type SinkMetricsProvider interface {
	SinkMetrics() SinkMetrics
}

// SchemaInfo describes the schema of a source or sink for validation.
type SchemaInfo struct {
	Columns []ColumnInfo
}

// ColumnInfo describes a single column.
type ColumnInfo struct {
	Name     string
	DataType string // e.g., "INT", "VARCHAR(255)", "DateTime64(3)"
	Nullable bool
}

// SchemaDescriptor is an optional interface that sources implement to describe
// their output schema. This enables schema validation and auto-create.
type SchemaDescriptor interface {
	Describe(ctx context.Context) (SchemaInfo, error)
}

// SchemaValidator is an optional interface that sinks implement to validate
// source schema compatibility before starting a pipeline.
type SchemaValidator interface {
	ValidateSchema(ctx context.Context, schema SchemaInfo) error
}

type Transform interface {
	Apply(ctx context.Context, rec Record) (Record, error)
	Name() string
}

// BatchTransform is an optional interface for transforms that operate on
// entire batches rather than single records. If any transform in the chain
// implements this, TransformChain.ApplyBatch will route through it.
// Useful for windowing, aggregation, multi-record enrichment, etc.
type BatchTransform interface {
	Transform
	// ApplyBatch receives the entire batch and returns the (possibly
	// modified, possibly fewer or more) batch.
	ApplyBatch(ctx context.Context, recs []Record) ([]Record, error)
}

// Flusher is an optional interface for stateful transforms that need to
// emit remaining buffered data when the pipeline is shutting down or
// flushing a partial batch.
type Flusher interface {
	// Flush returns any records accumulated in internal buffers.
	Flush(ctx context.Context) ([]Record, error)
}

type TransformChain []Transform

func (tc TransformChain) Apply(ctx context.Context, rec Record) (Record, error) {
	// Defensive copy of Data so one transform's in-place mutation can't leak
	// into another's state (e.g. join buffers the record pointer) or back into
	// the source's batch (TF-11). A shallow map copy isolates keyed writes;
	// nested values are shared but the documented hazard is top-level mutation.
	rec.Data = cloneDataMap(rec.Data)
	var err error
	for _, t := range tc {
		rec, err = t.Apply(ctx, rec)
		if err != nil {
			return rec, err
		}
	}
	return rec, nil
}

// cloneDataMap returns a shallow copy of m (new map, same values). nil-safe.
func cloneDataMap(m map[string]any) map[string]any {
	if m == nil {
		return nil
	}
	out := make(map[string]any, len(m))
	for k, v := range m {
		out[k] = v
	}
	return out
}

// ApplyBatch processes a batch through the chain. If any transform implements
// BatchTransform, the batch is routed through ApplyBatch; otherwise each
// record is processed individually via Apply.
func (tc TransformChain) ApplyBatch(ctx context.Context, recs []Record) ([]Record, error) {
	for _, t := range tc {
		if bt, ok := t.(BatchTransform); ok {
			out, err := bt.ApplyBatch(ctx, recs)
			if err != nil {
				return recs, err
			}
			recs = out
			if len(recs) == 0 {
				return recs, nil
			}
		} else {
			out := make([]Record, 0, len(recs))
			for _, rec := range recs {
				processed, err := t.Apply(ctx, rec)
				if err != nil {
					if errors.Is(err, ErrRecordFiltered) {
						continue
					}
					return recs, err
				}
				out = append(out, processed)
			}
			recs = out
		}
	}
	return recs, nil
}

// FlushChain calls Flush on any transform in the chain that implements Flusher.
func (tc TransformChain) FlushChain(ctx context.Context) ([]Record, error) {
	var all []Record
	for _, t := range tc {
		if f, ok := t.(Flusher); ok {
			out, err := f.Flush(ctx)
			if err != nil {
				return all, err
			}
			all = append(all, out...)
		}
	}
	return all, nil
}

// TransformCloser is an optional interface for transforms that spawn
// background goroutines or hold resources needing cleanup.
type TransformCloser interface {
	Close() error
}

// CloseChain calls Close on any transform in the chain that implements TransformCloser.
// This prevents goroutine leaks on pipeline stop/restart.
func (tc TransformChain) CloseChain() {
	for _, t := range tc {
		if c, ok := t.(TransformCloser); ok {
			_ = c.Close()
		}
	}
}

func (tc TransformChain) Name() string {
	return "chain"
}
