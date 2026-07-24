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
	Database   string    `json:"database,omitempty"`
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
	// ColumnTypes maps field name → declared source type (MySQL COLUMN_TYPE,
	// Debezium/Kafka Connect field type name, etc.). Sinks with auto_create
	// prefer these over sample-value inference when building target DDL.
	// Not serialized into DLQ payloads by default (omitempty keeps size down).
	ColumnTypes map[string]string `json:"column_types,omitempty"`
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

// SinkCommitMetadataProvider optionally exposes sink-native commit metadata
// after a successful Write call. The runner stores this together with the
// source position and state snapshot versions in the checkpoint envelope.
// Returning an error prevents checkpoint advancement: the sink write may have
// committed, so replay is safer than persisting an unverifiable boundary.
type SinkCommitMetadataProvider interface {
	SinkCommitMetadata(ctx context.Context) (map[string]any, error)
}

// SchemaInfo describes the schema of a source or sink for validation.
type SchemaInfo struct {
	Columns []ColumnInfo
}

// ColumnInfo describes a single column.
type ColumnInfo struct {
	Name      string
	DataType  string // e.g., "INT", "VARCHAR(255)", "DateTime64(3)"
	Nullable  bool
	Generated bool // true for MySQL VIRTUAL/STORED or PostgreSQL GENERATED columns; sinks must exclude them from INSERT/UPDATE column sets
}

// SchemaDescriptor is an optional interface that sources may implement to describe
// their output schema. When a source implements this, the runner calls Describe
// during startup to obtain column metadata. If the sink also implements
// SchemaValidator, the schema is validated before the pipeline starts reading records.
//
// Built-in coverage starts with mysql_batch and single-table MySQL CDC sources;
// custom sources can implement the same interface through the Source SDK. The
// runner treats sources that do not implement this interface as schema-unknown
// and skips startup validation.
type SchemaDescriptor interface {
	Describe(ctx context.Context) (SchemaInfo, error)
}

// SchemaValidator is an optional interface that sinks may implement to validate
// source schema compatibility before a pipeline starts. When both the source
// (SchemaDescriptor) and the sink (SchemaValidator) implement their respective
// interfaces, the runner calls ValidateSchema after Open to check that the source
// output columns are compatible with the sink's expectations.
//
// A validator should return an error describing the incompatibility if the
// schema cannot be accepted (e.g., missing required columns, type mismatches
// that the sink cannot coerce). Returning nil means the sink accepts the schema.
//
// Built-in coverage starts with MySQL, PostgreSQL, and ClickHouse sinks. Custom
// sinks can implement the same interface through the Sink SDK.
type SchemaValidator interface {
	ValidateSchema(ctx context.Context, schema SchemaInfo) error
}

type Transform interface {
	Apply(ctx context.Context, rec Record) (Record, error)
	Name() string
}

// TransformMetrics exposes transform-specific counters for node-level
// observability, for example join hit/miss or window emit/late counters.
type TransformMetrics struct {
	Node      string           `json:"node"`
	Transform string           `json:"transform"`
	Counters  map[string]int64 `json:"counters"`
}

// TransformMetricsProvider is an optional interface for transforms with
// domain-specific runtime counters.
type TransformMetricsProvider interface {
	TransformMetrics() TransformMetrics
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

// StateSnapshotter is an optional interface for stateful transforms that can
// expose the current durable state snapshot version for checkpoint envelopes.
type StateSnapshotter interface {
	SnapshotState(ctx context.Context) (node string, version string, ok bool, err error)
}

// StateMetrics describes the current durable state footprint for one
// stateful transform node.
type StateMetrics struct {
	Pipeline  string    `json:"pipeline"`
	Node      string    `json:"node"`
	Keys      int       `json:"keys"`
	Bytes     int64     `json:"bytes"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// StateMetricsProvider is an optional interface for stateful transforms that
// expose StateStore size/freshness metrics.
type StateMetricsProvider interface {
	StateMetrics(ctx context.Context) (StateMetrics, bool, error)
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
	var failures []TransformRecordFailure
	for _, t := range tc {
		if bt, ok := t.(BatchTransform); ok {
			out, err := bt.ApplyBatch(ctx, recs)
			if err != nil {
				var partial PartialTransformError
				if !errors.As(err, &partial) {
					return recs, err
				}
				failures = append(failures, partial.FailedRecords()...)
			}
			recs = out
			if len(recs) == 0 {
				break
			}
		} else {
			out := make([]Record, 0, len(recs))
			for _, rec := range recs {
				processed, err := t.Apply(ctx, rec)
				if err != nil {
					if errors.Is(err, ErrRecordFiltered) {
						continue
					}
					failures = append(failures, TransformRecordFailure{Record: rec, Err: err})
					continue
				}
				out = append(out, processed)
			}
			recs = out
		}
	}
	if len(failures) > 0 {
		return recs, NewPartialTransformError("partial transform failed", failures)
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

// StateSnapshotVersions collects durable state snapshot versions from
// transforms that implement StateSnapshotter.
func (tc TransformChain) StateSnapshotVersions(ctx context.Context) (map[string]string, error) {
	versions := make(map[string]string)
	for _, t := range tc {
		snapper, ok := t.(StateSnapshotter)
		if !ok {
			continue
		}
		node, version, hasState, err := snapper.SnapshotState(ctx)
		if err != nil {
			return nil, err
		}
		if hasState && node != "" && version != "" {
			versions[node] = version
		}
	}
	if len(versions) == 0 {
		return nil, nil
	}
	return versions, nil
}

// StateMetrics collects durable state size/freshness metrics from transforms
// that implement StateMetricsProvider.
func (tc TransformChain) StateMetrics(ctx context.Context) ([]StateMetrics, error) {
	var metrics []StateMetrics
	for _, t := range tc {
		provider, ok := t.(StateMetricsProvider)
		if !ok {
			continue
		}
		m, hasState, err := provider.StateMetrics(ctx)
		if err != nil {
			return nil, err
		}
		if hasState {
			metrics = append(metrics, m)
		}
	}
	return metrics, nil
}

// TransformMetrics collects domain-specific metrics from transforms that
// implement TransformMetricsProvider.
func (tc TransformChain) TransformMetrics() []TransformMetrics {
	var metrics []TransformMetrics
	for _, t := range tc {
		provider, ok := t.(TransformMetricsProvider)
		if !ok {
			continue
		}
		m := provider.TransformMetrics()
		if m.Transform == "" {
			m.Transform = t.Name()
		}
		if len(m.Counters) > 0 {
			metrics = append(metrics, m)
		}
	}
	return metrics
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
