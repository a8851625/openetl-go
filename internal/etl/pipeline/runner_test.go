package pipeline

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	"github.com/a8851625/openetl-go/internal/etl/checkpoint"
	"github.com/a8851625/openetl-go/internal/etl/core"

	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	_ "github.com/a8851625/openetl-go/internal/etl/source"
)

// recordingSink counts Write calls and optionally returns an error on every call.
type recordingSink struct {
	mu         sync.Mutex
	calls      int32
	alwaysFail bool
}

func (s *recordingSink) Name() string                 { return "recording" }
func (s *recordingSink) Open(_ context.Context) error { return nil }
func (s *recordingSink) Write(_ context.Context, _ []core.Record) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	atomic.AddInt32(&s.calls, 1)
	if s.alwaysFail {
		return errors.New("injected write error")
	}
	return nil
}
func (s *recordingSink) Close() error { return nil }

type partialBatchSink struct {
	batchCalls  int32
	singleCalls int32
}

func (s *partialBatchSink) Name() string               { return "partial-batch" }
func (s *partialBatchSink) Open(context.Context) error { return nil }
func (s *partialBatchSink) Write(_ context.Context, records []core.Record) error {
	if len(records) > 1 {
		atomic.AddInt32(&s.batchCalls, 1)
		return testPartialBatchError{}
	}
	atomic.AddInt32(&s.singleCalls, 1)
	return errors.New("single-row isolation should not run")
}
func (s *partialBatchSink) Close() error { return nil }

type testPartialBatchError struct{}

func (e testPartialBatchError) Error() string { return "partial batch failed" }
func (e testPartialBatchError) FailedRecordIndices() []int {
	return []int{1}
}
func (e testPartialBatchError) ErrorForRecord(index int) error {
	if index != 1 {
		return nil
	}
	return core.ClassifiedError{Class: core.ErrorClassSchema, Err: errors.New("record 1 schema mismatch")}
}

type transientThenSuccessSink struct {
	failures int32
	calls    int32
}

func (s *transientThenSuccessSink) Name() string               { return "transient-then-success" }
func (s *transientThenSuccessSink) Open(context.Context) error { return nil }
func (s *transientThenSuccessSink) Write(_ context.Context, _ []core.Record) error {
	call := atomic.AddInt32(&s.calls, 1)
	if call <= s.failures {
		return errors.New("Error 1205 (HY000): Lock wait timeout exceeded; try restarting transaction")
	}
	return nil
}
func (s *transientThenSuccessSink) Close() error { return nil }

type schemaSource struct {
	schema    core.SchemaInfo
	openCalls int32
}

func (s *schemaSource) Name() string { return "schema-source" }
func (s *schemaSource) Open(_ context.Context, _ *core.Checkpoint) (core.RecordReader, error) {
	atomic.AddInt32(&s.openCalls, 1)
	return checkpointTestReader{}, nil
}
func (s *schemaSource) Describe(_ context.Context) (core.SchemaInfo, error) {
	return s.schema, nil
}

type schemaValidatingSink struct {
	validateErr error
	openCalls   int32
	closeCalls  int32
}

func (s *schemaValidatingSink) Name() string { return "schema-validating-sink" }
func (s *schemaValidatingSink) Open(_ context.Context) error {
	atomic.AddInt32(&s.openCalls, 1)
	return nil
}
func (s *schemaValidatingSink) Write(_ context.Context, _ []core.Record) error { return nil }
func (s *schemaValidatingSink) Close() error {
	atomic.AddInt32(&s.closeCalls, 1)
	return nil
}
func (s *schemaValidatingSink) ValidateSchema(_ context.Context, _ core.SchemaInfo) error {
	return s.validateErr
}

type checkpointTestReader struct{}

func (r checkpointTestReader) Read(_ context.Context) (core.Record, error) {
	return core.Record{}, io.EOF
}
func (r checkpointTestReader) ReadBatch(_ context.Context, _ int) ([]core.Record, error) {
	return nil, io.EOF
}
func (r checkpointTestReader) Snapshot(_ context.Context) (core.Checkpoint, error) {
	return core.Checkpoint{}, nil
}
func (r checkpointTestReader) Close() error { return nil }
func (r checkpointTestReader) CheckpointForRecord(_ context.Context, rec core.Record) (core.Checkpoint, error) {
	pos, _ := json.Marshal(map[string]any{"offset": rec.Metadata.Offset})
	return core.Checkpoint{Source: "checkpoint-test", Position: pos}, nil
}

type batchCountingReader struct {
	mu         sync.Mutex
	batches    [][]core.Record
	readCalls  int32
	batchCalls int32
}

func (r *batchCountingReader) Read(context.Context) (core.Record, error) {
	atomic.AddInt32(&r.readCalls, 1)
	return core.Record{}, io.EOF
}
func (r *batchCountingReader) ReadBatch(_ context.Context, _ int) ([]core.Record, error) {
	atomic.AddInt32(&r.batchCalls, 1)
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.batches) == 0 {
		return nil, io.EOF
	}
	out := r.batches[0]
	r.batches = r.batches[1:]
	return out, nil
}
func (r *batchCountingReader) Snapshot(context.Context) (core.Checkpoint, error) {
	return core.Checkpoint{}, nil
}
func (r *batchCountingReader) Close() error { return nil }

type filterAllTransform struct{}

func (t filterAllTransform) Name() string { return "filter-all" }
func (t filterAllTransform) Apply(_ context.Context, rec core.Record) (core.Record, error) {
	return rec, core.ErrRecordFiltered
}

type failAllTransform struct{}

func (t failAllTransform) Name() string { return "fail-all" }
func (t failAllTransform) Apply(_ context.Context, rec core.Record) (core.Record, error) {
	return rec, errors.New("injected transform error")
}

type zeroBatchTransform struct{}

func (t zeroBatchTransform) Name() string { return "zero-batch" }
func (t zeroBatchTransform) Apply(_ context.Context, rec core.Record) (core.Record, error) {
	return rec, nil
}
func (t zeroBatchTransform) ApplyBatch(_ context.Context, _ []core.Record) ([]core.Record, error) {
	return nil, nil
}

type partialFailureTransform struct{}

func (t partialFailureTransform) Name() string { return "partial-failure" }
func (t partialFailureTransform) Apply(_ context.Context, rec core.Record) (core.Record, error) {
	return rec, nil
}
func (t partialFailureTransform) ApplyBatch(_ context.Context, recs []core.Record) ([]core.Record, error) {
	if len(recs) < 2 {
		return recs, nil
	}
	failures := []core.TransformRecordFailure{{
		Record: recs[1],
		Err:    core.ClassifiedError{Class: core.ErrorClassData, Err: errors.New("record 2 parse failed")},
	}}
	return []core.Record{recs[0]}, core.NewPartialTransformError("partial transform failed", failures)
}

type stateSnapshotTransform struct {
	node    string
	version string
}

func (t stateSnapshotTransform) Name() string { return "state-snapshot" }
func (t stateSnapshotTransform) Apply(_ context.Context, rec core.Record) (core.Record, error) {
	return rec, nil
}
func (t stateSnapshotTransform) SnapshotState(context.Context) (string, string, bool, error) {
	return t.node, t.version, true, nil
}

type concurrencyProbeTransform struct {
	inFlight    int64
	maxInFlight int64
	delay       time.Duration
}

func (t *concurrencyProbeTransform) Name() string { return "concurrency-probe" }
func (t *concurrencyProbeTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	cur := atomic.AddInt64(&t.inFlight, 1)
	for {
		max := atomic.LoadInt64(&t.maxInFlight)
		if cur <= max || atomic.CompareAndSwapInt64(&t.maxInFlight, max, cur) {
			break
		}
	}
	select {
	case <-time.After(t.delay):
	case <-ctx.Done():
		atomic.AddInt64(&t.inFlight, -1)
		return rec, ctx.Err()
	}
	atomic.AddInt64(&t.inFlight, -1)
	return rec, nil
}

type statefulConcurrencyProbeTransform struct {
	concurrencyProbeTransform
}

func (t *statefulConcurrencyProbeTransform) SnapshotState(context.Context) (string, string, bool, error) {
	return "stateful-probe", "v1", true, nil
}

type failingStateSnapshotTransform struct{}

func (t failingStateSnapshotTransform) Name() string { return "failing-state-snapshot" }
func (t failingStateSnapshotTransform) Apply(_ context.Context, rec core.Record) (core.Record, error) {
	return rec, nil
}
func (t failingStateSnapshotTransform) SnapshotState(context.Context) (string, string, bool, error) {
	return "window-0", "", false, errors.New("state store unavailable")
}

// captureDLQ records every DLQ write.
type captureDLQ struct {
	mu      sync.Mutex
	entries []DLQEntry
}

func (d *captureDLQ) WriteDLQ(_ context.Context, e DLQEntry) error {
	d.mu.Lock()
	d.entries = append(d.entries, e)
	d.mu.Unlock()
	return nil
}
func (d *captureDLQ) Close() error { return nil }

func makeRunnerSpec(t *testing.T, batchSize int) (*Spec, string) {
	t.Helper()
	tmpDir := t.TempDir()
	outPath := filepath.Join(tmpDir, "out.jsonl")
	return &Spec{
		Name:                  "test-runner",
		Source:                SourceSpec{Type: "demo", Config: map[string]any{"interval_ms": 1, "count": 5, "fields": []map[string]any{{"name": "v", "type": "counter"}}}},
		Sink:                  SinkSpec{Type: "file_sink", Config: map[string]any{"path": outPath, "format": "json"}},
		BatchSize:             batchSize,
		CheckpointIntervalSec: 1,
	}, outPath
}

func newCheckpointWriteBatchRunner(t *testing.T, transforms core.TransformChain, store core.CheckpointStore, dlq DLQWriter) *Runner {
	t.Helper()
	am := alert.NewManager()
	t.Cleanup(am.Close)
	return &Runner{
		spec:            &Spec{Name: "checkpoint-zero-survivor"},
		transforms:      transforms,
		sink:            &recordingSink{},
		checkpointStore: store,
		dlqWriter:       dlq,
		alertManager:    am,
		reader:          checkpointTestReader{},
		logBuf:          NewLogBuffer(20),
	}
}

func checkpointSaved(t *testing.T, store *memoryCPStore, jobName string) bool {
	t.Helper()
	cp, err := store.Load(context.Background(), jobName)
	if err != nil {
		t.Fatalf("Load checkpoint: %v", err)
	}
	return cp != nil
}

func checkpointTestBatch() []core.Record {
	return []core.Record{
		{Data: map[string]any{"id": 1}, Metadata: core.Metadata{Offset: 1}},
		{Data: map[string]any{"id": 2}, Metadata: core.Metadata{Offset: 2}},
	}
}

func TestRunnerFailsStartupOnSchemaValidationError(t *testing.T) {
	src := &schemaSource{schema: core.SchemaInfo{Columns: []core.ColumnInfo{{Name: "id", DataType: "bigint"}}}}
	snk := &schemaValidatingSink{validateErr: errors.New("missing target columns [id]")}
	r := &Runner{
		spec:   &Spec{Name: "schema-startup"},
		source: src,
		sink:   snk,
	}

	err := r.Start(context.Background())
	if err == nil {
		t.Fatal("Start() = nil, want schema validation error")
	}
	if !strings.Contains(err.Error(), "schema validation") {
		t.Fatalf("Start() error = %v, want schema validation", err)
	}
	if atomic.LoadInt32(&snk.openCalls) != 1 || atomic.LoadInt32(&snk.closeCalls) != 1 {
		t.Fatalf("sink open/close = %d/%d, want 1/1", atomic.LoadInt32(&snk.openCalls), atomic.LoadInt32(&snk.closeCalls))
	}
	if atomic.LoadInt32(&src.openCalls) != 0 {
		t.Fatalf("source Open calls = %d, want 0 before schema-compatible startup", atomic.LoadInt32(&src.openCalls))
	}
	if r.Status() != StatusFailed {
		t.Fatalf("status = %s, want failed", r.Status())
	}
}

func TestSchemaValidatorForSinkUnwrapsSinkWriteHook(t *testing.T) {
	snk := &schemaValidatingSink{}
	validator, ok := schemaValidatorForSink(&SinkWriteHook{Hooks: &MetricsHooks{}, Sink: snk})
	if !ok {
		t.Fatal("schemaValidatorForSink() ok = false, want true")
	}
	if validator != snk {
		t.Fatalf("schemaValidatorForSink() = %T, want wrapped sink", validator)
	}
}

func TestRunnerCheckpointAdvancesWhenAllRecordsFiltered(t *testing.T) {
	store := newMemoryCPStore()
	r := newCheckpointWriteBatchRunner(t, core.TransformChain{filterAllTransform{}}, store, noopDLQ{})

	r.writeBatch(context.Background(), checkpointTestBatch())

	if !checkpointSaved(t, store, r.spec.Name) {
		t.Fatal("expected checkpoint to advance when every record was intentionally filtered")
	}
}

func TestRunnerDoesNotCheckpointZeroSurvivorTransformFailures(t *testing.T) {
	store := newMemoryCPStore()
	dlq := &captureDLQ{}
	r := newCheckpointWriteBatchRunner(t, core.TransformChain{failAllTransform{}}, store, dlq)

	r.writeBatch(context.Background(), checkpointTestBatch())

	if checkpointSaved(t, store, r.spec.Name) {
		t.Fatal("checkpoint advanced for zero-survivor transform failures")
	}
	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	if len(dlq.entries) != len(checkpointTestBatch()) {
		t.Fatalf("DLQ entries = %d, want %d", len(dlq.entries), len(checkpointTestBatch()))
	}
}

func TestRunnerUsesPartialBatchErrorForDLQAttribution(t *testing.T) {
	dlq := &captureDLQ{}
	sink := &partialBatchSink{}
	r := newCheckpointWriteBatchRunner(t, nil, newMemoryCPStore(), dlq)
	r.sink = sink
	r.retryConfig.MaxAttempts = 1

	r.writeBatch(context.Background(), checkpointTestBatch())

	if atomic.LoadInt32(&sink.batchCalls) != 1 {
		t.Fatalf("batch calls = %d, want 1", atomic.LoadInt32(&sink.batchCalls))
	}
	if atomic.LoadInt32(&sink.singleCalls) != 0 {
		t.Fatalf("single-row isolation calls = %d, want 0 for partial batch error", atomic.LoadInt32(&sink.singleCalls))
	}
	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	if len(dlq.entries) != 1 {
		t.Fatalf("DLQ entries = %d, want 1", len(dlq.entries))
	}
	if got := dlq.entries[0].Record.Data["id"]; got != 2 {
		t.Fatalf("DLQ record id = %v, want 2", got)
	}
	if dlq.entries[0].ErrorClass != string(core.ErrorClassSchema) {
		t.Fatalf("DLQ error class = %q, want %q", dlq.entries[0].ErrorClass, core.ErrorClassSchema)
	}
	stats := r.Stats()
	if stats.RecordsWritten != 1 || stats.RecordsFailed != 1 || stats.RecordsDLQ != 1 {
		t.Fatalf("stats written/failed/dlq = %d/%d/%d, want 1/1/1", stats.RecordsWritten, stats.RecordsFailed, stats.RecordsDLQ)
	}
}

func TestRunnerRetriesTransientSinkFailureWithoutDLQ(t *testing.T) {
	store := newMemoryCPStore()
	dlq := &captureDLQ{}
	sink := &transientThenSuccessSink{failures: 1}
	r := newCheckpointWriteBatchRunner(t, nil, store, dlq)
	r.sink = sink
	r.retryConfig.MaxAttempts = 3
	r.retryConfig.InitialInterval = time.Millisecond
	r.retryConfig.MaxInterval = time.Millisecond
	r.retryConfig.Multiplier = 1

	r.writeBatch(context.Background(), checkpointTestBatch())

	if got := atomic.LoadInt32(&sink.calls); got != 2 {
		t.Fatalf("sink calls = %d, want 2", got)
	}
	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	if len(dlq.entries) != 0 {
		t.Fatalf("DLQ entries = %d, want 0", len(dlq.entries))
	}
	stats := r.Stats()
	if stats.RecordsWritten != int64(len(checkpointTestBatch())) || stats.RecordsFailed != 0 || stats.RecordsDLQ != 0 {
		t.Fatalf("stats written/failed/dlq = %d/%d/%d, want %d/0/0", stats.RecordsWritten, stats.RecordsFailed, stats.RecordsDLQ, len(checkpointTestBatch()))
	}
	if !checkpointSaved(t, store, r.spec.Name) {
		t.Fatal("checkpoint did not advance after transient sink retry succeeded")
	}
}

func TestRunnerUsesPartialTransformErrorForDLQAttribution(t *testing.T) {
	store := newMemoryCPStore()
	dlq := &captureDLQ{}
	r := newCheckpointWriteBatchRunner(t, core.TransformChain{partialFailureTransform{}}, store, dlq)

	r.writeBatch(context.Background(), checkpointTestBatch())

	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	if len(dlq.entries) != 1 {
		t.Fatalf("DLQ entries = %d, want 1", len(dlq.entries))
	}
	if got := dlq.entries[0].Record.Data["id"]; got != 2 {
		t.Fatalf("DLQ record id = %v, want 2", got)
	}
	if dlq.entries[0].ErrorClass != string(core.ErrorClassData) {
		t.Fatalf("DLQ error class = %q, want %q", dlq.entries[0].ErrorClass, core.ErrorClassData)
	}
	stats := r.Stats()
	if stats.RecordsWritten != 1 || stats.RecordsFailed != 1 || stats.RecordsDLQ != 1 {
		t.Fatalf("stats written/failed/dlq = %d/%d/%d, want 1/1/1", stats.RecordsWritten, stats.RecordsFailed, stats.RecordsDLQ)
	}
	if !checkpointSaved(t, store, r.spec.Name) {
		t.Fatal("checkpoint did not advance after survivor write and failed record DLQ")
	}
}

func TestRunnerTransformWorkersParallelizeStatelessRecordsInOrder(t *testing.T) {
	probe := &concurrencyProbeTransform{delay: 30 * time.Millisecond}
	r := &Runner{
		transforms:       core.TransformChain{probe},
		transformWorkers: 4,
	}
	batch := []core.Record{
		{Data: map[string]any{"id": 1}},
		{Data: map[string]any{"id": 2}},
		{Data: map[string]any{"id": 3}},
		{Data: map[string]any{"id": 4}},
	}

	got, filtered, failed := r.applyRecordTransforms(context.Background(), batch)

	if filtered != 0 || failed != 0 {
		t.Fatalf("filtered=%d failed=%d, want 0/0", filtered, failed)
	}
	if max := atomic.LoadInt64(&probe.maxInFlight); max < 2 {
		t.Fatalf("transform_workers did not parallelize stateless transform: max in-flight=%d", max)
	}
	if len(got) != len(batch) {
		t.Fatalf("got %d records, want %d", len(got), len(batch))
	}
	for i := range got {
		if got[i].Data["id"] != batch[i].Data["id"] {
			t.Fatalf("record order changed at %d: got %v want %v", i, got[i].Data["id"], batch[i].Data["id"])
		}
	}
}

func TestRunnerTransformWorkersKeepStatefulTransformsSerial(t *testing.T) {
	probe := &statefulConcurrencyProbeTransform{
		concurrencyProbeTransform: concurrencyProbeTransform{delay: 10 * time.Millisecond},
	}
	r := &Runner{
		transforms:       core.TransformChain{probe},
		transformWorkers: 4,
	}
	batch := []core.Record{
		{Data: map[string]any{"id": 1}},
		{Data: map[string]any{"id": 2}},
		{Data: map[string]any{"id": 3}},
	}

	got, filtered, failed := r.applyRecordTransforms(context.Background(), batch)

	if filtered != 0 || failed != 0 || len(got) != len(batch) {
		t.Fatalf("got len=%d filtered=%d failed=%d, want len=%d filtered=0 failed=0", len(got), filtered, failed, len(batch))
	}
	if max := atomic.LoadInt64(&probe.maxInFlight); max != 1 {
		t.Fatalf("stateful transform ran concurrently: max in-flight=%d, want 1", max)
	}
}

func TestRunnerDoesNotCheckpointZeroSurvivorBatchTransform(t *testing.T) {
	store := newMemoryCPStore()
	r := newCheckpointWriteBatchRunner(t, core.TransformChain{zeroBatchTransform{}}, store, noopDLQ{})

	r.writeBatch(context.Background(), checkpointTestBatch())

	if checkpointSaved(t, store, r.spec.Name) {
		t.Fatal("checkpoint advanced after batch transform produced no sink records")
	}
}

// TestRunnerCheckpointAfterCommit verifies saveCommittedCheckpoint is invoked
// after the sink Write returns, persisting a checkpoint with the right job name.
func TestRunnerCheckpointAfterCommit(t *testing.T) {
	spec, _ := makeRunnerSpec(t, 5)
	store := newMemoryCPStore()
	r, err := NewRunner(spec, store, noopDLQ{}, alert.NewManager())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	cp, err := store.Load(context.Background(), spec.Name)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cp == nil {
		t.Fatal("expected checkpoint after pipeline completion, got nil")
	}
	if cp.JobName != spec.Name {
		t.Errorf("checkpoint JobName = %q, want %q", cp.JobName, spec.Name)
	}
}

func TestRunnerCanRestartAfterCompletedCheckpointReset(t *testing.T) {
	spec, _ := makeRunnerSpec(t, 5)
	spec.CheckpointIntervalSec = 3600
	store := newMemoryCPStore()
	am := alert.NewManager()
	t.Cleanup(am.Close)
	r, err := NewRunner(spec, store, noopDLQ{}, am)
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("first Start: %v", err)
	}
	r.Wait()
	if r.Status() != StatusCompleted {
		t.Fatalf("first status = %s, want completed (stats=%#v)", r.Status(), r.Stats())
	}
	if got := r.Stats().RecordsWritten; got != 5 {
		t.Fatalf("first RecordsWritten = %d, want 5", got)
	}

	if err := store.Delete(context.Background(), spec.Name); err != nil {
		t.Fatalf("Delete checkpoint: %v", err)
	}
	if err := r.Start(ctx); err != nil {
		t.Fatalf("second Start: %v", err)
	}
	r.Wait()
	if r.Status() != StatusCompleted {
		t.Fatalf("second status = %s, want completed (stats=%#v)", r.Status(), r.Stats())
	}
	stats := r.Stats()
	if stats.RecordsWritten != 5 {
		t.Fatalf("second RecordsWritten = %d, want 5", stats.RecordsWritten)
	}
	if stats.LastCheckpoint.IsZero() {
		t.Fatal("second LastCheckpoint is zero, want checkpoint after replay")
	}
	cp, err := store.Load(context.Background(), spec.Name)
	if err != nil {
		t.Fatalf("Load second checkpoint: %v", err)
	}
	if cp == nil {
		t.Fatal("second checkpoint was not persisted after replay")
	}
	if strings.Contains(stats.LastError, "close of closed channel") {
		t.Fatalf("second LastError = %q", stats.LastError)
	}
}

func TestRunnerCheckpointIncludesStateSnapshotVersions(t *testing.T) {
	store := newMemoryCPStore()
	r := newCheckpointWriteBatchRunner(t, core.TransformChain{
		stateSnapshotTransform{node: "window-0", version: "state-v1"},
	}, store, noopDLQ{})

	r.writeBatch(context.Background(), checkpointTestBatch())

	cp, err := store.Load(context.Background(), r.spec.Name)
	if err != nil {
		t.Fatalf("Load checkpoint: %v", err)
	}
	if cp == nil {
		t.Fatal("checkpoint not saved")
	}
	env, ok, err := checkpoint.ParseEnvelope(cp.Position)
	if err != nil {
		t.Fatalf("ParseEnvelope: %v", err)
	}
	if !ok {
		t.Fatalf("checkpoint position not wrapped in envelope: %s", cp.Position)
	}
	if env.State["window-0"] != "state-v1" {
		t.Fatalf("state versions = %#v", env.State)
	}
	if string(env.Source) != `{"offset":2}` {
		t.Fatalf("source position = %s, want offset 2", env.Source)
	}
}

func TestRunnerDoesNotCheckpointWhenStateSnapshotFails(t *testing.T) {
	store := newMemoryCPStore()
	r := newCheckpointWriteBatchRunner(t, core.TransformChain{
		failingStateSnapshotTransform{},
	}, store, noopDLQ{})

	r.writeBatch(context.Background(), checkpointTestBatch())

	if checkpointSaved(t, store, r.spec.Name) {
		t.Fatal("checkpoint advanced after state snapshot failed")
	}
}

func TestUnwrapCheckpointForSourceExtractsEnvelopeSource(t *testing.T) {
	raw, err := checkpoint.BuildEnvelope(json.RawMessage(`{"offset":42}`), map[string]string{"window-0": "v1"}, nil)
	if err != nil {
		t.Fatalf("BuildEnvelope: %v", err)
	}
	cp := &core.Checkpoint{JobName: "p", Position: raw}

	got := unwrapCheckpointForSource(cp)

	if string(got.Position) != `{"offset":42}` {
		t.Fatalf("unwrapped position = %s", got.Position)
	}
	if string(cp.Position) != string(raw) {
		t.Fatalf("unwrap mutated original checkpoint: %s", cp.Position)
	}
}

// TestRunnerDLQOnSinkFailure verifies that when the sink fails, the records
// are routed to DLQ.
func TestRunnerDLQOnSinkFailure(t *testing.T) {
	tmpDir := t.TempDir()
	spec := &Spec{
		Name:                  "dlq-test",
		Source:                SourceSpec{Type: "demo", Config: map[string]any{"interval_ms": 1, "count": 3, "fields": []map[string]any{{"name": "v", "type": "counter"}}}},
		Sink:                  SinkSpec{Type: "file_sink", Config: map[string]any{"path": filepath.Join(tmpDir, "out.jsonl"), "format": "json"}},
		BatchSize:             2,
		CheckpointIntervalSec: 1,
		Retry:                 &RetrySpec{MaxAttempts: 1, InitialIntervalMs: 1, MaxIntervalMs: 1},
	}
	dlq := &captureDLQ{}
	store := newMemoryCPStore()

	r, err := NewRunner(spec, store, dlq, alert.NewManager())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	// Replace sink with an always-failing one.
	failingSink := &recordingSink{alwaysFail: true}
	r.sink = failingSink

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	dlq.mu.Lock()
	defer dlq.mu.Unlock()
	if len(dlq.entries) == 0 {
		t.Errorf("expected DLQ entries on sink failure, got 0 (sink calls=%d)", atomic.LoadInt32(&failingSink.calls))
	}
}

// TestRunnerPanicRecovery verifies a panic in sink Write is recovered
// and recorded as LastError.
func TestRunnerPanicRecovery(t *testing.T) {
	tmpDir := t.TempDir()
	spec := &Spec{
		Name:                  "panic-test",
		Source:                SourceSpec{Type: "demo", Config: map[string]any{"interval_ms": 1, "count": 5, "fields": []map[string]any{{"name": "v", "type": "counter"}}}},
		Sink:                  SinkSpec{Type: "file_sink", Config: map[string]any{"path": filepath.Join(tmpDir, "out.jsonl"), "format": "json"}},
		BatchSize:             2,
		CheckpointIntervalSec: 1,
	}
	r, err := NewRunner(spec, newMemoryCPStore(), noopDLQ{}, alert.NewManager())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	r.sink = &panicSink{}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	stats := r.Stats()
	if stats.LastError == "" {
		t.Errorf("expected LastError to be set after panic, got empty")
	}
	// Status should be a terminal state (completed/stopped/failed), not running.
	if r.Status() == StatusRunning {
		t.Errorf("Status = running, want terminal after panic")
	}
}

type panicSink struct{}

func (s *panicSink) Name() string                                   { return "panic" }
func (s *panicSink) Open(_ context.Context) error                   { return nil }
func (s *panicSink) Write(_ context.Context, _ []core.Record) error { panic("injected panic") }
func (s *panicSink) Close() error                                   { return nil }

// TestRunnerStatsAreConsistent verifies Stats() returns a consistent snapshot
// (no race between Stats() and concurrent writes).
func TestRunnerStatsAreConsistent(t *testing.T) {
	spec, _ := makeRunnerSpec(t, 5)
	store := newMemoryCPStore()
	r, err := NewRunner(spec, store, noopDLQ{}, alert.NewManager())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	_ = r.Start(ctx)

	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
				_ = r.Stats()
			}
		}
	}()
	time.Sleep(200 * time.Millisecond)
	close(done)
	r.Stop()
}

// TestRunnerStatsCounters verifies that after completion, the stats reflect
// the records processed.
func TestRunnerStatsCounters(t *testing.T) {
	spec, _ := makeRunnerSpec(t, 3)
	store := newMemoryCPStore()
	r, err := NewRunner(spec, store, noopDLQ{}, alert.NewManager())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	r.Wait()

	stats := r.Stats()
	if stats.RecordsRead == 0 {
		t.Error("expected RecordsRead > 0, got 0")
	}
	if stats.RecordsWritten == 0 {
		t.Error("expected RecordsWritten > 0, got 0")
	}
	if stats.RecordsRead != stats.RecordsWritten {
		t.Errorf("read/written mismatch: read=%d written=%d", stats.RecordsRead, stats.RecordsWritten)
	}
}

// TestRunnerStopIsIdempotent verifies Stop can be called multiple times safely.
func TestRunnerStopIsIdempotent(t *testing.T) {
	spec, _ := makeRunnerSpec(t, 100)
	r, _ := NewRunner(spec, newMemoryCPStore(), noopDLQ{}, alert.NewManager())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = r.Start(ctx)
	_ = r.Stop()
	_ = r.Stop()
	_ = r.Stop()
}

func TestRunnerUsesSourceReadBatchForMySQLBatch(t *testing.T) {
	reader := &batchCountingReader{batches: [][]core.Record{{
		{Data: map[string]any{"id": 1}, Metadata: core.Metadata{Offset: 1}},
		{Data: map[string]any{"id": 2}, Metadata: core.Metadata{Offset: 2}},
		{Data: map[string]any{"id": 3}, Metadata: core.Metadata{Offset: 3}},
		{Data: map[string]any{"id": 4}, Metadata: core.Metadata{Offset: 4}},
		{Data: map[string]any{"id": 5}, Metadata: core.Metadata{Offset: 5}},
	}}}
	sink := &recordingSink{}
	am := alert.NewManager()
	t.Cleanup(am.Close)
	done := make(chan struct{})
	r := &Runner{
		spec:               &Spec{Name: "batch-read", Source: SourceSpec{Type: "mysql_batch"}},
		reader:             reader,
		sink:               sink,
		transforms:         core.TransformChain{},
		batchSize:          3,
		flushInterval:      time.Hour,
		backpressureBuffer: 10,
		alertManager:       am,
		logBuf:             NewLogBuffer(20),
		done:               done,
		inflightBatch:      newInflightBatchTracker(),
	}
	r.retryConfig.MaxAttempts = 1

	r.runLoop(context.Background(), done)

	if got := atomic.LoadInt32(&reader.readCalls); got != 0 {
		t.Fatalf("Read() calls = %d, want 0", got)
	}
	if got := atomic.LoadInt32(&reader.batchCalls); got == 0 {
		t.Fatal("ReadBatch() was not called")
	}
	if got := atomic.LoadInt32(&sink.calls); got != 2 {
		t.Fatalf("sink Write calls = %d, want 2 batches (3 + final 2)", got)
	}
}

// slowSink blocks each Write call until releaseCh is closed or receives a
// value, then records the observed ctx error (if any). It is used to verify
// that Stop() during an in-flight writeBatch does not cancel the commit ctx
// passed to sink.Write / dlqWriter.WriteDLQ / checkpointStore.Save.
type slowSink struct {
	entered  chan struct{}
	release  chan struct{}
	writeErr atomic.Value // string — observed ctx.Err() string ("nil" if nil)
}

func newSlowSink() *slowSink {
	return &slowSink{
		entered: make(chan struct{}, 16),
		release: make(chan struct{}),
	}
}
func (s *slowSink) Name() string               { return "slow" }
func (s *slowSink) Open(context.Context) error { return nil }
func (s *slowSink) Close() error               { return nil }
func (s *slowSink) Write(ctx context.Context, _ []core.Record) error {
	s.entered <- struct{}{}
	<-s.release
	err := ctx.Err()
	if err == nil {
		s.writeErr.Store("nil")
	} else {
		s.writeErr.Store(err.Error())
	}
	return nil
}

// TestRunnerStopDuringWriteBatchDoesNotCancelCommit verifies that calling
// Stop() while a writeBatch is in progress does not cancel the context
// observed by the in-flight sink write. This is the regression test for the
// shutdown race that surfaced as spurious "context canceled" errors from
// sink.Write / dlqWriter.WriteDLQ / checkpointStore.Save during pipeline stop.
func TestRunnerStopDuringWriteBatchDoesNotCancelCommit(t *testing.T) {
	spec, _ := makeRunnerSpec(t, 2)
	store := newMemoryCPStore()
	r, err := NewRunner(spec, store, noopDLQ{}, alert.NewManager())
	if err != nil {
		t.Fatalf("NewRunner: %v", err)
	}
	slow := newSlowSink()
	r.sink = slow

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := r.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Wait until the sink is mid-Write, then Stop() while it is still
	// blocked. The commit ctx passed to Write must NOT be cancelled.
	select {
	case <-slow.entered:
	case <-time.After(3 * time.Second):
		_ = r.Stop()
		t.Fatal("sink.Write was never called within timeout")
	}

	stopDone := make(chan struct{})
	go func() {
		_ = r.Stop()
		close(stopDone)
	}()

	// Give Stop() a moment to observe the inflight batch, then release the
	// sink and confirm the write observed a non-cancelled ctx.
	time.Sleep(100 * time.Millisecond)
	close(slow.release)

	select {
	case <-stopDone:
	case <-time.After(40 * time.Second):
		t.Fatal("Stop() did not return within 40s")
	}

	got, _ := slow.writeErr.Load().(string)
	if got != "nil" {
		t.Fatalf("sink.Write observed ctx.Err() = %q, want nil — Stop() must not cancel the in-flight commit ctx", got)
	}
}

// TestRunnerStopDuringDLQWriteDoesNotCancelCommit verifies the same guarantee
// for the DLQ writer path: an in-flight DLQ write during Stop() must observe
// an uncancelled commit ctx, so the at-least-once DLQ delivery is preserved.
func TestRunnerStopDuringDLQWriteDoesNotCancelCommit(t *testing.T) {
	store := newMemoryCPStore()
	// Construct a runner that routes every record through a failing sink
	// straight into the DLQ. We avoid Start() and instead invoke writeBatch
	// directly to deterministically exercise the DLQ path while "stopping".
	r := newCheckpointWriteBatchRunner(t, nil, store, nil)
	// Replace the DLQ writer with a capture+slow variant.
	slow := &slowCaptureDLQ{entered: make(chan struct{}, 8), release: make(chan struct{})}
	r.dlqWriter = slow
	// Make the sink always fail so records go to DLQ.
	r.sink = &recordingSink{alwaysFail: true}
	// retry.Config zero-value would make retry.Do attempt 0 times and return
	// nil; configure a single attempt so the sink error surfaces to the DLQ
	// routing path.
	r.retryConfig.MaxAttempts = 1

	ctx, cancel := context.WithCancel(context.Background())
	stopDone := make(chan error, 1)
	go func() {
		r.writeBatch(ctx, checkpointTestBatch())
		stopDone <- nil
	}()

	select {
	case <-slow.entered:
	case <-time.After(3 * time.Second):
		t.Fatal("DLQ write was never entered")
	}
	// Simulate Stop()'s cancellation of the loop ctx while the DLQ write
	// is blocked. writeBatch's commit ctx is derived from Background() so
	// the DLQ writer must not observe a cancellation.
	cancel()
	close(slow.release)
	<-stopDone

	if got := slow.observedErr.Load(); got != nil && got != "nil" {
		t.Fatalf("DLQ WriteDLQ observed ctx.Err() = %v, want nil", got)
	}
}

type slowCaptureDLQ struct {
	entered     chan struct{}
	release     chan struct{}
	observedErr atomic.Value // string
}

func (d *slowCaptureDLQ) WriteDLQ(ctx context.Context, _ DLQEntry) error {
	d.entered <- struct{}{}
	<-d.release
	err := ctx.Err()
	if err == nil {
		d.observedErr.Store("nil")
	} else {
		d.observedErr.Store(err.Error())
	}
	return nil
}
func (d *slowCaptureDLQ) Close() error { return nil }
