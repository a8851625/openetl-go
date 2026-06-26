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
