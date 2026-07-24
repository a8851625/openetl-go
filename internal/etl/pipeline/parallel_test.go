package pipeline

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"

	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	_ "github.com/a8851625/openetl-go/internal/etl/source"
)

// noopDLQ satisfies DLQWriter.
type noopDLQ struct{}

func (noopDLQ) WriteDLQ(_ context.Context, _ DLQEntry) error { return nil }
func (noopDLQ) Close() error                                 { return nil }

// TestParallelRunnerSpawnsNShards verifies that NewParallelRunner creates N
// shards with the expected InstanceCount and Shards() length.
func TestParallelRunnerSpawnsNShards(t *testing.T) {
	tmpDir := t.TempDir()
	spec := &Spec{
		Name:                  "parallel-test",
		Source:                SourceSpec{Type: "demo", Config: map[string]any{"interval_ms": 5, "fields": []map[string]any{{"name": "v", "type": "counter"}}}},
		Sink:                  SinkSpec{Type: "file_sink", Config: map[string]any{"path": tmpDir + "/out.jsonl", "format": "json"}},
		BatchSize:             5,
		CheckpointIntervalSec: 1,
		Parallelism:           &ParallelismConfig{Count: 3, ShardStrategy: "pk_mod"},
	}

	pr, err := NewParallelRunner(spec, newMemoryCPStore(), noopDLQ{}, alert.NewManager())
	if err != nil {
		t.Fatalf("NewParallelRunner: %v", err)
	}
	if got := pr.InstanceCount(); got != 3 {
		t.Errorf("InstanceCount = %d, want 3", got)
	}
	shards := pr.Shards()
	if got := len(shards); got != 3 {
		t.Errorf("Shards() len = %d, want 3", got)
	}
	for i, sh := range shards {
		if sh.Index != i {
			t.Errorf("shard[%d].Index = %d, want %d", i, sh.Index, i)
		}
	}
}

// TestParallelRunnerShardConfigInjection verifies each shard sees its own
// shard_index/shard_total in source config.
func TestParallelRunnerShardConfigInjection(t *testing.T) {
	tmpDir := t.TempDir()
	spec := &Spec{
		Name:        "cfg-test",
		Source:      SourceSpec{Type: "demo", Config: map[string]any{"base": "x"}},
		Sink:        SinkSpec{Type: "file_sink", Config: map[string]any{"path": tmpDir + "/out.jsonl", "format": "json"}},
		Parallelism: &ParallelismConfig{Count: 4},
	}
	pr, err := NewParallelRunner(spec, newMemoryCPStore(), noopDLQ{}, alert.NewManager())
	if err != nil {
		t.Fatalf("NewParallelRunner: %v", err)
	}
	for i := 0; i < 4; i++ {
		inst := pr.Instance(i)
		if inst == nil {
			t.Fatalf("Instance(%d) = nil", i)
		}
		gotIdx := inst.spec.Source.Config["shard_index"]
		gotTotal := inst.spec.Source.Config["shard_total"]
		if gotIdx != i {
			t.Errorf("shard %d: shard_index = %v, want %d", i, gotIdx, i)
		}
		if gotTotal != 4 {
			t.Errorf("shard %d: shard_total = %v, want 4", i, gotTotal)
		}
		if base := inst.spec.Source.Config["base"]; base != "x" {
			t.Errorf("shard %d: base config lost: %v", i, base)
		}
	}
}

func TestParallelRunnerStructuredParallelismUsesLogicalShards(t *testing.T) {
	tmpDir := t.TempDir()
	spec := &Spec{
		Name:   "structured-cfg-test",
		Source: SourceSpec{Type: "demo", Config: map[string]any{"base": "x"}},
		Sink:   SinkSpec{Type: "file_sink", Config: map[string]any{"path": tmpDir + "/out.jsonl", "format": "json"}},
		Parallelism: &ParallelismConfig{
			Sharding: &ShardingConfig{
				Strategy:      "hash_modulo",
				Key:           "id",
				LogicalShards: 8,
			},
			Execution: &ParallelExecutionConfig{MaxActiveShards: 3},
		},
	}
	pr, err := NewParallelRunner(spec, newMemoryCPStore(), noopDLQ{}, alert.NewManager())
	if err != nil {
		t.Fatalf("NewParallelRunner: %v", err)
	}
	if got := pr.InstanceCount(); got != 8 {
		t.Fatalf("InstanceCount = %d, want logical shards 8", got)
	}
	if got := pr.MaxActiveShardCount(); got != 3 {
		t.Fatalf("MaxActiveShardCount = %d, want 3", got)
	}
	inst := pr.Instance(7)
	if inst == nil {
		t.Fatal("Instance(7) = nil")
	}
	if got := inst.spec.Source.Config["shard_total"]; got != 8 {
		t.Fatalf("shard_total = %v, want 8", got)
	}
	if got := inst.spec.Source.Config["shard_key"]; got != "id" {
		t.Fatalf("shard_key = %v, want id", got)
	}
}

func TestParallelRunnerSinkConcurrencyLimitsShardWrites(t *testing.T) {
	const (
		sourceName = "parallel-sink-concurrency-source"
		sinkName   = "parallel-sink-concurrency-sink"
	)
	probe := &sinkConcurrencyProbe{}
	registry.RegisterSource(sourceName, func(config map[string]any) (core.Source, error) {
		return finiteOneRecordSource{}, nil
	})
	registry.RegisterSink(sinkName, func(config map[string]any) (core.Sink, error) {
		return probe, nil
	})

	spec := &Spec{
		Name:                  "sink-concurrency",
		Source:                SourceSpec{Type: sourceName, Config: map[string]any{}},
		Sink:                  SinkSpec{Type: sinkName, Config: map[string]any{}},
		BatchSize:             1,
		CheckpointIntervalSec: 1,
		Parallelism: &ParallelismConfig{
			Sharding: &ShardingConfig{
				Strategy:      "id_range",
				LogicalShards: 4,
			},
			Execution: &ParallelExecutionConfig{
				MaxActiveShards: 4,
				SinkConcurrency: 1,
			},
		},
	}
	am := alert.NewManager()
	t.Cleanup(am.Close)
	pr, err := NewParallelRunner(spec, newMemoryCPStore(), noopDLQ{}, am)
	if err != nil {
		t.Fatalf("NewParallelRunner: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := pr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	pr.Wait()

	if got := atomic.LoadInt64(&probe.maxInFlight); got != 1 {
		t.Fatalf("max concurrent sink writes = %d, want 1", got)
	}
	if got := atomic.LoadInt64(&probe.calls); got != 4 {
		t.Fatalf("sink writes = %d, want one write per logical shard", got)
	}
}

// TestParallelRunnerLifecycle verifies Start/Stop work cleanly.
func TestParallelRunnerLifecycle(t *testing.T) {
	tmpDir := t.TempDir()
	spec := &Spec{
		Name:                  "lifecycle",
		Source:                SourceSpec{Type: "demo", Config: map[string]any{"interval_ms": 10, "fields": []map[string]any{{"name": "v", "type": "counter"}}}},
		Sink:                  SinkSpec{Type: "file_sink", Config: map[string]any{"path": tmpDir + "/out.jsonl", "format": "json"}},
		BatchSize:             2,
		CheckpointIntervalSec: 1,
		Parallelism:           &ParallelismConfig{Count: 2},
	}
	pr, err := NewParallelRunner(spec, newMemoryCPStore(), noopDLQ{}, alert.NewManager())
	if err != nil {
		t.Fatalf("NewParallelRunner: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := pr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if got := pr.Status(); got != StatusRunning {
		t.Errorf("Status = %v, want running", got)
	}
	if err := pr.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	select {
	case <-pr.Done():
	case <-time.After(time.Second):
		t.Fatal("ParallelRunner did not finish after Stop")
	}
}

type finiteOneRecordSource struct{}

func (finiteOneRecordSource) Name() string { return "finite-one-record-source" }
func (finiteOneRecordSource) Open(context.Context, *core.Checkpoint) (core.RecordReader, error) {
	return &finiteOneRecordReader{}, nil
}

type finiteOneRecordReader struct {
	read bool
}

func (r *finiteOneRecordReader) Read(context.Context) (core.Record, error) {
	if r.read {
		return core.Record{}, io.EOF
	}
	r.read = true
	return core.Record{Data: map[string]any{"id": 1}, Metadata: core.Metadata{Offset: 1}}, nil
}

func (r *finiteOneRecordReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
	rec, err := r.Read(ctx)
	if err != nil {
		return nil, err
	}
	return []core.Record{rec}, nil
}

func (r *finiteOneRecordReader) Snapshot(context.Context) (core.Checkpoint, error) {
	return core.Checkpoint{}, nil
}

func (r *finiteOneRecordReader) Close() error { return nil }

type sinkConcurrencyProbe struct {
	calls       int64
	inFlight    int64
	maxInFlight int64
}

func (s *sinkConcurrencyProbe) Name() string               { return "sink-concurrency-probe" }
func (s *sinkConcurrencyProbe) Open(context.Context) error { return nil }
func (s *sinkConcurrencyProbe) Write(ctx context.Context, records []core.Record) error {
	atomic.AddInt64(&s.calls, 1)
	cur := atomic.AddInt64(&s.inFlight, 1)
	defer atomic.AddInt64(&s.inFlight, -1)
	for {
		max := atomic.LoadInt64(&s.maxInFlight)
		if cur <= max || atomic.CompareAndSwapInt64(&s.maxInFlight, max, cur) {
			break
		}
	}
	select {
	case <-time.After(50 * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (s *sinkConcurrencyProbe) Close() error { return nil }

// TestNewDistributedPipelineDispatchesSingleShardPlacement proves master-role
// pipeline-level placement: unsharded (logical_shards=1) streaming specs still
// become a distributed ParallelRunner with one continuous shard task, instead
// of falling back to an inline Runner on the master process.
func TestNewDistributedPipelineDispatchesSingleShardPlacement(t *testing.T) {
	tmpDir := t.TempDir()
	spec := &Spec{
		Name:   "single-shard-place",
		Source: SourceSpec{Type: "demo", Config: map[string]any{"interval_ms": 50}},
		Sink:   SinkSpec{Type: "file_sink", Config: map[string]any{"path": tmpDir + "/out.jsonl", "format": "json"}},
		// No Parallelism — defaults to logical_shards=1.
	}
	disp := &recordingDispatcher{}
	runner, err := NewDistributedPipeline(spec, newMemoryCPStore(), noopDLQ{}, alert.NewManager(), disp)
	if err != nil {
		t.Fatalf("NewDistributedPipeline: %v", err)
	}
	pr, ok := runner.(*ParallelRunner)
	if !ok {
		t.Fatalf("runner type = %T, want *ParallelRunner for single-shard placement", runner)
	}
	if !pr.distributed || pr.dispatcher == nil {
		t.Fatalf("expected distributed ParallelRunner with dispatcher, distributed=%v dispatcher=%v", pr.distributed, pr.dispatcher != nil)
	}
	if got := pr.InstanceCount(); got != 1 {
		t.Fatalf("InstanceCount = %d, want 1", got)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := pr.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Give startDistributed a moment to call DispatchShards.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && disp.count() == 0 {
		time.Sleep(10 * time.Millisecond)
	}
	if got := disp.count(); got != 1 {
		t.Fatalf("DispatchShards count arg = %d, want 1 continuous shard task", got)
	}
	_ = pr.Stop()
}

// TestBuildShardRunnerSingleShardKeepsPlainCheckpointKey ensures promoting an
// unsharded pipeline to master-worker does not rename the checkpoint namespace.
func TestBuildShardRunnerSingleShardKeepsPlainCheckpointKey(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMemoryCPStore()
	// Seed a standalone-style checkpoint under the plain pipeline name.
	marker := []byte(`{"v":42}`)
	if err := store.Save(context.Background(), core.Checkpoint{JobName: "cdc-job", Position: marker}); err != nil {
		t.Fatal(err)
	}
	spec := &Spec{
		Name:   "cdc-job",
		Source: SourceSpec{Type: "demo", Config: map[string]any{}},
		Sink:   SinkSpec{Type: "file_sink", Config: map[string]any{"path": tmpDir + "/out.jsonl", "format": "json"}},
	}
	runner, err := BuildShardRunner(spec, store, noopDLQ{}, alert.NewManager(), 0, 1)
	if err != nil {
		t.Fatalf("BuildShardRunner: %v", err)
	}
	// The runner's store should load the plain-name checkpoint, not cdc-job.shard-0.
	cp, err := runner.checkpointStore.Load(context.Background(), "cdc-job")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cp == nil || string(cp.Position) != string(marker) {
		t.Fatalf("checkpoint = %#v, want plain-name Position=%s", cp, marker)
	}
}

// TestBuildShardRunnerMultiShardUsesScopedCheckpointKey keeps multi-shard
// isolation for ParallelRunner / worker reassignment.
func TestBuildShardRunnerMultiShardUsesScopedCheckpointKey(t *testing.T) {
	tmpDir := t.TempDir()
	store := newMemoryCPStore()
	marker := []byte(`{"v":7}`)
	if err := store.Save(context.Background(), core.Checkpoint{JobName: "batch-job.shard-1", Position: marker}); err != nil {
		t.Fatal(err)
	}
	spec := &Spec{
		Name:        "batch-job",
		Source:      SourceSpec{Type: "demo", Config: map[string]any{}},
		Sink:        SinkSpec{Type: "file_sink", Config: map[string]any{"path": tmpDir + "/out.jsonl", "format": "json"}},
		Parallelism: &ParallelismConfig{Count: 3},
	}
	runner, err := BuildShardRunner(spec, store, noopDLQ{}, alert.NewManager(), 1, 3)
	if err != nil {
		t.Fatalf("BuildShardRunner: %v", err)
	}
	cp, err := runner.checkpointStore.Load(context.Background(), "ignored")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cp == nil || string(cp.Position) != string(marker) {
		t.Fatalf("checkpoint = %#v, want shard-1 Position=%s", cp, marker)
	}
}

type recordingDispatcher struct {
	n int32
}

func (d *recordingDispatcher) DispatchShards(_ context.Context, _ string, count int, _ map[string]string) error {
	atomic.StoreInt32(&d.n, int32(count))
	return nil
}

func (d *recordingDispatcher) WaitShard(ctx context.Context, _ string, _ int) (Status, error) {
	<-ctx.Done()
	return StatusStopped, ctx.Err()
}

func (d *recordingDispatcher) count() int {
	return int(atomic.LoadInt32(&d.n))
}
