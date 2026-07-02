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
