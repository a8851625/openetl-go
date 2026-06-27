package pipeline

import (
	"context"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/alert"

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
