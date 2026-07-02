package server

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

type testScheduledRunner struct {
	starts atomic.Int64
	mu     sync.RWMutex
	status pipeline.Status
	done   chan struct{}
}

func newTestScheduledRunner() *testScheduledRunner {
	return &testScheduledRunner{done: make(chan struct{})}
}

func (r *testScheduledRunner) Start(ctx context.Context) error {
	r.starts.Add(1)
	r.mu.Lock()
	r.status = pipeline.StatusRunning
	r.done = make(chan struct{})
	done := r.done
	r.mu.Unlock()
	go func() {
		select {
		case <-ctx.Done():
		case <-time.After(10 * time.Millisecond):
		}
		r.mu.Lock()
		if ctx.Err() != nil {
			r.status = pipeline.StatusStopped
		} else {
			r.status = pipeline.StatusCompleted
		}
		r.mu.Unlock()
		close(done)
	}()
	return nil
}

func (r *testScheduledRunner) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status == pipeline.StatusRunning {
		r.status = pipeline.StatusStopped
	}
	return nil
}

func (r *testScheduledRunner) Pause() error                     { return nil }
func (r *testScheduledRunner) Resume(ctx context.Context) error { return r.Start(ctx) }
func (r *testScheduledRunner) Wait()                            { <-r.Done() }
func (r *testScheduledRunner) Done() <-chan struct{} {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.done
}
func (r *testScheduledRunner) Status() pipeline.Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status
}
func (r *testScheduledRunner) Stats() pipeline.Stats   { return pipeline.Stats{} }
func (r *testScheduledRunner) Duration() time.Duration { return 0 }
func (r *testScheduledRunner) MetricsSnapshot() pipeline.MetricsSnapshot {
	return pipeline.MetricsSnapshot{}
}
func (r *testScheduledRunner) LogBuffer() *pipeline.LogBuffer            { return pipeline.NewLogBuffer(1) }
func (r *testScheduledRunner) Shards() []pipeline.ShardInfo              { return nil }
func (r *testScheduledRunner) IncrementDLQReplay(n int64)                {}
func (r *testScheduledRunner) IncrementDLQDelete(n int64)                {}
func (r *testScheduledRunner) CircuitBreakerState() int                  { return 0 }
func (r *testScheduledRunner) SinkMetrics() []core.SinkMetrics           { return nil }
func (r *testScheduledRunner) StateMetrics() []core.StateMetrics         { return nil }
func (r *testScheduledRunner) TransformMetrics() []core.TransformMetrics { return nil }

func newSchedulerTestServer(t *testing.T) *Server {
	t.Helper()
	store, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	s, err := NewServer(store, t.TempDir())
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	return s
}

func TestStartAllRegistersCronScheduleWithoutImmediateStart(t *testing.T) {
	s := newSchedulerTestServer(t)
	runner := newTestScheduledRunner()
	spec := &pipeline.Spec{
		Name: "cron-batch",
		Schedule: &pipeline.ScheduleConfig{
			Type: "cron",
			Cron: "0 0 0 1 1 *",
		},
	}
	s.mu.Lock()
	s.registerPipelineLocked("pipe-cron", spec.Name, runner, spec, nil)
	s.mu.Unlock()
	if err := s.store.SavePipeline(context.Background(), &storage.PipelineRow{ID: "pipe-cron", Name: spec.Name, SpecYAML: "name: cron-batch", Status: "created"}); err != nil {
		t.Fatalf("SavePipeline: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.StartAll(ctx); err != nil {
		t.Fatalf("StartAll: %v", err)
	}
	t.Cleanup(func() { s.StopAll() })

	time.Sleep(50 * time.Millisecond)
	if got := runner.starts.Load(); got != 0 {
		t.Fatalf("cron scheduled runner started immediately; starts=%d", got)
	}
	row, err := s.store.GetPipeline(context.Background(), "pipe-cron")
	if err != nil {
		t.Fatalf("GetPipeline: %v", err)
	}
	if row != nil && row.Status != "scheduled" {
		t.Fatalf("status = %q, want scheduled", row.Status)
	}
}

func TestStartAllPeriodicScheduleTriggersRunner(t *testing.T) {
	s := newSchedulerTestServer(t)
	runner := newTestScheduledRunner()
	spec := &pipeline.Spec{
		Name: "periodic-batch",
		Schedule: &pipeline.ScheduleConfig{
			Type:        "periodic",
			IntervalSec: 1,
		},
	}
	s.mu.Lock()
	s.registerPipelineLocked("pipe-periodic", spec.Name, runner, spec, nil)
	s.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.StartAll(ctx); err != nil {
		t.Fatalf("StartAll: %v", err)
	}
	t.Cleanup(func() { s.StopAll() })

	deadline := time.After(2 * time.Second)
	for runner.starts.Load() == 0 {
		select {
		case <-deadline:
			t.Fatal("periodic schedule did not trigger runner within 2s")
		case <-time.After(20 * time.Millisecond):
		}
	}
}

func TestStartAllDependencyTriggerFiresDownstream(t *testing.T) {
	s := newSchedulerTestServer(t)

	upstream := newTestScheduledRunner()
	upstreamSpec := &pipeline.Spec{
		Name: "dep-upstream",
		Source: pipeline.SourceSpec{Type: "file", Config: map[string]any{"path": "u.jsonl", "format": "json"}},
		Sink:   pipeline.SinkSpec{Type: "file_sink", Config: map[string]any{"output_dir": "./out", "format": "jsonl"}},
	}
	s.mu.Lock()
	s.registerPipelineLocked("pipe-upstream", upstreamSpec.Name, upstream, upstreamSpec, nil)
	s.mu.Unlock()
	if err := s.store.SavePipeline(context.Background(), &storage.PipelineRow{ID: "pipe-upstream", Name: upstreamSpec.Name, SpecYAML: "name: dep-upstream", Status: "created"}); err != nil {
		t.Fatalf("SavePipeline: %v", err)
	}

	downstream := newTestScheduledRunner()
	downstreamSpec := &pipeline.Spec{
		Name: "dep-downstream",
		Schedule: &pipeline.ScheduleConfig{Type: "dependency", DependsOn: []string{"pipe-upstream"}},
		Source: pipeline.SourceSpec{Type: "file", Config: map[string]any{"path": "d.jsonl", "format": "json"}},
		Sink:   pipeline.SinkSpec{Type: "file_sink", Config: map[string]any{"output_dir": "./out", "format": "jsonl"}},
	}
	s.mu.Lock()
	s.registerPipelineLocked("pipe-downstream", downstreamSpec.Name, downstream, downstreamSpec, nil)
	s.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := s.StartAll(ctx); err != nil {
		t.Fatalf("StartAll: %v", err)
	}
	t.Cleanup(func() { s.StopAll() })

	deadline := time.After(3 * time.Second)
	for downstream.starts.Load() == 0 {
		select {
		case <-deadline:
			t.Fatalf("dependency schedule did not trigger downstream within 3s (starts=%d)", downstream.starts.Load())
		case <-time.After(20 * time.Millisecond):
		}
	}
}


