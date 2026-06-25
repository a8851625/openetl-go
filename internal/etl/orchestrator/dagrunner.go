package orchestrator

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

// DAGRunnerWrapper adapts DAGExecutor to implement pipeline.RunnerInterface,
// allowing DAG-format pipelines to be managed alongside linear pipelines in the server.
type DAGRunnerWrapper struct {
	exec      *DAGExecutor
	logBuf    *pipeline.LogBuffer
	dlqReplay int64
	dlqDelete int64

	mu             sync.Mutex
	done           chan struct{}
	started        bool
	startedAt      time.Time
	frozenDuration time.Duration
}

func NewDAGRunnerWrapper(exec *DAGExecutor) *DAGRunnerWrapper {
	return &DAGRunnerWrapper{
		exec:   exec,
		logBuf: pipeline.NewLogBuffer(500),
		done:   make(chan struct{}),
	}
}

func (w *DAGRunnerWrapper) Start(ctx context.Context) error {
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return nil
	}
	w.done = make(chan struct{})
	w.started = true
	w.frozenDuration = 0
	w.startedAt = time.Now()
	w.mu.Unlock()

	if err := w.exec.Start(ctx); err != nil {
		w.mu.Lock()
		w.started = false
		w.mu.Unlock()
		w.logBuf.Errorf("Start failed: %v", err)
		return err
	}

	w.logBuf.Infof("Pipeline %s started (DAG mode)", w.exec.spec.Name)
	go func() {
		w.exec.Wait()
		w.mu.Lock()
		if !w.startedAt.IsZero() {
			w.frozenDuration += time.Since(w.startedAt)
			w.startedAt = time.Time{}
		}
		w.mu.Unlock()
		w.logBuf.Infof("Pipeline %s finished (DAG mode)", w.exec.spec.Name)
		close(w.done)
	}()
	return nil
}

func (w *DAGRunnerWrapper) Stop() error {
	w.mu.Lock()
	if !w.startedAt.IsZero() {
		w.frozenDuration += time.Since(w.startedAt)
		w.startedAt = time.Time{}
	}
	w.mu.Unlock()
	w.logBuf.Infof("Pipeline %s stopping", w.exec.spec.Name)
	return w.exec.Stop()
}

func (w *DAGRunnerWrapper) Wait() {
	<-w.Done()
}

func (w *DAGRunnerWrapper) Done() <-chan struct{} {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.done
}

func (w *DAGRunnerWrapper) Status() pipeline.Status {
	s := w.exec.Status()
	switch s {
	case "running":
		return pipeline.StatusRunning
	case "failed":
		return pipeline.StatusFailed
	case "completed":
		return pipeline.StatusCompleted
	default:
		return pipeline.StatusStopped
	}
}

func (w *DAGRunnerWrapper) Duration() time.Duration {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.startedAt.IsZero() {
		return (w.frozenDuration + time.Since(w.startedAt)).Truncate(time.Second)
	}
	return w.frozenDuration.Truncate(time.Second)
}

func (w *DAGRunnerWrapper) Stats() pipeline.Stats {
	s := w.exec.Stats()
	w.mu.Lock()
	startedAt := w.startedAt
	frozen := w.frozenDuration
	w.mu.Unlock()

	var startedPtr *time.Time
	if !startedAt.IsZero() {
		startedPtr = &startedAt
	}
	var uptime string
	if startedPtr == nil {
		uptime = frozen.Truncate(time.Second).String()
	} else {
		uptime = (frozen + time.Since(*startedPtr)).Truncate(time.Second).String()
	}

	return pipeline.Stats{
		RecordsRead:    s.RecordsRead,
		RecordsWritten: s.RecordsWritten,
		RecordsFailed:  s.RecordsFailed,
		RecordsDLQ:     s.RecordsDLQ,
		DLQReplayCount: atomic.LoadInt64(&w.dlqReplay),
		DLQDeleteCount: atomic.LoadInt64(&w.dlqDelete),
		LastCheckpoint: time.Now(),
		StartedAt:      startedPtr,
		Uptime:         uptime,
	}
}

func (w *DAGRunnerWrapper) MetricsSnapshot() pipeline.MetricsSnapshot {
	s := w.exec.Stats()
	return pipeline.MetricsSnapshot{
		LastBatchSize: int(s.RecordsWritten),
		BatchCount:    s.RecordsRead,
	}
}

func (w *DAGRunnerWrapper) LogBuffer() *pipeline.LogBuffer {
	return w.logBuf
}

func (w *DAGRunnerWrapper) Shards() []pipeline.ShardInfo {
	return nil
}

func (w *DAGRunnerWrapper) IncrementDLQReplay(n int64) {
	atomic.AddInt64(&w.dlqReplay, n)
}

func (w *DAGRunnerWrapper) IncrementDLQDelete(n int64) {
	atomic.AddInt64(&w.dlqDelete, n)
}

func (w *DAGRunnerWrapper) Pause() error {
	return w.exec.Stop()
}

func (w *DAGRunnerWrapper) Resume(ctx context.Context) error {
	return w.Start(ctx)
}

// CircuitBreakerState returns the worst breaker state across all DAG sinks.
func (w *DAGRunnerWrapper) CircuitBreakerState() int {
	worst := 0
	for _, breaker := range w.exec.breakers {
		if s := breaker.StateCode(); s > worst {
			worst = s
		}
	}
	return worst
}

// SinkMetrics collects per-sink metrics from DAG sinks.
func (w *DAGRunnerWrapper) SinkMetrics() []core.SinkMetrics {
	var result []core.SinkMetrics
	for id, sink := range w.exec.sinks {
		if provider, ok := sink.(core.SinkMetricsProvider); ok {
			result = append(result, provider.SinkMetrics())
		} else {
			result = append(result, core.SinkMetrics{SinkName: id})
		}
	}
	return result
}

func (w *DAGRunnerWrapper) StateMetrics() []core.StateMetrics {
	return w.exec.StateMetrics(context.Background())
}

func (w *DAGRunnerWrapper) TransformMetrics() []core.TransformMetrics {
	return w.exec.TransformMetrics()
}
