package pipeline

import (
	"context"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	"github.com/a8851625/openetl-go/internal/etl/core"
)

// RunnerInterface is a unified interface for single and parallel runners.
// Both *Runner and *ParallelRunner implement this.
type RunnerInterface interface {
	Start(ctx context.Context) error
	Stop() error
	Pause() error
	Resume(ctx context.Context) error
	Wait()
	Done() <-chan struct{}
	Status() Status
	Stats() Stats
	Duration() time.Duration
	MetricsSnapshot() MetricsSnapshot
	LogBuffer() *LogBuffer
	Shards() []ShardInfo
	IncrementDLQReplay(n int64)
	IncrementDLQDelete(n int64)
	CircuitBreakerState() int
	SinkMetrics() []core.SinkMetrics
	StateMetrics() []core.StateMetrics
	TransformMetrics() []core.TransformMetrics
}

// Ensure *Runner and *ParallelRunner satisfy the interface.
var _ RunnerInterface = (*Runner)(nil)
var _ RunnerInterface = (*ParallelRunner)(nil)

// Implement missing methods on ParallelRunner:

func (pr *ParallelRunner) Stats() Stats {
	return pr.AggregatedStats()
}

func (pr *ParallelRunner) MetricsSnapshot() MetricsSnapshot {
	if len(pr.instances) == 0 {
		return MetricsSnapshot{}
	}
	if len(pr.instances) == 1 {
		return pr.instances[0].MetricsSnapshot()
	}
	// Aggregate latency/batch metrics across all shards
	var totalRead, totalWrite float64
	var readCount, writeCount int
	var totalBatch int64
	var batchCount int
	var maxCDCLag int64
	for _, inst := range pr.instances {
		m := inst.MetricsSnapshot()
		if m.SourceReadLatencyMs > 0 {
			totalRead += m.SourceReadLatencyMs
			readCount++
		}
		if m.SinkWriteLatencyMs > 0 {
			totalWrite += m.SinkWriteLatencyMs
			writeCount++
		}
		if m.AvgBatchSize > 0 {
			totalBatch += m.AvgBatchSize
			batchCount++
		}
		if m.CDCLagMs > maxCDCLag {
			maxCDCLag = m.CDCLagMs
		}
	}
	snap := pr.instances[0].MetricsSnapshot()
	if readCount > 0 {
		snap.SourceReadLatencyMs = totalRead / float64(readCount)
	}
	if writeCount > 0 {
		snap.SinkWriteLatencyMs = totalWrite / float64(writeCount)
	}
	if batchCount > 0 {
		snap.AvgBatchSize = totalBatch / int64(batchCount)
	}
	snap.CDCLagMs = maxCDCLag
	return snap
}

func (pr *ParallelRunner) IncrementDLQReplay(n int64) {
	for _, inst := range pr.instances {
		inst.IncrementDLQReplay(n)
	}
}

func (pr *ParallelRunner) IncrementDLQDelete(n int64) {
	for _, inst := range pr.instances {
		inst.IncrementDLQDelete(n)
	}
}

func (pr *ParallelRunner) Pause() error {
	for _, inst := range pr.instances {
		_ = inst.Pause()
	}
	return nil
}

func (pr *ParallelRunner) Resume(ctx context.Context) error {
	for _, inst := range pr.instances {
		_ = inst.Resume(ctx)
	}
	return nil
}

// CircuitBreakerState returns the worst breaker state across all instances.
func (pr *ParallelRunner) CircuitBreakerState() int {
	worst := 0
	for _, inst := range pr.instances {
		if s := inst.CircuitBreakerState(); s > worst {
			worst = s
		}
	}
	return worst
}

// SinkMetrics aggregates per-sink metrics across all instances.
func (pr *ParallelRunner) SinkMetrics() []core.SinkMetrics {
	seen := map[string]core.SinkMetrics{}
	for _, inst := range pr.instances {
		for _, sm := range inst.SinkMetrics() {
			if existing, ok := seen[sm.SinkName]; ok {
				existing.RowsWritten += sm.RowsWritten
				existing.BatchesSent += sm.BatchesSent
				existing.Errors += sm.Errors
				if sm.WriteLatency > 0 {
					existing.WriteLatency = (existing.WriteLatency + sm.WriteLatency) / 2
				}
				seen[sm.SinkName] = existing
			} else {
				seen[sm.SinkName] = sm
			}
		}
	}
	var result []core.SinkMetrics
	for _, sm := range seen {
		result = append(result, sm)
	}
	return result
}

func (pr *ParallelRunner) StateMetrics() []core.StateMetrics {
	seen := map[string]core.StateMetrics{}
	for _, inst := range pr.instances {
		for _, sm := range inst.StateMetrics() {
			key := sm.Pipeline + "/" + sm.Node
			if existing, ok := seen[key]; ok {
				existing.Keys += sm.Keys
				existing.Bytes += sm.Bytes
				if sm.UpdatedAt.After(existing.UpdatedAt) {
					existing.UpdatedAt = sm.UpdatedAt
				}
				seen[key] = existing
			} else {
				seen[key] = sm
			}
		}
	}
	var result []core.StateMetrics
	for _, sm := range seen {
		result = append(result, sm)
	}
	return result
}

func (pr *ParallelRunner) TransformMetrics() []core.TransformMetrics {
	seen := map[string]core.TransformMetrics{}
	for _, inst := range pr.instances {
		for _, tm := range inst.TransformMetrics() {
			key := tm.Node + "/" + tm.Transform
			existing, ok := seen[key]
			if !ok {
				existing = core.TransformMetrics{
					Node:      tm.Node,
					Transform: tm.Transform,
					Counters:  map[string]int64{},
				}
			}
			for name, value := range tm.Counters {
				existing.Counters[name] += value
			}
			seen[key] = existing
		}
	}
	var result []core.TransformMetrics
	for _, tm := range seen {
		result = append(result, tm)
	}
	return result
}

// NewPipeline creates a single or parallel runner based on spec.
func NewPipeline(spec *Spec, cpStore core.CheckpointStore, dlqW DLQWriter, am *alert.Manager) (RunnerInterface, error) {
	if spec.Parallelism != nil && spec.Parallelism.LogicalShardCount() > 1 {
		return NewParallelRunner(spec, cpStore, dlqW, am)
	}
	return NewRunner(spec, cpStore, dlqW, am)
}

// NewDistributedPipeline creates a ParallelRunner that delegates shard execution
// to worker processes via the dispatcher instead of running shards inline
// (A11-redo). Used by the master role; standalone keeps using NewPipeline.
//
// Placement rules:
//   - logical_shards > 1: one continuous/batch task per shard (existing path).
//   - logical_shards <= 1: still dispatch ONE continuous shard task so the
//     pipeline is placed on a worker rather than the master control plane.
//     This is pipeline-level placement for single-shard streaming CDC, not
//     multi-active HA (still one replica; worker loss reassigns the task).
//
// Checkpoint keys for the single-shard path keep the plain pipeline name
// (same as standalone NewRunner) so promoting an unsharded CDC pipeline to
// master-worker does not orphan existing checkpoints. Multi-shard paths still
// use "{name}.shard-{idx}".
func NewDistributedPipeline(spec *Spec, cpStore core.CheckpointStore, dlqW DLQWriter, am *alert.Manager, dispatcher ShardDispatcher) (RunnerInterface, error) {
	// Work on a shallow copy so we never mutate the caller's stored Spec
	// (e.g. Server.specs map entries returned by the API).
	runSpec := *spec
	if runSpec.Parallelism != nil {
		pc := *runSpec.Parallelism
		runSpec.Parallelism = &pc
	} else {
		runSpec.Parallelism = &ParallelismConfig{Count: 1}
	}
	runSpec.Parallelism.ApplyDefaults()
	if runSpec.Parallelism.LogicalShardCount() < 1 {
		runSpec.Parallelism.Count = 1
		if runSpec.Parallelism.Sharding == nil {
			runSpec.Parallelism.Sharding = &ShardingConfig{LogicalShards: 1}
		} else {
			sh := *runSpec.Parallelism.Sharding
			sh.LogicalShards = 1
			runSpec.Parallelism.Sharding = &sh
		}
	}
	pr, err := NewParallelRunner(&runSpec, cpStore, dlqW, am)
	if err != nil {
		return nil, err
	}
	pr.distributed = true
	pr.dispatcher = dispatcher
	return pr, nil
}
