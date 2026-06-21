package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"openetl-go/internal/etl/alert"
	"openetl-go/internal/etl/core"
)

// ParallelRunner manages N concurrent pipeline instances.
//
// Each instance receives `shard_index` (0..N-1) and `shard_total` (N) in its
// source config. The source plugin decides how to use these values:
//
//	Kafka     → consumer group auto-balance (no code change needed)
//	File      → may split by file pattern, or ignore
//	HTTP      → may split by page range, or ignore
//	MySQL CDC → may split by table list, or ignore
//	Postgres  → may use slot-per-shard, or ignore
//	MySQL batch → WHERE id % shard_total = shard_index
//
// Sources that cannot parallelize simply ignore the extra config fields.
//
// Checkpoint isolation: each instance writes to `{pipeline}.shard-{N}`,
// so shards never clash and can resume independently.
type ParallelRunner struct {
	spec        *Spec
	cpStore     core.CheckpointStore
	dlqWriter   DLQWriter
	alertMgr    *alert.Manager
	parallelism int
	logBuf      *LogBuffer

	mu             sync.RWMutex
	status         Status
	instances      []*Runner
	cancel         context.CancelFunc
	done           chan struct{}
	startedAt      time.Time
	frozenDuration time.Duration
}

// NewParallelRunner creates N independent runners. If spec.Parallelism is nil or
// Count <= 1, a single-runner ParallelRunner is returned (works identically to
// a regular Runner, but exposes aggregated stats).
func NewParallelRunner(spec *Spec, cpStore core.CheckpointStore, dlqW DLQWriter, am *alert.Manager) (*ParallelRunner, error) {
	cfg := spec.Parallelism
	if cfg == nil {
		cfg = &ParallelismConfig{Count: 1}
	}
	cfg.ApplyDefaults()

	pr := &ParallelRunner{
		spec:        spec,
		cpStore:     cpStore,
		dlqWriter:   dlqW,
		alertMgr:    am,
		parallelism: cfg.Count,
		logBuf:      NewLogBuffer(500),
		status:      StatusStopped,
		done:        make(chan struct{}),
		instances:   make([]*Runner, cfg.Count),
	}

	for i := 0; i < cfg.Count; i++ {
		shardSpec := *spec // shallow copy of value fields

		// Deep-copy all config maps to prevent cross-shard mutation
		shardSpec.Source.Config = cloneConfigWithShard(spec.Source.Config, i, cfg.Count, cfg.ShardStrategy)
		shardSpec.Sink.Config = cloneConfig(spec.Sink.Config)
		shardSpec.Transforms = make([]TransformSpec, len(spec.Transforms))
		for j, tf := range spec.Transforms {
			shardSpec.Transforms[j] = TransformSpec{
				Type:   tf.Type,
				Config: cloneConfig(tf.Config),
			}
		}

		// Each shard gets its own checkpoint namespace
		shardCPStore := newShardCheckpointStore(cpStore, spec.Name, i)
		runner, err := NewRunner(&shardSpec, shardCPStore, dlqW, am)
		if err != nil {
			return nil, fmt.Errorf("shard-%d: %w", i, err)
		}
		pr.instances[i] = runner
	}

	return pr, nil
}

// cloneConfig returns a shallow copy of a config map.
// This prevents shards from sharing the same map reference, which could
// cause race conditions when sinks/transforms mutate their config.
func cloneConfig(original map[string]any) map[string]any {
	if original == nil {
		return nil
	}
	out := make(map[string]any, len(original))
	for k, v := range original {
		out[k] = v
	}
	return out
}

// cloneConfigWithShard returns a copy of config with shard_index / shard_total / shard_strategy injected.
func cloneConfigWithShard(original map[string]any, shardIdx, shardTotal int, strategy string) map[string]any {
	out := make(map[string]any, len(original)+2)
	for k, v := range original {
		out[k] = v
	}
	out["shard_index"] = shardIdx
	out["shard_total"] = shardTotal
	out["shard_strategy"] = strategy
	return out
}

func (pr *ParallelRunner) LogBuffer() *LogBuffer { return pr.logBuf }

func (pr *ParallelRunner) Instance(i int) *Runner {
	if i < 0 || i >= len(pr.instances) {
		return nil
	}
	return pr.instances[i]
}

func (pr *ParallelRunner) Shards() []ShardInfo {
	shards := make([]ShardInfo, len(pr.instances))
	for i, inst := range pr.instances {
		shards[i] = ShardInfo{Index: i, Status: inst.Status(), Stats: inst.Stats()}
	}
	return shards
}

// Start launches all N parallel instances.
func (pr *ParallelRunner) Start(ctx context.Context) error {
	pr.mu.Lock()
	if pr.status == StatusRunning {
		pr.mu.Unlock()
		return fmt.Errorf("pipeline %s: already running", pr.spec.Name)
	}
	pr.status = StatusRunning
	pr.frozenDuration = 0
	pr.startedAt = time.Now()
	pr.mu.Unlock()

	ctx, pr.cancel = context.WithCancel(ctx)

	for i, inst := range pr.instances {
		if err := inst.Start(ctx); err != nil {
			pr.stopAll()
			return fmt.Errorf("shard-%d start: %w", i, err)
		}
	}

	go func() {
		for _, inst := range pr.instances {
			inst.Wait()
		}
		pr.mu.Lock()
		if pr.status == StatusRunning {
			pr.freezeDurationLocked()
			pr.status = StatusCompleted
		}
		pr.mu.Unlock()
		close(pr.done)
	}()

	return nil
}

func (pr *ParallelRunner) Stop() error {
	pr.mu.Lock()
	defer pr.mu.Unlock()
	if pr.status != StatusRunning {
		return nil
	}
	pr.freezeDurationLocked()
	pr.status = StatusStopped
	pr.stopAll()
	return nil
}

func (pr *ParallelRunner) stopAll() {
	for _, inst := range pr.instances {
		_ = inst.Stop()
	}
	if pr.cancel != nil {
		pr.cancel()
	}
}

func (pr *ParallelRunner) Wait()                 { <-pr.done }
func (pr *ParallelRunner) Done() <-chan struct{} { return pr.done }
func (pr *ParallelRunner) Status() Status        { pr.mu.RLock(); defer pr.mu.RUnlock(); return pr.status }
func (pr *ParallelRunner) InstanceCount() int    { return pr.parallelism }

func (pr *ParallelRunner) freezeDurationLocked() {
	if pr.startedAt.IsZero() {
		return
	}
	pr.frozenDuration += time.Since(pr.startedAt)
	pr.startedAt = time.Time{}
}

func (pr *ParallelRunner) Duration() time.Duration {
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	if !pr.startedAt.IsZero() {
		return (pr.frozenDuration + time.Since(pr.startedAt)).Truncate(time.Second)
	}
	return pr.frozenDuration.Truncate(time.Second)
}

// AggregatedStats sums stats across all instances.
func (pr *ParallelRunner) AggregatedStats() Stats {
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	var total Stats
	startedAt := pr.startedAt
	if !startedAt.IsZero() {
		total.StartedAt = &startedAt
		total.Uptime = (pr.frozenDuration + time.Since(startedAt)).Truncate(time.Second).String()
	} else {
		total.Uptime = pr.frozenDuration.Truncate(time.Second).String()
	}
	for _, inst := range pr.instances {
		s := inst.Stats()
		total.RecordsRead += s.RecordsRead
		total.RecordsWritten += s.RecordsWritten
		total.RecordsFailed += s.RecordsFailed
		total.RecordsDLQ += s.RecordsDLQ
		total.DLQReplayCount += s.DLQReplayCount
		total.DLQDeleteCount += s.DLQDeleteCount
		if s.LastError != "" && total.LastError == "" {
			total.LastError = s.LastError
		}
		if s.LastCheckpoint.After(total.LastCheckpoint) {
			total.LastCheckpoint = s.LastCheckpoint
		}
	}
	return total
}

// ── Shard-scoped checkpoint store ─────────────────────────────────────

type shardCheckpointStore struct {
	inner    core.CheckpointStore
	baseName string
	shardIdx int
}

func newShardCheckpointStore(inner core.CheckpointStore, baseName string, shardIdx int) core.CheckpointStore {
	return &shardCheckpointStore{inner: inner, baseName: baseName, shardIdx: shardIdx}
}

func (s *shardCheckpointStore) shardKey() string {
	return fmt.Sprintf("%s.shard-%d", s.baseName, s.shardIdx)
}
func (s *shardCheckpointStore) Save(ctx context.Context, cp core.Checkpoint) error {
	cp.JobName = s.shardKey()
	return s.inner.Save(ctx, cp)
}
func (s *shardCheckpointStore) Load(ctx context.Context, _ string) (*core.Checkpoint, error) {
	return s.inner.Load(ctx, s.shardKey())
}
func (s *shardCheckpointStore) Delete(ctx context.Context, _ string) error {
	return s.inner.Delete(ctx, s.shardKey())
}
func (s *shardCheckpointStore) List(ctx context.Context) ([]core.Checkpoint, error) {
	all, _ := s.inner.List(ctx)
	var mine []core.Checkpoint
	for _, cp := range all {
		if cp.JobName == s.shardKey() {
			mine = append(mine, cp)
		}
	}
	return mine, nil
}
