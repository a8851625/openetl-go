package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	"github.com/a8851625/openetl-go/internal/etl/checkpoint"
	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/retry"
)

// DLQWriter is the interface for dead-letter queue persistence.
// Any type implementing Write(ctx, DLQEntry) error can serve as a DLQ writer.
type DLQWriter interface {
	WriteDLQ(ctx context.Context, entry DLQEntry) error
}

// DLQEntry is the pipeline-level representation of a dead-letter record.
type DLQEntry struct {
	JobName         string
	Record          core.Record
	Error           string
	ErrorClass      string
	Attempt         int
	PipelineVersion int
	DAGNode         string
}

type Status string

const (
	StatusRunning   Status = "running"
	StatusPaused    Status = "paused"
	StatusStopped   Status = "stopped"
	StatusFailed    Status = "failed"
	StatusCompleted Status = "completed"
)

type Stats struct {
	RecordsRead    int64      `json:"records_read"`
	RecordsWritten int64      `json:"records_written"`
	RecordsFailed  int64      `json:"records_failed"`
	RecordsDLQ     int64      `json:"records_dlq"`
	BytesRead      int64      `json:"bytes_read"`
	BytesWritten   int64      `json:"bytes_written"`
	DLQReplayCount int64      `json:"dlq_replay_count"`
	DLQDeleteCount int64      `json:"dlq_delete_count"`
	LastError      string     `json:"last_error,omitempty"`
	LastCheckpoint time.Time  `json:"last_checkpoint"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
	Uptime         string     `json:"uptime"`
}

type Runner struct {
	spec            *Spec
	source          core.Source
	transforms      core.TransformChain
	processors      core.RecordProcessorChain
	sink            core.Sink
	checkpointStore core.CheckpointStore
	dlqWriter       DLQWriter
	alertManager    *alert.Manager
	retryConfig     retry.Config
	metricsHooks    *MetricsHooks
	logBuf          *LogBuffer

	status Status
	stats  Stats
	mu     sync.RWMutex

	// frozenDuration captures total runtime when the pipeline enters a
	// non-running state (stopped/completed/failed/paused). For running
	// pipelines, Stats() computes a live value from StartedAt.
	frozenDuration time.Duration

	cancel context.CancelFunc
	stopCh chan struct{}
	done   chan struct{}
	// runActive is true after Start has reserved a runLoop slot and remains
	// true until that runLoop has closed its own done channel.
	runActive bool
	reader    core.RecordReader

	hooks map[core.HookKind]core.LifecycleHook

	checkpointInterval time.Duration
	flushInterval      time.Duration
	batchSize          int
	backpressureBuffer int
	transformWorkers   int

	// recordsCh holds a reference to the backpressure channel for metrics.
	recordsCh chan core.Record

	// lastCheckpointSave tracks when we last persisted a checkpoint.
	// Checkpoints are saved at most once per checkpointInterval,
	// plus always on shutdown/EOF.
	lastCheckpointSave time.Time
	// uncheckpointedBatches counts batches written since last checkpoint save.
	uncheckpointedBatches int64
	// pendingCheckpoint* retains the latest sink-acknowledged source boundary
	// when checkpoint saves are throttled. A timer or shutdown flush persists
	// it even if no later record arrives.
	pendingCheckpointProcessed []core.Record
	pendingCheckpointCommitted []core.Record
	// checkpointBlocked prevents a later successful batch from advancing past
	// a record that could reach neither the sink nor durable DLQ storage. It is
	// reset on the next Start, when the source reopens from the last durable
	// checkpoint and replays the unsafe range.
	checkpointBlocked     bool
	checkpointBlockReason string

	// circuitBreaker pauses the source when sink failures exceed threshold.
	circuitBreaker *CircuitBreaker
	// alertChecker evaluates threshold-based alert rules periodically.
	alertChecker *AlertRuleChecker
	// lastRecordAt tracks when the last record was read (for stall detection).
	lastRecordAt time.Time
	// sinkWriteLimiter optionally limits concurrent sink.Write calls across
	// shard runners that share a ParallelRunner in the same process.
	sinkWriteLimiter chan struct{}

	// inflightBatch tracks writeBatch invocations in progress so that
	// Stop() can wait for the commit (sink write + DLQ + checkpoint) to
	// finish before tearing down the runner. Without this, Stop() races
	// with an in-flight writeBatch: it cancels the loop ctx mid-commit,
	// surfacing as spurious "context canceled" errors from sink.Write /
	// DLQ writer / checkpoint store (see runActive/stopCh shutdown flow).
	//
	// We deliberately avoid sync.WaitGroup here: its Wait must not be
	// called concurrently from multiple goroutines, but Stop() is
	// documented as idempotent and may be invoked repeatedly. The custom
	// tracker below supports concurrent waiters safely.
	inflightBatch inflightBatchTracker
}

// inflightBatchTracker is a counter guarded by a mutex plus a signaling
// channel that is closed/recreated each time the count drops to zero. It
// supports any number of concurrent Wait() callers (unlike sync.WaitGroup).
type inflightBatchTracker struct {
	mu   sync.Mutex
	n    int
	zero chan struct{}
}

func newInflightBatchTracker() inflightBatchTracker {
	return inflightBatchTracker{zero: make(chan struct{})}
}

func (t *inflightBatchTracker) add() {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.zero == nil {
		t.zero = make(chan struct{})
	}
	if t.n == 0 {
		// Leaving zero: close the old signal channel so current waiters
		// that raced with us still wake, but future Wait callers block on
		// a fresh channel.
		close(t.zero)
		t.zero = make(chan struct{})
	}
	t.n++
}

func (t *inflightBatchTracker) done() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.n--
	if t.n < 0 {
		t.n = 0
	}
	if t.n == 0 {
		close(t.zero)
		t.zero = make(chan struct{})
	}
}

// wait blocks until the tracker reports zero in-flight batches, or until
// the timer fires (safety bound against a wedged sink). Returns true if
// the wait completed, false on timeout.
func (t *inflightBatchTracker) wait(timeout time.Duration) bool {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	for {
		t.mu.Lock()
		if t.n == 0 {
			t.mu.Unlock()
			return true
		}
		ch := t.zero
		t.mu.Unlock()
		if ch == nil {
			// Not initialized yet — treat as zero.
			return true
		}

		select {
		case <-ch:
			// either re-check (loop) or return
		case <-deadline.C:
			return false
		}
	}
}

func NewRunner(spec *Spec, cpStore core.CheckpointStore, dlqW DLQWriter, am *alert.Manager) (*Runner, error) {
	source, err := registry.BuildSource(spec.Source.Type, spec.Source.Config)
	if err != nil {
		return nil, fmt.Errorf("build source: %w", err)
	}

	var transforms core.TransformChain
	for i, tc := range spec.Transforms {
		config := InjectStateDefaults(spec.Name, TransformStateNodeID(i, tc.Type), tc.Config)
		t, err := registry.BuildTransform(tc.Type, config)
		if err != nil {
			return nil, fmt.Errorf("build transform %s: %w", tc.Type, err)
		}
		transforms = append(transforms, t)
	}

	sink, err := registry.BuildSink(spec.Sink.Type, spec.Sink.Config)
	if err != nil {
		return nil, fmt.Errorf("build sink: %w", err)
	}

	cfg := retry.DefaultConfig()
	if spec.Retry != nil {
		cfg.MaxAttempts = spec.Retry.MaxAttempts
		cfg.InitialInterval = time.Duration(spec.Retry.InitialIntervalMs) * time.Millisecond
		cfg.MaxInterval = time.Duration(spec.Retry.MaxIntervalMs) * time.Millisecond
	}

	cpInterval := time.Duration(spec.CheckpointIntervalSec) * time.Second
	if cpInterval == 0 {
		cpInterval = 30 * time.Second
	}

	bs := spec.BatchSize
	if bs == 0 {
		bs = 1000
	}

	bp := spec.BackpressureBuffer
	if bp == 0 {
		bp = 100
	}

	// Flush interval: how often to flush a partial batch to the sink.
	// Default 1 second. Configurable via flush_interval_ms in spec.
	flushIv := time.Second
	if spec.FlushIntervalMs > 0 {
		flushIv = time.Duration(spec.FlushIntervalMs) * time.Millisecond
	}
	transformWorkers := 1
	if spec.Parallelism != nil {
		transformWorkers = spec.Parallelism.TransformWorkerCount()
	}

	hooks := &MetricsHooks{}
	lifecycleHooks := BuildHooks(spec.Name, spec.Hooks)
	processors := BuildProcessors(spec)

	r := &Runner{
		spec:               spec,
		source:             source,
		transforms:         transforms,
		processors:         processors,
		sink:               &SinkWriteHook{Hooks: hooks, Sink: sink},
		checkpointStore:    cpStore,
		dlqWriter:          dlqW,
		alertManager:       am,
		retryConfig:        cfg,
		metricsHooks:       hooks,
		logBuf:             NewLogBuffer(500),
		status:             StatusStopped,
		stopCh:             make(chan struct{}),
		done:               make(chan struct{}),
		checkpointInterval: cpInterval,
		flushInterval:      flushIv,
		batchSize:          bs,
		backpressureBuffer: bp,
		transformWorkers:   transformWorkers,
		hooks:              lifecycleHooks,
		inflightBatch:      newInflightBatchTracker(),
	}
	if spec.CircuitBreaker != nil {
		r.circuitBreaker = NewCircuitBreaker(*spec.CircuitBreaker, am, spec.Name)
	}
	if spec.AlertRules != nil {
		r.alertChecker = NewAlertRuleChecker(*spec.AlertRules, am, spec.Name)
	}
	return r, nil
}

func (r *Runner) Status() Status {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.status
}

func (r *Runner) freezeDurationLocked() {
	if r.stats.StartedAt == nil {
		return
	}
	r.frozenDuration += time.Since(*r.stats.StartedAt)
	r.stats.StartedAt = nil
}

func (r *Runner) Duration() time.Duration {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.stats.StartedAt != nil {
		return (r.frozenDuration + time.Since(*r.stats.StartedAt)).Truncate(time.Second)
	}
	return r.frozenDuration.Truncate(time.Second)
}

func (r *Runner) Stats() Stats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	s := r.stats
	if s.StartedAt == nil {
		s.Uptime = r.frozenDuration.Truncate(time.Second).String()
	} else {
		total := r.frozenDuration + time.Since(*s.StartedAt)
		s.Uptime = total.Truncate(time.Second).String()
	}
	return s
}

func (r *Runner) LogBuffer() *LogBuffer { return r.logBuf }

func (r *Runner) setSinkWriteLimiter(limiter chan struct{}) {
	r.sinkWriteLimiter = limiter
}

func (r *Runner) Shards() []ShardInfo {
	return []ShardInfo{{Index: 0, Status: r.Status(), Stats: r.Stats()}}
}

func (r *Runner) logInfo(msg string) {
	g.Log().Infof(context.Background(), "[%s] %s", r.spec.Name, msg)
	r.logBuf.Info(msg)
}

func (r *Runner) logError(msg string) {
	g.Log().Errorf(context.Background(), "[%s] %s", r.spec.Name, msg)
	r.logBuf.Error(msg)
}

func (r *Runner) logWarn(msg string) {
	g.Log().Warningf(context.Background(), "[%s] %s", r.spec.Name, msg)
	r.logBuf.Warn(msg)
}

func (r *Runner) logDebug(msg string) {
	r.logBuf.Debug(msg)
}

func (r *Runner) MetricsSnapshot() MetricsSnapshot {
	if r.metricsHooks == nil {
		return MetricsSnapshot{}
	}
	snap := r.metricsHooks.Snapshot()
	if r.recordsCh != nil {
		snap.BackpressureDepth = len(r.recordsCh)
		snap.BackpressureCapacity = cap(r.recordsCh)
	}
	return snap
}

func (r *Runner) IncrementDLQReplay(n int64) {
	r.mu.Lock()
	r.stats.DLQReplayCount += n
	r.mu.Unlock()
}

func (r *Runner) IncrementDLQDelete(n int64) {
	r.mu.Lock()
	r.stats.DLQDeleteCount += n
	r.mu.Unlock()
}

// CircuitBreakerState returns the breaker state code for Prometheus metrics.
func (r *Runner) CircuitBreakerState() int {
	if r.circuitBreaker == nil {
		return 0
	}
	return r.circuitBreaker.StateCode()
}

// SinkMetrics returns per-sink metrics from sinks that implement SinkMetricsProvider.
func (r *Runner) SinkMetrics() []core.SinkMetrics {
	if provider, ok := r.sink.(core.SinkMetricsProvider); ok {
		return []core.SinkMetrics{provider.SinkMetrics()}
	}
	return nil
}

func (r *Runner) StateMetrics() []core.StateMetrics {
	metrics, err := r.transforms.StateMetrics(context.Background())
	if err != nil {
		r.logWarn(fmt.Sprintf("state metrics collection failed: %v", err))
		return nil
	}
	return metrics
}

func (r *Runner) TransformMetrics() []core.TransformMetrics {
	return r.transforms.TransformMetrics()
}

// addRecordsRead atomically increments RecordsRead under mutex and returns new value.
func (r *Runner) addRecordsRead(delta int64) int64 {
	r.mu.Lock()
	r.stats.RecordsRead += delta
	v := r.stats.RecordsRead
	r.mu.Unlock()
	return v
}

func (r *Runner) addRecordsWritten(delta int64) int64 {
	r.mu.Lock()
	r.stats.RecordsWritten += delta
	v := r.stats.RecordsWritten
	r.mu.Unlock()
	return v
}

func (r *Runner) addRecordsFailed(delta int64) {
	r.mu.Lock()
	r.stats.RecordsFailed += delta
	r.mu.Unlock()
}

func (r *Runner) addRecordsDLQ(delta int64) {
	r.mu.Lock()
	r.stats.RecordsDLQ += delta
	r.mu.Unlock()
}

func (r *Runner) setLastError(msg string) {
	r.mu.Lock()
	r.stats.LastError = msg
	r.mu.Unlock()
}

func (r *Runner) setLastCheckpoint(t time.Time) {
	r.mu.Lock()
	r.stats.LastCheckpoint = t
	r.mu.Unlock()
}

// recordsRead returns the current RecordsRead count under the lock.
func (r *Runner) recordsRead() int64 {
	r.mu.RLock()
	v := r.stats.RecordsRead
	r.mu.RUnlock()
	return v
}

func (r *Runner) recordsWritten() int64 {
	r.mu.RLock()
	v := r.stats.RecordsWritten
	r.mu.RUnlock()
	return v
}

func (r *Runner) recordsFailed() int64 {
	r.mu.RLock()
	v := r.stats.RecordsFailed
	r.mu.RUnlock()
	return v
}

func (r *Runner) Start(ctx context.Context) error {
	r.mu.Lock()
	if r.status == StatusRunning {
		r.mu.Unlock()
		return fmt.Errorf("pipeline %s is already running", r.spec.Name)
	}
	if r.runActive {
		r.mu.Unlock()
		return fmt.Errorf("pipeline %s is still stopping", r.spec.Name)
	}
	r.logBuf = NewLogBuffer(500)
	r.status = StatusRunning
	r.frozenDuration = 0
	now := time.Now()
	r.stats = Stats{StartedAt: &now}
	r.lastCheckpointSave = time.Time{}
	r.uncheckpointedBatches = 0
	r.pendingCheckpointProcessed = nil
	r.pendingCheckpointCommitted = nil
	r.checkpointBlocked = false
	r.checkpointBlockReason = ""
	r.done = make(chan struct{})
	r.runActive = true
	done := r.done
	r.mu.Unlock()

	ctx, r.cancel = context.WithCancel(ctx)
	markStartFailed := func() {
		r.mu.Lock()
		active := r.runActive
		if active {
			r.runActive = false
		}
		r.mu.Unlock()
		if active {
			close(done)
		}
		if r.cancel != nil {
			r.cancel()
		}
	}

	g.Log().Infof(ctx, "[%s] Opening sink...", r.spec.Name)
	if err := r.sink.Open(ctx); err != nil {
		r.setStatus(StatusFailed)
		r.logError(fmt.Sprintf("Failed to open sink: %v", err))
		markStartFailed()
		return fmt.Errorf("open sink: %w", err)
	}

	// Optional schema validation + typed auto_create: if source describes its
	// schema, feed it to sinks that implement SourceSchemaConsumer (so
	// information_schema types drive target DDL) and to SchemaValidators.
	if descriptor, ok := r.source.(core.SchemaDescriptor); ok {
		schema, err := descriptor.Describe(ctx)
		if err != nil {
			r.setStatus(StatusFailed)
			r.logError(fmt.Sprintf("Source schema description failed: %v", err))
			_ = r.sink.Close()
			markStartFailed()
			return fmt.Errorf("describe source schema: %w", err)
		}
		if len(schema.Columns) > 0 {
			if consumer, ok := r.sink.(core.SourceSchemaConsumer); ok {
				consumer.SetSourceSchema(schema)
				r.logInfo(fmt.Sprintf("Source schema applied to sink for typed auto_create: %d columns", len(schema.Columns)))
			}
			if validator, ok := schemaValidatorForSink(r.sink); ok {
				if err := validator.ValidateSchema(ctx, schema); err != nil {
					r.setStatus(StatusFailed)
					r.logError(fmt.Sprintf("Schema validation failed: %v", err))
					_ = r.sink.Close()
					markStartFailed()
					return fmt.Errorf("schema validation: %w", err)
				}
				r.logInfo(fmt.Sprintf("Schema validated: %d columns", len(schema.Columns)))
			}
		}
	}

	var cp *core.Checkpoint
	if r.checkpointStore != nil {
		loaded, err := r.checkpointStore.Load(ctx, r.spec.Name)
		if err == nil && loaded != nil {
			cp = unwrapCheckpointForSource(loaded)
			r.logInfo("Resuming from checkpoint")
		}
	}

	g.Log().Infof(ctx, "[%s] Opening source...", r.spec.Name)
	reader, err := r.source.Open(ctx, cp)
	if err != nil {
		r.setStatus(StatusFailed)
		r.logError(fmt.Sprintf("Failed to open source: %v", err))
		_ = r.sink.Close()
		markStartFailed()
		return fmt.Errorf("open source: %w", err)
	}

	// Wrap reader with framework-level shard filter when parallelism > 1
	// for sources that don't handle sharding natively.
	if r.spec.Parallelism != nil && r.spec.Parallelism.LogicalShardCount() > 1 {
		switch r.spec.Source.Type {
		case "mysql_batch", "mysql_cdc", "mysql_snapshot_cdc", "postgres_cdc", "kafka", "http":
			// These sources handle sharding natively (SQL-level, consumer-group,
			// or page-modulo). The decorator would double-filter — skip it.
		default:
			sc := core.ReadShardConfig(r.spec.Source.Config)
			reader = core.NewShardedReader(reader, sc)
		}
	}
	r.reader = reader

	// Fire OnInit hook after source+sink are opened.
	fireHook(ctx, r.hooks, core.HookOnInit, core.HookContext{
		PipelineName: r.spec.Name,
		Config:       r.spec.Hooks.getConfig(core.HookOnInit),
	})

	r.logInfo(fmt.Sprintf("Pipeline started (%s → %s)", r.spec.Source.Type, r.spec.Sink.Type))
	go r.runLoop(ctx, done)
	return nil
}

func schemaValidatorForSink(sink core.Sink) (core.SchemaValidator, bool) {
	if validator, ok := sink.(core.SchemaValidator); ok {
		return validator, true
	}
	if hook, ok := sink.(*SinkWriteHook); ok {
		validator, ok := hook.Sink.(core.SchemaValidator)
		return validator, ok
	}
	return nil, false
}

func (r *Runner) Stop() error {
	r.mu.Lock()
	if r.status != StatusRunning {
		r.mu.Unlock()
		return nil
	}
	r.freezeDurationLocked()
	r.status = StatusStopped
	r.mu.Unlock()

	// Cancel the loop ctx so readLoop stops reading new records and
	// writeLoop exits its select loop. Do NOT hold r.mu across the cancel
	// — writeBatch/saveCommittedCheckpoint acquire r.mu internally.
	if r.cancel != nil {
		r.cancel()
	}

	// Wait for any in-flight batch commit to finish before returning so
	// that sink writes / DLQ routing / checkpoint saves already underway
	// are not observed as "context canceled" failures. writeBatch derives
	// its own background commit ctx, so this wait is bounded by that
	// ctx's timeout (30s worst case). The tracker supports concurrent
	// Stop() callers (Stop is documented idempotent), unlike
	// sync.WaitGroup which forbids concurrent Wait.
	r.inflightBatch.wait(35 * time.Second)

	r.mu.Lock()
	r.logInfo(fmt.Sprintf("Pipeline stopped. written=%d read=%d", r.stats.RecordsWritten, r.stats.RecordsRead))
	r.mu.Unlock()
	return nil
}

// Pause temporarily stops reading new records from the source while keeping
// the pipeline's checkpoint and sink connection alive. The in-flight batch
// is allowed to flush. Resume by calling Start() again (which reloads from
// the existing checkpoint). For CDC pipelines sharing a replication slot,
// this is safer than Stop+Start because the slot is only released on full Stop.
func (r *Runner) Pause() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status != StatusRunning {
		return fmt.Errorf("pipeline %s is not running (status=%s)", r.spec.Name, r.status)
	}
	r.freezeDurationLocked()
	r.status = StatusPaused
	if r.cancel != nil {
		r.cancel()
	}
	r.logInfo("Pipeline paused — source reading suspended, checkpoint preserved")
	return nil
}

// Resume restarts reading from the existing checkpoint after a Pause.
// Internally it calls Start() which loads the checkpoint.
func (r *Runner) Resume(ctx context.Context) error {
	return r.Start(ctx)
}

func (r *Runner) setStatus(s Status) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if s != StatusRunning && r.status == StatusRunning {
		r.freezeDurationLocked()
	}
	r.status = s
}

func (r *Runner) runLoop(ctx context.Context, done chan struct{}) {
	defer func() {
		if rec := recover(); rec != nil {
			r.mu.Lock()
			r.stats.LastError = fmt.Sprintf("pipeline panic: %v", rec)
			r.freezeDurationLocked()
			r.mu.Unlock()
			r.setStatus(StatusFailed)
			r.logError(fmt.Sprintf("Pipeline panic: %v", rec))
		}
		if r.reader != nil {
			r.reader.Close()
		}
		r.transforms.CloseChain()
		r.sink.Close()

		// Fire OnShutdown hook before final cleanup.
		fireHook(context.Background(), r.hooks, core.HookOnShutdown, core.HookContext{
			PipelineName: r.spec.Name,
			Config:       r.spec.Hooks.getConfig(core.HookOnShutdown),
		})

		r.mu.Lock()
		if r.status == StatusRunning {
			r.freezeDurationLocked()
			r.status = StatusStopped
		}
		r.mu.Unlock()
		r.logInfo(fmt.Sprintf("Pipeline finished. written=%d read=%d failed=%d", r.recordsWritten(), r.recordsRead(), r.recordsFailed()))
		if r.cancel != nil {
			r.cancel()
		}
		r.mu.Lock()
		if r.done == done {
			r.runActive = false
		}
		r.mu.Unlock()
		close(done)
	}()

	records := make(chan core.Record, r.backpressureBuffer)
	r.recordsCh = records
	var rwg sync.WaitGroup

	rwg.Add(1)
	go func() {
		defer rwg.Done()
		defer close(records)
		defer func() {
			if rec := recover(); rec != nil {
				r.mu.Lock()
				r.stats.LastError = fmt.Sprintf("readLoop panic: %v", rec)
				r.status = StatusFailed
				r.mu.Unlock()
				r.logError(fmt.Sprintf("readLoop panic: %v", rec))
			}
		}()
		r.readLoop(ctx, records)
	}()

	rwg.Add(1)
	go func() {
		defer rwg.Done()
		defer func() {
			if rec := recover(); rec != nil {
				r.mu.Lock()
				r.stats.LastError = fmt.Sprintf("writeLoop panic: %v", rec)
				r.status = StatusFailed
				r.mu.Unlock()
				r.logError(fmt.Sprintf("writeLoop panic: %v", rec))
			}
		}()
		r.writeLoop(ctx, records)
	}()

	// Alert rule checker: evaluate thresholds periodically.
	if r.alertChecker != nil {
		rwg.Add(1)
		go func() {
			defer rwg.Done()
			ticker := time.NewTicker(r.alertChecker.Interval())
			defer ticker.Stop()
			for {
				select {
				case <-ticker.C:
					// Read lastRecordAt under RLock: it is a 3-word time.Time
					// written from readLoop under r.mu (PC-3). An unlocked read
					// is a torn read and a -race failure.
					r.mu.RLock()
					lastRecordAt := r.lastRecordAt
					r.mu.RUnlock()
					r.alertChecker.Check(ctx, r.Stats(), r.metricsHooks.Snapshot(), lastRecordAt)
				case <-ctx.Done():
					return
				}
			}
		}()
	}

	rwg.Wait()
}

func (r *Runner) Wait() {
	<-r.done
}

func (r *Runner) Done() <-chan struct{} { return r.done }

func (r *Runner) readLoop(ctx context.Context, records chan<- core.Record) {
	consecutiveErrors := 0
	for {
		start := time.Now()
		if r.useSourceBatchRead() {
			batch, err := r.reader.ReadBatch(ctx, r.batchSize)
			if r.metricsHooks != nil {
				r.metricsHooks.RecordSourceRead(start)
			}
			if err != nil {
				if r.handleReadError(ctx, err, &consecutiveErrors) {
					return
				}
				continue
			}
			if len(batch) == 0 {
				r.markSourceExhausted()
				return
			}
			consecutiveErrors = 0
			for _, rec := range batch {
				if !r.emitReadRecord(ctx, records, rec) {
					return
				}
			}
			continue
		}

		rec, err := r.reader.Read(ctx)
		if r.metricsHooks != nil {
			r.metricsHooks.RecordSourceRead(start)
		}
		if err != nil {
			if r.handleReadError(ctx, err, &consecutiveErrors) {
				return
			}
			continue
		}
		consecutiveErrors = 0

		if !r.emitReadRecord(ctx, records, rec) {
			return
		}
	}
}

func (r *Runner) useSourceBatchRead() bool {
	// mysql_batch can fetch an indexed page from the database. Calling Read()
	// would force ReadBatch(ctx, 1), turning a configured page into one SQL
	// query per row. Other readers keep the existing single-record behavior
	// until their batch checkpoint semantics are audited.
	return r.spec.Source.Type == "mysql_batch"
}

func (r *Runner) handleReadError(ctx context.Context, err error, consecutiveErrors *int) bool {
	if ctx.Err() != nil {
		return true
	}
	if errors.Is(err, io.EOF) {
		r.markSourceExhausted()
		return true
	}
	(*consecutiveErrors)++
	r.mu.Lock()
	r.stats.LastError = err.Error()
	r.mu.Unlock()

	r.logWarn(fmt.Sprintf("Read error (x%d): %v", *consecutiveErrors, err))

	r.alertManager.Send(ctx, alert.Event{
		Level:   alert.LevelError,
		Title:   "Pipeline read error",
		Message: err.Error(),
		JobName: r.spec.Name,
	})

	backoff := time.Duration(*consecutiveErrors) * time.Second
	if backoff > 30*time.Second {
		backoff = 30 * time.Second
	}
	select {
	case <-time.After(backoff):
		return false
	case <-ctx.Done():
		return true
	}
}

func (r *Runner) markSourceExhausted() {
	r.mu.Lock()
	r.freezeDurationLocked()
	r.status = StatusCompleted
	r.mu.Unlock()
	r.logInfo(fmt.Sprintf("Source exhausted (EOF). written=%d read=%d", r.recordsWritten(), r.recordsRead()))
}

func (r *Runner) emitReadRecord(ctx context.Context, records chan<- core.Record, rec core.Record) bool {
	rec.Metadata.Source = r.spec.Name

	// Apply pipeline-level record processors (table mapping, enrichment, masking, etc.).
	if len(r.processors) > 0 {
		processed, pErr := r.processors.Apply(ctx, rec)
		if pErr != nil {
			r.logWarn(fmt.Sprintf("record processor error: %v", pErr))
		} else {
			rec = processed
		}
	}

	select {
	case records <- rec:
		r.mu.Lock()
		r.lastRecordAt = time.Now()
		r.mu.Unlock()
		read := r.addRecordsRead(1)
		if read%1000 == 0 {
			written := r.recordsWritten()
			r.logDebug(fmt.Sprintf("read=%d written=%d", read, written))
		}
		return true
	case <-ctx.Done():
		return false
	}
}

func (r *Runner) writeLoop(ctx context.Context, records <-chan core.Record) {
	batch := make([]core.Record, 0, r.batchSize)
	ticker := time.NewTicker(r.flushInterval)
	defer ticker.Stop()

	flush := func() {
		if len(batch) == 0 {
			return
		}
		r.writeBatch(ctx, batch)
		batch = batch[:0]
	}

	// forceFlushAndCheckpoint writes the batch and forces a checkpoint save
	// (used on EOF/shutdown to ensure the last position is persisted). It uses
	// a fresh background context because the loop ctx may already be cancelled
	// by Stop() — without this, the final flush would be aborted by ctx.Err()
	// and the in-flight batch would be lost (PC-2). Mirrors the ctx.Done branch.
	forceFlushAndCheckpoint := func() {
		if len(batch) > 0 {
			// Force checkpoint save on final flush.
			r.mu.Lock()
			r.uncheckpointedBatches = 10
			r.mu.Unlock()
			flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			r.writeBatch(flushCtx, batch)
			cancel()
			batch = batch[:0]
		}
		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		r.flushPendingCheckpoint(flushCtx, true)
		cancel()
	}

	for {
		select {
		case rec, ok := <-records:
			if !ok {
				forceFlushAndCheckpoint()
				// Drain any buffered state in transforms (e.g. windows).
				// Use a fresh background ctx for the same reason as the flush
				// above — the loop ctx may be cancelled on Stop (PC-2).
				flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				if flushed, err := r.transforms.FlushChain(flushCtx); err == nil && len(flushed) > 0 {
					r.mu.Lock()
					r.uncheckpointedBatches = 10
					r.mu.Unlock()
					r.writeBatch(flushCtx, flushed)
				}
				cancel()
				return
			}
			batch = append(batch, rec)
			if len(batch) >= r.batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
			r.flushPendingCheckpoint(ctx, false)
		case <-ctx.Done():
			// On context cancellation (stop/pause/config-change), flush the
			// in-flight batch using a fresh background context so the sink
			// write isn't immediately cancelled. This prevents data loss
			// when the batch was already read from the source.
			flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
			if len(batch) > 0 {
				r.mu.Lock()
				r.uncheckpointedBatches = 10
				r.mu.Unlock()
				r.writeBatch(flushCtx, batch)
			}
			r.flushPendingCheckpoint(flushCtx, true)
			flushCancel()
			batch = batch[:0]
			return
		}
	}
}

func (r *Runner) writeBatch(ctx context.Context, batch []core.Record) {
	// A batch commit (sink write + DLQ routing + checkpoint save) must
	// survive Stop(): the loop ctx is cancelled by Stop() the instant the
	// user requests shutdown, but abandoning a half-written batch would
	// surface as spurious "context canceled" errors from sink.Write /
	// dlqWriter.WriteDLQ / checkpointStore.Save and risk data loss. We
	// therefore derive a commit ctx from context.Background() with a
	// bounded timeout. If the caller supplied an explicit deadline (e.g.
	// the 10s shutdown flush path), honor the shorter of the two so that
	// forced shutdowns still time out.
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer commitCancel()
	if dl, ok := ctx.Deadline(); ok {
		if dlSub, subErr := context.WithDeadline(commitCtx, dl); subErr == nil {
			commitCtx, commitCancel = context.WithTimeout(dlSub, time.Until(dl))
			defer commitCancel()
		}
	}

	r.inflightBatch.add()
	defer r.inflightBatch.done()

	// Use commitCtx for everything that must reach a durable store: the
	// transform chain (state checkpoints), the sink write, single-row
	// isolation, DLQ writes, and the checkpoint save. The original ctx
	// remains the source-of-truth for "should we still be running", but
	// by the time we reach writeBatch the batch was already read from the
	// source — its commit is the at-least-once guarantee.
	ctx = commitCtx

	var transformed []core.Record
	processed := make([]core.Record, len(batch))
	copy(processed, batch)
	filteredCount := 0
	transformFailureCount := 0
	batchTransformZeroOutput := false
	checkpointBoundarySafe := true

	hasBatch := r.hasBatchTransform()

	if hasBatch {
		// Batch path: process the entire batch through the chain.
		out, err := r.transforms.ApplyBatch(ctx, batch)
		if err != nil {
			var partial core.PartialTransformError
			if !errors.As(err, &partial) {
				for _, rec := range batch {
					if !r.handleFailedRecord(ctx, rec, err) {
						checkpointBoundarySafe = false
					}
				}
				return
			}
			failures := partial.FailedRecords()
			transformFailureCount = len(failures)
			for _, failure := range failures {
				if !r.handleFailedRecord(ctx, failure.Record, failure.Err) {
					checkpointBoundarySafe = false
				}
			}
		}
		transformed = out
		batchTransformZeroOutput = len(transformed) == 0
	} else {
		transformed, filteredCount, transformFailureCount, checkpointBoundarySafe = r.applyRecordTransforms(ctx, batch)
	}

	if len(transformed) == 0 {
		if !hasBatch && filteredCount == len(processed) && transformFailureCount == 0 {
			r.saveCommittedCheckpoint(ctx, processed, nil)
			return
		}
		reason := fmt.Sprintf("filtered=%d failed=%d", filteredCount, transformFailureCount)
		if batchTransformZeroOutput {
			reason = "batch transform produced no records"
		}
		r.logWarn(fmt.Sprintf("zero-survivor batch not checkpointed (%s); records may replay on restart", reason))
		if r.alertManager != nil {
			r.alertManager.Send(ctx, alert.Event{
				Level:   alert.LevelWarning,
				Title:   "Zero-survivor batch not checkpointed",
				Message: fmt.Sprintf("Pipeline %s produced no sink records for a batch (%s); checkpoint was not advanced to avoid data loss.", r.spec.Name, reason),
				JobName: r.spec.Name,
			})
		}
		return
	}

	// Circuit breaker check: if tripped, wait for cooldown before attempting write.
	if r.circuitBreaker != nil && !r.circuitBreaker.Allow() {
		r.logInfo("Circuit breaker open, waiting for cooldown before sink write...")
		select {
		case <-time.After(time.Duration(r.circuitBreaker.cfg.CooldownSec) * time.Second):
		case <-ctx.Done():
			return
		}
	}

	// Fire OnPreBatch hook before writing to sink.
	fireHook(ctx, r.hooks, core.HookOnPreBatch, core.HookContext{
		PipelineName: r.spec.Name,
		RecordCount:  len(transformed),
		Config:       r.spec.Hooks.getConfig(core.HookOnPreBatch),
	})

	err := retry.Do(ctx, r.retryConfig, core.IsRetryableError, func() error {
		return r.writeSink(ctx, transformed)
	})

	if err != nil {
		if r.circuitBreaker != nil {
			r.circuitBreaker.RecordFailure(ctx, err)
		}
		r.mu.Lock()
		r.stats.LastError = err.Error()
		r.mu.Unlock()

		r.logError(fmt.Sprintf("Write error (batch=%d): %v", len(transformed), err))

		// Prefer sink-provided partial batch details when available. For
		// example, Elasticsearch bulk responses identify failed item indexes,
		// so successful records do not need to be written again during
		// isolation.
		goodCount, failures, usedPartialBatch := r.partialBatchFailures(err, transformed)
		if !usedPartialBatch {
			// Single-row error isolation: retry each record individually so
			// only the genuinely failing records go to DLQ. Good records
			// in the same batch are still written successfully.
			goodCount, failures = r.writeRecordsIndividually(ctx, transformed)
		}
		if goodCount > 0 {
			r.addRecordsWritten(int64(goodCount))
			if usedPartialBatch {
				r.logInfo(fmt.Sprintf("Partial batch error accepted %d/%d records", goodCount, len(transformed)))
			} else {
				r.logInfo(fmt.Sprintf("Single-row isolation recovered %d/%d records", goodCount, len(transformed)))
			}
		}

		for _, f := range failures {
			r.handleFailedRecord(ctx, transformed[f.idx], f.err)
		}
		dlqCount := len(failures)

		r.alertManager.Send(ctx, alert.Event{
			Level:   alert.LevelError,
			Title:   "Pipeline write error",
			Message: fmt.Sprintf("Batch write failed (%d records, %d recovered individually, %d to DLQ): %v", len(transformed), goodCount, dlqCount, err),
			JobName: r.spec.Name,
		})
		return
	}

	written := r.addRecordsWritten(int64(len(transformed)))
	read := r.recordsRead()
	if r.circuitBreaker != nil {
		r.circuitBreaker.RecordSuccess()
	}
	r.logDebug(fmt.Sprintf("Flushed: %d records (total w=%d r=%d)", len(transformed), written, read))

	// Fire OnPostBatch hook after successful sink write.
	fireHook(ctx, r.hooks, core.HookOnPostBatch, core.HookContext{
		PipelineName: r.spec.Name,
		RecordCount:  len(transformed),
		Config:       r.spec.Hooks.getConfig(core.HookOnPostBatch),
	})

	if !checkpointBoundarySafe {
		r.handleCheckpointBoundaryError(ctx, "DLQ persistence failed; checkpoint not advanced so the source range will replay")
		return
	}

	// Throttle checkpoint saves while retaining the latest acknowledged
	// boundary. The write-loop timer persists this pending boundary even if the
	// stream becomes idle, so checkpoint_interval_sec cannot strand a final
	// successful batch indefinitely.
	r.mu.Lock()
	r.uncheckpointedBatches++
	r.pendingCheckpointProcessed = append(r.pendingCheckpointProcessed[:0], processed...)
	r.pendingCheckpointCommitted = append(r.pendingCheckpointCommitted[:0], transformed...)
	shouldSave := time.Since(r.lastCheckpointSave) >= r.checkpointInterval || r.uncheckpointedBatches >= 10
	r.mu.Unlock()
	if shouldSave {
		r.flushPendingCheckpoint(ctx, true)
	}
}

func (r *Runner) flushPendingCheckpoint(ctx context.Context, force bool) bool {
	r.mu.RLock()
	if len(r.pendingCheckpointProcessed) == 0 {
		r.mu.RUnlock()
		return true
	}
	if !force && time.Since(r.lastCheckpointSave) < r.checkpointInterval && r.uncheckpointedBatches < 10 {
		r.mu.RUnlock()
		return true
	}
	processed := append([]core.Record(nil), r.pendingCheckpointProcessed...)
	committed := append([]core.Record(nil), r.pendingCheckpointCommitted...)
	r.mu.RUnlock()

	if !r.saveCommittedCheckpoint(ctx, processed, committed) {
		return false
	}
	r.mu.Lock()
	r.pendingCheckpointProcessed = nil
	r.pendingCheckpointCommitted = nil
	r.mu.Unlock()
	return true
}

func (r *Runner) hasBatchTransform() bool {
	for _, t := range r.transforms {
		if _, ok := t.(core.BatchTransform); ok {
			return true
		}
	}
	return false
}

func (r *Runner) canParallelizeRecordTransforms() bool {
	if r.transformWorkers <= 1 || len(r.transforms) == 0 {
		return false
	}
	for _, t := range r.transforms {
		if _, ok := t.(core.BatchTransform); ok {
			return false
		}
		if _, ok := t.(core.Flusher); ok {
			return false
		}
		if _, ok := t.(core.StateSnapshotter); ok {
			return false
		}
		if _, ok := t.(core.StateMetricsProvider); ok {
			return false
		}
	}
	return true
}

func (r *Runner) applyRecordTransforms(ctx context.Context, batch []core.Record) ([]core.Record, int, int, bool) {
	if r.canParallelizeRecordTransforms() && len(batch) > 1 {
		return r.applyRecordTransformsParallel(ctx, batch)
	}
	return r.applyRecordTransformsSerial(ctx, batch)
}

func (r *Runner) applyRecordTransformsSerial(ctx context.Context, batch []core.Record) ([]core.Record, int, int, bool) {
	transformed := make([]core.Record, 0, len(batch))
	filteredCount := 0
	transformFailureCount := 0
	checkpointBoundarySafe := true
	for _, rec := range batch {
		tRec, err := r.transforms.Apply(ctx, rec)
		if err != nil {
			if errors.Is(err, core.ErrRecordFiltered) {
				filteredCount++
				continue
			}
			transformFailureCount++
			if !r.handleFailedRecord(ctx, rec, err) {
				checkpointBoundarySafe = false
			}
			continue
		}
		transformed = append(transformed, tRec)
	}
	return transformed, filteredCount, transformFailureCount, checkpointBoundarySafe
}

type recordTransformResult struct {
	index    int
	record   core.Record
	err      error
	filtered bool
}

func (r *Runner) applyRecordTransformsParallel(ctx context.Context, batch []core.Record) ([]core.Record, int, int, bool) {
	workers := r.transformWorkers
	if workers > len(batch) {
		workers = len(batch)
	}
	jobs := make(chan int)
	results := make(chan recordTransformResult, len(batch))
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for idx := range jobs {
				rec := batch[idx]
				tRec, err := r.transforms.Apply(ctx, rec)
				result := recordTransformResult{index: idx, record: tRec, err: err}
				if errors.Is(err, core.ErrRecordFiltered) {
					result.filtered = true
				}
				results <- result
			}
		}()
	}
	for i := range batch {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(results)

	ordered := make([]recordTransformResult, len(batch))
	for result := range results {
		ordered[result.index] = result
	}
	transformed := make([]core.Record, 0, len(batch))
	filteredCount := 0
	transformFailureCount := 0
	checkpointBoundarySafe := true
	for i, result := range ordered {
		if result.filtered {
			filteredCount++
			continue
		}
		if result.err != nil {
			transformFailureCount++
			if !r.handleFailedRecord(ctx, batch[i], result.err) {
				checkpointBoundarySafe = false
			}
			continue
		}
		transformed = append(transformed, result.record)
	}
	return transformed, filteredCount, transformFailureCount, checkpointBoundarySafe
}

// writeRecordsIndividually attempts to write each record one at a time after
// a batch write failure. This isolates the failing records and allows the
// good records in the batch to succeed. Returns (successCount, []failedIndexErr).
func (r *Runner) writeRecordsIndividually(ctx context.Context, records []core.Record) (int, []failedIndexErr) {
	good := 0
	var failures []failedIndexErr
	for i, rec := range records {
		singleErr := retry.Do(ctx, r.retryConfig, core.IsRetryableError, func() error {
			return r.writeSink(ctx, []core.Record{rec})
		})
		if singleErr == nil {
			good++
		} else {
			failures = append(failures, failedIndexErr{idx: i, err: singleErr})
		}
	}
	return good, failures
}

func (r *Runner) writeSink(ctx context.Context, records []core.Record) error {
	if r.sinkWriteLimiter == nil {
		return r.sink.Write(ctx, records)
	}
	select {
	case r.sinkWriteLimiter <- struct{}{}:
		defer func() { <-r.sinkWriteLimiter }()
		return r.sink.Write(ctx, records)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (r *Runner) partialBatchFailures(err error, records []core.Record) (int, []failedIndexErr, bool) {
	var partial core.PartialBatchError
	if !errors.As(err, &partial) {
		return 0, nil, false
	}
	indices := partial.FailedRecordIndices()
	if len(indices) == 0 {
		return 0, nil, false
	}

	seen := make(map[int]bool, len(indices))
	failures := make([]failedIndexErr, 0, len(indices))
	for _, idx := range indices {
		if idx < 0 || idx >= len(records) {
			return 0, nil, false
		}
		if seen[idx] {
			continue
		}
		seen[idx] = true
		recordErr := partial.ErrorForRecord(idx)
		if recordErr == nil {
			recordErr = err
		}
		failures = append(failures, failedIndexErr{idx: idx, err: recordErr})
	}
	return len(records) - len(failures), failures, true
}

// failedIndexErr pairs a record index with its per-record write error,
// enabling accurate DLQ error attribution after single-row isolation.
type failedIndexErr struct {
	idx int
	err error
}

func (r *Runner) handleFailedRecord(ctx context.Context, rec core.Record, err error) bool {
	r.addRecordsFailed(1)

	// Fire OnError hook.
	fireHook(ctx, r.hooks, core.HookOnError, core.HookContext{
		PipelineName: r.spec.Name,
		Metadata:     rec.Metadata,
		ErrorMessage: err.Error(),
		Config:       r.spec.Hooks.getConfig(core.HookOnError),
	})

	if r.dlqWriter != nil {
		entry := DLQEntry{
			JobName:    r.spec.Name,
			Record:     rec,
			Error:      err.Error(),
			ErrorClass: string(core.ClassifyError(err)),
		}
		dlqCtx, dlqCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer dlqCancel()
		if dlqErr := r.dlqWriter.WriteDLQ(dlqCtx, entry); dlqErr != nil {
			// DLQ write failed — this is a data-loss risk. Log loudly, alert,
			// and trip the circuit breaker so a persistently-down DLQ backend
			// triggers cooldown rather than losing every record at full tilt.
			if r.circuitBreaker != nil {
				r.circuitBreaker.RecordFailure(ctx, dlqErr)
			}
			r.logError(fmt.Sprintf("DLQ write FAILED for record (table=%s): %v | original error: %v",
				rec.Metadata.Table, dlqErr, err))
			r.alertManager.Send(ctx, alert.Event{
				Level:   alert.LevelError,
				Title:   "DLQ write failure — potential data loss",
				Message: fmt.Sprintf("Pipeline %s: DLQ write failed: %v. Original record error: %v", r.spec.Name, dlqErr, err),
				JobName: r.spec.Name,
			})
			r.blockCheckpoint("DLQ persistence failed: " + dlqErr.Error())
			return false
		} else {
			r.addRecordsDLQ(1)
			return true
		}
	} else {
		// No DLQ writer configured — the record can neither reach the sink nor
		// the DLQ. Surface it loudly (never silent) per the zero-loss rule
		// (§6.1). The server always wires a DLQ writer; this path is for
		// direct/SDK usage where the caller opted out of a DLQ.
		r.logError(fmt.Sprintf("record failed with no DLQ configured (table=%s): %v", rec.Metadata.Table, err))
		r.alertManager.Send(ctx, alert.Event{
			Level:   alert.LevelError,
			Title:   "Record dropped — no DLQ configured",
			Message: fmt.Sprintf("Pipeline %s: record failed (table=%s) with no DLQ writer; record could not be stored. Error: %v", r.spec.Name, rec.Metadata.Table, err),
			JobName: r.spec.Name,
		})
		r.blockCheckpoint("record failure occurred without a configured DLQ writer")
		return false
	}
}

func (r *Runner) blockCheckpoint(reason string) {
	r.mu.Lock()
	r.checkpointBlocked = true
	if r.checkpointBlockReason == "" {
		r.checkpointBlockReason = reason
	}
	r.mu.Unlock()
}

func (r *Runner) saveCommittedCheckpoint(ctx context.Context, processed, committed []core.Record) bool {
	if r.reader == nil || r.checkpointStore == nil || len(processed) == 0 {
		return false
	}
	checkpointer, ok := r.reader.(core.RecordCheckpointer)
	if !ok {
		return false
	}
	r.mu.RLock()
	blocked := r.checkpointBlocked
	blockReason := r.checkpointBlockReason
	r.mu.RUnlock()
	if blocked {
		r.handleCheckpointBoundaryError(ctx, "checkpoint blocked until restart: "+blockReason)
		return false
	}
	cp, err := checkpointer.CheckpointForRecord(ctx, processed[len(processed)-1])
	if err != nil {
		return false
	}
	cp.JobName = r.spec.Name
	cp.ID = fmt.Sprintf("%s-%d", r.spec.Name, time.Now().UnixNano())
	stateVersions, err := r.transforms.StateSnapshotVersions(ctx)
	if err != nil {
		r.handleCheckpointBoundaryError(ctx, fmt.Sprintf("state snapshot version collection failed: %v", err))
		return false
	}
	var sinkCommit map[string]any
	if len(committed) > 0 {
		sinkCommit, err = checkpoint.BuildSinkCommitMetadata(ctx, r.sink, committed, "")
		if err != nil {
			r.handleCheckpointBoundaryError(ctx, fmt.Sprintf("sink commit metadata collection failed: %v", err))
			return false
		}
	}
	if len(stateVersions) > 0 || len(sinkCommit) > 0 {
		if wrapped, wrapErr := checkpoint.BuildEnvelope(cp.Position, stateVersions, sinkCommit); wrapErr != nil {
			r.handleCheckpointBoundaryError(ctx, fmt.Sprintf("checkpoint envelope build failed: %v", wrapErr))
			return false
		} else {
			cp.Position = wrapped
		}
	}

	if saveErr := r.checkpointStore.Save(ctx, cp); saveErr != nil {
		errMsg := fmt.Sprintf("checkpoint save failed: %v", saveErr)
		r.mu.Lock()
		r.stats.LastError = errMsg
		r.mu.Unlock()

		// Checkpoint save failure means already-written data's position
		// is lost. This is a critical condition: trip the circuit breaker
		// so the pipeline pauses before writing more data that cannot be
		// checkpointed, and fire a critical alert.
		if r.circuitBreaker != nil {
			r.circuitBreaker.RecordFailure(ctx, fmt.Errorf("%s: %w", errMsg, saveErr))
		}
		r.alertManager.Send(ctx, alert.Event{
			Level:   alert.LevelError,
			Title:   "Checkpoint save failure — pipeline at risk",
			Message: fmt.Sprintf("[%s] %s. Pipeline may replay data on restart.", r.spec.Name, errMsg),
			JobName: r.spec.Name,
		})
		r.logError(errMsg + " — circuit breaker tripped, pipeline will pause")
		return false
	}

	r.mu.Lock()
	r.stats.LastCheckpoint = time.Now()
	r.lastCheckpointSave = time.Now()
	r.uncheckpointedBatches = 0
	r.mu.Unlock()

	// Fire OnCheckpoint hook.
	fireHook(ctx, r.hooks, core.HookOnCheckpoint, core.HookContext{
		PipelineName:  r.spec.Name,
		CheckpointPos: cp.Position,
		Config:        r.spec.Hooks.getConfig(core.HookOnCheckpoint),
	})
	return true
}

func (r *Runner) handleCheckpointBoundaryError(ctx context.Context, errMsg string) {
	r.mu.Lock()
	r.stats.LastError = errMsg
	r.mu.Unlock()
	if r.circuitBreaker != nil {
		r.circuitBreaker.RecordFailure(ctx, errors.New(errMsg))
	}
	if r.alertManager != nil {
		r.alertManager.Send(ctx, alert.Event{
			Level:   alert.LevelError,
			Title:   "Checkpoint boundary failure — pipeline at risk",
			Message: fmt.Sprintf("[%s] %s. Source checkpoint was not advanced because state could not be bound to it.", r.spec.Name, errMsg),
			JobName: r.spec.Name,
		})
	}
	r.logError(errMsg + " — checkpoint not saved")
}

func unwrapCheckpointForSource(cp *core.Checkpoint) *core.Checkpoint {
	if cp == nil {
		return nil
	}
	env, ok, err := checkpoint.ParseEnvelope(cp.Position)
	if err != nil || !ok {
		return cp
	}
	unwrapped := *cp
	unwrapped.Position = append([]byte(nil), env.Source...)
	return &unwrapped
}
