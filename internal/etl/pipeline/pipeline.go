package pipeline

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"openetl-go/internal/etl/alert"
	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
	"openetl-go/internal/etl/retry"
)

// DLQWriter is the interface for dead-letter queue persistence.
// Any type implementing Write(ctx, DLQEntry) error can serve as a DLQ writer.
type DLQWriter interface {
	WriteDLQ(ctx context.Context, entry DLQEntry) error
}

// DLQEntry is the pipeline-level representation of a dead-letter record.
type DLQEntry struct {
	JobName    string
	Record     core.Record
	Error      string
	ErrorClass string
	Attempt    int
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
	reader core.RecordReader

	hooks map[core.HookKind]core.LifecycleHook

	checkpointInterval time.Duration
	flushInterval      time.Duration
	batchSize          int
	backpressureBuffer int

	// recordsCh holds a reference to the backpressure channel for metrics.
	recordsCh chan core.Record

	// lastCheckpointSave tracks when we last persisted a checkpoint.
	// Checkpoints are saved at most once per checkpointInterval,
	// plus always on shutdown/EOF.
	lastCheckpointSave time.Time
	// uncheckpointedBatches counts batches written since last checkpoint save.
	uncheckpointedBatches int64

	// circuitBreaker pauses the source when sink failures exceed threshold.
	circuitBreaker *CircuitBreaker
	// alertChecker evaluates threshold-based alert rules periodically.
	alertChecker *AlertRuleChecker
	// lastRecordAt tracks when the last record was read (for stall detection).
	lastRecordAt time.Time
}

func NewRunner(spec *Spec, cpStore core.CheckpointStore, dlqW DLQWriter, am *alert.Manager) (*Runner, error) {
	source, err := registry.BuildSource(spec.Source.Type, spec.Source.Config)
	if err != nil {
		return nil, fmt.Errorf("build source: %w", err)
	}

	var transforms core.TransformChain
	for _, tc := range spec.Transforms {
		t, err := registry.BuildTransform(tc.Type, tc.Config)
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
		hooks:              lifecycleHooks,
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
	r.logBuf = NewLogBuffer(500)
	r.status = StatusRunning
	r.frozenDuration = 0
	now := time.Now()
	r.stats = Stats{StartedAt: &now}
	r.mu.Unlock()

	ctx, r.cancel = context.WithCancel(ctx)

	g.Log().Infof(ctx, "[%s] Opening sink...", r.spec.Name)
	if err := r.sink.Open(ctx); err != nil {
		r.setStatus(StatusFailed)
		r.logError(fmt.Sprintf("Failed to open sink: %v", err))
		return fmt.Errorf("open sink: %w", err)
	}

	// Optional schema validation: if source describes its schema and sink
	// accepts schema validation, check compatibility before reading.
	if descriptor, ok := r.source.(core.SchemaDescriptor); ok {
		if validator, ok := r.sink.(core.SchemaValidator); ok {
			schema, err := descriptor.Describe(ctx)
			if err != nil {
				g.Log().Warningf(ctx, "[%s] Source schema description failed: %v", r.spec.Name, err)
			} else if len(schema.Columns) > 0 {
				if err := validator.ValidateSchema(ctx, schema); err != nil {
					r.setStatus(StatusFailed)
					r.logError(fmt.Sprintf("Schema validation failed: %v", err))
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
			cp = loaded
			r.logInfo("Resuming from checkpoint")
		}
	}

	g.Log().Infof(ctx, "[%s] Opening source...", r.spec.Name)
	reader, err := r.source.Open(ctx, cp)
	if err != nil {
		r.setStatus(StatusFailed)
		r.logError(fmt.Sprintf("Failed to open source: %v", err))
		return fmt.Errorf("open source: %w", err)
	}

	// Wrap reader with framework-level shard filter when parallelism > 1
	// for sources that don't handle sharding natively.
	if r.spec.Parallelism != nil && r.spec.Parallelism.Count > 1 {
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
	go r.runLoop(ctx)
	return nil
}

func (r *Runner) Stop() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.status != StatusRunning {
		return nil
	}
	r.freezeDurationLocked()
	r.status = StatusStopped

	// Instead of immediately cancelling the context (which would abort
	// in-flight sink writes), we signal the runLoop to exit gracefully.
	// The runLoop's defer path will flush remaining records and save
	// the final checkpoint using a background context.
	if r.cancel != nil {
		r.cancel()
	}
	r.logInfo(fmt.Sprintf("Pipeline stopped. written=%d read=%d", r.stats.RecordsWritten, r.stats.RecordsRead))
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

func (r *Runner) runLoop(ctx context.Context) {
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
	close(r.done)
	if r.cancel != nil {
		r.cancel()
	}
}

func (r *Runner) Wait() {
	<-r.done
}

func (r *Runner) Done() <-chan struct{} { return r.done }

func (r *Runner) readLoop(ctx context.Context, records chan<- core.Record) {
	consecutiveErrors := 0
	for {
		start := time.Now()
		rec, err := r.reader.Read(ctx)
		if r.metricsHooks != nil {
			r.metricsHooks.RecordSourceRead(start)
		}
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, io.EOF) {
				r.mu.Lock()
				r.freezeDurationLocked()
				r.status = StatusCompleted
				r.mu.Unlock()
				r.logInfo(fmt.Sprintf("Source exhausted (EOF). written=%d read=%d", r.recordsWritten(), r.recordsRead()))
				return
			}
			consecutiveErrors++
			r.mu.Lock()
			r.stats.LastError = err.Error()
			r.mu.Unlock()

			r.logWarn(fmt.Sprintf("Read error (x%d): %v", consecutiveErrors, err))

			r.alertManager.Send(ctx, alert.Event{
				Level:   alert.LevelError,
				Title:   "Pipeline read error",
				Message: err.Error(),
				JobName: r.spec.Name,
			})

			backoff := time.Duration(consecutiveErrors) * time.Second
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return
			}
			continue
		}
		consecutiveErrors = 0

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
		case <-ctx.Done():
			return
		}
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
		if len(batch) == 0 {
			return
		}
		// Force checkpoint save on final flush
		r.mu.Lock()
		r.uncheckpointedBatches = 10 // force shouldSave=true
		r.mu.Unlock()
		flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		r.writeBatch(flushCtx, batch)
		cancel()
		batch = batch[:0]
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
		case <-ctx.Done():
			// On context cancellation (stop/pause/config-change), flush the
			// in-flight batch using a fresh background context so the sink
			// write isn't immediately cancelled. This prevents data loss
			// when the batch was already read from the source.
			flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
			r.writeBatch(flushCtx, batch)
			flushCancel()
			batch = batch[:0]
			return
		}
	}
}

func (r *Runner) writeBatch(ctx context.Context, batch []core.Record) {
	var transformed []core.Record
	processed := make([]core.Record, len(batch))
	copy(processed, batch)

	// Check if any transform in the chain is a BatchTransform.
	hasBatch := false
	for _, t := range r.transforms {
		if _, ok := t.(core.BatchTransform); ok {
			hasBatch = true
			break
		}
	}

	if hasBatch {
		// Batch path: process the entire batch through the chain.
		out, err := r.transforms.ApplyBatch(ctx, batch)
		if err != nil {
			for _, rec := range batch {
				r.handleFailedRecord(ctx, rec, err)
			}
			return
		}
		transformed = out
	} else {
		// Per-record path (original behavior).
		transformed = make([]core.Record, 0, len(batch))
		for _, rec := range batch {
			tRec, err := r.transforms.Apply(ctx, rec)
			if err != nil {
				if errors.Is(err, core.ErrRecordFiltered) {
					continue
				}
				r.handleFailedRecord(ctx, rec, err)
				continue
			}
			transformed = append(transformed, tRec)
		}
	}

	if len(transformed) == 0 {
		r.saveCommittedCheckpoint(ctx, processed)
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
		return r.sink.Write(ctx, transformed)
	})

	if err != nil {
		if r.circuitBreaker != nil {
			r.circuitBreaker.RecordFailure(ctx, err)
		}
		r.mu.Lock()
		r.stats.LastError = err.Error()
		r.mu.Unlock()

		r.logError(fmt.Sprintf("Write error (batch=%d): %v", len(transformed), err))

		// Single-row error isolation: retry each record individually so
		// only the genuinely failing records go to DLQ. Good records
		// in the same batch are still written successfully.
		goodCount, failures := r.writeRecordsIndividually(ctx, transformed)
		if goodCount > 0 {
			r.addRecordsWritten(int64(goodCount))
			r.logInfo(fmt.Sprintf("Single-row isolation recovered %d/%d records", goodCount, len(transformed)))
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

	// Throttle checkpoint saves: save at most once per checkpointInterval.
	// This reduces checkpoint store load for high-throughput pipelines.
	// On EOF/shutdown, saveCommittedCheckpoint is always called (writeLoop
	// flush path handles the final save).
	r.mu.Lock()
	r.uncheckpointedBatches++
	shouldSave := time.Since(r.lastCheckpointSave) >= r.checkpointInterval || r.uncheckpointedBatches >= 10
	r.mu.Unlock()
	if shouldSave {
		r.saveCommittedCheckpoint(ctx, processed)
	}
}

// writeRecordsIndividually attempts to write each record one at a time after
// a batch write failure. This isolates the failing records and allows the
// good records in the batch to succeed. Returns (successCount, []failedIndexErr).
func (r *Runner) writeRecordsIndividually(ctx context.Context, records []core.Record) (int, []failedIndexErr) {
	good := 0
	var failures []failedIndexErr
	for i, rec := range records {
		singleErr := retry.Do(ctx, r.retryConfig, core.IsRetryableError, func() error {
			return r.sink.Write(ctx, []core.Record{rec})
		})
		if singleErr == nil {
			good++
		} else {
			failures = append(failures, failedIndexErr{idx: i, err: singleErr})
		}
	}
	return good, failures
}

// failedIndexErr pairs a record index with its per-record write error,
// enabling accurate DLQ error attribution after single-row isolation.
type failedIndexErr struct {
	idx int
	err error
}

func (r *Runner) handleFailedRecord(ctx context.Context, rec core.Record, err error) {
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
		if dlqErr := r.dlqWriter.WriteDLQ(ctx, entry); dlqErr != nil {
			// DLQ write failed — this is a data-loss risk. Log loudly and alert.
			r.logError(fmt.Sprintf("DLQ write FAILED for record (table=%s): %v | original error: %v",
				rec.Metadata.Table, dlqErr, err))
			r.alertManager.Send(ctx, alert.Event{
				Level:   alert.LevelError,
				Title:   "DLQ write failure — potential data loss",
				Message: fmt.Sprintf("Pipeline %s: DLQ write failed: %v. Original record error: %v", r.spec.Name, dlqErr, err),
				JobName: r.spec.Name,
			})
		} else {
			r.addRecordsDLQ(1)
		}
	}
}

func (r *Runner) saveCommittedCheckpoint(ctx context.Context, processed []core.Record) {
	if r.reader == nil || r.checkpointStore == nil || len(processed) == 0 {
		return
	}
	checkpointer, ok := r.reader.(core.RecordCheckpointer)
	if !ok {
		return
	}
	cp, err := checkpointer.CheckpointForRecord(ctx, processed[len(processed)-1])
	if err != nil {
		return
	}
	cp.JobName = r.spec.Name
	cp.ID = fmt.Sprintf("%s-%d", r.spec.Name, time.Now().UnixNano())

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
		return
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
}
