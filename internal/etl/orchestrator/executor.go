package orchestrator

import (
	"context"
	"fmt"
	"regexp"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"openetl-go/internal/etl/alert"
	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/pipeline"
	"openetl-go/internal/etl/registry"
	"openetl-go/internal/etl/retry"
	"openetl-go/internal/etl/storage"
)

// ExecutionConfig controls how a DAG pipeline executes.
type ExecutionConfig struct {
	Workers          int `yaml:"workers,omitempty" json:"workers,omitempty"`
	BatchSize        int `yaml:"batch_size,omitempty" json:"batch_size,omitempty"`
	BackpressureBuf  int `yaml:"backpressure_buffer,omitempty" json:"backpressure_buffer,omitempty"`
	CheckpointEveryS int `yaml:"checkpoint_interval_sec,omitempty" json:"checkpoint_interval_sec,omitempty"`
}

// RetryConfig mirrors pipeline.RetrySpec but kept here to avoid cross-package coupling.
type RetryConfig struct {
	MaxAttempts       int `yaml:"max_attempts" json:"max_attempts"`
	InitialIntervalMs int `yaml:"initial_interval_ms" json:"initial_interval_ms"`
	MaxIntervalMs     int `yaml:"max_interval_ms" json:"max_interval_ms"`
}

// ScheduleType classifies trigger modes.
type ScheduleType string

const (
	ScheduleStreaming  ScheduleType = "streaming"
	ScheduleCron       ScheduleType = "cron"
	SchedulePeriodic   ScheduleType = "periodic"
	ScheduleOnce       ScheduleType = "once"
	ScheduleDependency ScheduleType = "dependency"
)

// ScheduleConfig defines when a pipeline runs.
type ScheduleConfig struct {
	Type      ScheduleType `yaml:"type" json:"type"`
	Cron      string       `yaml:"cron,omitempty" json:"cron,omitempty"`
	IntervalS int          `yaml:"interval_sec,omitempty" json:"interval_sec,omitempty"`
	DependsOn []string     `yaml:"depends_on,omitempty" json:"depends_on,omitempty"`
}

// PipelineSpec is the full DAG-based pipeline definition.
type PipelineSpec struct {
	Name           string                   `yaml:"name" json:"name"`
	DAG            DAG                      `yaml:"dag" json:"dag"`
	Schedule       *ScheduleConfig          `yaml:"schedule,omitempty" json:"schedule,omitempty"`
	Execution      *ExecutionConfig         `yaml:"execution,omitempty" json:"execution,omitempty"`
	Retry          *RetryConfig             `yaml:"retry,omitempty" json:"retry,omitempty"`
	DLQ            *DLQConfig               `yaml:"dlq,omitempty" json:"dlq,omitempty"`
	Tags           []string                 `yaml:"tags,omitempty" json:"tags,omitempty"`
	Hooks          *pipeline.HooksSpec      `yaml:"hooks,omitempty" json:"hooks,omitempty"`
	RestartPolicy  *pipeline.RestartPolicy  `yaml:"restart_policy,omitempty" json:"restart_policy,omitempty"`
	WorkerSelector *pipeline.WorkerSelector `yaml:"worker_selector,omitempty" json:"worker_selector,omitempty"`
}

// DLQConfig enables dead-letter queuing.
type DLQConfig struct {
	Enable bool `yaml:"enable" json:"enable"`
}

// DAGExecutor executes a DAG-based pipeline.
type DAGExecutor struct {
	spec         *PipelineSpec
	sources      map[string]core.Source
	readers      map[string]core.RecordReader
	transforms   map[string]core.Transform
	sinks        map[string]core.Sink
	cpAdapter    *storage.CheckpointStoreAdapter
	dlqWriter    *storage.DLQCompatWriter
	alertMgr     *alert.Manager
	retryConfig  retry.Config
	batchSize    int
	backpressure int

	// Per-sink circuit breakers to prevent cascading failures
	breakers map[string]*pipeline.CircuitBreaker

	status         string
	mu             sync.RWMutex
	cancel         context.CancelFunc
	done           chan struct{}
	stats          ExecutorStats
	frozenDuration time.Duration
}

// ExecutorStats tracks per-pipeline execution metrics.
type ExecutorStats struct {
	RecordsRead    int64      `json:"records_read"`
	RecordsWritten int64      `json:"records_written"`
	RecordsFailed  int64      `json:"records_failed"`
	RecordsDLQ     int64      `json:"records_dlq"`
	StartedAt      *time.Time `json:"started_at,omitempty"`
}

// NewDAGExecutor builds all plugins from the spec and returns an executor.
func NewDAGExecutor(spec *PipelineSpec, cpStore *storage.CheckpointStoreAdapter, dlqW *storage.DLQCompatWriter, am *alert.Manager) (*DAGExecutor, error) {
	if err := spec.DAG.Validate(); err != nil {
		return nil, fmt.Errorf("validate dag: %w", err)
	}

	exec := &DAGExecutor{
		spec:       spec,
		sources:    map[string]core.Source{},
		readers:    map[string]core.RecordReader{},
		transforms: map[string]core.Transform{},
		sinks:      map[string]core.Sink{},
		breakers:   map[string]*pipeline.CircuitBreaker{},
		cpAdapter:  cpStore,
		dlqWriter:  dlqW,
		alertMgr:   am,
		status:     "stopped",
		done:       make(chan struct{}),
	}

	// Build plugins from registry
	for _, node := range spec.DAG.Nodes {
		var err error
		switch node.Kind {
		case KindSource:
			exec.sources[node.ID], err = registry.BuildSource(node.Plugin, node.Config)
			if err != nil {
				return nil, fmt.Errorf("build source %s (%s): %w", node.ID, node.Plugin, err)
			}
		case KindTransform:
			exec.transforms[node.ID], err = registry.BuildTransform(node.Plugin, node.Config)
			if err != nil {
				return nil, fmt.Errorf("build transform %s (%s): %w", node.ID, node.Plugin, err)
			}
		case KindSink:
			exec.sinks[node.ID], err = registry.BuildSink(node.Plugin, node.Config)
			if err != nil {
				return nil, fmt.Errorf("build sink %s (%s): %w", node.ID, node.Plugin, err)
			}
			// Create a circuit breaker for this sink
			exec.breakers[node.ID] = pipeline.NewCircuitBreaker(pipeline.CircuitBreakerCfg{}, am, spec.Name)
		}
	}

	// Apply defaults
	exec.batchSize = 1000
	exec.backpressure = 100
	if spec.Execution != nil {
		if spec.Execution.BatchSize > 0 {
			exec.batchSize = spec.Execution.BatchSize
		}
		if spec.Execution.BackpressureBuf > 0 {
			exec.backpressure = spec.Execution.BackpressureBuf
		}
	}

	exec.retryConfig = retry.DefaultConfig()
	if spec.Retry != nil {
		exec.retryConfig.MaxAttempts = spec.Retry.MaxAttempts
		exec.retryConfig.InitialInterval = time.Duration(spec.Retry.InitialIntervalMs) * time.Millisecond
		exec.retryConfig.MaxInterval = time.Duration(spec.Retry.MaxIntervalMs) * time.Millisecond
	}

	return exec, nil
}

func (e *DAGExecutor) Status() string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return e.status
}

func (e *DAGExecutor) Duration() time.Duration {
	e.mu.RLock()
	defer e.mu.RUnlock()
	if e.stats.StartedAt != nil {
		return (e.frozenDuration + time.Since(*e.stats.StartedAt)).Truncate(time.Second)
	}
	return e.frozenDuration.Truncate(time.Second)
}

func (e *DAGExecutor) Stats() ExecutorStats {
	e.mu.RLock()
	started := e.stats.StartedAt
	e.mu.RUnlock()
	return ExecutorStats{
		RecordsRead:    atomic.LoadInt64(&e.stats.RecordsRead),
		RecordsWritten: atomic.LoadInt64(&e.stats.RecordsWritten),
		RecordsFailed:  atomic.LoadInt64(&e.stats.RecordsFailed),
		RecordsDLQ:     atomic.LoadInt64(&e.stats.RecordsDLQ),
		StartedAt:      started,
	}
}

// Start launches the DAG pipeline. For streaming sources it runs until stopped.
// For batch sources it runs to completion.
func (e *DAGExecutor) Start(ctx context.Context) error {
	e.mu.Lock()
	if e.status == "running" {
		e.mu.Unlock()
		return fmt.Errorf("pipeline %s is already running", e.spec.Name)
	}
	e.status = "running"
	e.frozenDuration = 0
	now := time.Now()
	e.stats = ExecutorStats{StartedAt: &now}
	e.mu.Unlock()

	ctx, e.cancel = context.WithCancel(ctx)

	// Open all sinks
	for id, sink := range e.sinks {
		if err := sink.Open(ctx); err != nil {
			e.setStatus("failed")
			return fmt.Errorf("open sink %s: %w", id, err)
		}
	}

	go e.runDAG(ctx)
	return nil
}

func (e *DAGExecutor) Stop() error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.status != "running" {
		return nil
	}
	if e.stats.StartedAt != nil {
		e.frozenDuration += time.Since(*e.stats.StartedAt)
		e.stats.StartedAt = nil
	}
	e.status = "stopped"
	if e.cancel != nil {
		e.cancel()
	}
	return nil
}

func (e *DAGExecutor) Wait() {
	<-e.done
}

func (e *DAGExecutor) setStatus(s string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.status = s
}

// runDAG is the main execution loop. It reads from all sources concurrently,
// routes records through transforms based on edge conditions, and writes to sinks.
func (e *DAGExecutor) runDAG(ctx context.Context) {
	defer close(e.done)
	defer func() {
		if rec := recover(); rec != nil {
			g.Log().Errorf(ctx, "DAG pipeline %s panic: %v", e.spec.Name, rec)
			e.setStatus("failed")
		}
		e.mu.Lock()
		if e.stats.StartedAt != nil {
			e.frozenDuration += time.Since(*e.stats.StartedAt)
			e.stats.StartedAt = nil
		}
		if e.status == "running" {
			e.status = "stopped"
		}
		e.mu.Unlock()
		for _, sink := range e.sinks {
			sink.Close()
		}
	}()

	records := make(chan recordMsg, e.backpressure)
	var wg sync.WaitGroup

	// Launch source readers
	for sourceID, src := range e.sources {
		sourceID := sourceID
		src := src
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if rec := recover(); rec != nil {
					g.Log().Errorf(ctx, "source %s panic: %v", sourceID, rec)
				}
			}()
			e.readSource(ctx, src, sourceID, records)
		}()
	}

	// Launch the router/writer goroutine
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		defer func() {
			if rec := recover(); rec != nil {
				g.Log().Errorf(ctx, "DAG pipeline %s router/writer panic: %v\n%s", e.spec.Name, rec, debug.Stack())
				e.alertMgr.Send(ctx, alert.Event{
					Level:   alert.LevelError,
					Title:   "DAG router/writer panic",
					Message: fmt.Sprintf("pipeline %s: %v", e.spec.Name, rec),
					JobName: e.spec.Name,
				})
				e.setStatus("failed")
			}
		}()
		e.routeAndWrite(ctx, records)
	}()

	// Wait for all sources to finish reading, then close the channel
	go func() {
		wg.Wait()
		close(records)
	}()

	<-writerDone
}

// readSource continuously reads from a source and sends records to the channel.
func (e *DAGExecutor) readSource(ctx context.Context, src core.Source, sourceID string, records chan<- recordMsg) {
	cp, _ := e.cpAdapter.Load(ctx, e.spec.Name+"-"+sourceID)
	reader, err := src.Open(ctx, cp)
	if err != nil {
		g.Log().Errorf(ctx, "open source %s: %v", sourceID, err)
		e.alertMgr.Send(ctx, alert.Event{
			Level:   alert.LevelError,
			Title:   "Source open failed",
			Message: err.Error(),
			JobName: e.spec.Name,
		})
		return
	}
	defer reader.Close()

	// Wrap with sharded reader if DAG source node has sharding config.
	// Skip sources that handle sharding natively (SQL-level, consumer-group, page-modulo).
	if node := e.spec.DAG.GetNode(sourceID); node != nil {
		switch node.Plugin {
		case "mysql_batch", "mysql_cdc", "mysql_snapshot_cdc", "postgres_cdc", "kafka", "http":
			// These sources handle sharding natively — skip decorator.
		default:
			sc := core.ReadShardConfig(node.Config)
			reader = core.NewShardedReader(reader, sc)
		}
	}

	e.mu.Lock()
	e.readers[sourceID] = reader
	e.mu.Unlock()
	defer func() {
		e.mu.Lock()
		delete(e.readers, sourceID)
		e.mu.Unlock()
	}()

	consecutiveErrors := 0
	for {
		rec, err := reader.Read(ctx)
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			if isEOF(err) {
				return
			}
			consecutiveErrors++
			g.Log().Warningf(ctx, "source %s read error: %v", sourceID, err)
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
		rec.Metadata.Source = e.spec.Name
		atomic.AddInt64(&e.stats.RecordsRead, 1)

		select {
		case records <- recordMsg{rec: rec, sourceID: sourceID}:
		case <-ctx.Done():
			return
		}
	}
}

// routeAndWrite processes records from sources, routes them through the DAG
// edges (applying transforms and conditions), and writes to sinks.
func (e *DAGExecutor) routeAndWrite(ctx context.Context, records <-chan recordMsg) {
	batchBySink := map[string][]core.Record{}
	lastRecBySource := map[string]core.Record{}
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	flushAll := func() {
		// Terminal/best-effort flush: use a fresh background context so a
		// cancelled pipeline ctx (Stop/SIGTERM) does not abort the in-flight
		// write and lose the batch. Mirrors the linear Runner's ctx.Done flush
		// branch (pipeline.go). The enclosing ctx may already be cancelled
		// when this runs on the ctx.Done / EOF paths.
		flushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		for sinkID, batch := range batchBySink {
			if len(batch) > 0 {
				e.writeToSink(flushCtx, sinkID, batch, lastRecBySource)
				batchBySink[sinkID] = batch[:0]
			}
		}
		lastRecBySource = map[string]core.Record{}
		cancel()
	}

	for {
		select {
		case msg, ok := <-records:
			if !ok {
				flushAll()
				return
			}
			lastRecBySource[msg.sourceID] = msg.rec
			e.route(ctx, msg.sourceID, msg.rec, batchBySink)

			for sinkID, batch := range batchBySink {
				if len(batch) >= e.batchSize {
					e.writeToSink(ctx, sinkID, batch, lastRecBySource)
					batchBySink[sinkID] = batch[:0]
					lastRecBySource = map[string]core.Record{}
				}
			}

		case <-ticker.C:
			flushAll()

		case <-ctx.Done():
			flushAll()
			return
		}
	}
}

// route traverses the DAG from a given node, applying transforms and routing
// records to sinks based on edge conditions. Records are accumulated in batchBySink.
func (e *DAGExecutor) route(ctx context.Context, nodeID string, rec core.Record, batchBySink map[string][]core.Record) {
	node := e.spec.DAG.GetNode(nodeID)
	if node == nil {
		return
	}

	// Apply transform if this is a transform node
	if node.Kind == KindTransform {
		if t, ok := e.transforms[nodeID]; ok {
			transformed, err := applyTransformSafely(ctx, t, rec)
			if err != nil {
				if err == core.ErrRecordFiltered {
					return
				}
				e.handleFailed(ctx, rec, err)
				return
			}
			rec = transformed
		}
	}

	// If this is a sink, add to batch
	if node.Kind == KindSink {
		batchBySink[nodeID] = append(batchBySink[nodeID], rec)
		return
	}

	// Route to downstream nodes based on edge conditions
	for _, edge := range e.spec.DAG.Edges {
		if edge.From != nodeID {
			continue
		}
		if edge.Condition != nil && !evalCondition(*edge.Condition, rec) {
			continue
		}
		// Clone the record for each downstream path to avoid shared mutation
		clone := cloneRecord(rec)
		e.route(ctx, edge.To, clone, batchBySink)
	}
}

// writeToSink writes a batch of records to a sink with retry.
// After a successful write, saves a checkpoint for each source that contributed
// records to this batch.
func (e *DAGExecutor) writeToSink(ctx context.Context, sinkID string, batch []core.Record, lastRecBySource map[string]core.Record) {
	if len(batch) == 0 {
		return
	}
	sink, ok := e.sinks[sinkID]
	if !ok {
		return
	}

	// Circuit breaker check
	breaker := e.breakers[sinkID]
	if breaker != nil && !breaker.Allow() {
		g.Log().Warningf(ctx, "Circuit breaker open for sink %s, waiting cooldown...", sinkID)
		select {
		case <-ctx.Done():
			return
		default:
			// Brief wait before retrying; the breaker will transition to half-open after cooldown
			time.Sleep(5 * time.Second)
		}
	}

	err := retry.Do(ctx, e.retryConfig, core.IsRetryableError, func() error {
		return sink.Write(ctx, batch)
	})
	if err != nil {
		if breaker != nil {
			breaker.RecordFailure(ctx, err)
		}
		g.Log().Errorf(ctx, "sink %s write error: %v", sinkID, err)
		for _, rec := range batch {
			e.handleFailed(ctx, rec, err)
		}
		e.alertMgr.Send(ctx, alert.Event{
			Level:   alert.LevelError,
			Title:   "Sink write error",
			Message: fmt.Sprintf("sink %s: %v", sinkID, err),
			JobName: e.spec.Name,
		})
		return
	}
	if breaker != nil {
		breaker.RecordSuccess()
	}
	atomic.AddInt64(&e.stats.RecordsWritten, int64(len(batch)))

	// Save checkpoints for sources that contributed to this batch.
	if e.cpAdapter != nil {
		for sourceID, lastRec := range lastRecBySource {
			cp, err := e.checkpointForRecord(ctx, sourceID, lastRec)
			if err != nil {
				g.Log().Warningf(ctx, "checkpoint source %s: %v", sourceID, err)
				continue
			}
			saveKey := e.spec.Name + "-" + sourceID
			cp.JobName = saveKey
			cp.ID = fmt.Sprintf("%s-%d", saveKey, time.Now().UnixNano())
			if saveErr := e.cpAdapter.Save(ctx, cp); saveErr != nil {
				// Checkpoint save failed — records already written to the sink
				// will be re-delivered on restart (at-least-once). Trip the
				// breaker and alert so the failure is not silent. Mirror the
				// linear Runner (pipeline.go:925-946).
				g.Log().Errorf(ctx, "DAG pipeline %s: checkpoint save failed for source %s: %v (already-written records will replay on restart)", e.spec.Name, sourceID, saveErr)
				if breaker != nil {
					breaker.RecordFailure(ctx, saveErr)
				}
				e.alertMgr.Send(ctx, alert.Event{
					Level:   alert.LevelError,
					Title:   "DAG checkpoint save failure",
					Message: fmt.Sprintf("pipeline %s source %s: %v", e.spec.Name, sourceID, saveErr),
					JobName: e.spec.Name,
				})
			}
		}
	}
}

// checkpointForRecord generates a checkpoint from the source's reader based on the last committed record.
func (e *DAGExecutor) checkpointForRecord(ctx context.Context, sourceID string, rec core.Record) (core.Checkpoint, error) {
	e.mu.RLock()
	reader := e.readers[sourceID]
	e.mu.RUnlock()
	if reader == nil {
		return core.Checkpoint{}, fmt.Errorf("no reader for source %s", sourceID)
	}
	if checkpointer, ok := reader.(core.RecordCheckpointer); ok {
		return checkpointer.CheckpointForRecord(ctx, rec)
	}
	return reader.Snapshot(ctx)
}

func (e *DAGExecutor) handleFailed(ctx context.Context, rec core.Record, err error) {
	atomic.AddInt64(&e.stats.RecordsFailed, 1)
	if e.dlqWriter == nil {
		// No DLQ configured — never drop silently. Surface at error level so a
		// record that can reach neither sink nor DLQ is visible to operators
		// (§6.1). The server always wires a DLQ writer; this is the direct/SDK
		// path.
		g.Log().Errorf(ctx, "DAG pipeline %s: record failed with no DLQ configured (source=%s): %v", e.spec.Name, rec.Metadata.Source, err)
		e.alertMgr.Send(ctx, alert.Event{
			Level:   alert.LevelError,
			Title:   "DAG record dropped — no DLQ configured",
			Message: fmt.Sprintf("pipeline %s: record failed (source=%s) with no DLQ writer: %v", e.spec.Name, rec.Metadata.Source, err),
			JobName: e.spec.Name,
		})
		return
	}
	entry := pipeline.DLQEntry{
		JobName:    e.spec.Name,
		Record:     rec,
		Error:      err.Error(),
		ErrorClass: string(core.ClassifyError(err)),
	}
	if dlqErr := e.dlqWriter.WriteDLQ(ctx, entry); dlqErr != nil {
		// DLQ write itself failed — this is a potential data-loss event.
		// Do NOT increment RecordsDLQ (the record was not durably captured),
		// and escalate loudly so operators notice. Mirror the linear Runner
		// (pipeline.go) which alerts on DLQ-write failure.
		g.Log().Errorf(ctx, "DAG pipeline %s: DLQ write failed (potential data loss): source=%s original_err=%v dlq_err=%v", e.spec.Name, rec.Metadata.Source, err, dlqErr)
		e.alertMgr.Send(ctx, alert.Event{
			Level:   alert.LevelError,
			Title:   "DAG DLQ write failure — potential data loss",
			Message: fmt.Sprintf("pipeline %s: failed to write record to DLQ (%v); original error: %v", e.spec.Name, dlqErr, err),
			JobName: e.spec.Name,
		})
		return
	}
	atomic.AddInt64(&e.stats.RecordsDLQ, 1)
}

// applyTransformSafely invokes a transform with panic recovery so a single
// malformed record or buggy plugin cannot crash the whole pipeline (TF-10). A
// panic is converted to an error and routed to handleFailed (→ DLQ), preserving
// the zero-loss rule while isolating the poison record.
func applyTransformSafely(ctx context.Context, t core.Transform, rec core.Record) (out core.Record, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("transform panic: %v", r)
			g.Log().Errorf(ctx, "DAG: transform panic recovered (record routed to DLQ): %v", r)
			out = core.Record{}
		}
	}()
	return t.Apply(ctx, rec)
}

// ── Helpers ──────────────────────────────────────────────────────────

// recordMsg carries a record along with the ID of its source node.
type recordMsg struct {
	rec      core.Record
	sourceID string
}

func isEOF(err error) bool {
	if err == nil {
		return false
	}
	if err.Error() == "EOF" || err.Error() == "io: EOF" {
		return true
	}
	return false
}

func cloneRecord(rec core.Record) core.Record {
	clone := rec
	if rec.Data != nil {
		clone.Data = make(map[string]any, len(rec.Data))
		for k, v := range rec.Data {
			clone.Data[k] = v
		}
	}
	return clone
}

func evalCondition(cond Condition, rec core.Record) bool {
	val, ok := rec.Data[cond.Field]
	if !ok {
		return false
	}
	switch cond.Operator {
	case OpEq:
		return fmt.Sprint(val) == fmt.Sprint(cond.Value)
	case OpNe:
		return fmt.Sprint(val) != fmt.Sprint(cond.Value)
	case OpContains:
		return contains(val, cond.Value)
	case OpGt:
		return compareNumeric(val, cond.Value) > 0
	case OpLt:
		return compareNumeric(val, cond.Value) < 0
	case OpGe:
		return compareNumeric(val, cond.Value) >= 0
	case OpLe:
		return compareNumeric(val, cond.Value) <= 0
	case OpRegex:
		return matchRegex(val, cond.Value)
	default:
		g.Log().Warningf(context.Background(), "DAG: unknown condition operator %q for field %s", cond.Operator, cond.Field)
		return false
	}
}

// compareNumeric compares two values numerically when both are convertible to float64.
// Falls back to string comparison when either value is non-numeric.
func compareNumeric(a, b interface{}) int {
	af, aok := toFloat64(a)
	bf, bok := toFloat64(b)
	if aok && bok {
		switch {
		case af < bf:
			return -1
		case af > bf:
			return 1
		default:
			return 0
		}
	}
	// String fallback
	as := fmt.Sprint(a)
	bs := fmt.Sprint(b)
	switch {
	case as < bs:
		return -1
	case as > bs:
		return 1
	default:
		return 0
	}
}

// toFloat64 attempts to convert a value to float64.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int8:
		return float64(n), true
	case int16:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	case uint:
		return float64(n), true
	case uint8:
		return float64(n), true
	case uint16:
		return float64(n), true
	case uint32:
		return float64(n), true
	case uint64:
		return float64(n), true
	default:
		return 0, false
	}
}

// matchRegex tests whether the string representation of val matches the regex pattern.
func matchRegex(val, pattern interface{}) bool {
	s := fmt.Sprint(val)
	p := fmt.Sprint(pattern)
	matched, err := regexp.MatchString(p, s)
	if err != nil {
		g.Log().Warningf(context.Background(), "DAG: invalid regex pattern %q: %v", p, err)
		return false
	}
	return matched
}

func contains(haystack, needle interface{}) bool {
	h := fmt.Sprint(haystack)
	n := fmt.Sprint(needle)
	return len(h) >= len(n) && (h[:len(n)] == n || containsStr(h, n))
}

func containsStr(h, n string) bool {
	for i := 0; i <= len(h)-len(n); i++ {
		if h[i:i+len(n)] == n {
			return true
		}
	}
	return false
}
