package orchestrator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/robfig/cron/v3"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// Scheduler manages the lifecycle of pipeline triggers.
// It supports cron, periodic, streaming, once, and dependency-based triggers.
//
// Scheduler is agnostic to the runner implementation: it drives any
// pipeline.RunnerInterface (linear Runner, ParallelRunner, or DAGRunnerWrapper),
// so both linear and DAG specs can be cron/periodic scheduled.
type Scheduler struct {
	store                   storage.Storage
	cronLib                 *cron.Cron
	mu                      sync.Mutex
	schedules               map[string]*pipelineSchedule
	runners                 map[string]pipeline.RunnerInterface
	ctx                     context.Context
	registerFailureInjector func(string, *ScheduleConfig) error
}

type pipelineSchedule struct {
	Name   string
	Config ScheduleConfig
	cronID cron.EntryID
	ticker *time.Ticker
	stopCh chan struct{}
	depCtx *dependencyTrigger
}

type dependencyTrigger struct {
	dependsOn []string
}

// NewScheduler creates a new scheduler. It does not start until Run(ctx) is called.
func NewScheduler(store storage.Storage) *Scheduler {
	return &Scheduler{
		store:     store,
		cronLib:   cron.New(cron.WithParser(scheduleCronParser)),
		schedules: map[string]*pipelineSchedule{},
		runners:   map[string]pipeline.RunnerInterface{},
	}
}

// The UI and API historically expose five-field cron expressions while some
// YAML specs use an explicit leading seconds field. Accept both forms and use
// the same parser for preflight and runtime registration.
var scheduleCronParser = cron.NewParser(
	cron.SecondOptional | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor,
)

// SetContext binds the server lifecycle context before registrations happen.
// RegisterExecutor may start periodic ticker goroutines immediately, so callers
// should set this before registering cron/periodic/dependency schedules.
func (s *Scheduler) SetContext(ctx context.Context) {
	s.mu.Lock()
	s.ctx = ctx
	s.mu.Unlock()
}

// SetRegisterFailureInjector installs a test-only seam for exercising the
// server's scheduler activation compensation. Production callers should leave
// it nil. The hook runs after schedule validation and before any scheduler
// state is changed.
func (s *Scheduler) SetRegisterFailureInjector(fn func(string, *ScheduleConfig) error) {
	s.mu.Lock()
	s.registerFailureInjector = fn
	s.mu.Unlock()
}

func validateAndParseScheduleConfig(scheduleName string, cfg *ScheduleConfig) (cron.Schedule, error) {
	if cfg == nil || cfg.Type == "" || cfg.Type == ScheduleStreaming || cfg.Type == ScheduleOnce {
		return nil, nil
	}
	switch cfg.Type {
	case ScheduleCron:
		if cfg.Cron == "" {
			return nil, fmt.Errorf("pipeline %s: cron schedule requires 'cron' field", scheduleName)
		}
		parsed, err := scheduleCronParser.Parse(cfg.Cron)
		if err != nil {
			return nil, fmt.Errorf("pipeline %s: invalid cron expression %q: %w", scheduleName, cfg.Cron, err)
		}
		return parsed, nil
	case SchedulePeriodic:
		if cfg.IntervalS <= 0 {
			return nil, fmt.Errorf("pipeline %s: periodic schedule requires interval_sec > 0", scheduleName)
		}
	case ScheduleDependency:
		if len(cfg.DependsOn) == 0 {
			return nil, fmt.Errorf("pipeline %s: dependency schedule requires depends_on list", scheduleName)
		}
	default:
		return nil, fmt.Errorf("pipeline %s: unknown schedule type %q", scheduleName, cfg.Type)
	}
	return nil, nil
}

// ValidateScheduleConfig checks every failure that RegisterExecutor can report
// for a deferred schedule without mutating scheduler state. Keeping this
// validation separate lets the control plane reject an unusable schedule
// before committing a pipeline version.
func ValidateScheduleConfig(scheduleName string, cfg *ScheduleConfig) error {
	_, err := validateAndParseScheduleConfig(scheduleName, cfg)
	return err
}

type executorSnapshot struct {
	runner      pipeline.RunnerInterface
	hadRunner   bool
	schedule    *ScheduleConfig
	hadSchedule bool
}

func cloneScheduleConfig(cfg *ScheduleConfig) *ScheduleConfig {
	if cfg == nil {
		return nil
	}
	cp := *cfg
	if cfg.DependsOn != nil {
		cp.DependsOn = append([]string(nil), cfg.DependsOn...)
	}
	return &cp
}

func (s *Scheduler) snapshotExecutorLocked(name string) executorSnapshot {
	snapshot := executorSnapshot{}
	if runner, ok := s.runners[name]; ok {
		snapshot.runner = runner
		snapshot.hadRunner = true
	}
	if ps, ok := s.schedules[name]; ok && ps != nil {
		snapshot.schedule = cloneScheduleConfig(&ps.Config)
		snapshot.hadSchedule = true
	}
	return snapshot
}

// removeExecutorLocked removes schedule bookkeeping without stopping the
// runner. Prepared replacements keep the previous runner available until the
// caller has swapped its in-memory control-plane state and can stop it safely.
func (s *Scheduler) removeExecutorLocked(name string) {
	if ps, ok := s.schedules[name]; ok && ps != nil {
		if ps.ticker != nil {
			close(ps.stopCh)
		}
		if ps.cronID != 0 {
			s.cronLib.Remove(ps.cronID)
		}
		delete(s.schedules, name)
	}
	delete(s.runners, name)
}

func (s *Scheduler) restoreExecutorLocked(name string, snapshot executorSnapshot) error {
	s.removeExecutorLocked(name)
	if !snapshot.hadSchedule || snapshot.schedule == nil {
		if snapshot.hadRunner {
			s.runners[name] = snapshot.runner
		}
		return nil
	}
	parsedCron, err := validateAndParseScheduleConfig(name, snapshot.schedule)
	if err != nil {
		return err
	}
	s.registerDeferredLocked(name, snapshot.runner, snapshot.schedule, parsedCron)
	return nil
}

func (s *Scheduler) registerFailure(name string, cfg *ScheduleConfig) error {
	s.mu.Lock()
	hook := s.registerFailureInjector
	s.mu.Unlock()
	if hook != nil {
		return hook(name, cloneScheduleConfig(cfg))
	}
	return nil
}

// registerDeferredLocked installs a validated cron/periodic/dependency
// schedule. It does not invoke the failure injector and does not stop any old
// runner, which makes it safe for restoring a provisional activation.
func (s *Scheduler) registerDeferredLocked(name string, runner pipeline.RunnerInterface, cfg *ScheduleConfig, parsedCron cron.Schedule) {
	ps := &pipelineSchedule{
		Name:   name,
		Config: *cloneScheduleConfig(cfg),
		stopCh: make(chan struct{}),
	}

	switch cfg.Type {
	case ScheduleCron:
		id := s.cronLib.Schedule(parsedCron, cron.FuncJob(func() {
			s.triggerPipeline(name, runner)
		}))
		ps.cronID = id
		g.Log().Infof(s.ctx, "Scheduled pipeline %s with cron %q (entry %d)", name, cfg.Cron, id)

	case SchedulePeriodic:
		ps.ticker = time.NewTicker(time.Duration(cfg.IntervalS) * time.Second)
		go func() {
			for {
				select {
				case <-ps.ticker.C:
					s.triggerPipeline(name, runner)
				case <-ps.stopCh:
					ps.ticker.Stop()
					return
				}
			}
		}()
		g.Log().Infof(s.ctx, "Scheduled pipeline %s every %s", name, time.Duration(cfg.IntervalS)*time.Second)

	case ScheduleDependency:
		ps.depCtx = &dependencyTrigger{dependsOn: append([]string(nil), cfg.DependsOn...)}
		g.Log().Infof(s.ctx, "Pipeline %s waiting for dependencies: %v", name, cfg.DependsOn)
	}

	s.schedules[name] = ps
	s.runners[name] = runner
}

// ExecutorReplacement is a validated scheduler swap that remains inert until
// Commit. Preparing it before persistence catches configuration/injected
// failures without disabling the old schedule or allowing the candidate runner
// to fire before the durable spec commits.
type ExecutorReplacement struct {
	scheduler  *Scheduler
	name       string
	runner     pipeline.RunnerInterface
	config     *ScheduleConfig
	parsedCron cron.Schedule

	mu        sync.Mutex
	old       executorSnapshot
	committed bool
	canceled  bool
}

// PrepareExecutor validates a deferred executor replacement without mutating
// live scheduler state. Commit is infallible because cron parsing and all
// schedule validation have already completed.
func (s *Scheduler) PrepareExecutor(name string, runner pipeline.RunnerInterface, cfg *ScheduleConfig) (*ExecutorReplacement, error) {
	parsedCron, err := validateAndParseScheduleConfig(name, cfg)
	if err != nil {
		return nil, err
	}
	if err := s.registerFailure(name, cfg); err != nil {
		return nil, err
	}
	return &ExecutorReplacement{
		scheduler:  s,
		name:       name,
		runner:     runner,
		config:     cloneScheduleConfig(cfg),
		parsedCron: parsedCron,
	}, nil
}

// Commit atomically swaps the prepared executor while holding the scheduler
// lock. It returns the previously registered runner, which remains the
// caller's responsibility to stop after the control-plane memory swap.
func (r *ExecutorReplacement) Commit() pipeline.RunnerInterface {
	if r == nil || r.scheduler == nil {
		return nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.canceled || r.committed {
		return r.old.runner
	}

	s := r.scheduler
	s.mu.Lock()
	r.old = s.snapshotExecutorLocked(r.name)
	s.removeExecutorLocked(r.name)
	if isDeferredScheduleConfig(r.config) {
		s.registerDeferredLocked(r.name, r.runner, r.config, r.parsedCron)
	}
	s.mu.Unlock()
	r.committed = true
	return r.old.runner
}

// Rollback cancels an uncommitted replacement or restores the previous
// executor after a committed swap. It is idempotent.
func (r *ExecutorReplacement) Rollback() {
	if r == nil || r.scheduler == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.canceled {
		return
	}
	r.canceled = true
	if !r.committed {
		return
	}

	s := r.scheduler
	s.mu.Lock()
	if err := s.restoreExecutorLocked(r.name, r.old); err != nil {
		g.Log().Errorf(s.ctx, "Restore scheduler executor %s failed: %v", r.name, err)
	}
	s.mu.Unlock()
	r.committed = false
}

func isDeferredScheduleConfig(cfg *ScheduleConfig) bool {
	return cfg != nil && cfg.Type != "" && cfg.Type != ScheduleStreaming && cfg.Type != ScheduleOnce
}

// RegisterExecutor associates a runner with the scheduler.
// The scheduler will start/stop the runner based on its schedule.
//
//   - nil/empty/streaming/once schedule → start immediately (one-shot run).
//   - cron/periodic/dependency schedule → register a trigger; the runner is
//     NOT started now, it will be Start()'d on each tick by triggerPipeline.
//
// scheduleName is the pipeline reference used as the schedule key. Server code
// passes the stable pipeline ID so status updates and run_history entries remain
// queryable via /api/v2/pipelines/{id}/history.
func (s *Scheduler) RegisterExecutor(scheduleName string, runner pipeline.RunnerInterface, cfg *ScheduleConfig) error {
	parsedCron, err := validateAndParseScheduleConfig(scheduleName, cfg)
	if err != nil {
		return err
	}
	if err := s.registerFailure(scheduleName, cfg); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()

	if cfg == nil || cfg.Type == "" || cfg.Type == ScheduleStreaming || cfg.Type == ScheduleOnce {
		// Default: start immediately (streaming or once)
		if err := s.startRunnerLocked(scheduleName, runner); err != nil {
			return err
		}
		s.runners[scheduleName] = runner
		return nil
	}
	s.registerDeferredLocked(scheduleName, runner, cfg, parsedCron)
	return nil
}

// Unregister removes a pipeline's schedule entry (cron entry / ticker / dep ctx)
// and stops any runner currently executing for it. Safe to call on a name that
// was never registered (no-op). Used by spec delete / reload.
func (s *Scheduler) Unregister(scheduleName string) {
	s.mu.Lock()
	ps, ok := s.schedules[scheduleName]
	if ok {
		if ps.ticker != nil {
			close(ps.stopCh)
		}
		if ps.cronID != 0 {
			s.cronLib.Remove(ps.cronID)
		}
		delete(s.schedules, scheduleName)
	}
	runner, hasRunner := s.runners[scheduleName]
	delete(s.runners, scheduleName)
	s.mu.Unlock()

	if hasRunner && runner != nil {
		_ = runner.Stop()
	}
}

// startRunnerLocked starts the runner immediately (streaming/once mode).
func (s *Scheduler) startRunnerLocked(name string, runner pipeline.RunnerInterface) error {
	if err := runner.Start(s.ctx); err != nil {
		return fmt.Errorf("start pipeline %s: %w", name, err)
	}
	g.Log().Infof(s.ctx, "Started pipeline %s (streaming/once)", name)
	return nil
}

// triggerPipeline starts a runner, waits for it to complete, then records the
// run. Used for cron/periodic/dependency triggers. If the runner is already
// running (previous tick hasn't finished), the trigger is skipped — this is
// the at-least-once safety against overlapping batch runs.
func (s *Scheduler) triggerPipeline(name string, runner pipeline.RunnerInterface) {
	if runner == nil {
		g.Log().Warningf(s.ctx, "Pipeline %s has no runner, skipping trigger", name)
		return
	}
	status := runner.Status()
	if status == pipeline.StatusRunning {
		g.Log().Warningf(s.ctx, "Pipeline %s already running, skipping trigger", name)
		return
	}

	g.Log().Infof(s.ctx, "Triggering pipeline %s", name)
	if err := runner.Start(s.ctx); err != nil {
		if errors.Is(err, pipeline.ErrRunnerStopping) {
			g.Log().Warningf(s.ctx, "Pipeline %s is still cleaning up its previous run; skipping trigger", name)
			return
		}
		g.Log().Errorf(s.ctx, "Failed to start pipeline %s: %v", name, err)
		_ = s.store.UpdatePipelineStatus(s.ctx, name, "failed")
		return
	}
	_ = s.store.UpdatePipelineStatus(s.ctx, name, "running")

	// Record run start
	runID, _ := s.store.RecordRunStart(s.ctx, name)

	// Wait for completion in a goroutine
	go func() {
		runner.Wait()
		stats := runner.Stats()
		dur := runner.Duration()
		status := string(runner.Status())
		if status == "" || status == "running" {
			status = "completed"
		}
		_ = s.store.UpdatePipelineStatus(s.ctx, name, status)
		_ = s.store.RecordRunEnd(s.ctx, runID, status, stats.RecordsRead, stats.RecordsWritten, stats.RecordsFailed, stats.RecordsDLQ, dur.Milliseconds())
		g.Log().Infof(s.ctx, "Pipeline %s completed: status=%s read=%d written=%d failed=%d",
			name, status, stats.RecordsRead, stats.RecordsWritten, stats.RecordsFailed)

		// Notify dependent pipelines
		s.notifyDependents(name)
	}()
}

// notifyDependents checks if any pipelines depend on the given name and triggers them.
func (s *Scheduler) notifyDependents(completedName string) {
	s.NotifyDependents(completedName)
}

// NotifyDependents triggers any pipeline whose dependency schedule depends on
// completedName. This is the public hook used by the server when a streaming
// or once-scheduled upstream finishes, so that downstream dependency-scheduled
// pipelines fire.
func (s *Scheduler) NotifyDependents(completedName string) {
	s.mu.Lock()
	for name, ps := range s.schedules {
		if ps.depCtx == nil {
			continue
		}
		for _, dep := range ps.depCtx.dependsOn {
			if dep == completedName {
				runner := s.runners[name]
				s.mu.Unlock()
				g.Log().Infof(s.ctx, "Dependency trigger: %s -> %s", completedName, name)
				s.triggerPipeline(name, runner)
				s.mu.Lock()
				break
			}
		}
	}
	s.mu.Unlock()
}

// Run starts the scheduler's cron engine. Blocks until ctx is cancelled.
// Must be called once after all RegisterExecutor calls; cron entries added
// before Run are still picked up because robfig/cron starts its own goroutine.
func (s *Scheduler) Run(ctx context.Context) {
	s.mu.Lock()
	if s.ctx == nil {
		s.ctx = ctx
	}
	s.mu.Unlock()
	s.cronLib.Start()
	g.Log().Info(ctx, "Scheduler started")
	<-ctx.Done()
	s.cronLib.Stop()
	g.Log().Info(ctx, "Scheduler stopped")
}

// StopAll stops all running pipelines and periodic tickers.
func (s *Scheduler) StopAll() {
	s.mu.Lock()
	runners := make([]pipeline.RunnerInterface, 0, len(s.runners))
	for name, ps := range s.schedules {
		if ps.ticker != nil {
			close(ps.stopCh)
		}
		if ps.cronID != 0 {
			s.cronLib.Remove(ps.cronID)
		}
		if runner, ok := s.runners[name]; ok && runner != nil {
			runners = append(runners, runner)
		}
	}
	s.schedules = map[string]*pipelineSchedule{}
	s.runners = map[string]pipeline.RunnerInterface{}
	s.mu.Unlock()

	for _, runner := range runners {
		_ = runner.Stop()
	}
}
