package orchestrator

import (
	"context"
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
	store     storage.Storage
	cronLib   *cron.Cron
	mu        sync.Mutex
	schedules map[string]*pipelineSchedule
	runners   map[string]pipeline.RunnerInterface
	ctx       context.Context
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
		cronLib:   cron.New(cron.WithSeconds()),
		schedules: map[string]*pipelineSchedule{},
		runners:   map[string]pipeline.RunnerInterface{},
	}
}

// SetContext binds the server lifecycle context before registrations happen.
// RegisterExecutor may start periodic ticker goroutines immediately, so callers
// should set this before registering cron/periodic/dependency schedules.
func (s *Scheduler) SetContext(ctx context.Context) {
	s.mu.Lock()
	s.ctx = ctx
	s.mu.Unlock()
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
	s.mu.Lock()
	defer s.mu.Unlock()

	s.runners[scheduleName] = runner

	if cfg == nil || cfg.Type == "" || cfg.Type == ScheduleStreaming || cfg.Type == ScheduleOnce {
		// Default: start immediately (streaming or once)
		return s.startRunnerLocked(scheduleName, runner)
	}

	ps := &pipelineSchedule{
		Name:   scheduleName,
		Config: *cfg,
		stopCh: make(chan struct{}),
	}

	switch cfg.Type {
	case ScheduleCron:
		if cfg.Cron == "" {
			return fmt.Errorf("pipeline %s: cron schedule requires 'cron' field", scheduleName)
		}
		id, err := s.cronLib.AddFunc(cfg.Cron, func() {
			s.triggerPipeline(scheduleName, runner)
		})
		if err != nil {
			return fmt.Errorf("pipeline %s: invalid cron expression %q: %w", scheduleName, cfg.Cron, err)
		}
		ps.cronID = id
		g.Log().Infof(s.ctx, "Scheduled pipeline %s with cron %q (entry %d)", scheduleName, cfg.Cron, id)

	case SchedulePeriodic:
		interval := time.Duration(cfg.IntervalS) * time.Second
		if interval <= 0 {
			return fmt.Errorf("pipeline %s: periodic schedule requires interval_sec > 0", scheduleName)
		}
		ps.ticker = time.NewTicker(interval)
		go func() {
			for {
				select {
				case <-ps.ticker.C:
					s.triggerPipeline(scheduleName, runner)
				case <-ps.stopCh:
					ps.ticker.Stop()
					return
				}
			}
		}()
		g.Log().Infof(s.ctx, "Scheduled pipeline %s every %s", scheduleName, interval)

	case ScheduleDependency:
		if len(cfg.DependsOn) == 0 {
			return fmt.Errorf("pipeline %s: dependency schedule requires depends_on list", scheduleName)
		}
		ps.depCtx = &dependencyTrigger{dependsOn: cfg.DependsOn}
		g.Log().Infof(s.ctx, "Pipeline %s waiting for dependencies: %v", scheduleName, cfg.DependsOn)

	default:
		return fmt.Errorf("pipeline %s: unknown schedule type %q", scheduleName, cfg.Type)
	}

	s.schedules[scheduleName] = ps
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
