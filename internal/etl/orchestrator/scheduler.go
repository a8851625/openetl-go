package orchestrator

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"
	"github.com/robfig/cron/v3"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// Scheduler manages the lifecycle of pipeline triggers.
// It supports cron, periodic, streaming, once, and dependency-based triggers.
type Scheduler struct {
	store     storage.Storage
	cronLib   *cron.Cron
	mu        sync.Mutex
	schedules map[string]*pipelineSchedule
	executors map[string]*DAGExecutor
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
		executors: map[string]*DAGExecutor{},
	}
}

// RegisterExecutor associates a DAG executor with the scheduler.
// The scheduler will start/stop the executor based on its schedule.
func (s *Scheduler) RegisterExecutor(name string, exec *DAGExecutor, cfg *ScheduleConfig) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.executors[name] = exec

	if cfg == nil || cfg.Type == "" || cfg.Type == ScheduleStreaming || cfg.Type == ScheduleOnce {
		// Default: start immediately (streaming or once)
		return s.startExecutorLocked(name, exec)
	}

	ps := &pipelineSchedule{
		Name:   name,
		Config: *cfg,
		stopCh: make(chan struct{}),
	}

	switch cfg.Type {
	case ScheduleCron:
		if cfg.Cron == "" {
			return fmt.Errorf("pipeline %s: cron schedule requires 'cron' field", name)
		}
		id, err := s.cronLib.AddFunc(cfg.Cron, func() {
			s.triggerPipeline(name, exec)
		})
		if err != nil {
			return fmt.Errorf("pipeline %s: invalid cron expression %q: %w", name, cfg.Cron, err)
		}
		ps.cronID = id
		g.Log().Infof(s.ctx, "Scheduled pipeline %s with cron %q (entry %d)", name, cfg.Cron, id)

	case SchedulePeriodic:
		interval := time.Duration(cfg.IntervalS) * time.Second
		if interval <= 0 {
			return fmt.Errorf("pipeline %s: periodic schedule requires interval_sec > 0", name)
		}
		ps.ticker = time.NewTicker(interval)
		go func() {
			for {
				select {
				case <-ps.ticker.C:
					s.triggerPipeline(name, exec)
				case <-ps.stopCh:
					ps.ticker.Stop()
					return
				}
			}
		}()
		g.Log().Infof(s.ctx, "Scheduled pipeline %s every %s", name, interval)

	case ScheduleDependency:
		if len(cfg.DependsOn) == 0 {
			return fmt.Errorf("pipeline %s: dependency schedule requires depends_on list", name)
		}
		ps.depCtx = &dependencyTrigger{dependsOn: cfg.DependsOn}
		g.Log().Infof(s.ctx, "Pipeline %s waiting for dependencies: %v", name, cfg.DependsOn)

	default:
		return fmt.Errorf("pipeline %s: unknown schedule type %q", name, cfg.Type)
	}

	s.schedules[name] = ps
	return nil
}

// startExecutorLocked starts the executor immediately (streaming/once mode).
func (s *Scheduler) startExecutorLocked(name string, exec *DAGExecutor) error {
	if err := exec.Start(s.ctx); err != nil {
		return fmt.Errorf("start pipeline %s: %w", name, err)
	}
	g.Log().Infof(s.ctx, "Started pipeline %s (streaming/once)", name)
	return nil
}

// triggerPipeline starts a pipeline, waits for it to complete, then stops it.
// Used for cron/periodic/dependency triggers.
func (s *Scheduler) triggerPipeline(name string, exec *DAGExecutor) {
	s.mu.Lock()
	_, alreadyRunning := s.schedules[name]
	s.mu.Unlock()

	if alreadyRunning {
		status := exec.Status()
		if status == "running" {
			g.Log().Warningf(s.ctx, "Pipeline %s already running, skipping trigger", name)
			return
		}
	}

	g.Log().Infof(s.ctx, "Triggering pipeline %s", name)
	if err := exec.Start(s.ctx); err != nil {
		g.Log().Errorf(s.ctx, "Failed to start pipeline %s: %v", name, err)
		return
	}

	// Record run start
	runID, _ := s.store.RecordRunStart(s.ctx, name)

	// Wait for completion in a goroutine
	go func() {
		exec.Wait()
		stats := exec.Stats()
		dur := exec.Duration()
		status := exec.Status()
		if status == "" {
			status = "completed"
		}
		_ = s.store.RecordRunEnd(s.ctx, runID, status, stats.RecordsRead, stats.RecordsWritten, stats.RecordsFailed, stats.RecordsDLQ, dur.Milliseconds())
		g.Log().Infof(s.ctx, "Pipeline %s completed: status=%s read=%d written=%d failed=%d",
			name, status, stats.RecordsRead, stats.RecordsWritten, stats.RecordsFailed)

		// Notify dependent pipelines
		s.notifyDependents(name)
	}()
}

// notifyDependents checks if any pipelines depend on the given name and triggers them.
func (s *Scheduler) notifyDependents(completedName string) {
	s.mu.Lock()
	for name, ps := range s.schedules {
		if ps.depCtx == nil {
			continue
		}
		for _, dep := range ps.depCtx.dependsOn {
			if dep == completedName {
				exec := s.executors[name]
				s.mu.Unlock()
				g.Log().Infof(s.ctx, "Dependency trigger: %s -> %s", completedName, name)
				s.triggerPipeline(name, exec)
				s.mu.Lock()
				break
			}
		}
	}
	s.mu.Unlock()
}

// Run starts the scheduler's cron engine. Blocks until ctx is cancelled.
func (s *Scheduler) Run(ctx context.Context) {
	s.ctx = ctx
	s.cronLib.Start()
	g.Log().Info(ctx, "Scheduler started")
	<-ctx.Done()
	s.cronLib.Stop()
	g.Log().Info(ctx, "Scheduler stopped")
}

// StopAll stops all running pipelines and periodic tickers.
func (s *Scheduler) StopAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for name, ps := range s.schedules {
		if ps.ticker != nil {
			close(ps.stopCh)
		}
		if exec, ok := s.executors[name]; ok {
			exec.Stop()
		}
	}
}
