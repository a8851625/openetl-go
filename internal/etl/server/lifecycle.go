package server

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

func (s *Server) lifecycleLock(id string) *sync.Mutex {
	s.lifecycleLocksMu.Lock()
	defer s.lifecycleLocksMu.Unlock()
	lock := s.lifecycleLocks[id]
	if lock == nil {
		lock = &sync.Mutex{}
		s.lifecycleLocks[id] = lock
	}
	return lock
}

// startManagedPipeline is the only production boundary that starts a runner.
// It allocates one durable generation first and binds that immutable token to
// the run context, so a later reset can fence any in-flight checkpoint from
// this generation. Callers must persist desired_state=running before entry.
func (s *Server) startManagedPipeline(ctx context.Context, id string, runner pipeline.RunnerInterface) error {
	if runner == nil {
		return fmt.Errorf("pipeline %s has no runner", id)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	lock := s.lifecycleLock(id)
	lock.Lock()
	defer lock.Unlock()
	return s.startManagedPipelineLocked(ctx, id, runner)
}

func (s *Server) startManagedPipelineLocked(ctx context.Context, id string, runner pipeline.RunnerInterface) error {
	if runner.Status() == pipeline.StatusRunning {
		return fmt.Errorf("pipeline %s is already running", id)
	}
	generation, err := s.lifecycleStore.BeginPipelineGeneration(ctx, id)
	if err != nil {
		return fmt.Errorf("allocate pipeline generation: %w", err)
	}
	runCtx := storage.WithCheckpointFence(ctx, id, generation)
	if err := runner.Start(runCtx); err != nil {
		if persistErr := s.lifecycleStore.UpdatePipelineObservedState(context.Background(), id, generation, "failed"); persistErr != nil && !errors.Is(persistErr, storage.ErrPipelineGenerationFenced) {
			g.Log().Errorf(context.Background(), "Pipeline %s generation %d start failed and observed-state persistence also failed: %v", id, generation, persistErr)
		}
		return err
	}
	if err := s.lifecycleStore.UpdatePipelineObservedState(ctx, id, generation, "running"); err != nil {
		_ = runner.Stop()
		return fmt.Errorf("persist running observed state: %w", err)
	}
	runID, runErr := s.store.RecordRunStart(ctx, id)
	if runErr != nil {
		g.Log().Warningf(ctx, "Pipeline %s generation %d run-history start persistence failed: %v", id, generation, runErr)
	}
	done := runner.Done()
	go s.observeManagedPipeline(id, generation, runID, runner, done)
	return nil
}

func (s *Server) observeManagedPipeline(id string, generation, runID int64, runner pipeline.RunnerInterface, done <-chan struct{}) {
	<-done
	stats := runner.Stats()
	duration := runner.Duration()
	observed := string(runner.Status())
	if observed == "" || observed == "running" {
		observed = "completed"
	}
	ctx := context.Background()
	if err := s.lifecycleStore.UpdatePipelineObservedState(ctx, id, generation, observed); err != nil {
		if errors.Is(err, storage.ErrPipelineGenerationFenced) {
			g.Log().Warningf(ctx, "Pipeline %s generation %d terminal state %s fenced by newer generation", id, generation, observed)
		} else {
			g.Log().Errorf(ctx, "Pipeline %s generation %d terminal observed-state persistence failed: %v", id, generation, err)
		}
	}
	if runID > 0 {
		if err := s.store.RecordRunEnd(ctx, runID, observed, stats.RecordsRead, stats.RecordsWritten, stats.RecordsFailed, stats.RecordsDLQ, duration.Milliseconds()); err != nil {
			g.Log().Warningf(ctx, "Pipeline %s generation %d run-history completion persistence failed: %v", id, generation, err)
		}
	}
	if s.scheduler != nil {
		s.scheduler.NotifyDependents(id)
	}
}

func (s *Server) pipelineLifecycle(ctx context.Context, id string) (*storage.PipelineRow, error) {
	row, err := s.store.GetPipeline(ctx, id)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, fmt.Errorf("pipeline %q not found", id)
	}
	storage.NormalizePipelineLifecycle(row)
	return row, nil
}

func (s *Server) requestPipelineStart(ctx context.Context, id string, runner pipeline.RunnerInterface) error {
	if ctx == nil {
		ctx = context.Background()
	}
	lock := s.lifecycleLock(id)
	lock.Lock()
	defer lock.Unlock()
	if err := s.lifecycleStore.UpdatePipelineDesiredState(ctx, id, storage.PipelineDesiredRunning); err != nil {
		return fmt.Errorf("persist desired_state=running: %w", err)
	}
	return s.startManagedPipelineLocked(ctx, id, runner)
}

func (s *Server) requestPipelineQuiesce(ctx context.Context, id, desired string, runner pipeline.RunnerInterface) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if desired != storage.PipelineDesiredStopped && desired != storage.PipelineDesiredPaused {
		return fmt.Errorf("invalid quiescent desired state %q", desired)
	}
	lock := s.lifecycleLock(id)
	lock.Lock()
	defer lock.Unlock()
	if err := s.lifecycleStore.UpdatePipelineDesiredState(ctx, id, desired); err != nil {
		return fmt.Errorf("persist desired_state=%s: %w", desired, err)
	}
	var err error
	if desired == storage.PipelineDesiredPaused {
		err = runner.Pause()
	} else {
		err = runner.Stop()
	}
	if err != nil {
		return err
	}
	if err := s.lifecycleStore.UpdatePipelineObservedState(ctx, id, 0, desired); err != nil {
		return fmt.Errorf("persist observed_state=%s: %w", desired, err)
	}
	return nil
}

func (s *Server) resetManagedCheckpoint(ctx context.Context, id string, runner pipeline.RunnerInterface) (int64, error) {
	lock := s.lifecycleLock(id)
	lock.Lock()
	defer lock.Unlock()
	if runner != nil && runner.Status() == pipeline.StatusRunning {
		return 0, fmt.Errorf("%w: pipeline %s observed_state=running", storage.ErrPipelineNotQuiescent, id)
	}
	return s.lifecycleStore.ResetPipelineCheckpoint(ctx, id)
}

func (s *Server) setManagedCheckpoint(ctx context.Context, id string, runner pipeline.RunnerInterface, cp *storage.CheckpointRecord) (int64, error) {
	lock := s.lifecycleLock(id)
	lock.Lock()
	defer lock.Unlock()
	if runner != nil && runner.Status() == pipeline.StatusRunning {
		return 0, fmt.Errorf("%w: pipeline %s observed_state=running", storage.ErrPipelineNotQuiescent, id)
	}
	return s.lifecycleStore.SetPipelineCheckpoint(ctx, id, cp)
}
