package master

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// TaskDispatcher extracts shard runners from a ParallelRunner and assigns them
// to available workers. If no workers are available, shards run inline.
type TaskDispatcher struct {
	store       storage.Storage
	registry    *WorkerRegistry
	mu          sync.Mutex
	leaseTTL    time.Duration
	maxAttempts int
}

// NewTaskDispatcher creates a task dispatch coordinator.
func NewTaskDispatcher(store storage.Storage, registry *WorkerRegistry) *TaskDispatcher {
	return &TaskDispatcher{
		store:       store,
		registry:    registry,
		leaseTTL:    storage.DefaultTaskLeaseTTL,
		maxAttempts: storage.DefaultTaskMaxAttempts,
	}
}

// SetLeaseTTL overrides the ownership lease granted on claim/renew.
func (d *TaskDispatcher) SetLeaseTTL(ttl time.Duration) {
	if ttl > 0 {
		d.leaseTTL = ttl
	}
}

// SetMaxAttempts overrides the requeue budget before a task is permanently failed.
func (d *TaskDispatcher) SetMaxAttempts(n int) {
	if n > 0 {
		d.maxAttempts = n
	}
}

// ShardSource represents something that has numbered instances (ParallelRunner).
type ShardSource interface {
	InstanceCount() int
}

// DispatchShards implements pipeline.ShardDispatcher (A11-redo). It creates
// task_assignments for each of `count` shards of the named pipeline, each
// carrying shard metadata (ShardIndex/ShardTotal) so a claiming worker knows
// which single-shard Runner to build. requiredLabels, when non-empty, is
// persisted on each task so only workers whose registered Labels match all
// entries may claim them (worker_selector.match_labels enforcement).
func (d *TaskDispatcher) DispatchShards(ctx context.Context, pipelineName string, count int, requiredLabels map[string]string) error {
	if count <= 1 {
		return nil
	}
	g.Log().Infof(ctx, "Dispatching %d shards for pipeline %s (labels=%v)", count, pipelineName, requiredLabels)

	for i := 0; i < count; i++ {
		taskID := fmt.Sprintf("%s-shard-%d", pipelineName, i)
		task := &storage.TaskAssignment{
			TaskID:         taskID,
			Pipeline:       pipelineName,
			ShardIndex:     i, // A11-redo: worker reads these to build the right single-shard Runner
			ShardTotal:     count,
			Status:         "pending",
			Generation:     0,
			Attempt:        0,
			RequiredLabels: requiredLabels,
		}
		if err := d.store.CreateTask(ctx, task); err != nil {
			g.Log().Warningf(ctx, "CreateTask %s: %v", taskID, err)
		}
	}
	return nil
}

// DispatchRunnerShards is the ShardSource-based adapter used by the standalone
// cosmetic-dispatch path (Master.DispatchParallelShards). It delegates to
// DispatchShards with the count derived from the runner and forwards the
// pipeline's worker_selector.match_labels.
func (d *TaskDispatcher) DispatchRunnerShards(ctx context.Context, pr ShardSource, pipelineName string, labels map[string]string) error {
	return d.DispatchShards(ctx, pipelineName, pr.InstanceCount(), labels)
}

// WaitShard implements pipeline.ShardDispatcher. It polls the shared store for
// shard `idx`'s task (`{pipelineName}-shard-{idx}`) and returns when it reaches
// a terminal state (completed/failed), or when ctx is cancelled (master Stop).
//
// For continuous/CDC shards the worker leaves the task "running" indefinitely,
// so WaitShard returns only on ctx cancel — yielding StatusStopped, the honest
// terminal state for a streaming shard. The master's ParallelRunner therefore
// reaches StatusStopped (never StatusCompleted) for continuous pipelines.
func (d *TaskDispatcher) WaitShard(ctx context.Context, pipelineName string, idx int) (pipeline.Status, error) {
	taskID := fmt.Sprintf("%s-shard-%d", pipelineName, idx)
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		// Prefer GetTask so terminal rows are visible even after ListTasks
		// filters them out of the active set.
		if getter, ok := d.store.(interface {
			GetTask(context.Context, string) (*storage.TaskAssignment, error)
		}); ok {
			if t, err := getter.GetTask(ctx, taskID); err == nil && t != nil {
				switch t.Status {
				case "completed":
					return pipeline.StatusCompleted, nil
				case "failed":
					return pipeline.StatusFailed, fmt.Errorf("shard %s failed: %s", taskID, t.LastError)
				}
			}
		} else {
			tasks, err := d.store.ListTasks(ctx, pipelineName)
			if err != nil {
				return pipeline.StatusFailed, fmt.Errorf("list tasks for %s: %w", taskID, err)
			}
			for _, t := range tasks {
				if t.TaskID != taskID {
					continue
				}
				switch t.Status {
				case "completed":
					return pipeline.StatusCompleted, nil
				case "failed":
					return pipeline.StatusFailed, fmt.Errorf("shard %s failed: %s", taskID, t.LastError)
				}
			}
		}
		select {
		case <-ctx.Done():
			return pipeline.StatusStopped, ctx.Err()
		case <-ticker.C:
		}
	}
}

// AssignNextTask is called by the worker poll endpoint. It CAS-claims the
// oldest pending task whose RequiredLabels are satisfied by the given worker's
// registered Labels. A task with no RequiredLabels is claimable by any worker;
// a task with RequiredLabels requires the worker to match every key/value
// exactly (worker_selector.match_labels enforcement).
func (d *TaskDispatcher) AssignNextTask(ctx context.Context, workerID string) (*storage.TaskAssignment, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	workerLabels, err := d.lookupWorkerLabels(ctx, workerID)
	if err != nil {
		// Don't fail hard — a worker whose registration row is momentarily
		// missing should still be able to claim unlabeled tasks (back-compat).
		g.Log().Warningf(ctx, "AssignNextTask: lookup worker %s labels failed: %v", workerID, err)
		workerLabels = nil
	}

	task, err := d.store.ClaimTask(ctx, workerID, workerLabels, d.leaseTTL)
	if err != nil {
		return nil, fmt.Errorf("claim task: %w", err)
	}
	if task == nil {
		return nil, nil
	}
	g.Log().Infof(ctx, "Task %s assigned to worker %s gen=%d attempt=%d (task labels=%v, worker labels=%v)",
		task.TaskID, workerID, task.Generation, task.Attempt, task.RequiredLabels, workerLabels)
	return task, nil
}

// lookupWorkerLabels returns the Labels registered for the given worker ID.
// Returns nil (no error) for a worker with no labels.
func (d *TaskDispatcher) lookupWorkerLabels(ctx context.Context, workerID string) (map[string]string, error) {
	workers, err := d.store.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}
	for _, w := range workers {
		if w.ID == workerID {
			return w.Labels, nil
		}
	}
	return nil, fmt.Errorf("worker %s not registered", workerID)
}

// labelsMatch returns true if the worker's labels satisfy every required
// key/value. Empty required labels always matches (default pool).
func labelsMatch(workerLabels, required map[string]string) bool {
	if len(required) == 0 {
		return true
	}
	for k, v := range required {
		if workerLabels == nil || workerLabels[k] != v {
			return false
		}
	}
	return true
}

// ReportTaskResult updates the task status after execution using generation CAS.
// An old worker whose lease was reassigned cannot overwrite the new owner.
func (d *TaskDispatcher) ReportTaskResult(ctx context.Context, taskID, workerID string, generation int64, status, lastError string) error {
	task, err := d.store.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	if task == nil {
		return fmt.Errorf("task %s not found", taskID)
	}
	// Ownership check before write so diagnostics stay clear.
	if workerID != "" && task.WorkerID != "" && task.WorkerID != workerID {
		return storage.ErrTaskFenced
	}
	if generation > 0 && task.Generation != generation {
		return storage.ErrTaskFenced
	}
	now := time.Now()
	task.Status = status
	task.FinishedAt = &now
	if lastError != "" {
		task.LastError = lastError
	}
	if workerID != "" {
		task.WorkerID = workerID
	}
	if generation > 0 {
		task.Generation = generation
	}
	if err := d.store.CASUpdateTask(ctx, task); err != nil {
		return err
	}
	return nil
}

// ReassignStaleTasks checks for tasks whose worker is offline or whose lease
// has expired and requeues them (bounded by maxAttempts). Attempt history is
// retained; generation is left unchanged until the next successful claim.
func (d *TaskDispatcher) ReassignStaleTasks(ctx context.Context) {
	d.mu.Lock()
	defer d.mu.Unlock()

	workers, _ := d.store.ListWorkers(ctx)
	online := make(map[string]bool)
	for _, w := range workers {
		if w.Status == "online" && time.Since(w.LastHeartbeat) <= 30*time.Second {
			online[w.ID] = true
		}
	}

	now := time.Now()
	tasks, _ := d.store.ListTasks(ctx, "")
	for _, t := range tasks {
		if t.Status != "assigned" && t.Status != "running" {
			continue
		}
		stale := false
		reason := ""
		if t.WorkerID != "" && !online[t.WorkerID] {
			stale = true
			reason = fmt.Sprintf("worker %s offline", t.WorkerID)
		} else if t.LeaseExpiresAt != nil && now.After(*t.LeaseExpiresAt) {
			stale = true
			reason = "lease expired"
		}
		if !stale {
			continue
		}

		// Bound requeue budget: if attempts already at/above max, fail visibly.
		if t.Attempt >= d.maxAttempts {
			g.Log().Warningf(ctx, "Task %s exceeded max attempts (%d); marking failed (reason=%s)", t.TaskID, d.maxAttempts, reason)
			t.Status = "failed"
			t.LastError = fmt.Sprintf("exceeded max attempts (%d): %s", d.maxAttempts, reason)
			finished := now
			t.FinishedAt = &finished
			// Use unconditional UpdateTask for master-side terminalization so a
			// dead generation does not leave the task stuck.
			_ = d.store.UpdateTask(ctx, t)
			continue
		}

		g.Log().Warningf(ctx, "Reassigning stale task %s from worker %s gen=%d attempt=%d reason=%s",
			t.TaskID, t.WorkerID, t.Generation, t.Attempt, reason)
		t.Status = "pending"
		t.WorkerID = ""
		t.AssignedAt = nil
		t.StartedAt = nil
		t.FinishedAt = nil
		t.LeaseExpiresAt = nil
		t.LastError = reason
		// Keep generation so a late completion from the old owner still fails CAS
		// (new claim will bump generation further). Attempt is preserved and
		// incremented on the next claim.
		_ = d.store.UpdateTask(ctx, t)
	}
}
