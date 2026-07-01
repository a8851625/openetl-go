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
	store    storage.Storage
	registry *WorkerRegistry
	mu       sync.Mutex
}

// NewTaskDispatcher creates a task dispatch coordinator.
func NewTaskDispatcher(store storage.Storage, registry *WorkerRegistry) *TaskDispatcher {
	return &TaskDispatcher{store: store, registry: registry}
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
				return pipeline.StatusFailed, fmt.Errorf("shard %s failed", taskID)
			}
			// pending/assigned/running → keep waiting
		}
		select {
		case <-ctx.Done():
			return pipeline.StatusStopped, ctx.Err()
		case <-ticker.C:
		}
	}
}

// AssignNextTask is called by the worker poll endpoint. It searches for the
// oldest pending task whose RequiredLabels are satisfied by the given worker's
// registered Labels, and assigns it. A task with no RequiredLabels is claimable
// by any worker; a task with RequiredLabels requires the worker to match every
// key/value exactly (worker_selector.match_labels enforcement).
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

	tasks, err := d.store.ListTasks(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	for _, t := range tasks {
		if t.Status != "pending" {
			continue
		}
		if !labelsMatch(workerLabels, t.RequiredLabels) {
			continue
		}
		now := time.Now()
		t.WorkerID = workerID
		t.Status = "assigned"
		t.AssignedAt = &now
		if err := d.store.UpdateTask(ctx, t); err != nil {
			g.Log().Warningf(ctx, "UpdateTask %s: %v", t.TaskID, err)
			continue
		}
		g.Log().Infof(ctx, "Task %s assigned to worker %s (task labels=%v, worker labels=%v)",
			t.TaskID, workerID, t.RequiredLabels, workerLabels)
		return t, nil
	}
	return nil, nil
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

// ReportTaskResult updates the task status after execution.
func (d *TaskDispatcher) ReportTaskResult(ctx context.Context, taskID string, status string) error {
	tasks, err := d.store.ListTasks(ctx, "")
	if err != nil {
		return err
	}
	for _, t := range tasks {
		if t.TaskID == taskID {
			now := time.Now()
			t.Status = status
			t.FinishedAt = &now
			return d.store.UpdateTask(ctx, t)
		}
	}
	return fmt.Errorf("task %s not found", taskID)
}

// ReassignStaleTasks checks for tasks assigned to workers whose heartbeat is
// stale and reassigns them back to pending.
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

	tasks, _ := d.store.ListTasks(ctx, "")
	for _, t := range tasks {
		if t.Status == "assigned" || t.Status == "running" {
			if t.WorkerID != "" && !online[t.WorkerID] {
				g.Log().Warningf(ctx, "Reassigning stale task %s from offline worker %s", t.TaskID, t.WorkerID)
				t.Status = "pending"
				t.WorkerID = ""
				t.AssignedAt = nil
				_ = d.store.UpdateTask(ctx, t)
			}
		}
	}
}
