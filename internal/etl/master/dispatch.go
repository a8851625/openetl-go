package master

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"openetl-go/internal/etl/storage"
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

// DispatchShards takes a ShardSource and creates task entries for each shard
// so workers can claim them via the poll endpoint.
func (d *TaskDispatcher) DispatchShards(ctx context.Context, pr ShardSource, pipelineName string, labels map[string]string) error {
	shardCount := pr.InstanceCount()
	if shardCount <= 1 {
		return nil
	}
	g.Log().Infof(ctx, "Dispatching %d shards for pipeline %s", shardCount, pipelineName)

	for i := 0; i < shardCount; i++ {
		taskID := fmt.Sprintf("%s-shard-%d", pipelineName, i)
		task := &storage.TaskAssignment{
			TaskID:   taskID,
			Pipeline: pipelineName,
			Status:   "pending",
		}
		if err := d.store.CreateTask(ctx, task); err != nil {
			g.Log().Warningf(ctx, "CreateTask %s: %v", taskID, err)
		}
	}
	return nil
}

// AssignNextTask is called by the worker poll endpoint. It searches for the
// oldest pending task and assigns it to the given worker.
func (d *TaskDispatcher) AssignNextTask(ctx context.Context, workerID string) (*storage.TaskAssignment, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	tasks, err := d.store.ListTasks(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	for _, t := range tasks {
		if t.Status == "pending" {
			now := time.Now()
			t.WorkerID = workerID
			t.Status = "assigned"
			t.AssignedAt = &now
			if err := d.store.UpdateTask(ctx, t); err != nil {
				g.Log().Warningf(ctx, "UpdateTask %s: %v", t.TaskID, err)
				continue
			}
			g.Log().Infof(ctx, "Task %s assigned to worker %s", t.TaskID, workerID)
			return t, nil
		}
	}
	return nil, nil
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
