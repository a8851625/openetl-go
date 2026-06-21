package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"openetl-go/internal/etl/pipeline"
	"openetl-go/internal/etl/storage"
)

// pollTask polls the master for unassigned tasks. In standalone mode
// (masterURL empty), polls directly from the shared store instead of HTTP.
func (w *Worker) pollTask(ctx context.Context) (*storage.TaskAssignment, error) {
	if w.masterURL == "" {
		return w.pollTaskFromStore(ctx)
	}
	reqBody := bytes.NewReader([]byte{})
	url := w.masterURL + "/api/v2/workers/" + w.ID + "/poll"
	req, err := http.NewRequestWithContext(ctx, "POST", url, reqBody)
	if err != nil {
		return nil, fmt.Errorf("poll request: %w", err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result struct {
		Status string                  `json:"status"`
		Task   *storage.TaskAssignment `json:"task,omitempty"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decode poll response: %w", err)
	}
	if result.Status == "idle" || result.Status == "" {
		return nil, nil
	}
	return result.Task, nil
}

// pollTaskFromStore finds the first pending task in the shared store and
// atomically claims it for this worker. Used in standalone mode when the
// master and worker share a process (and a storage backend).
func (w *Worker) pollTaskFromStore(ctx context.Context) (*storage.TaskAssignment, error) {
	tasks, err := w.store.ListTasks(ctx, "")
	if err != nil {
		return nil, fmt.Errorf("list tasks: %w", err)
	}
	for _, t := range tasks {
		if t.Status == "pending" {
			now := time.Now()
			t.WorkerID = w.ID
			t.Status = "assigned"
			t.AssignedAt = &now
			if err := w.store.UpdateTask(ctx, t); err != nil {
				continue
			}
			g.Log().Infof(ctx, "Worker %s claimed task %s from store (standalone)", w.ID, t.TaskID)
			return t, nil
		}
	}
	return nil, nil
}

// reportTaskDone notifies the master that a task has completed.
func (w *Worker) reportTaskDone(ctx context.Context, taskID string) {
	body := map[string]string{"task_id": taskID}
	bodyJSON, _ := json.Marshal(body)
	url := w.masterURL + "/api/v2/workers/" + w.ID + "/poll"
	req, _ := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(bodyJSON))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		g.Log().Warningf(ctx, "Report task result: %v", err)
		return
	}
	resp.Body.Close()
}

// PollLoop continuously polls for tasks and runs them. This runs in a
// separate goroutine alongside the heartbeat loop.
func (w *Worker) PollLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		default:
		}

		w.mu.RLock()
		slotsUsed := len(w.executors)
		slotsMax := w.Slots
		w.mu.RUnlock()

		if slotsUsed >= slotsMax {
			time.Sleep(2 * time.Second)
			continue
		}

		task, err := w.pollTask(ctx)
		if err != nil {
			g.Log().Warningf(ctx, "Poll task failed: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		if task == nil {
			time.Sleep(2 * time.Second)
			continue
		}

		// Execute the task using the registered task executor.
		taskID := task.TaskID
		pipelineName := task.Pipeline

		w.mu.Lock()
		task.Status = "running"
		now := time.Now()
		task.StartedAt = &now
		_ = w.store.UpdateTask(ctx, task)
		w.mu.Unlock()

		g.Log().Infof(ctx, "Worker %s claimed task %s (pipeline=%s)",
			w.ID, taskID, pipelineName)

		go func(t *storage.TaskAssignment) {
			defer func() {
				if rec := recover(); rec != nil {
					g.Log().Errorf(ctx, "Task %s panic: %v", t.TaskID, rec)
				}
				finished := time.Now()
				t.Status = "completed"
				t.FinishedAt = &finished
				_ = w.store.UpdateTask(ctx, t)
				w.reportTaskDone(ctx, t.TaskID)
				g.Log().Infof(ctx, "Task %s completed", t.TaskID)
			}()

			if w.taskExecutor != nil {
				if err := w.taskExecutor(ctx, t.Pipeline, ""); err != nil {
					g.Log().Errorf(ctx, "Task %s execution error: %v", t.TaskID, err)
					finished := time.Now()
					t.Status = "failed"
					t.FinishedAt = &finished
					_ = w.store.UpdateTask(ctx, t)
				}
			} else {
				g.Log().Warningf(ctx, "No task executor registered — task %s cannot run", t.TaskID)
				finished := time.Now()
				t.Status = "failed"
				t.FinishedAt = &finished
				_ = w.store.UpdateTask(ctx, t)
			}
		}(task)
	}
}

// ensure pipeline imported for future use
var _ = pipeline.StatusRunning
