package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// pollTask polls the master for unassigned tasks. In standalone mode
// (masterURL empty), polls directly from the shared store instead of HTTP.
func (w *Worker) pollTask(ctx context.Context) (*storage.TaskAssignment, error) {
	if w.masterURL == "" || w.client == nil {
		return w.pollTaskFromStore(ctx)
	}
	urlPath := "/api/v2/workers/" + w.ID + "/poll"
	resp, err := w.client.PostJSON(ctx, urlPath, nil, false)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("poll unauthorized (status %d): check ETL_API_TOKEN", resp.StatusCode)
	}
	if resp.StatusCode >= 300 {
		return nil, fmt.Errorf("poll returned %d", resp.StatusCode)
	}

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

// pollTaskFromStore finds the first pending task whose RequiredLabels are
// satisfied by this worker's registered Labels, and CAS-claims it for this
// worker. Used in standalone mode when the master and worker share a process
// (and a storage backend).
func (w *Worker) pollTaskFromStore(ctx context.Context) (*storage.TaskAssignment, error) {
	task, err := w.store.ClaimTask(ctx, w.ID, w.Labels, w.leaseTTL)
	if err != nil {
		return nil, fmt.Errorf("claim task from store: %w", err)
	}
	if task != nil {
		g.Log().Infof(ctx, "Worker %s claimed task %s from store (standalone, gen=%d attempt=%d labels=%v)",
			w.ID, task.TaskID, task.Generation, task.Attempt, task.RequiredLabels)
	}
	return task, nil
}

// reportTaskResult notifies the master that a task reached a terminal status.
// Includes generation so the master can fence stale owners (PR-D1.2).
func (w *Worker) reportTaskResult(ctx context.Context, task *storage.TaskAssignment, status, lastError string) {
	if w.masterURL == "" || w.client == nil || task == nil {
		return
	}
	body := map[string]any{
		"task_id":    task.TaskID,
		"generation": task.Generation,
		"status":     status,
	}
	if lastError != "" {
		body["last_error"] = lastError
	}
	bodyJSON, _ := json.Marshal(body)
	// Prefer dedicated /report; fall back to legacy poll-with-body path.
	resp, err := w.client.PostJSON(ctx, "/api/v2/workers/"+w.ID+"/report", bodyJSON, false)
	if err != nil {
		g.Log().Warningf(ctx, "Report task result via /report failed: %v; trying poll path", err)
		resp, err = w.client.PostJSON(ctx, "/api/v2/workers/"+w.ID+"/poll", bodyJSON, false)
		if err != nil {
			g.Log().Warningf(ctx, "Report task result: %v", err)
			return
		}
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusConflict {
		g.Log().Warningf(ctx, "Report task %s fenced (generation=%d) — ownership lost", task.TaskID, task.Generation)
		return
	}
	if resp.StatusCode >= 300 {
		g.Log().Warningf(ctx, "Report task %s returned %d", task.TaskID, resp.StatusCode)
	}
}

// casMarkRunning transitions the claimed task to running under generation CAS.
// Returns false if fencing lost ownership.
func (w *Worker) casMarkRunning(ctx context.Context, task *storage.TaskAssignment) bool {
	now := time.Now()
	task.Status = "running"
	task.StartedAt = &now
	if task.LeaseExpiresAt == nil || task.LeaseExpiresAt.Before(now) {
		lease := now.Add(w.leaseTTL)
		task.LeaseExpiresAt = &lease
	}
	if err := w.store.CASUpdateTask(ctx, task); err != nil {
		if errors.Is(err, storage.ErrTaskFenced) {
			g.Log().Warningf(ctx, "Task %s fenced before start (gen=%d)", task.TaskID, task.Generation)
			return false
		}
		// Fall back to unconditional update for stores that predate CAS.
		_ = w.store.UpdateTask(ctx, task)
	}
	return true
}

// casMarkTerminal records a terminal status under generation CAS.
func (w *Worker) casMarkTerminal(ctx context.Context, task *storage.TaskAssignment, status, lastError string) {
	finished := time.Now()
	task.Status = status
	task.FinishedAt = &finished
	if lastError != "" {
		task.LastError = lastError
	}
	if err := w.store.CASUpdateTask(ctx, task); err != nil {
		if errors.Is(err, storage.ErrTaskFenced) {
			g.Log().Warningf(ctx, "Task %s fenced on terminal %s (gen=%d) — not overwriting new owner",
				task.TaskID, status, task.Generation)
			return
		}
		_ = w.store.UpdateTask(ctx, task)
	}
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

		slotsUsed := atomic.LoadInt64(&w.inFlight)
		slotsMax := int64(w.Slots)

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
		owned := w.casMarkRunning(ctx, task)
		w.mu.Unlock()
		if !owned {
			continue
		}

		g.Log().Infof(ctx, "Worker %s claimed task %s (pipeline=%s gen=%d attempt=%d)",
			w.ID, taskID, pipelineName, task.Generation, task.Attempt)

		atomic.AddInt64(&w.inFlight, 1)
		go func(t *storage.TaskAssignment) {
			defer atomic.AddInt64(&w.inFlight, -1)
			defer func() {
				if rec := recover(); rec != nil {
					g.Log().Errorf(ctx, "Task %s panic: %v", t.TaskID, rec)
					w.casMarkTerminal(ctx, t, "failed", fmt.Sprintf("panic: %v", rec))
					w.reportTaskResult(ctx, t, "failed", fmt.Sprintf("panic: %v", rec))
				}
			}()

			if w.taskExecutor == nil {
				g.Log().Warningf(ctx, "No task executor registered — task %s cannot run", t.TaskID)
				w.casMarkTerminal(ctx, t, "failed", "no task executor registered")
				w.reportTaskResult(ctx, t, "failed", "no task executor registered")
				return
			}

			execErr := w.taskExecutor(ctx, t)

			// If the worker's ctx was cancelled (shutdown), leave the task
			// "running" so master.ReassignStaleTasks re-queues it once this
			// worker deregisters. A shard (batch caught at shutdown OR a
			// continuous/CDC shard) must NOT be marked completed just because
			// the worker stopped — otherwise it would never resume elsewhere.
			if ctx.Err() != nil {
				g.Log().Infof(ctx, "Task %s interrupted by worker shutdown — left running for reassignment", t.TaskID)
				return
			}

			if execErr != nil {
				g.Log().Errorf(ctx, "Task %s execution error: %v", t.TaskID, execErr)
				w.casMarkTerminal(ctx, t, "failed", execErr.Error())
				w.reportTaskResult(ctx, t, "failed", execErr.Error())
			} else {
				w.casMarkTerminal(ctx, t, "completed", "")
				w.reportTaskResult(ctx, t, "completed", "")
			}
			g.Log().Infof(ctx, "Task %s final status: %s", t.TaskID, t.Status)
		}(task)
	}
}

// labelsSatisfy returns true if workerLabels matches every key/value in
// required. Empty required always matches (default pool).
func labelsSatisfy(workerLabels, required map[string]string) bool {
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

// ensure pipeline imported for future use
var _ = pipeline.StatusRunning
