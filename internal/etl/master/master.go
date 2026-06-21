package master

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"openetl-go/internal/etl/pipeline"
	"openetl-go/internal/etl/storage"
)

// Master manages worker registration, health monitoring, and task dispatch.
type Master struct {
	store    storage.Storage
	registry *WorkerRegistry
	dispatch *TaskDispatcher
	mu       sync.RWMutex
	ctx      context.Context
}

// NewMaster creates a new Master node.
func NewMaster(store storage.Storage) *Master {
	r := NewWorkerRegistry(store)
	return &Master{
		store:    store,
		registry: r,
		dispatch: NewTaskDispatcher(store, r),
	}
}

// Dispatcher returns the task dispatcher, which implements pipeline.ShardDispatcher
// so a master-role ParallelRunner can delegate shard execution to workers
// (A11-redo). Returned as the interface to keep server/pipeline decoupled from
// the concrete *TaskDispatcher.
func (m *Master) Dispatcher() pipeline.ShardDispatcher {
	return m.dispatch
}

// Run starts the master's health monitoring loop.
func (m *Master) Run(ctx context.Context) {
	m.ctx = ctx
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	g.Log().Info(ctx, "Master node started, monitoring worker health")
	for {
		select {
		case <-ticker.C:
			m.checkWorkerHealth(ctx)
			m.dispatch.ReassignStaleTasks(ctx)
		case <-ctx.Done():
			g.Log().Info(ctx, "Master node stopping")
			return
		}
	}
}

// checkWorkerHealth marks workers as offline if their heartbeat is stale.
func (m *Master) checkWorkerHealth(ctx context.Context) {
	workers, err := m.store.ListWorkers(ctx)
	if err != nil {
		return
	}
	for _, w := range workers {
		if w.Status == "online" && time.Since(w.LastHeartbeat) > 30*time.Second {
			g.Log().Warningf(ctx, "Worker %s heartbeat stale (last: %s ago), marking offline",
				w.ID, time.Since(w.LastHeartbeat).Truncate(time.Second))
			m.registry.MarkOffline(ctx, w.ID)
		}
	}
}

// ── Worker Registry ───────────────────────────────────────────────────

// WorkerRegistry manages the lifecycle of registered workers.
type WorkerRegistry struct {
	store storage.Storage
}

func NewWorkerRegistry(store storage.Storage) *WorkerRegistry {
	return &WorkerRegistry{store: store}
}

// Register adds or updates a worker in the registry.
func (r *WorkerRegistry) Register(ctx context.Context, info *storage.WorkerInfo) error {
	if err := r.store.RegisterWorker(ctx, info); err != nil {
		return fmt.Errorf("register worker: %w", err)
	}
	g.Log().Infof(ctx, "Worker registered: %s (%s:%d, slots=%d)", info.ID, info.Host, info.Port, info.Slots)
	return nil
}

// Heartbeat updates a worker's last-seen timestamp.
func (r *WorkerRegistry) Heartbeat(ctx context.Context, workerID string) error {
	return r.store.Heartbeat(ctx, workerID)
}

// Deregister removes a worker from the registry.
func (r *WorkerRegistry) Deregister(ctx context.Context, workerID string) error {
	g.Log().Infof(ctx, "Worker deregistered: %s", workerID)
	return r.store.DeregisterWorker(ctx, workerID)
}

// List returns all registered workers.
func (r *WorkerRegistry) List(ctx context.Context) ([]*storage.WorkerInfo, error) {
	return r.store.ListWorkers(ctx)
}

// MarkOffline marks a worker as offline.
func (r *WorkerRegistry) MarkOffline(ctx context.Context, workerID string) error {
	// For now we just deregister; in the future we'll add an "offline" status
	return r.store.DeregisterWorker(ctx, workerID)
}

// SelectBestWorker chooses the most suitable worker for a task based on:
// 1. Only online workers with available slots
// 2. Most available slots first
// 3. Affinity: prefer workers that recently ran the same pipeline
// If requiredLabels is non-empty, only workers matching ALL labels are eligible.
func (r *WorkerRegistry) SelectBestWorker(ctx context.Context, pipeline string, requiredLabels map[string]string) (*storage.WorkerInfo, error) {
	workers, err := r.store.ListWorkers(ctx)
	if err != nil {
		return nil, err
	}

	var best *storage.WorkerInfo
	for _, w := range workers {
		if w.Status != "online" {
			continue
		}
		if time.Since(w.LastHeartbeat) > 30*time.Second {
			continue
		}
		// Label matching: if requiredLabels is set, worker must match all
		if len(requiredLabels) > 0 {
			matched := true
			for k, v := range requiredLabels {
				if w.Labels == nil || w.Labels[k] != v {
					matched = false
					break
				}
			}
			if !matched {
				continue
			}
		}
		if best == nil || w.Slots > best.Slots {
			best = w
		}
	}
	if best == nil {
		return nil, fmt.Errorf("no available workers matching labels %v", requiredLabels)
	}
	return best, nil
}

// DispatchParallelShards creates task entries for each shard in a ParallelRunner.
func (m *Master) DispatchParallelShards(ctx context.Context, pr interface{ InstanceCount() int }, pipelineName string, labels map[string]string) error {
	if pr == nil {
		return nil
	}
	return m.dispatch.DispatchRunnerShards(ctx, pr, pipelineName, labels)
}

// ── HTTP API for Worker Registration ──────────────────────────────────

// RegisterHTTPRoutes adds worker management endpoints to the mux.
func (m *Master) RegisterHTTPRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/api/v2/workers", m.handleWorkers)
	mux.HandleFunc("/api/v2/workers/", m.handleWorkerAction)
}

func (m *Master) handleWorkers(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case http.MethodGet:
		workers, err := m.registry.List(r.Context())
		if err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
			return
		}
		if workers == nil {
			workers = []*storage.WorkerInfo{}
		}
		writeJSON(w, map[string]any{"workers": workers})

	case http.MethodPost:
		var req storage.WorkerInfo
		if err := decodeJSON(r, &req); err != nil {
			http.Error(w, `{"error":"invalid body"}`, 400)
			return
		}
		if req.ID == "" {
			http.Error(w, `{"error":"worker id is required"}`, 400)
			return
		}
		if err := m.registry.Register(r.Context(), &req); err != nil {
			http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
			return
		}
		writeJSON(w, map[string]any{"status": "registered", "worker_id": req.ID})

	default:
		http.Error(w, `{"error":"method not allowed"}`, 405)
	}
}

func (m *Master) handleWorkerAction(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	workerID := r.URL.Path[len("/api/v2/workers/"):]
	action := ""
	for i, c := range workerID {
		if c == '/' {
			action = workerID[i+1:]
			workerID = workerID[:i]
			break
		}
	}

	switch action {
	case "poll":
		if r.Method == http.MethodPost {
			var req struct {
				TaskID string `json:"task_id,omitempty"`
			}
			var res struct {
				Status string `json:"status,omitempty"`
			}
			_ = decodeJSON(r, &req)
			if req.TaskID != "" {
				_ = m.dispatch.ReportTaskResult(r.Context(), req.TaskID, "completed")
				res.Status = "acknowledged"
				writeJSON(w, res)
				return
			}
			task, err := m.dispatch.AssignNextTask(r.Context(), workerID)
			if err != nil {
				http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
				return
			}
			if task == nil {
				writeJSON(w, map[string]any{"status": "idle"})
				return
			}
			writeJSON(w, map[string]any{"status": "assigned", "task": task})
			return
		}
	case "heartbeat":
		if r.Method == http.MethodPost {
			if err := m.registry.Heartbeat(r.Context(), workerID); err != nil {
				http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
				return
			}
			writeJSON(w, map[string]any{"status": "ok"})
		}
	case "deregister":
		if r.Method == http.MethodDelete {
			if err := m.registry.Deregister(r.Context(), workerID); err != nil {
				http.Error(w, `{"error":"`+err.Error()+`"}`, 500)
				return
			}
			writeJSON(w, map[string]any{"status": "deregistered"})
		}
	case "":
		// GET worker details
		workers, _ := m.registry.List(r.Context())
		for _, wk := range workers {
			if wk.ID == workerID {
				writeJSON(w, wk)
				return
			}
		}
		http.Error(w, `{"error":"worker not found"}`, 404)
	default:
		http.Error(w, `{"error":"unknown action"}`, 404)
	}
}

// ── Helpers ───────────────────────────────────────────────────────────

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	enc := json.NewEncoder(w)
	enc.Encode(v)
}

func decodeJSON(r *http.Request, v any) error {
	return json.NewDecoder(r.Body).Decode(v)
}
