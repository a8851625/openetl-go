package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/gogf/gf/v2/frame/g"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/orchestrator"
	"openetl-go/internal/etl/storage"
)

// Worker represents a task execution node that registers with a Master.
// In standalone mode, the Master and Worker run in the same process.
type Worker struct {
	ID        string
	Host      string
	Port      int
	Slots     int
	masterURL string

	store     storage.Storage
	executors map[string]*orchestrator.DAGExecutor
	mu        sync.RWMutex

	// taskExecutor is called when the worker claims a task. In standalone
	// mode, this is set by the server to construct a Runner from the
	// pipeline spec. In distributed mode, the worker fetches the spec
	// from the master via HTTP.
	taskExecutor func(ctx context.Context, pipelineName, shardID string) error

	ctx       context.Context
	cancel    context.CancelFunc
	stopCh    chan struct{}
	heartbeat *time.Ticker
}

// SetTaskExecutor registers a function that can execute a pipeline task.
// This is called by the server in standalone mode.
func (w *Worker) SetTaskExecutor(fn func(ctx context.Context, pipelineName, shardID string) error) {
	w.taskExecutor = fn
}

// Config holds worker configuration.
type Config struct {
	ID        string
	Host      string
	Port      int
	Slots     int
	MasterURL string
	Store     storage.Storage
}

// New creates a new Worker instance.
func New(cfg Config) *Worker {
	if cfg.ID == "" {
		cfg.ID = fmt.Sprintf("worker-%d", os.Getpid())
	}
	if cfg.Slots <= 0 {
		cfg.Slots = 4
	}
	return &Worker{
		ID:        cfg.ID,
		Host:      cfg.Host,
		Port:      cfg.Port,
		Slots:     cfg.Slots,
		masterURL: cfg.MasterURL,
		store:     cfg.Store,
		executors: map[string]*orchestrator.DAGExecutor{},
		stopCh:    make(chan struct{}),
	}
}

// Start registers the worker with the master and begins heartbeats.
func (w *Worker) Start(ctx context.Context) error {
	w.ctx, w.cancel = context.WithCancel(ctx)

	// Register with master
	if err := w.register(ctx); err != nil {
		return fmt.Errorf("worker register: %w", err)
	}

	// Start heartbeat ticker
	w.heartbeat = time.NewTicker(5 * time.Second)
	go w.heartbeatLoop(ctx)
	go w.PollLoop(w.ctx)

	g.Log().Infof(ctx, "Worker started: id=%s host=%s:%d slots=%d master=%s",
		w.ID, w.Host, w.Port, w.Slots, w.masterURL)
	return nil
}

// Stop deregisters and shuts down all running tasks.
func (w *Worker) Stop() {
	if w.cancel != nil {
		w.cancel()
	}
	if w.heartbeat != nil {
		w.heartbeat.Stop()
	}
	close(w.stopCh)

	w.mu.Lock()
	for name, exec := range w.executors {
		g.Log().Infof(context.Background(), "Stopping pipeline %s on worker %s", name, w.ID)
		exec.Stop()
	}
	w.mu.Unlock()

	// Deregister from master
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w.deregister(ctx)
}

func (w *Worker) register(ctx context.Context) error {
	body := map[string]any{
		"id":    w.ID,
		"host":  w.Host,
		"port":  w.Port,
		"slots": w.Slots,
	}
	bodyJSON, _ := json.Marshal(body)
	resp, err := http.Post(w.masterURL+"/api/v2/workers", "application/json", jsonReader(bodyJSON))
	if err != nil {
		return fmt.Errorf("register request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("register returned %d", resp.StatusCode)
	}
	return nil
}

func (w *Worker) deregister(ctx context.Context) {
	if w.masterURL == "" {
		return
	}
	req, _ := http.NewRequestWithContext(ctx, "DELETE", w.masterURL+"/api/v2/workers/"+w.ID+"/deregister", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		g.Log().Warningf(ctx, "Worker deregister failed: %v", err)
		return
	}
	resp.Body.Close()
}

func (w *Worker) heartbeatLoop(ctx context.Context) {
	for {
		select {
		case <-w.heartbeat.C:
			if err := w.sendHeartbeat(ctx); err != nil {
				g.Log().Warningf(ctx, "Heartbeat failed: %v", err)
				// Try to re-register
				time.Sleep(5 * time.Second)
				if err := w.register(ctx); err != nil {
					g.Log().Errorf(ctx, "Re-registration failed: %v", err)
				}
			}
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		}
	}
}

func (w *Worker) sendHeartbeat(ctx context.Context) error {
	req, _ := http.NewRequestWithContext(ctx, "POST", w.masterURL+"/api/v2/workers/"+w.ID+"/heartbeat", nil)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	return nil
}

// AssignTask starts a pipeline execution on this worker.
func (w *Worker) AssignTask(ctx context.Context, exec *orchestrator.DAGExecutor, pipelineName string) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if len(w.executors) >= w.Slots {
		return fmt.Errorf("worker %s has no available slots (%d/%d used)", w.ID, len(w.executors), w.Slots)
	}

	if err := exec.Start(ctx); err != nil {
		return fmt.Errorf("start pipeline %s: %w", pipelineName, err)
	}

	w.executors[pipelineName] = exec
	g.Log().Infof(ctx, "Pipeline %s assigned to worker %s (slots: %d/%d)", pipelineName, w.ID, len(w.executors), w.Slots)
	return nil
}

// RunningTasks returns the names of pipelines currently executing on this worker.
func (w *Worker) RunningTasks() []string {
	w.mu.RLock()
	defer w.mu.RUnlock()
	result := make([]string, 0, len(w.executors))
	for name := range w.executors {
		result = append(result, name)
	}
	return result
}

// AvailableSlots returns the number of free execution slots.
func (w *Worker) AvailableSlots() int {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.Slots - len(w.executors)
}

// ── Standalone mode ───────────────────────────────────────────────────

// StandaloneWorker wraps a Worker for single-process mode.
// In standalone mode, the worker executes tasks directly without
// HTTP registration to a separate master process.
type StandaloneWorker struct {
	Worker *Worker
}

// NewStandalone creates a worker that runs in the same process as the master.
func NewStandalone(store storage.Storage, slots int) *StandaloneWorker {
	w := New(Config{
		ID:    "standalone-worker",
		Host:  "localhost",
		Port:  0,
		Slots: slots,
		Store: store,
	})
	return &StandaloneWorker{Worker: w}
}

// SetTaskExecutor registers a task executor on the underlying Worker.
func (s *StandaloneWorker) SetTaskExecutor(fn func(ctx context.Context, pipelineName, shardID string) error) {
	s.Worker.SetTaskExecutor(fn)
}

// Start registers the worker in the local storage and begins heartbeat.
func (s *StandaloneWorker) Start(ctx context.Context) error {
	// In standalone mode, we register directly to the store (no HTTP)
	info := &storage.WorkerInfo{
		ID:    s.Worker.ID,
		Host:  s.Worker.Host,
		Port:  s.Worker.Port,
		Slots: s.Worker.Slots,
	}
	if err := s.Worker.store.RegisterWorker(ctx, info); err != nil {
		return fmt.Errorf("standalone register: %w", err)
	}
	s.Worker.ctx, s.Worker.cancel = context.WithCancel(ctx)

	// Start heartbeat loop (updates last_heartbeat every 5s)
	go func() {
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = s.Worker.store.Heartbeat(s.Worker.ctx, s.Worker.ID)
			case <-s.Worker.ctx.Done():
				return
			}
		}
	}()

	g.Log().Infof(ctx, "Standalone worker started: id=%s slots=%d", s.Worker.ID, s.Worker.Slots)

	// Start poll loop so this worker processes tasks dispatched by the master.
	go s.Worker.PollLoop(s.Worker.ctx)

	return nil
}

// Stop cleans up the standalone worker.
func (s *StandaloneWorker) Stop() {
	if s.Worker.cancel != nil {
		s.Worker.cancel()
	}
	s.Worker.store.DeregisterWorker(context.Background(), s.Worker.ID)
}

// ── Helpers ───────────────────────────────────────────────────────────

func jsonReader(data []byte) *bytesReader {
	return &bytesReader{data: data}
}

type bytesReader struct {
	data []byte
	pos  int
}

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, fmt.Errorf("EOF")
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *bytesReader) Close() error { return nil }

// Ensure core is used (placeholder for future imports)
var _ = core.OpInsert
