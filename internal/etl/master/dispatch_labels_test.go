package master

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

// newTestStore opens a fresh SQLite store backed by a temp file.
func newTestStore(t *testing.T) *sqlite.Store {
	t.Helper()
	store, err := sqlite.New(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { store.Close() })
	return store
}

// TestDispatchShardsPersistsRequiredLabels proves that match_labels flow from
// DispatchShards onto each persisted task assignment. This is the foundation
// for worker_selector enforcement: without labels on the task row, no
// downstream filter could work.
func TestDispatchShardsPersistsRequiredLabels(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	d := NewTaskDispatcher(store, NewWorkerRegistry(store))

	labels := map[string]string{"zone": "us-east-1", "gpu": "true"}
	if err := d.DispatchShards(ctx, "label-pipe", 3, labels); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	tasks, err := store.ListTasks(ctx, "label-pipe")
	if err != nil {
		t.Fatalf("list tasks: %v", err)
	}
	if len(tasks) != 3 {
		t.Fatalf("expected 3 tasks, got %d", len(tasks))
	}
	for _, tk := range tasks {
		if tk.Status != "pending" {
			t.Errorf("task %s status=%q, want pending", tk.TaskID, tk.Status)
		}
		if len(tk.RequiredLabels) != 2 || tk.RequiredLabels["zone"] != "us-east-1" || tk.RequiredLabels["gpu"] != "true" {
			t.Errorf("task %s RequiredLabels=%v, want %v", tk.TaskID, tk.RequiredLabels, labels)
		}
	}
}

// TestDispatchShardsNilLabelsPersistsEmpty proves the back-compat path: tasks
// dispatched without labels are claimable by any worker (no RequiredLabels set).
func TestDispatchShardsNilLabelsPersistsEmpty(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	d := NewTaskDispatcher(store, NewWorkerRegistry(store))

	if err := d.DispatchShards(ctx, "plain-pipe", 2, nil); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	tasks, _ := store.ListTasks(ctx, "plain-pipe")
	if len(tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(tasks))
	}
	for _, tk := range tasks {
		if len(tk.RequiredLabels) != 0 {
			t.Errorf("task %s RequiredLabels=%v, want empty", tk.TaskID, tk.RequiredLabels)
		}
	}
}

// TestAssignNextTaskFiltersByLabels proves the core worker_selector enforcement:
// a worker registered with labels can claim tasks whose RequiredLabels it
// satisfies, and is SKIPPED over for tasks whose labels it does not match.
// A worker with no labels can only claim unlabeled tasks.
func TestAssignNextTaskFiltersByLabels(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	d := NewTaskDispatcher(store, NewWorkerRegistry(store))

	// Two pipelines: one label-restricted, one open.
	if err := d.DispatchShards(ctx, "gpu-pipe", 2, map[string]string{"gpu": "true"}); err != nil {
		t.Fatalf("dispatch gpu-pipe: %v", err)
	}
	if err := d.DispatchShards(ctx, "open-pipe", 2, nil); err != nil {
		t.Fatalf("dispatch open-pipe: %v", err)
	}

	// Register two workers: gpu-worker (gpu=true) and cpu-worker (no labels).
	if err := store.RegisterWorker(ctx, &storage.WorkerInfo{
		ID: "gpu-worker", Host: "127.0.0.1", Port: 9001, Slots: 4, Status: "online",
		Labels: map[string]string{"gpu": "true"},
	}); err != nil {
		t.Fatalf("register gpu-worker: %v", err)
	}
	if err := store.RegisterWorker(ctx, &storage.WorkerInfo{
		ID: "cpu-worker", Host: "127.0.0.1", Port: 9002, Slots: 4, Status: "online",
	}); err != nil {
		t.Fatalf("register cpu-worker: %v", err)
	}

	// gpu-worker can claim gpu-pipe task (label match).
	task, err := d.AssignNextTask(ctx, "gpu-worker")
	if err != nil || task == nil {
		t.Fatalf("gpu-worker should claim a gpu-pipe task, got task=%v err=%v", task, err)
	}
	if task.RequiredLabels["gpu"] != "true" {
		t.Errorf("gpu-worker claimed task %s with labels=%v, want gpu=true", task.TaskID, task.RequiredLabels)
	}

	// cpu-worker CANNOT claim any gpu-pipe task — it must skip them and fall
	// through to the open-pipe task (no required labels).
	task2, err := d.AssignNextTask(ctx, "cpu-worker")
	if err != nil || task2 == nil {
		t.Fatalf("cpu-worker should claim the open-pipe task (skipping gpu-pipe), got task=%v err=%v", task2, err)
	}
	if len(task2.RequiredLabels) != 0 {
		t.Errorf("cpu-worker claimed labeled task %s (labels=%v); should only get unlabeled",
			task2.TaskID, task2.RequiredLabels)
	}
	if task2.Pipeline != "open-pipe" {
		t.Errorf("cpu-worker claimed task from %s, want open-pipe", task2.Pipeline)
	}
}

// TestAssignNextTaskNoMatchingWorkerStaysPending proves that when NO registered
// worker matches the required labels, the task stays pending — the honest
// behavior (no silent misassignment). This is the key fix: previously labels
// were dropped and any worker would grab the task.
func TestAssignNextTaskNoMatchingWorkerStaysPending(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	d := NewTaskDispatcher(store, NewWorkerRegistry(store))

	if err := d.DispatchShards(ctx, "iso-pipe", 2, map[string]string{"zone": "isolated"}); err != nil {
		t.Fatalf("dispatch: %v", err)
	}
	// Only a default-pool worker is registered; it does NOT match zone=isolated.
	if err := store.RegisterWorker(ctx, &storage.WorkerInfo{
		ID: "default-worker", Host: "127.0.0.1", Port: 9003, Slots: 4, Status: "online",
	}); err != nil {
		t.Fatalf("register: %v", err)
	}

	// Neither iso-pipe shard is claimable by default-worker.
	for i := 0; i < 2; i++ {
		task, err := d.AssignNextTask(ctx, "default-worker")
		if err != nil {
			t.Fatalf("AssignNextTask #%d error: %v", i, err)
		}
		if task != nil {
			t.Fatalf("default-worker must NOT claim label-restricted task, but got %s (labels=%v)",
				task.TaskID, task.RequiredLabels)
		}
	}

	// Both iso-pipe tasks must still be pending (not silently claimed).
	tasks, _ := store.ListTasks(ctx, "iso-pipe")
	if len(tasks) != 2 {
		t.Fatalf("expected 2 iso-pipe tasks to remain, got %d", len(tasks))
	}
	for _, tk := range tasks {
		if tk.Status != "pending" {
			t.Errorf("task %s status=%q, want pending", tk.TaskID, tk.Status)
		}
	}

	// Now register a matching worker — it should claim the task.
	if err := store.RegisterWorker(ctx, &storage.WorkerInfo{
		ID: "iso-worker", Host: "127.0.0.1", Port: 9004, Slots: 4, Status: "online",
		Labels: map[string]string{"zone": "isolated"},
	}); err != nil {
		t.Fatalf("register iso-worker: %v", err)
	}
	task2, err := d.AssignNextTask(ctx, "iso-worker")
	if err != nil || task2 == nil {
		t.Fatalf("iso-worker should now claim the pending task, got task=%v err=%v", task2, err)
	}
	if task2.Pipeline != "iso-pipe" {
		t.Errorf("iso-worker claimed task from %s, want iso-pipe", task2.Pipeline)
	}
}

// TestLabelsMatchUnit covers the pure matcher directly.
func TestLabelsMatchUnit(t *testing.T) {
	cases := []struct {
		name     string
		worker   map[string]string
		required map[string]string
		want     bool
	}{
		{"empty required always matches", map[string]string{"a": "1"}, nil, true},
		{"exact match", map[string]string{"a": "1"}, map[string]string{"a": "1"}, true},
		{"superset worker matches", map[string]string{"a": "1", "b": "2"}, map[string]string{"a": "1"}, true},
		{"missing key fails", map[string]string{"a": "1"}, map[string]string{"c": "3"}, false},
		{"wrong value fails", map[string]string{"a": "1"}, map[string]string{"a": "2"}, false},
		{"nil worker with required fails", nil, map[string]string{"a": "1"}, false},
		{"both empty matches", nil, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := labelsMatch(c.worker, c.required); got != c.want {
				t.Errorf("labelsMatch(%v, %v) = %v, want %v", c.worker, c.required, got, c.want)
			}
		})
	}
}
