package worker

import (
	"context"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/factory"
)

func TestPollLoopRespectsSlotsLimit(t *testing.T) {
	dir := t.TempDir()
	store, err := factory.NewStore(context.Background(), "sqlite", filepath.Join(dir, "cp"), filepath.Join(dir, "dlq"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}

	task1 := &storage.TaskAssignment{TaskID: "task-1", Pipeline: "p", Status: "pending", ShardIndex: 0, ShardTotal: 2}
	task2 := &storage.TaskAssignment{TaskID: "task-2", Pipeline: "p", Status: "pending", ShardIndex: 1, ShardTotal: 2}
	if err := store.CreateTask(context.Background(), task1); err != nil {
		t.Fatalf("CreateTask(task1): %v", err)
	}
	if err := store.CreateTask(context.Background(), task2); err != nil {
		t.Fatalf("CreateTask(task2): %v", err)
	}

	w := New(Config{
		ID:    "w-p5-11",
		Host:  "127.0.0.1",
		Slots: 1,
		Store: store,
	})

	var current int64
	var maxSeen int64
	started := make(chan string, 4)
	release := make(chan struct{})
	w.SetTaskExecutor(func(ctx context.Context, task *storage.TaskAssignment) error {
		now := atomic.AddInt64(&current, 1)
		defer atomic.AddInt64(&current, -1)
		for {
			prev := atomic.LoadInt64(&maxSeen)
			if now <= prev || atomic.CompareAndSwapInt64(&maxSeen, prev, now) {
				break
			}
		}
		started <- task.TaskID
		<-release
		return nil
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go w.PollLoop(ctx)

	select {
	case got := <-started:
		if got != "task-1" && got != "task-2" {
			t.Fatalf("unexpected first task %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first task to start")
	}

	time.Sleep(250 * time.Millisecond)
	select {
	case got := <-started:
		t.Fatalf("second task %q started before slot was released", got)
	default:
	}

	if got := atomic.LoadInt64(&maxSeen); got != 1 {
		t.Fatalf("max concurrent tasks = %d, want 1", got)
	}

	close(release)

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for second task to start after release")
	}
}

// TestPollTaskFromStoreFiltersByLabels proves the standalone worker poll path
// honors worker_selector.match_labels: a worker whose registered Labels do not
// match a task's RequiredLabels cannot claim it, while a matching worker can.
func TestPollTaskFromStoreFiltersByLabels(t *testing.T) {
	dir := t.TempDir()
	store, err := factory.NewStore(context.Background(), "sqlite", filepath.Join(dir, "cp"), filepath.Join(dir, "dlq"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	ctx := context.Background()

	// Two tasks: one label-restricted, one open.
	if err := store.CreateTask(ctx, &storage.TaskAssignment{
		TaskID: "gpu-task", Pipeline: "gpu-pipe", Status: "pending",
		ShardIndex: 0, ShardTotal: 1, RequiredLabels: map[string]string{"gpu": "true"},
	}); err != nil {
		t.Fatalf("CreateTask gpu-task: %v", err)
	}
	if err := store.CreateTask(ctx, &storage.TaskAssignment{
		TaskID: "open-task", Pipeline: "open-pipe", Status: "pending",
		ShardIndex: 0, ShardTotal: 1,
	}); err != nil {
		t.Fatalf("CreateTask open-task: %v", err)
	}

	// cpu-worker (no labels) must skip gpu-task and claim open-task.
	cpu := New(Config{ID: "cpu-worker", Host: "127.0.0.1", Slots: 2, Store: store})
	got, err := cpu.pollTaskFromStore(ctx)
	if err != nil || got == nil {
		t.Fatalf("cpu-worker should claim open-task, got task=%v err=%v", got, err)
	}
	if got.TaskID != "open-task" {
		t.Errorf("cpu-worker claimed %s, want open-task (should skip gpu-task)", got.TaskID)
	}

	// gpu-worker (gpu=true) claims the remaining gpu-task.
	gpu := New(Config{ID: "gpu-worker", Host: "127.0.0.1", Slots: 2,
		Labels: map[string]string{"gpu": "true"}, Store: store})
	got2, err := gpu.pollTaskFromStore(ctx)
	if err != nil || got2 == nil {
		t.Fatalf("gpu-worker should claim gpu-task, got task=%v err=%v", got2, err)
	}
	if got2.TaskID != "gpu-task" {
		t.Errorf("gpu-worker claimed %s, want gpu-task", got2.TaskID)
	}

	// gpu-task is now claimed; re-polling with a third unmatched worker yields nil.
	other := New(Config{ID: "other-worker", Host: "127.0.0.1", Slots: 2,
		Labels: map[string]string{"zone": "other"}, Store: store})
	got3, err := other.pollTaskFromStore(ctx)
	if err != nil {
		t.Fatalf("poll error: %v", err)
	}
	if got3 != nil {
		t.Errorf("no eligible tasks should remain, but claimed %s", got3.TaskID)
	}
}

// TestLabelsSatisfyUnit covers the pure matcher directly.
func TestLabelsSatisfyUnit(t *testing.T) {
	cases := []struct {
		name     string
		worker   map[string]string
		required map[string]string
		want     bool
	}{
		{"empty required always matches", map[string]string{"a": "1"}, nil, true},
		{"exact match", map[string]string{"gpu": "true"}, map[string]string{"gpu": "true"}, true},
		{"superset worker matches", map[string]string{"gpu": "true", "zone": "x"}, map[string]string{"gpu": "true"}, true},
		{"missing key fails", map[string]string{"a": "1"}, map[string]string{"gpu": "true"}, false},
		{"wrong value fails", map[string]string{"gpu": "false"}, map[string]string{"gpu": "true"}, false},
		{"nil worker with required fails", nil, map[string]string{"gpu": "true"}, false},
		{"both empty matches", nil, nil, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := labelsSatisfy(c.worker, c.required); got != c.want {
				t.Errorf("labelsSatisfy(%v, %v) = %v, want %v", c.worker, c.required, got, c.want)
			}
		})
	}
}
