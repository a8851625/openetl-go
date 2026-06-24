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
