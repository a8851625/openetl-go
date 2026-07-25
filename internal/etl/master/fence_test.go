package master_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/master"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func TestReportTaskResultFencesStaleOwner(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.New(filepath.Join(dir, "report.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	m := master.NewMaster(store)
	if err := m.Dispatcher().DispatchShards(ctx, "fence-pipe", 2, nil); err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	// Claim as w-old.
	task, err := store.ClaimTask(ctx, "w-old", nil, time.Second)
	if err != nil || task == nil {
		t.Fatalf("claim: %v %v", task, err)
	}
	oldGen := task.Generation

	// Requeue + claim as w-new (simulates lease expiry path).
	task.Status = "pending"
	task.WorkerID = ""
	task.LeaseExpiresAt = nil
	_ = store.UpdateTask(ctx, task)
	newTask, err := store.ClaimTask(ctx, "w-new", nil, time.Second)
	if err != nil || newTask == nil {
		t.Fatalf("reclaim: %v %v", newTask, err)
	}

	// Old owner reports completed → fenced.
	d := m.Dispatcher().(*master.TaskDispatcher)
	err = d.ReportTaskResult(ctx, newTask.TaskID, "w-old", oldGen, "completed", "")
	if !errors.Is(err, storage.ErrTaskFenced) {
		t.Fatalf("stale report err = %v, want ErrTaskFenced", err)
	}

	// New owner can complete.
	if err := d.ReportTaskResult(ctx, newTask.TaskID, "w-new", newTask.Generation, "completed", ""); err != nil {
		t.Fatalf("new owner report: %v", err)
	}
	got, _ := store.GetTask(ctx, newTask.TaskID)
	if got.Status != "completed" || got.WorkerID != "w-new" {
		t.Fatalf("final = %+v", got)
	}
}

func TestReassignStaleTasksLeaseAndMaxAttempts(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.New(filepath.Join(dir, "reassign.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	m := master.NewMaster(store)
	d := m.Dispatcher().(*master.TaskDispatcher)
	d.SetMaxAttempts(2)
	d.SetLeaseTTL(50 * time.Millisecond)

	// Task held by offline worker.
	past := time.Now().Add(-time.Minute)
	if err := store.CreateTask(ctx, &storage.TaskAssignment{
		TaskID: "t-stale", Pipeline: "p", Status: "running",
		WorkerID: "dead-worker", Generation: 1, Attempt: 1,
		LeaseExpiresAt: &past,
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	// No worker registered → offline.
	d.ReassignStaleTasks(ctx)
	got, _ := store.GetTask(ctx, "t-stale")
	if got.Status != "pending" || got.WorkerID != "" {
		t.Fatalf("after requeue = %+v", got)
	}
	if got.LastError == "" {
		t.Fatal("expected last_error set on requeue")
	}

	// Exhaust attempts: attempt already 2 after next claim+requeue cycle.
	claimed, err := store.ClaimTask(ctx, "w-temp", nil, time.Millisecond)
	if err != nil || claimed == nil {
		t.Fatalf("claim: %v %v", claimed, err)
	}
	// Force lease expired while worker is "online" so lease path triggers.
	_ = store.RegisterWorker(ctx, &storage.WorkerInfo{ID: "w-temp", Host: "h", Slots: 1, Status: "online"})
	_ = store.Heartbeat(ctx, "w-temp")
	expired := time.Now().Add(-time.Second)
	claimed.Status = "running"
	claimed.LeaseExpiresAt = &expired
	_ = store.UpdateTask(ctx, claimed)

	// attempt is 2, maxAttempts=2 → permanent fail.
	d.ReassignStaleTasks(ctx)
	final, _ := store.GetTask(ctx, "t-stale")
	if final.Status != "failed" {
		t.Fatalf("expected failed after max attempts, got %+v", final)
	}
	if final.LastError == "" {
		t.Fatal("expected last_error on permanent failure")
	}
}
