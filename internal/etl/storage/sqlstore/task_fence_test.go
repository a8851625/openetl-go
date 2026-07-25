package sqlstore_test

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func TestClaimAndCASFence(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.New(filepath.Join(dir, "fence.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	if err := store.CreateTask(ctx, &storage.TaskAssignment{
		TaskID: "t-fence-1", Pipeline: "p", Status: "pending",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}

	claimed, err := store.ClaimTask(ctx, "w-a", nil, 2*time.Second)
	if err != nil || claimed == nil {
		t.Fatalf("claim: task=%v err=%v", claimed, err)
	}
	if claimed.Generation != 1 || claimed.Attempt != 1 || claimed.WorkerID != "w-a" {
		t.Fatalf("claim fields = gen=%d attempt=%d worker=%s", claimed.Generation, claimed.Attempt, claimed.WorkerID)
	}
	if claimed.LeaseExpiresAt == nil {
		t.Fatal("expected lease_expires_at")
	}

	// Second claim should find nothing pending.
	again, err := store.ClaimTask(ctx, "w-b", nil, time.Second)
	if err != nil {
		t.Fatalf("second claim err: %v", err)
	}
	if again != nil {
		t.Fatalf("expected nil claim, got %+v", again)
	}

	// Owner can CAS-update to running.
	now := time.Now()
	claimed.Status = "running"
	claimed.StartedAt = &now
	if err := store.CASUpdateTask(ctx, claimed); err != nil {
		t.Fatalf("cas running: %v", err)
	}

	// Stale owner (wrong generation) is fenced.
	stale := *claimed
	stale.Generation = 0
	stale.Status = "completed"
	finished := time.Now()
	stale.FinishedAt = &finished
	if err := store.CASUpdateTask(ctx, &stale); !errors.Is(err, storage.ErrTaskFenced) {
		t.Fatalf("stale gen CAS err = %v, want ErrTaskFenced", err)
	}

	// Wrong worker is fenced.
	wrongWorker := *claimed
	wrongWorker.WorkerID = "w-b"
	wrongWorker.Status = "completed"
	wrongWorker.FinishedAt = &finished
	if err := store.CASUpdateTask(ctx, &wrongWorker); !errors.Is(err, storage.ErrTaskFenced) {
		t.Fatalf("wrong worker CAS err = %v, want ErrTaskFenced", err)
	}

	// Legitimate completion succeeds.
	claimed.Status = "completed"
	claimed.FinishedAt = &finished
	if err := store.CASUpdateTask(ctx, claimed); err != nil {
		t.Fatalf("owner complete: %v", err)
	}

	got, err := store.GetTask(ctx, "t-fence-1")
	if err != nil || got == nil {
		t.Fatalf("get: %v %v", got, err)
	}
	if got.Status != "completed" || got.Generation != 1 {
		t.Fatalf("final task = %+v", got)
	}
}

func TestReclaimAfterRequeueBumpsGeneration(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.New(filepath.Join(dir, "reclaim.db"))
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	if err := store.CreateTask(ctx, &storage.TaskAssignment{
		TaskID: "t-reclaim", Pipeline: "p", Status: "pending",
	}); err != nil {
		t.Fatalf("create: %v", err)
	}
	first, err := store.ClaimTask(ctx, "w-old", nil, time.Millisecond)
	if err != nil || first == nil {
		t.Fatalf("first claim: %v %v", first, err)
	}
	// Simulate master requeue: status pending, keep generation, clear worker.
	first.Status = "pending"
	first.WorkerID = ""
	first.LeaseExpiresAt = nil
	first.LastError = "lease expired"
	if err := store.UpdateTask(ctx, first); err != nil {
		t.Fatalf("requeue: %v", err)
	}

	second, err := store.ClaimTask(ctx, "w-new", nil, time.Second)
	if err != nil || second == nil {
		t.Fatalf("second claim: %v %v", second, err)
	}
	if second.Generation != first.Generation+1 {
		t.Fatalf("generation after reclaim = %d, want %d", second.Generation, first.Generation+1)
	}
	if second.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", second.Attempt)
	}

	// Old owner completion must not overwrite.
	oldDone := *first
	oldDone.Status = "completed"
	fin := time.Now()
	oldDone.FinishedAt = &fin
	if err := store.CASUpdateTask(ctx, &oldDone); !errors.Is(err, storage.ErrTaskFenced) {
		t.Fatalf("old owner complete err = %v, want ErrTaskFenced", err)
	}
	got, _ := store.GetTask(ctx, "t-reclaim")
	if got.WorkerID != "w-new" || got.Status != "assigned" {
		t.Fatalf("task after fenced complete = %+v", got)
	}
}
