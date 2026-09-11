package backup_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/backup"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

// TestRestoreAtomicityLeavesNoHalfState seeds a store, exports a snapshot,
// clears it, and runs a restore whose injected failure aborts mid-way. The
// pre-restore state must be fully intact afterwards and a retry must reach a
// consistent final state (IT-3/T3.4 acceptance 8).
func TestRestoreAtomicityLeavesNoHalfState(t *testing.T) {
	ctx := context.Background()
	raw, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })

	seedState := func(db *sqlite.Store) {
		if err := db.SavePipeline(ctx, &storage.PipelineRow{Name: "alpha", Status: "running"}); err != nil {
			t.Fatalf("seed pipeline: %v", err)
		}
		if err := db.SavePipeline(ctx, &storage.PipelineRow{Name: "beta", Status: "stopped"}); err != nil {
			t.Fatalf("seed pipeline: %v", err)
		}
		if err := db.WriteAudit(ctx, &storage.AuditEntry{Action: "seed"}); err != nil {
			t.Fatalf("seed audit: %v", err)
		}
	}
	seedState(raw)

	snap, err := backup.Export(ctx, raw, backup.Options{Backend: "sqlite"})
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	// Wipe and restore with an injected run-history row whose id collides with
	// an earlier row in the same snapshot: the second insert violates the
	// primary key and fails after pipeline + first run rows were written.
	snap2 := *snap
	seed := &storage.RunRecord{
		ID: 987654, JobName: "collide", Status: "succeeded", StartedAt: time.Now().UTC(),
		FinishedAt: ptrTime(time.Now().UTC()),
	}
	snap2.RunHistory = append(append([]*storage.RunRecord{}, snap.RunHistory...), seed, seed)
	if err := backup.Restore(ctx, raw, &snap2, backup.Options{ClearBeforeRestore: true}); err == nil {
		t.Fatalf("expected restore to fail on duplicate run id")
	}

	// Atomicity: the invalid rows are gone, but the original pipelines/audit
	// from the pre-restore state are still present (rollback, not half-clear).
	pipes, err := raw.ListPipelines(ctx)
	if err != nil {
		t.Fatalf("list pipelines: %v", err)
	}
	names := map[string]bool{}
	for _, p := range pipes {
		names[p.Name] = true
	}
	if !names["alpha"] || !names["beta"] {
		t.Fatalf("pre-restore state lost after failed restore: %+v", names)
	}

	// Retry with the clean snapshot: consistent final state.
	if err := backup.Restore(ctx, raw, snap, backup.Options{ClearBeforeRestore: true}); err != nil {
		t.Fatalf("retry restore: %v", err)
	}
	pipes2, err := raw.ListPipelines(ctx)
	if err != nil || len(pipes2) != 2 {
		t.Fatalf("post-retry pipelines=%d err=%v", len(pipes2), err)
	}
}

// TestRestoreFidelityPreservesIDsAndTimestamps verifies run history IDs,
// timestamps and status survive restore byte-identically instead of being
// recomputed (IT-3/T3.4 acceptance 9).
func ptrTime(t time.Time) *time.Time { return &t }

func TestRestoreFidelityPreservesIDsAndTimestamps(t *testing.T) {
	ctx := context.Background()
	raw, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })

	fixed := time.Date(2026, 8, 7, 8, 30, 0, 123000000, time.UTC)
	before := &storage.RunRecord{
		ID: 901, JobName: "fidelity", Status: "failed", StartedAt: fixed,
		FinishedAt: &fixed, DurationMs: 4321, RecordsRead: 100,
		RecordsFailed: 100, RecordsDLQ: 7,
	}
	snap := &backup.Snapshot{FormatVersion: backup.FormatVersion, RunHistory: []*storage.RunRecord{before}}

	raw2, err := sqlite.New(filepath.Join(t.TempDir(), "restore.db"))
	if err != nil {
		t.Fatalf("sqlite.New restore: %v", err)
	}
	t.Cleanup(func() { _ = raw2.Close() })
	if err := backup.Restore(ctx, raw2, snap, backup.Options{ClearBeforeRestore: true}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	restored, err := raw2.ListRunHistoryPaged(ctx, 0, 10)
	if err != nil || len(restored) != 1 {
		t.Fatalf("restored runs=%d err=%v", len(restored), err)
	}
	after := restored[0]
	if after.ID != before.ID {
		t.Fatalf("run id changed: %d -> %d", before.ID, after.ID)
	}
	if !after.StartedAt.Equal(before.StartedAt) {
		t.Fatalf("started_at changed: %v -> %v", before.StartedAt, after.StartedAt)
	}
	if after.FinishedAt == nil || !after.FinishedAt.Equal(*before.FinishedAt) {
		t.Fatalf("finished_at changed")
	}
	if after.Status != before.Status || after.DurationMs != before.DurationMs {
		t.Fatalf("status/duration changed: %+v vs %+v", after, before)
	}
	if after.RecordsFailed != before.RecordsFailed || after.RecordsDLQ != before.RecordsDLQ {
		t.Fatalf("stats changed")
	}
}
