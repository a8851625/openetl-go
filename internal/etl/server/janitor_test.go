package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/alert"
	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
	"github.com/a8851625/openetl-go/internal/etl/telemetry"
)

func TestRetentionJanitorPurgesDLQAndAudit(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.New(filepath.Join(dir, "janitor.db"))
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	// Seed DLQ + audit.
	for i := 0; i < 3; i++ {
		if err := store.WriteDeadLetter(ctx, &storage.DLQRecord{
			JobName: "p",
			Record:  core.Record{Data: map[string]any{"i": i}},
			Error:   "x",
		}); err != nil {
			t.Fatalf("dlq: %v", err)
		}
	}
	if err := store.WriteAudit(ctx, &storage.AuditEntry{Action: "a", Method: "GET", Path: "/"}); err != nil {
		t.Fatalf("audit: %v", err)
	}

	s := &Server{
		store: store,
		retention: RetentionConfig{
			DLQTTL:     time.Nanosecond, // everything is older
			AuditTTL:   time.Nanosecond,
			BatchLimit: 1000,
			Interval:   time.Minute,
		},
		janitor: &janitorState{},
	}
	// Ensure rows are older than the TTL cutoff.
	time.Sleep(2 * time.Millisecond)
	s.runRetentionJanitor(ctx)

	status := s.JanitorStatusSnapshot()
	if status.LastDeleted < 1 {
		t.Fatalf("expected deletions, status=%+v", status)
	}
	n, err := store.CountDeadLetters(ctx, "p")
	if err != nil {
		t.Fatalf("count dlq: %v", err)
	}
	if n != 0 {
		t.Fatalf("dlq remaining = %d, want 0", n)
	}
	var purger storage.RetentionPurger = store
	counts, err := purger.CountObjects(ctx)
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	if counts.AuditLogs != 0 {
		t.Fatalf("audit remaining = %d", counts.AuditLogs)
	}
}

func TestRetentionConfigHardCaps(t *testing.T) {
	t.Setenv("ETL_DLQ_MAX_COUNT", "99999999")
	t.Setenv("ETL_JANITOR_BATCH_LIMIT", "99999999")
	t.Setenv("ETL_DLQ_TTL", "1h")
	cfg := loadRetentionConfig(context.Background(), 0, 0)
	if cfg.DLQMaxCount != MaxDLQMaxCount {
		t.Errorf("dlq max = %d, want %d", cfg.DLQMaxCount, MaxDLQMaxCount)
	}
	if cfg.BatchLimit != MaxJanitorBatchLimit {
		t.Errorf("batch = %d, want %d", cfg.BatchLimit, MaxJanitorBatchLimit)
	}
	if !cfg.enabled() {
		t.Fatal("expected enabled")
	}
}

func TestEmptyJobNameDLQPurge(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.New(filepath.Join(dir, "dlq.db"))
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	_ = store.WriteDeadLetter(ctx, &storage.DLQRecord{
		JobName: "a", Record: core.Record{Data: map[string]any{"x": 1}}, Error: "e",
	})
	_ = store.WriteDeadLetter(ctx, &storage.DLQRecord{
		JobName: "b", Record: core.Record{Data: map[string]any{"x": 2}}, Error: "e",
	})
	n, err := store.DeleteDeadLettersByFilter(ctx, storage.DLQFilter{
		Until: time.Now().Add(time.Hour),
		Limit: 100,
	})
	if err != nil {
		t.Fatalf("purge: %v", err)
	}
	if n != 2 {
		t.Fatalf("deleted = %d, want 2 (cross-pipeline)", n)
	}
}

// TestRetentionJanitorMaxCountEvictsOldest is the regression for the
// implementation-defined DELETE ... LIMIT: max-count eviction must remove the
// OLDEST rows deterministically, never recent failures an operator has not seen
// yet, and it must be observable (metric + alert) instead of a silent Info log.
func TestRetentionJanitorMaxCountEvictsOldest(t *testing.T) {
	dir := t.TempDir()
	store, err := sqlite.New(filepath.Join(dir, "janitor-max.db"))
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 6; i++ {
		if err := store.WriteDeadLetter(ctx, &storage.DLQRecord{
			JobName:   "max-count",
			Record:    core.Record{Data: map[string]any{"i": i}},
			Error:     "x",
			CreatedAt: base.Add(time.Duration(i) * time.Minute),
		}); err != nil {
			t.Fatalf("dlq %d: %v", i, err)
		}
	}

	am := alert.NewManager()
	t.Cleanup(am.Close)
	s := &Server{
		store: store,
		retention: RetentionConfig{
			DLQMaxCount: 4,
			BatchLimit:  100,
			Interval:    time.Minute,
		},
		janitor:      &janitorState{},
		alertManager: am,
	}
	before := telemetry.DefaultRegistry.Counter("dlq_evicted_total").Value()
	s.runRetentionJanitor(ctx)

	remaining, err := store.ListDeadLetters(ctx, storage.DLQFilter{JobName: "max-count", Limit: 100, Ascending: true})
	if err != nil {
		t.Fatalf("list remaining: %v", err)
	}
	if len(remaining) != 4 {
		t.Fatalf("remaining = %d, want 4", len(remaining))
	}
	// Oldest two (i=0,1) must be evicted; the newest failures survive.
	for _, rec := range remaining {
		if rec.Record.Data["i"] == 0 || rec.Record.Data["i"] == 1 {
			t.Fatalf("max-count evicted a recent failure instead of the oldest: %#v", rec.Record.Data)
		}
	}
	if evicted := telemetry.DefaultRegistry.Counter("dlq_evicted_total").Value() - before; evicted != 2 {
		t.Fatalf("dlq_evicted_total delta = %d, want 2", evicted)
	}
}
