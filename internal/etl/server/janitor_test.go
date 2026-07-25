package server

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
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
