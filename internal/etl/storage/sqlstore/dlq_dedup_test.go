package sqlstore_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	_ "modernc.org/sqlite"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

// TestMigrationDeduplicatesLegacyDeadLetters is the PR-DLQ-2 migration
// acceptance: a database written before the (job_name, record_hash) unique key
// can contain duplicate rows and rows with an empty record_hash. Migration must
// backfill the hash, drop duplicates keeping the oldest row, and leave a unique
// index that makes future writes idempotent.
func TestMigrationDeduplicatesLegacyDeadLetters(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatalf("open raw sqlite: %v", err)
	}
	// Pre-v24 schema: no unique index, no record_hash values.
	if _, err := raw.Exec(`CREATE TABLE dead_letters (
		id          INTEGER PRIMARY KEY AUTOINCREMENT,
		job_name    TEXT NOT NULL,
		record_json TEXT NOT NULL,
		error       TEXT,
		error_class TEXT,
		identity_context_json TEXT,
		attempt     INTEGER DEFAULT 0,
		record_hash TEXT,
		pipeline_version INTEGER DEFAULT 0,
		dag_node    TEXT,
		created_at  DATETIME DEFAULT CURRENT_TIMESTAMP
	)`); err != nil {
		t.Fatalf("create legacy table: %v", err)
	}
	insertLegacy := func(job, recordJSON string) {
		t.Helper()
		if _, err := raw.Exec(
			`INSERT INTO dead_letters (job_name, record_json, error, error_class, attempt, record_hash, created_at)
			 VALUES (?, ?, 'boom', 'sink', 1, '', CURRENT_TIMESTAMP)`, job, recordJSON); err != nil {
			t.Fatalf("insert legacy row: %v", err)
		}
	}
	// The legacy rows use the exact JSON the storage layer produces for the same
	// record, so the dedup key matches what a live write would compute.
	rec1 := mustRecordJSON(t, map[string]any{"id": 1})
	rec2 := mustRecordJSON(t, map[string]any{"id": 2})
	// Three captures of the same record (source replay / restart) plus one
	// distinct record for the same job and one identical record for another job.
	insertLegacy("pipe-a", rec1)
	insertLegacy("pipe-a", rec1)
	insertLegacy("pipe-a", rec1)
	insertLegacy("pipe-a", rec2)
	insertLegacy("pipe-b", rec1)
	if err := raw.Close(); err != nil {
		t.Fatalf("close raw: %v", err)
	}

	// Reopening through the real store runs the migrations.
	store, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("sqlite.New after legacy schema: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	countA, err := store.CountDeadLetters(ctx, "pipe-a")
	if err != nil {
		t.Fatalf("count pipe-a: %v", err)
	}
	if countA != 2 {
		t.Fatalf("pipe-a rows = %d, want 2 (deduplicated by record hash)", countA)
	}
	countB, err := store.CountDeadLetters(ctx, "pipe-b")
	if err != nil {
		t.Fatalf("count pipe-b: %v", err)
	}
	if countB != 1 {
		t.Fatalf("pipe-b rows = %d, want 1 (same record in another job is not a duplicate)", countB)
	}

	rows, err := store.ListDeadLetters(ctx, storage.DLQFilter{JobName: "pipe-a", Limit: 10})
	if err != nil {
		t.Fatalf("list pipe-a: %v", err)
	}
	for _, row := range rows {
		if row.RecordHash == "" {
			t.Fatalf("record_hash was not backfilled: %#v", row)
		}
	}

	// The unique index must exist now: a second write of the same record is an
	// upsert (attempt incremented) rather than a new row.
	if err := store.WriteDeadLetter(ctx, &storage.DLQRecord{
		JobName:    "pipe-a",
		Record:     core.Record{Data: map[string]any{"id": 1}},
		Error:      "boom again",
		ErrorClass: "sink",
	}); err != nil {
		t.Fatalf("rewrite duplicate after migration: %v", err)
	}
	after, err := store.ListDeadLetters(ctx, storage.DLQFilter{JobName: "pipe-a", Limit: 10})
	if err != nil {
		t.Fatalf("list after rewrite: %v", err)
	}
	if len(after) != 2 {
		t.Fatalf("rows after rewrite = %d, want 2", len(after))
	}
	for _, row := range after {
		if row.Record.Data["id"] == float64(1) {
			// The migration keeps the oldest row (attempt 1); the live rewrite
			// then increments it to 2.
			if row.Attempt != 2 {
				t.Fatalf("attempt = %d, want 2 after dedup + duplicate capture", row.Attempt)
			}
		}
	}
}

// TestDeletePipelineWithCheckpointPurgesDeadLetters proves deleting a pipeline
// no longer leaves unreplayable DLQ rows that stay invisible in the UI while
// still consuming the global DLQ retention budget.
func mustRecordJSON(t *testing.T, data map[string]any) string {
	t.Helper()
	raw, err := json.Marshal(core.Record{Data: data})
	if err != nil {
		t.Fatalf("marshal record: %v", err)
	}
	return string(raw)
}

func TestDeletePipelineWithCheckpointPurgesDeadLetters(t *testing.T) {
	store, err := sqlite.New(filepath.Join(t.TempDir(), "lifecycle.db"))
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	if err := store.SavePipeline(ctx, &storage.PipelineRow{ID: "pipe-id", Name: "pipe-name", Status: "stopped"}); err != nil {
		t.Fatalf("save pipeline: %v", err)
	}
	if err := store.SaveCheckpoint(ctx, &storage.CheckpointRecord{JobName: "pipe-id", Source: "file", Position: []byte(`{}`)}); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}
	for i := 0; i < 2; i++ {
		if err := store.WriteDeadLetter(ctx, &storage.DLQRecord{
			JobName: "pipe-id",
			Record:  core.Record{Data: map[string]any{"id": i}},
			Error:   "x",
		}); err != nil {
			t.Fatalf("write dlq: %v", err)
		}
	}

	if err := store.DeletePipelineWithCheckpoint(ctx, "pipe-id"); err != nil {
		t.Fatalf("delete pipeline: %v", err)
	}
	n, err := store.CountDeadLetters(ctx, "pipe-id")
	if err != nil {
		t.Fatalf("count dlq: %v", err)
	}
	if n != 0 {
		t.Fatalf("dead letters remaining after pipeline delete = %d, want 0", n)
	}
}

// TestListDeadLettersAscendingOrdersOldestFirst proves replay can drain a
// backlog oldest-first instead of only ever retrying the newest page.
func TestListDeadLettersAscendingOrdersOldestFirst(t *testing.T) {
	store, err := sqlite.New(filepath.Join(t.TempDir(), "order.db"))
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	base := time.Now().UTC().Add(-time.Hour)
	raw := store.DB()
	for i := 0; i < 3; i++ {
		rec, err := json.Marshal(core.Record{Data: map[string]any{"id": i}})
		if err != nil {
			t.Fatalf("marshal: %v", err)
		}
		if _, err := raw.ExecContext(ctx,
			`INSERT INTO dead_letters (job_name, record_json, error, error_class, attempt, created_at)
			 VALUES ('order-pipe', ?, 'boom', 'sink', 1, ?)`, rec, base.Add(time.Duration(i)*time.Minute)); err != nil {
			t.Fatalf("insert: %v", err)
		}
	}

	desc, err := store.ListDeadLetters(ctx, storage.DLQFilter{JobName: "order-pipe", Limit: 10})
	if err != nil {
		t.Fatalf("list desc: %v", err)
	}
	asc, err := store.ListDeadLetters(ctx, storage.DLQFilter{JobName: "order-pipe", Limit: 10, Ascending: true})
	if err != nil {
		t.Fatalf("list asc: %v", err)
	}
	if len(desc) != 3 || len(asc) != 3 {
		t.Fatalf("row counts desc/asc = %d/%d, want 3/3", len(desc), len(asc))
	}
	if desc[0].Record.Data["id"] != float64(2) {
		t.Fatalf("descending first id = %v, want newest (2)", desc[0].Record.Data["id"])
	}
	if asc[0].Record.Data["id"] != float64(0) {
		t.Fatalf("ascending first id = %v, want oldest (0)", asc[0].Record.Data["id"])
	}
}

// TestPurgeOldestDeadLettersIsDeterministic covers the dedicated max-count path
// introduced so retention never depends on a DELETE ... LIMIT without ORDER BY.
func TestPurgeOldestDeadLettersIsDeterministic(t *testing.T) {
	store, err := sqlite.New(filepath.Join(t.TempDir(), "purge.db"))
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	defer store.Close()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		if err := store.WriteDeadLetter(ctx, &storage.DLQRecord{
			JobName: "purge-pipe",
			Record:  core.Record{Data: map[string]any{"id": i}},
			Error:   "x",
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}
	deleted, err := store.PurgeOldestDeadLetters(ctx, 2)
	if err != nil {
		t.Fatalf("purge oldest: %v", err)
	}
	if deleted != 2 {
		t.Fatalf("deleted = %d, want 2", deleted)
	}
	remaining, err := store.ListDeadLetters(ctx, storage.DLQFilter{JobName: "purge-pipe", Limit: 10, Ascending: true})
	if err != nil {
		t.Fatalf("list remaining: %v", err)
	}
	if len(remaining) != 3 {
		t.Fatalf("remaining = %d, want 3", len(remaining))
	}
	if remaining[0].Record.Data["id"] != float64(2) {
		t.Fatalf("oldest surviving id = %v, want 2 (the two oldest were purged)", remaining[0].Record.Data["id"])
	}
}
