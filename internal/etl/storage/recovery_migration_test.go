package storage_test

import (
	"context"
	"database/sql"
	"os"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/mysql"
	"github.com/a8851625/openetl-go/internal/etl/storage/postgres"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func recoveryReopener(t *testing.T, s storage.Storage) (*sql.DB, func() storage.Storage, string) {
	t.Helper()
	var db *sql.DB
	var open func() (storage.Storage, error)
	dropIndex := "DROP INDEX idx_dlq_recovery_event"
	switch store := s.(type) {
	case *sqlite.Store:
		db = store.DB()
		var seq int
		var name, path string
		if err := db.QueryRow("PRAGMA database_list").Scan(&seq, &name, &path); err != nil {
			t.Fatal(err)
		}
		open = func() (storage.Storage, error) { return sqlite.New(path) }
	case *mysql.Store:
		db = store.DB()
		dropIndex += " ON dead_letters"
		open = func() (storage.Storage, error) { return mysql.New(os.Getenv("MYSQL_DSN")) }
	case *postgres.Store:
		db = store.DB()
		open = func() (storage.Storage, error) { return postgres.New(context.Background(), os.Getenv("POSTGRES_DSN")) }
	default:
		t.Fatalf("unsupported recovery test store %T", s)
	}
	return db, func() storage.Storage {
		next, err := open()
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = next.Close() })
		return next
	}, dropIndex
}

func testRecoveryLegacyMigration(t *testing.T, factory func(*testing.T) (storage.Storage, func())) {
	s, cleanup := factory(t)
	defer cleanup()
	ctx := context.Background()
	const job = "ac1bd7fd-b6e0-453d-8917-5820519bca07"
	if err := s.SavePipeline(ctx, &storage.PipelineRow{ID: job, Name: "legacy recovery", Status: "stopped", SpecYAML: "name: legacy recovery"}); err != nil {
		t.Fatal(err)
	}
	record := &storage.DLQRecord{JobName: job, Record: core.Record{Data: map[string]any{"id": 1}}, Error: "old outage"}
	if err := s.WriteDeadLetter(ctx, record); err != nil {
		t.Fatal(err)
	}
	before, err := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: job})
	if err != nil || len(before) != 1 {
		t.Fatal("legacy fixture missing")
	}
	db, reopen, dropIndex := recoveryReopener(t, s)
	// Reconstruct the previously advertised schema, with the original row intact.
	// Each backend runs its real startup migration against this schema.
	for _, query := range []string{
		dropIndex, "DROP TABLE recovery_operations", "DROP TABLE recovery_events",
		"ALTER TABLE dead_letters DROP COLUMN recovery_event_id",
		"ALTER TABLE dead_letters DROP COLUMN recovery_operation_id",
		"ALTER TABLE dead_letters DROP COLUMN recovery_context_json",
		"ALTER TABLE dead_letters DROP COLUMN recovery_tracking_error",
		"DELETE FROM _schema_version WHERE version >= 26",
	} {
		if _, err := db.Exec(query); err != nil {
			t.Fatalf("legacy schema fixture %s: %v", query, err)
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = reopen()
	after, err := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: job})
	if err != nil || len(after) != 1 || after[0].ID != before[0].ID || after[0].RecordHash != before[0].RecordHash || after[0].RecoveryEventID != "" || after[0].RecoveryContext.Endpoint != "" {
		t.Fatalf("legacy migration altered identity or guessed association: %+v err=%v", after, err)
	}
	tracked := &storage.DLQRecord{JobName: job, Record: core.Record{Data: map[string]any{"id": 2}}, Error: "new outage",
		RecoveryContext: core.RecoveryCaptureContext{Endpoint: "sink", ConnectionKnown: true}}
	if err := s.WriteDeadLetter(ctx, tracked); err != nil || tracked.RecoveryEventID == "" {
		t.Fatalf("capture after migration: %+v err=%v", tracked, err)
	}
	events, err := s.(storage.RecoveryStore).ListRecoveryEvents(ctx, storage.RecoveryFilter{PipelineID: job})
	if err != nil || len(events) != 1 {
		t.Fatalf("new events=%+v err=%v", events, err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = reopen()
	event, err := s.(storage.RecoveryStore).GetRecoveryEvent(ctx, events[0].ID)
	if err != nil || event == nil || event.State != "open" || event.DLQOpenRows != 1 || event.RecordCount != 1 {
		t.Fatalf("restart lost recovery: %+v err=%v", event, err)
	}
	after, err = s.ListDeadLetters(ctx, storage.DLQFilter{JobName: job})
	if err != nil || len(after) != 2 {
		t.Fatalf("restart changed retained rows: %+v err=%v", after, err)
	}
}
