package storage_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func TestBackupSQLStoreAndSecretScan(t *testing.T) {
	dir := t.TempDir()
	st, err := sqlite.New(filepath.Join(dir, "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()

	secret := "super-secret-token-xyz"
	// Write a connection with plaintext config (simulating pre-envelope bad data)
	if err := st.SaveConnection(ctx, &storage.ConnectionEntry{
		Name:   "bad",
		Kind:   "source",
		Type:   "mysql",
		Config: map[string]any{"password": secret},
	}); err != nil {
		t.Logf("SaveConnection: %v", err)
	}

	// Always insert a pipeline so counts > 0
	if err := st.SavePipeline(ctx, &storage.PipelineRow{
		ID: "p1", Name: "p1", SpecYAML: "name: p1\n", Status: "stopped",
	}); err != nil {
		t.Fatalf("save pipeline: %v", err)
	}

	out := filepath.Join(dir, "backups")
	man, err := storage.BackupSQLStore(ctx, st, out, []string{secret, "never-present-zzzz"})
	if err != nil {
		t.Fatalf("backup: %v", err)
	}
	if man.Counts.Pipelines < 1 {
		t.Fatalf("expected pipelines in backup: %+v", man.Counts)
	}
	if _, err := os.Stat(filepath.Join(man.Path, "manifest.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(man.Path, "pipelines.jsonl")); err != nil {
		t.Fatal(err)
	}
	// If plaintext secret was stored, scan must fail closed
	if man.SecretScan.PlaintextHits > 0 && man.SecretScan.OK {
		t.Fatal("secret scan should not be OK when plaintext hits > 0")
	}
}

func TestApplyRetentionDeletesAgedRows(t *testing.T) {
	dir := t.TempDir()
	st, err := sqlite.New(filepath.Join(dir, "etl.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	ctx := context.Background()
	db := st.DB()

	old := time.Now().UTC().Add(-48 * time.Hour).Format(time.RFC3339Nano)
	if _, err := db.ExecContext(ctx, `INSERT INTO audit_logs (action, created_at) VALUES (?, ?)`, "old", old); err != nil {
		t.Fatal(err)
	}
	if _, err := db.ExecContext(ctx, `INSERT INTO audit_logs (action, created_at) VALUES (?, ?)`, "new", time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
		t.Fatal(err)
	}

	if rep, err := storage.ApplyRetention(ctx, db, time.Now().UTC(), storage.RetentionPolicy{AuditLogs: 24 * time.Hour, DLQ: time.Hour}); err == nil || rep != nil {
		t.Fatal("raw DLQ policy did not fail before mutation")
	}
	var retained int
	if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM audit_logs").Scan(&retained); err != nil || retained != 2 {
		t.Fatal("unsupported DLQ policy partially mutated audit retention")
	}
	rep, err := storage.ApplyRetention(ctx, db, time.Now().UTC(), storage.RetentionPolicy{AuditLogs: 24 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if rep.AuditDeleted < 1 {
		t.Fatalf("expected audit delete, got %+v", rep)
	}
	var n int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_logs`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("remaining audit=%d want 1", n)
	}
}

func TestLegacyJSONLBackupIncludesRecoveryFacts(t *testing.T) {
	store, err := sqlite.New(filepath.Join(t.TempDir(), "recovery-backup.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.SavePipeline(ctx, &storage.PipelineRow{ID: "backup-recovery", Name: "backup recovery", Status: "stopped", SpecYAML: "name: backup recovery"}); err != nil {
		t.Fatal(err)
	}
	record := &storage.DLQRecord{JobName: "backup-recovery", Record: core.Record{Data: map[string]any{"id": 1}}, Error: "fixture failure",
		RecoveryContext: core.RecoveryCaptureContext{Endpoint: "sink", ConnectionKnown: true}}
	if err := store.WriteDeadLetter(ctx, record); err != nil {
		t.Fatal(err)
	}
	rows, err := store.ListDeadLetters(ctx, storage.DLQFilter{JobName: "backup-recovery"})
	if err != nil || len(rows) != 1 {
		t.Fatal("fixture missing")
	}
	op, err := store.BeginRecoveryOperation(ctx, "backup-recovery", rows[0].ID, "dry_run")
	if err != nil {
		t.Fatal(err)
	}
	if err := store.FinishRecoveryOperation(ctx, op, "preview_passed", ""); err != nil {
		t.Fatal(err)
	}
	manifest, err := storage.BackupSQLStore(ctx, store, t.TempDir(), nil)
	if err != nil || manifest.Counts.RecoveryEvents != 1 || manifest.Counts.RecoveryOperations != 1 {
		t.Fatalf("legacy backup counts=%+v err=%v", manifest, err)
	}
	for _, file := range []string{"recovery_events.jsonl", "recovery_operations.jsonl"} {
		content, err := os.ReadFile(filepath.Join(manifest.Path, file))
		if err != nil || len(content) == 0 {
			t.Fatalf("missing retained backup facts %s: %v", file, err)
		}
	}
}
