package backup_test

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/backup"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

func openTestStore(t *testing.T) storage.Storage {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "backup.db")
	s, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func seedControlPlane(t *testing.T, s storage.Storage) {
	t.Helper()
	ctx := context.Background()

	row := &storage.PipelineRow{
		Name:     "backup-pipe",
		SpecYAML: "name: backup-pipe\nsource:\n  type: file\nsink:\n  type: file\n",
		Status:   "stopped",
	}
	if err := s.SavePipeline(ctx, row); err != nil {
		t.Fatalf("save pipeline: %v", err)
	}
	if _, err := s.SavePipelineVersion(ctx, "backup-pipe", row.SpecYAML); err != nil {
		t.Fatalf("save version: %v", err)
	}
	if err := s.SaveCheckpoint(ctx, &storage.CheckpointRecord{
		JobName:   "backup-pipe",
		Source:    "file",
		Position:  json.RawMessage(`{"offset":42}`),
		Timestamp: time.Now().UTC(),
	}); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}
	if err := s.WriteDeadLetter(ctx, &storage.DLQRecord{
		JobName: "backup-pipe",
		Record:  core.Record{Data: map[string]any{"id": 1}},
		Error:   "boom",
	}); err != nil {
		t.Fatalf("write dlq: %v", err)
	}
	if err := s.WriteAudit(ctx, &storage.AuditEntry{
		Action: "pipeline.create", Method: "POST", Path: "/api/v2/pipelines", Target: "backup-pipe",
	}); err != nil {
		t.Fatalf("write audit: %v", err)
	}
	runID, err := s.RecordRunStart(ctx, "backup-pipe")
	if err != nil {
		t.Fatalf("run start: %v", err)
	}
	if err := s.RecordRunEnd(ctx, runID, "succeeded", 10, 10, 0, 0, 5); err != nil {
		t.Fatalf("run end: %v", err)
	}
	if err := s.RegisterWorker(ctx, &storage.WorkerInfo{
		ID: "w1", Host: "127.0.0.1", Port: 9000, Slots: 2, Status: "online",
	}); err != nil {
		t.Fatalf("register worker: %v", err)
	}
	if err := s.CreateTask(ctx, &storage.TaskAssignment{
		TaskID: "t1", Pipeline: "backup-pipe", Status: "completed", ShardIndex: 0, ShardTotal: 1,
	}); err != nil {
		t.Fatalf("create task: %v", err)
	}
	if err := s.SavePlugin(ctx, &storage.PluginEntry{
		Name: "p1", Kind: "transform", WASMPath: "/tmp/p1.wasm", Version: "1.0.0", Enabled: true,
	}); err != nil {
		t.Fatalf("save plugin: %v", err)
	}
	if err := s.SaveConnection(ctx, &storage.ConnectionEntry{
		Name: "mysql-src", Kind: "source", Type: "mysql",
		Config: map[string]any{"password": "enc:v1:test"},
	}); err != nil {
		t.Fatalf("save connection: %v", err)
	}
	if err := s.SetSetting(ctx, "llm.api_key", "enc:v1:secret"); err != nil {
		t.Fatalf("set setting: %v", err)
	}
}

func TestBackupRestoreRoundTripSQLite(t *testing.T) {
	src := openTestStore(t)
	seedControlPlane(t, src)
	ctx := context.Background()

	snap, err := backup.Export(ctx, src, backup.Options{Backend: "sqlite"})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	want := backup.CountSnapshot(snap)
	if want.Pipelines != 1 || want.Checkpoints != 1 || want.DeadLetters != 1 {
		t.Fatalf("unexpected export counts: %+v", want)
	}
	if want.Connections != 1 || want.Settings != 1 || want.Plugins != 1 {
		t.Fatalf("missing catalog objects: %+v", want)
	}

	// Secrets must stay enveloped (no plaintext password in JSON).
	raw, err := json.Marshal(snap)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	if strings.Contains(string(raw), `"password":"s3cret"`) {
		t.Fatal("backup leaked plaintext password")
	}

	path := filepath.Join(t.TempDir(), "snap.json")
	if err := backup.WriteFile(path, snap); err != nil {
		t.Fatalf("write file: %v", err)
	}
	loaded, err := backup.ReadFile(path)
	if err != nil {
		t.Fatalf("read file: %v", err)
	}

	dst := openTestStore(t)
	if err := backup.Restore(ctx, dst, loaded, backup.Options{ClearBeforeRestore: true}); err != nil {
		t.Fatalf("restore: %v", err)
	}

	report, ok, err := backup.Reconcile(ctx, dst, loaded)
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if !ok {
		t.Fatalf("reconcile mismatch:\n%s", report)
	}

	// Critical content checks.
	pipe, err := dst.GetPipeline(ctx, "backup-pipe")
	if err != nil || pipe == nil {
		t.Fatalf("get pipeline: %v %+v", err, pipe)
	}
	cp, err := dst.LoadCheckpoint(ctx, "backup-pipe")
	if err != nil || cp == nil {
		t.Fatalf("load checkpoint: %v %+v", err, cp)
	}
	var pos map[string]any
	if err := json.Unmarshal(cp.Position, &pos); err != nil {
		t.Fatalf("checkpoint position json: %v (%s)", err, cp.Position)
	}
	if pos["offset"] != float64(42) {
		t.Errorf("checkpoint position = %s", cp.Position)
	}
	dlq, err := dst.ListDeadLetters(ctx, storage.DLQFilter{JobName: "backup-pipe", Limit: 10})
	if err != nil || len(dlq) != 1 {
		t.Fatalf("dlq: %v len=%d", err, len(dlq))
	}
	setting, err := dst.GetSetting(ctx, "llm.api_key")
	if err != nil || setting != "enc:v1:secret" {
		t.Fatalf("setting: %v %q", err, setting)
	}
	conn, err := dst.GetConnection(ctx, "mysql-src")
	if err != nil || conn == nil {
		t.Fatalf("connection: %v", err)
	}
}

func TestBackupRestoreClearsPreviousState(t *testing.T) {
	ctx := context.Background()
	src := openTestStore(t)
	seedControlPlane(t, src)
	snap, err := backup.Export(ctx, src, backup.Options{Backend: "sqlite"})
	if err != nil {
		t.Fatalf("export: %v", err)
	}

	dst := openTestStore(t)
	// Pre-seed a different pipeline that must disappear after clear+restore.
	if err := dst.SavePipeline(ctx, &storage.PipelineRow{
		Name: "stale", SpecYAML: "name: stale\n", Status: "stopped",
	}); err != nil {
		t.Fatalf("seed stale: %v", err)
	}
	if err := backup.Restore(ctx, dst, snap, backup.Options{ClearBeforeRestore: true}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	stale, err := dst.GetPipeline(ctx, "stale")
	if err != nil {
		t.Fatalf("get stale: %v", err)
	}
	if stale != nil {
		t.Fatalf("stale pipeline survived clear+restore: %+v", stale)
	}
	pipe, _ := dst.GetPipeline(ctx, "backup-pipe")
	if pipe == nil {
		t.Fatal("expected restored pipeline")
	}
}

func TestRetentionPurgeAuditRunTasks(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	purger, ok := s.(storage.RetentionPurger)
	if !ok {
		t.Fatal("sqlite store must implement RetentionPurger")
	}

	// Seed audit + finished run + finished task.
	if err := s.WriteAudit(ctx, &storage.AuditEntry{Action: "old", Method: "GET", Path: "/x"}); err != nil {
		t.Fatalf("audit: %v", err)
	}
	runID, err := s.RecordRunStart(ctx, "p")
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if err := s.RecordRunEnd(ctx, runID, "succeeded", 1, 1, 0, 0, 1); err != nil {
		t.Fatalf("run end: %v", err)
	}
	now := time.Now().UTC()
	if err := s.CreateTask(ctx, &storage.TaskAssignment{
		TaskID: "old-task", Pipeline: "p", Status: "completed",
		FinishedAt: &now,
	}); err != nil {
		t.Fatalf("task: %v", err)
	}
	// Force finished_at via UpdateTask for backends that ignore CreateTask FinishedAt.
	_ = s.UpdateTask(ctx, &storage.TaskAssignment{
		TaskID: "old-task", Pipeline: "p", Status: "completed", FinishedAt: &now,
	})

	future := time.Now().Add(time.Hour)
	if n, err := purger.PurgeAuditBefore(ctx, future, 1000); err != nil || n < 1 {
		t.Fatalf("purge audit: n=%d err=%v", n, err)
	}
	if n, err := purger.PurgeRunHistoryBefore(ctx, future, 1000); err != nil || n < 1 {
		t.Fatalf("purge runs: n=%d err=%v", n, err)
	}
	if _, err := purger.PurgeFinishedTasksBefore(ctx, future, 1000); err != nil {
		t.Fatalf("purge tasks: %v", err)
	}

	counts, err := purger.CountObjects(ctx)
	if err != nil {
		t.Fatalf("counts: %v", err)
	}
	if counts.AuditLogs != 0 {
		t.Errorf("audit remaining = %d", counts.AuditLogs)
	}
	if counts.RunHistory != 0 {
		t.Errorf("runs remaining = %d", counts.RunHistory)
	}
}

func TestSchemaVersionsPresent(t *testing.T) {
	s := openTestStore(t)
	purger := s.(storage.RetentionPurger)
	vers, err := purger.SchemaVersions(context.Background())
	if err != nil {
		t.Fatalf("schema versions: %v", err)
	}
	if len(vers) == 0 {
		t.Fatal("expected at least one applied schema version")
	}
}

func TestWriteFilePermissions(t *testing.T) {
	snap := &backup.Snapshot{FormatVersion: backup.FormatVersion, CreatedAt: time.Now().UTC()}
	path := filepath.Join(t.TempDir(), "snap.json")
	if err := backup.WriteFile(path, snap); err != nil {
		t.Fatalf("write: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Errorf("backup file too open: %v", info.Mode())
	}
}
