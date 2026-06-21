package sqlite_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/storage"
	"openetl-go/internal/etl/storage/sqlite"
)

func tempDB(t *testing.T) (*sqlite.Store, func()) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")
	s, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return s, func() {
		s.Close()
		os.RemoveAll(dir)
	}
}

func TestCheckpointCRUD(t *testing.T) {
	s, cleanup := tempDB(t)
	defer cleanup()
	ctx := context.Background()

	rec := &storage.CheckpointRecord{
		JobName:   "test-job",
		Source:    "mysql_cdc",
		Position:  []byte(`{"binlog_file":"mysql-bin.000001","binlog_pos":1234}`),
		Timestamp: time.Now(),
	}
	if err := s.SaveCheckpoint(ctx, rec); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := s.LoadCheckpoint(ctx, "test-job")
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded == nil {
		t.Fatal("expected non-nil checkpoint")
	}
	if loaded.Source != "mysql_cdc" {
		t.Errorf("source = %s, want mysql_cdc", loaded.Source)
	}

	cps, err := s.ListCheckpoints(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(cps) != 1 {
		t.Errorf("list len = %d, want 1", len(cps))
	}

	if err := s.DeleteCheckpoint(ctx, "test-job"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	loaded2, _ := s.LoadCheckpoint(ctx, "test-job")
	if loaded2 != nil {
		t.Error("expected nil after delete")
	}
}

func TestDLQCRUD(t *testing.T) {
	s, cleanup := tempDB(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		rec := &storage.DLQRecord{
			JobName:    "test-pipe",
			Record:     core.Record{Operation: core.OpInsert, Data: map[string]any{"id": i}},
			Error:      "schema mismatch: unknown column",
			ErrorClass: "schema",
			Attempt:    1,
			CreatedAt:  time.Now(),
		}
		if err := s.WriteDeadLetter(ctx, rec); err != nil {
			t.Fatalf("write dlq %d: %v", i, err)
		}
	}

	items, err := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: "test-pipe", Limit: 10})
	if err != nil {
		t.Fatalf("list dlq: %v", err)
	}
	if len(items) != 5 {
		t.Errorf("dlq count = %d, want 5", len(items))
	}

	// Filter by error class
	filtered, _ := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: "test-pipe", ErrorClass: "schema", Limit: 10})
	if len(filtered) != 5 {
		t.Errorf("filtered by class = %d, want 5", len(filtered))
	}

	// Delete by filter
	count, err := s.DeleteDeadLettersByFilter(ctx, storage.DLQFilter{JobName: "test-pipe", ErrorClass: "schema"})
	if err != nil {
		t.Fatalf("delete filtered: %v", err)
	}
	if count != 5 {
		t.Errorf("deleted = %d, want 5", count)
	}

	remaining, _ := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: "test-pipe", Limit: 10})
	if len(remaining) != 0 {
		t.Errorf("remaining = %d, want 0", len(remaining))
	}
}

func TestPipelineSpecPersistence(t *testing.T) {
	s, cleanup := tempDB(t)
	defer cleanup()
	ctx := context.Background()

	row := &storage.PipelineRow{Name: "my-pipe", SpecYAML: "name: my-pipe\nsource:\n  type: file\n", Status: "loaded"}
	if err := s.SavePipeline(ctx, row); err != nil {
		t.Fatalf("save pipeline: %v", err)
	}

	loaded, _ := s.GetPipeline(ctx, "my-pipe")
	if loaded == nil || loaded.Name != "my-pipe" {
		t.Fatal("expected loaded pipeline")
	}

	list, _ := s.ListPipelines(ctx)
	if len(list) != 1 {
		t.Errorf("list len = %d, want 1", len(list))
	}

	// Version
	ver, err := s.SavePipelineVersion(ctx, "my-pipe", "v1 yaml")
	if err != nil {
		t.Fatalf("save version: %v", err)
	}
	if ver != 1 {
		t.Errorf("version = %d, want 1", ver)
	}
	ver2, _ := s.SavePipelineVersion(ctx, "my-pipe", "v2 yaml")
	if ver2 != 2 {
		t.Errorf("version2 = %d, want 2", ver2)
	}

	versions, _ := s.ListPipelineVersions(ctx, "my-pipe")
	if len(versions) != 2 {
		t.Errorf("versions = %d, want 2", len(versions))
	}

	s.DeletePipeline(ctx, "my-pipe")
	deleted, _ := s.GetPipeline(ctx, "my-pipe")
	if deleted != nil {
		t.Error("expected nil after delete")
	}
}

func TestAuditLog(t *testing.T) {
	s, cleanup := tempDB(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		entry := &storage.AuditEntry{
			Action: "pipeline.start",
			Method: "POST",
			Path:   "/api/v2/pipelines/test/start",
			Target: "test",
		}
		if err := s.WriteAudit(ctx, entry); err != nil {
			t.Fatalf("write audit: %v", err)
		}
	}

	entries, err := s.ListAudit(ctx, 10)
	if err != nil {
		t.Fatalf("list audit: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("audit count = %d, want 3", len(entries))
	}
}

func TestWorkerRegistry(t *testing.T) {
	s, cleanup := tempDB(t)
	defer cleanup()
	ctx := context.Background()

	w := &storage.WorkerInfo{
		ID:    "worker-1",
		Host:  "10.0.0.1",
		Port:  9001,
		Slots: 4,
	}
	if err := s.RegisterWorker(ctx, w); err != nil {
		t.Fatalf("register worker: %v", err)
	}

	if err := s.Heartbeat(ctx, "worker-1"); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	workers, _ := s.ListWorkers(ctx)
	if len(workers) != 1 {
		t.Errorf("workers = %d, want 1", len(workers))
	}

	s.DeregisterWorker(ctx, "worker-1")
	workers2, _ := s.ListWorkers(ctx)
	if len(workers2) != 0 {
		t.Errorf("after deregister = %d, want 0", len(workers2))
	}
}

func TestRunHistory(t *testing.T) {
	s, cleanup := tempDB(t)
	defer cleanup()
	ctx := context.Background()

	runID, err := s.RecordRunStart(ctx, "test-pipe")
	if err != nil {
		t.Fatalf("record run start: %v", err)
	}
	if runID == 0 {
		t.Error("expected non-zero run ID")
	}

	if err := s.RecordRunEnd(ctx, runID, "completed", 100, 95, 5, 0, 5000); err != nil {
		t.Fatalf("record run end: %v", err)
	}

	runs, _ := s.ListRunHistory(ctx, "test-pipe", 10)
	if len(runs) != 1 {
		t.Errorf("runs = %d, want 1", len(runs))
	}
	if runs[0].Status != "completed" {
		t.Errorf("status = %s, want completed", runs[0].Status)
	}
}
