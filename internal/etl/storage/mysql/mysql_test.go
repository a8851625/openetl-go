package mysql_test

import (
	"context"
	"os"
	"testing"
	"time"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/storage"
	"openetl-go/internal/etl/storage/mysql"
)

func dsn(t *testing.T) string {
	t.Helper()
	d := os.Getenv("MYSQL_DSN")
	if d == "" {
		t.Skipf("MYSQL_DSN not set; skipping MySQL integration test")
	}
	return d
}

func openStore(t *testing.T) (*mysql.Store, func()) {
	t.Helper()
	s, err := mysql.New(dsn(t))
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	ctx := context.Background()
	for _, q := range []string{
		`DELETE FROM dead_letters`,
		`DELETE FROM audit_logs`,
		`DELETE FROM run_history`,
		`DELETE FROM checkpoints`,
		`DELETE FROM pipelines`,
		`DELETE FROM pipeline_versions`,
		`DELETE FROM workers`,
		`DELETE FROM plugins`,
		`DELETE FROM settings`,
		`DELETE FROM task_assignments`,
	} {
		if _, err := s.DB().ExecContext(ctx, q); err != nil {
			s.Close()
			t.Fatalf("cleanup %q: %v", q, err)
		}
	}
	return s, func() { s.Close() }
}

func TestPipelineCRUD(t *testing.T) {
	s, cleanup := openStore(t)
	defer cleanup()
	ctx := context.Background()

	row := &storage.PipelineRow{
		Name:     "my-pipe",
		SpecYAML: "name: my-pipe\nsource:\n  type: file\n",
		Status:   "loaded",
	}
	if err := s.SavePipeline(ctx, row); err != nil {
		t.Fatalf("save: %v", err)
	}
	loaded, err := s.GetPipeline(ctx, "my-pipe")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if loaded == nil || loaded.Name != "my-pipe" || loaded.Status != "loaded" {
		t.Fatalf("unexpected pipeline: %+v", loaded)
	}

	list, err := s.ListPipelines(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("list len = %d, want 1", len(list))
	}

	if err := s.DeletePipeline(ctx, "my-pipe"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got, _ := s.GetPipeline(ctx, "my-pipe"); got != nil {
		t.Error("expected nil after delete")
	}
}

func TestPipelineVersion(t *testing.T) {
	s, cleanup := openStore(t)
	defer cleanup()
	ctx := context.Background()

	v1, err := s.SavePipelineVersion(ctx, "p", "yaml-1")
	if err != nil {
		t.Fatalf("save v1: %v", err)
	}
	if v1 != 1 {
		t.Errorf("v1 = %d, want 1", v1)
	}
	v2, _ := s.SavePipelineVersion(ctx, "p", "yaml-2")
	if v2 != 2 {
		t.Errorf("v2 = %d, want 2", v2)
	}
	got, err := s.GetPipelineVersion(ctx, "p", 1)
	if err != nil || got == nil || got.Version != 1 || got.SpecYAML != "yaml-1" {
		t.Fatalf("get version 1: %v %+v", err, got)
	}
	list, err := s.ListPipelineVersions(ctx, "p")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(list) != 2 || list[0].Version != 2 {
		t.Errorf("list = %+v", list)
	}
}

func TestCheckpointCRUD(t *testing.T) {
	s, cleanup := openStore(t)
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
	if loaded == nil || loaded.Source != "mysql_cdc" {
		t.Fatalf("unexpected: %+v", loaded)
	}
	if string(loaded.Position) == "" {
		t.Error("empty position")
	}

	cps, err := s.ListCheckpoints(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(cps) != 1 {
		t.Errorf("len = %d, want 1", len(cps))
	}

	if err := s.DeleteCheckpoint(ctx, "test-job"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got, _ := s.LoadCheckpoint(ctx, "test-job"); got != nil {
		t.Error("expected nil after delete")
	}
}

func TestDLQCRUD(t *testing.T) {
	s, cleanup := openStore(t)
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
			t.Fatalf("write %d: %v", i, err)
		}
	}

	items, err := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: "test-pipe", Limit: 10})
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(items) != 5 {
		t.Errorf("count = %d, want 5", len(items))
	}

	filtered, _ := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: "test-pipe", ErrorClass: "schema", Limit: 10})
	if len(filtered) != 5 {
		t.Errorf("filtered = %d, want 5", len(filtered))
	}

	contains, _ := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: "test-pipe", ErrorContains: "mismatch", Limit: 10})
	if len(contains) != 5 {
		t.Errorf("contains = %d, want 5", len(contains))
	}

	if err := s.DeleteDeadLetterByID(ctx, items[0].ID); err != nil {
		t.Fatalf("delete by id: %v", err)
	}
	rest, _ := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: "test-pipe", Limit: 10})
	if len(rest) != 4 {
		t.Errorf("after delete by id = %d, want 4", len(rest))
	}

	n, err := s.DeleteDeadLettersByFilter(ctx, storage.DLQFilter{JobName: "test-pipe", ErrorClass: "schema"})
	if err != nil {
		t.Fatalf("delete by filter: %v", err)
	}
	if n != 4 {
		t.Errorf("deleted = %d, want 4", n)
	}

	for i := 0; i < 3; i++ {
		_ = s.WriteDeadLetter(ctx, &storage.DLQRecord{
			JobName: "test-pipe",
			Record:  core.Record{Data: map[string]any{"k": i}},
		})
	}
	if err := s.DeleteAllDeadLetters(ctx, "test-pipe"); err != nil {
		t.Fatalf("delete all: %v", err)
	}
	remaining, _ := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: "test-pipe", Limit: 10})
	if len(remaining) != 0 {
		t.Errorf("remaining = %d, want 0", len(remaining))
	}
}

func TestAudit(t *testing.T) {
	s, cleanup := openStore(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := s.WriteAudit(ctx, &storage.AuditEntry{
			Action: "pipeline.start", Method: "POST",
			Path: "/api/v2/pipelines/test/start", Target: "test",
		}); err != nil {
			t.Fatalf("write: %v", err)
		}
	}
	entries, err := s.ListAudit(ctx, 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(entries) != 3 {
		t.Errorf("count = %d, want 3", len(entries))
	}

	def, _ := s.ListAudit(ctx, 0)
	if len(def) != 3 {
		t.Errorf("default limit count = %d, want 3", len(def))
	}
}

func TestRunHistory(t *testing.T) {
	s, cleanup := openStore(t)
	defer cleanup()
	ctx := context.Background()

	runID, err := s.RecordRunStart(ctx, "test-pipe")
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	if runID == 0 {
		t.Error("expected non-zero run ID")
	}
	if err := s.RecordRunEnd(ctx, runID, "completed", 100, 95, 5, 0, 5000); err != nil {
		t.Fatalf("end: %v", err)
	}
	runs, err := s.ListRunHistory(ctx, "test-pipe", 10)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(runs) != 1 {
		t.Fatalf("runs = %d, want 1", len(runs))
	}
	if runs[0].Status != "completed" || runs[0].RecordsRead != 100 || runs[0].RecordsWritten != 95 {
		t.Errorf("unexpected run: %+v", runs[0])
	}
	if runs[0].FinishedAt == nil {
		t.Error("expected non-nil finished_at")
	}
}

func TestWorkerRegistry(t *testing.T) {
	s, cleanup := openStore(t)
	defer cleanup()
	ctx := context.Background()

	w := &storage.WorkerInfo{
		ID:    "worker-1",
		Host:  "10.0.0.1",
		Port:  9001,
		Slots: 4,
	}
	if err := s.RegisterWorker(ctx, w); err != nil {
		t.Fatalf("register: %v", err)
	}
	if err := s.Heartbeat(ctx, "worker-1"); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	workers, err := s.ListWorkers(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(workers) != 1 || workers[0].ID != "worker-1" {
		t.Errorf("unexpected: %+v", workers)
	}

	w2 := &storage.WorkerInfo{
		ID:     "worker-1",
		Host:   "10.0.0.2",
		Port:   9002,
		Slots:  8,
		Labels: map[string]string{"zone": "us-east"},
	}
	if err := s.RegisterWorker(ctx, w2); err != nil {
		t.Fatalf("re-register: %v", err)
	}
	workers, _ = s.ListWorkers(ctx)
	if len(workers) != 1 || workers[0].Host != "10.0.0.2" || workers[0].Slots != 8 {
		t.Errorf("upsert failed: %+v", workers[0])
	}
	if workers[0].Labels["zone"] != "us-east" {
		t.Errorf("labels not persisted: %+v", workers[0].Labels)
	}

	if err := s.DeregisterWorker(ctx, "worker-1"); err != nil {
		t.Fatalf("deregister: %v", err)
	}
	workers, _ = s.ListWorkers(ctx)
	if len(workers) != 0 {
		t.Errorf("after deregister = %d, want 0", len(workers))
	}
}

func TestPluginCRUD(t *testing.T) {
	s, cleanup := openStore(t)
	defer cleanup()
	ctx := context.Background()

	p := &storage.PluginEntry{
		Name: "echo", Kind: "transform",
		WASMPath: "/data/echo.wasm", Version: "1.0.0", Enabled: true,
	}
	if err := s.SavePlugin(ctx, p); err != nil {
		t.Fatalf("save: %v", err)
	}
	got, err := s.GetPlugin(ctx, "echo")
	if err != nil || got == nil || !got.Enabled || got.Kind != "transform" {
		t.Fatalf("get: %v %+v", err, got)
	}
	list, err := s.ListPlugins(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("list len = %d, want 1", len(list))
	}
	p2 := &storage.PluginEntry{Name: "echo", Kind: "sink", WASMPath: "/data/echo2.wasm", Version: "2.0.0", Enabled: false}
	if err := s.SavePlugin(ctx, p2); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	got2, _ := s.GetPlugin(ctx, "echo")
	if got2.Kind != "sink" || got2.Version != "2.0.0" || got2.Enabled {
		t.Errorf("upsert not applied: %+v", got2)
	}
	if err := s.DeletePlugin(ctx, "echo"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	got3, _ := s.GetPlugin(ctx, "echo")
	if got3 != nil {
		t.Error("expected nil after delete")
	}
}

func TestSettings(t *testing.T) {
	s, cleanup := openStore(t)
	defer cleanup()
	ctx := context.Background()

	val, err := s.GetSetting(ctx, "missing")
	if err != nil {
		t.Fatalf("get missing: %v", err)
	}
	if val != "" {
		t.Errorf("expected empty, got %q", val)
	}
	if err := s.SetSetting(ctx, "k1", "v1"); err != nil {
		t.Fatalf("set: %v", err)
	}
	val, _ = s.GetSetting(ctx, "k1")
	if val != "v1" {
		t.Errorf("got %q", val)
	}
	if err := s.SetSetting(ctx, "k1", "v2"); err != nil {
		t.Fatalf("update: %v", err)
	}
	val, _ = s.GetSetting(ctx, "k1")
	if val != "v2" {
		t.Errorf("got %q after update", val)
	}
	all, err := s.ListSettings(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if all["k1"] != "v2" {
		t.Errorf("list = %+v", all)
	}
}

func TestTasks(t *testing.T) {
	s, cleanup := openStore(t)
	defer cleanup()
	ctx := context.Background()

	task := &storage.TaskAssignment{
		TaskID:   "t-1",
		Pipeline: "p-1",
		WorkerID: "w-1",
		Status:   "pending",
	}
	if err := s.CreateTask(ctx, task); err != nil {
		t.Fatalf("create: %v", err)
	}
	started := time.Now()
	if err := s.UpdateTask(ctx, &storage.TaskAssignment{
		TaskID: "t-1", Status: "running", WorkerID: "w-1", StartedAt: &started,
	}); err != nil {
		t.Fatalf("update: %v", err)
	}
	list, err := s.ListTasks(ctx, "p-1")
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 || list[0].Status != "running" {
		t.Errorf("unexpected: %+v", list)
	}
}
