package storage_test

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

// runConformanceSuite exercises the full storage.Storage contract against a
// backend. Each subtest receives a fresh, empty store from the factory so they
// are independent. This is the single source of truth for the Storage contract;
// per-backend tests must not duplicate these assertions (SPEC §3.3, ROADMAP A10).
func runConformanceSuite(t *testing.T, newStore func(t *testing.T) (storage.Storage, func())) {
	t.Helper()

	t.Run("PipelineCRUD", func(t *testing.T) { testPipelineCRUD(t, newStore) })
	t.Run("PipelineStatus", func(t *testing.T) { testPipelineStatus(t, newStore) })
	t.Run("PipelineVersions", func(t *testing.T) { testPipelineVersions(t, newStore) })
	t.Run("CheckpointCRUD", func(t *testing.T) { testCheckpointCRUD(t, newStore) })
	t.Run("CheckpointConcurrent", func(t *testing.T) { testCheckpointConcurrent(t, newStore) })
	t.Run("DLQ", func(t *testing.T) { testDLQ(t, newStore) })
	t.Run("DLQTTLPurge", func(t *testing.T) { testDLQTTLPurge(t, newStore) })
	t.Run("Audit", func(t *testing.T) { testAudit(t, newStore) })
	t.Run("RunHistory", func(t *testing.T) { testRunHistory(t, newStore) })
	t.Run("WorkerRegistry", func(t *testing.T) { testWorkerRegistry(t, newStore) })
	t.Run("Tasks", func(t *testing.T) { testTasks(t, newStore) })
	t.Run("Plugins", func(t *testing.T) { testPlugins(t, newStore) })
	t.Run("Settings", func(t *testing.T) { testSettings(t, newStore) })
}

// ── Pipeline definitions ─────────────────────────────────────────────

func testPipelineCRUD(t *testing.T, newStore func(t *testing.T) (storage.Storage, func())) {
	s, cleanup := newStore(t)
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
	if loaded.SpecYAML != row.SpecYAML {
		t.Errorf("spec yaml mismatch: got %q", loaded.SpecYAML)
	}

	list, err := s.ListPipelines(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("list len = %d, want 1", len(list))
	}

	// Upsert: saving the same name replaces.
	row.SpecYAML = "name: my-pipe\nsource:\n  type: kafka\n"
	row.Status = "running"
	if err := s.SavePipeline(ctx, row); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	updated, _ := s.GetPipeline(ctx, "my-pipe")
	if updated.Status != "running" {
		t.Errorf("upsert status = %q, want running", updated.Status)
	}
	if updated.SpecYAML != row.SpecYAML {
		t.Errorf("upsert spec not applied")
	}
	list2, _ := s.ListPipelines(ctx)
	if len(list2) != 1 {
		t.Errorf("after upsert list len = %d, want 1", len(list2))
	}

	// Get missing → nil, not error.
	missing, err := s.GetPipeline(ctx, "does-not-exist")
	if err != nil {
		t.Errorf("get missing should not error: %v", err)
	}
	if missing != nil {
		t.Errorf("get missing should return nil")
	}

	if err := s.DeletePipeline(ctx, "my-pipe"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got, _ := s.GetPipeline(ctx, "my-pipe"); got != nil {
		t.Error("expected nil after delete")
	}
}

func testPipelineStatus(t *testing.T, newStore func(t *testing.T) (storage.Storage, func())) {
	s, cleanup := newStore(t)
	defer cleanup()
	ctx := context.Background()

	if err := s.SavePipeline(ctx, &storage.PipelineRow{Name: "p", SpecYAML: "x", Status: "stopped"}); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdatePipelineStatus(ctx, "p", "running"); err != nil {
		t.Fatalf("update status: %v", err)
	}
	got, _ := s.GetPipeline(ctx, "p")
	if got.Status != "running" {
		t.Errorf("status = %q, want running", got.Status)
	}
}

func testPipelineVersions(t *testing.T, newStore func(t *testing.T) (storage.Storage, func())) {
	s, cleanup := newStore(t)
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
		t.Fatalf("get version 1: err=%v got=%+v", err, got)
	}

	list, err := s.ListPipelineVersions(ctx, "p")
	if err != nil {
		t.Fatalf("list versions: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("versions = %d, want 2", len(list))
	}
	// Most-recent first.
	if list[0].Version != 2 {
		t.Errorf("first version = %d, want 2 (most recent first)", list[0].Version)
	}
}

// ── Checkpoints ──────────────────────────────────────────────────────

func testCheckpointCRUD(t *testing.T, newStore func(t *testing.T) (storage.Storage, func())) {
	s, cleanup := newStore(t)
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

	// Upsert: same job name replaces. Compare parsed JSON (not raw bytes)
	// because backends may store position in a JSON column that canonicalizes
	// key order (e.g. MySQL JSON type).
	rec.Position = []byte(`{"binlog_file":"mysql-bin.000002","binlog_pos":5678}`)
	if err := s.SaveCheckpoint(ctx, rec); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	loaded2, _ := s.LoadCheckpoint(ctx, "test-job")
	if !jsonEqual(loaded2.Position, rec.Position) {
		t.Errorf("upsert not applied: got %s, want %s", loaded2.Position, rec.Position)
	}
	cps2, _ := s.ListCheckpoints(ctx)
	if len(cps2) != 1 {
		t.Errorf("after upsert len = %d, want 1", len(cps2))
	}

	if err := s.DeleteCheckpoint(ctx, "test-job"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if got, _ := s.LoadCheckpoint(ctx, "test-job"); got != nil {
		t.Error("expected nil after delete")
	}
}

// testCheckpointConcurrent verifies two concurrent writers to the same
// checkpoint key do not corrupt state: both saves succeed and the final
// load returns a valid (last-writer-wins) value. (SPEC §5.3, ROADMAP A10)
func testCheckpointConcurrent(t *testing.T, newStore func(t *testing.T) (storage.Storage, func())) {
	s, cleanup := newStore(t)
	defer cleanup()
	ctx := context.Background()

	const workers = 10
	const iters = 20
	var wg sync.WaitGroup
	errs := make(chan error, workers*iters)
	for w := 0; w < workers; w++ {
		w := w
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < iters; i++ {
				err := s.SaveCheckpoint(ctx, &storage.CheckpointRecord{
					JobName:   "concurrent-job",
					Source:    "mysql_cdc",
					Position:  []byte(`{"w":` + itoa(w) + `,"i":` + itoa(i) + `}`),
					Timestamp: time.Now(),
				})
				if err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent save failed: %v", err)
	}

	// After all writes, exactly one checkpoint exists and is readable.
	cps, err := s.ListCheckpoints(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(cps) != 1 {
		t.Fatalf("expected 1 checkpoint after concurrent writes, got %d", len(cps))
	}
	loaded, err := s.LoadCheckpoint(ctx, "concurrent-job")
	if err != nil || loaded == nil || len(loaded.Position) == 0 {
		t.Fatalf("final load: err=%v loaded=%+v", err, loaded)
	}
}

// ── Dead letters ─────────────────────────────────────────────────────

func testDLQ(t *testing.T, newStore func(t *testing.T) (storage.Storage, func())) {
	s, cleanup := newStore(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		rec := &storage.DLQRecord{
			JobName:    "test-pipe",
			Record:     core.Record{Operation: core.OpInsert, Data: map[string]any{"id": i}},
			Error:      "schema mismatch: unknown column",
			ErrorClass: "schema",
			Attempt:    1,
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

	// Count.
	count, err := s.CountDeadLetters(ctx, "test-pipe")
	if err != nil {
		t.Fatalf("count: %v", err)
	}
	if count != 5 {
		t.Errorf("count = %d, want 5", count)
	}

	// Filter by error class.
	filtered, _ := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: "test-pipe", ErrorClass: "schema", Limit: 10})
	if len(filtered) != 5 {
		t.Errorf("filtered by class = %d, want 5", len(filtered))
	}

	// Filter by error text contains.
	contains, _ := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: "test-pipe", ErrorContains: "mismatch", Limit: 10})
	if len(contains) != 5 {
		t.Errorf("contains = %d, want 5", len(contains))
	}

	// Limit.
	limited, _ := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: "test-pipe", Limit: 2})
	if len(limited) != 2 {
		t.Errorf("limit = %d, want 2", len(limited))
	}

	// Delete by ID.
	if err := s.DeleteDeadLetterByID(ctx, items[0].ID); err != nil {
		t.Fatalf("delete by id: %v", err)
	}
	rest, _ := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: "test-pipe", Limit: 10})
	if len(rest) != 4 {
		t.Errorf("after delete by id = %d, want 4", len(rest))
	}

	// Delete by filter.
	n, err := s.DeleteDeadLettersByFilter(ctx, storage.DLQFilter{JobName: "test-pipe", ErrorClass: "schema"})
	if err != nil {
		t.Fatalf("delete by filter: %v", err)
	}
	if n != 4 {
		t.Errorf("deleted = %d, want 4", n)
	}

	// Delete all.
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

// testDLQTTLPurge verifies the Until filter purges records at-or-before a cutoff
// and leaves newer records intact. (SPEC §1.5 data retention, ROADMAP A10)
func testDLQTTLPurge(t *testing.T, newStore func(t *testing.T) (storage.Storage, func())) {
	s, cleanup := newStore(t)
	defer cleanup()
	ctx := context.Background()

	for i := 0; i < 3; i++ {
		if err := s.WriteDeadLetter(ctx, &storage.DLQRecord{
			JobName:    "ttl-pipe",
			Record:     core.Record{Data: map[string]any{"id": i}},
			ErrorClass: "transient",
		}); err != nil {
			t.Fatalf("write %d: %v", i, err)
		}
	}

	// Purge with a cutoff in the past → deletes nothing (all records newer).
	past := time.Now().Add(-1 * time.Hour)
	n, err := s.DeleteDeadLettersByFilter(ctx, storage.DLQFilter{JobName: "ttl-pipe", Until: past})
	if err != nil {
		t.Fatalf("past purge: %v", err)
	}
	if n != 0 {
		t.Errorf("past purge deleted %d, want 0", n)
	}
	remaining, _ := s.ListDeadLetters(ctx, storage.DLQFilter{JobName: "ttl-pipe", Limit: 10})
	if len(remaining) != 3 {
		t.Errorf("after past purge = %d, want 3", len(remaining))
	}

	// Purge with cutoff = now → deletes all (all created_at <= now).
	now := time.Now()
	n2, err := s.DeleteDeadLettersByFilter(ctx, storage.DLQFilter{JobName: "ttl-pipe", Until: now})
	if err != nil {
		t.Fatalf("now purge: %v", err)
	}
	if n2 != 3 {
		t.Errorf("now purge deleted %d, want 3", n2)
	}
}

// ── Audit ────────────────────────────────────────────────────────────

func testAudit(t *testing.T, newStore func(t *testing.T) (storage.Storage, func())) {
	s, cleanup := newStore(t)
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
	// Default limit.
	def, _ := s.ListAudit(ctx, 0)
	if len(def) != 3 {
		t.Errorf("default limit count = %d, want 3", len(def))
	}
}

// ── Run history ──────────────────────────────────────────────────────

func testRunHistory(t *testing.T, newStore func(t *testing.T) (storage.Storage, func())) {
	s, cleanup := newStore(t)
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
	r := runs[0]
	if r.Status != "completed" || r.RecordsRead != 100 || r.RecordsWritten != 95 ||
		r.RecordsFailed != 5 || r.RecordsDLQ != 0 || r.DurationMs != 5000 {
		t.Errorf("unexpected run: %+v", r)
	}
	if r.FinishedAt == nil {
		t.Error("expected non-nil finished_at")
	}
}

// ── Worker registry ──────────────────────────────────────────────────

func testWorkerRegistry(t *testing.T, newStore func(t *testing.T) (storage.Storage, func())) {
	s, cleanup := newStore(t)
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

	// Re-register = upsert (same ID replaces).
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

// ── Task assignments ─────────────────────────────────────────────────

func testTasks(t *testing.T, newStore func(t *testing.T) (storage.Storage, func())) {
	s, cleanup := newStore(t)
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

// ── Plugins ──────────────────────────────────────────────────────────

func testPlugins(t *testing.T, newStore func(t *testing.T) (storage.Storage, func())) {
	s, cleanup := newStore(t)
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
		t.Fatalf("get: err=%v got=%+v", err, got)
	}
	list, err := s.ListPlugins(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("list len = %d, want 1", len(list))
	}
	// Upsert.
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
	if got3, _ := s.GetPlugin(ctx, "echo"); got3 != nil {
		t.Error("expected nil after delete")
	}
}

// ── Settings ─────────────────────────────────────────────────────────

func testSettings(t *testing.T, newStore func(t *testing.T) (storage.Storage, func())) {
	s, cleanup := newStore(t)
	defer cleanup()
	ctx := context.Background()

	// Missing key → empty, no error.
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
	// Update.
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

// jsonEqual reports whether two raw JSON documents are semantically equal,
// ignoring key order and whitespace. Needed because backends that store
// position in a JSON column (MySQL JSON type) canonicalize key order.
func jsonEqual(a, b []byte) bool {
	var ja, jb any
	if err := json.Unmarshal(a, &ja); err != nil {
		return false
	}
	if err := json.Unmarshal(b, &jb); err != nil {
		return false
	}
	return reflect.DeepEqual(ja, jb)
}

// itoa is a local strconv.Itoa to avoid pulling strconv into every subtest file.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
