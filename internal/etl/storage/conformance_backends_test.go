package storage_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"openetl-go/internal/etl/storage"
	"openetl-go/internal/etl/storage/mysql"
	"openetl-go/internal/etl/storage/postgres"
	"openetl-go/internal/etl/storage/sqlite"
)

// ── SQLite (hermetic, always runs — the default/demo mode per SPEC §1.2) ──

func newSQLiteStore(t *testing.T) (storage.Storage, func()) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "conformance.db")
	s, err := sqlite.New(path)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	return s, func() {
		s.Close()
		os.RemoveAll(dir)
	}
}

func TestSQLiteConformance(t *testing.T) {
	runConformanceSuite(t, newSQLiteStore)
}

// ── MySQL (scalable mode; skipped without MYSQL_DSN) ──────────────────

// mysqlTableWiper returns a cleanup that empties every storage table so each
// conformance subtest starts clean. Reused by the migration-parity test.
func mysqlTableWiper(t *testing.T, s *mysql.Store) func() {
	t.Helper()
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
			t.Fatalf("mysql cleanup %q: %v", q, err)
		}
	}
	return func() {}
}

func newMySQLStore(t *testing.T) (storage.Storage, func()) {
	t.Helper()
	dsn := os.Getenv("MYSQL_DSN")
	if dsn == "" {
		t.Skipf("MYSQL_DSN not set; skipping MySQL conformance")
	}
	s, err := mysql.New(dsn)
	if err != nil {
		t.Fatalf("open mysql: %v", err)
	}
	mysqlTableWiper(t, s)
	return s, func() { s.Close() }
}

func TestMySQLConformance(t *testing.T) {
	runConformanceSuite(t, newMySQLStore)
}

// ── PostgreSQL (scalable mode; skipped without POSTGRES_DSN) ──────────

func pgTableWiper(t *testing.T, s *postgres.Store) func() {
	t.Helper()
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
		if _, err := s.Pool().Exec(ctx, q); err != nil {
			s.Close()
			t.Fatalf("postgres cleanup %q: %v", q, err)
		}
	}
	return func() {}
}

func newPostgresStore(t *testing.T) (storage.Storage, func()) {
	t.Helper()
	dsn := os.Getenv("POSTGRES_DSN")
	if dsn == "" {
		t.Skipf("POSTGRES_DSN not set; skipping Postgres conformance")
	}
	ctx := context.Background()
	s, err := postgres.New(ctx, dsn)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	pgTableWiper(t, s)
	return s, func() { s.Close() }
}

func TestPostgresConformance(t *testing.T) {
	runConformanceSuite(t, newPostgresStore)
}

// ── Migration parity: a spec defined under SQLite round-trips identically
// through MySQL. Guards against dialect drift in the pipeline-persistence
// layer. (SPEC §5.2 dual-mode test matrix, ROADMAP A10) ─────────────────

func TestMigrationParitySQLiteToMySQL(t *testing.T) {
	mysqlDSN := os.Getenv("MYSQL_DSN")
	if mysqlDSN == "" {
		t.Skipf("MYSQL_DSN not set; skipping migration-parity test")
	}

	const specYAML = "name: parity-pipe\nsource:\n  type: mysql_cdc\n  config:\n    host: db\nsink:\n  type: clickhouse\n"

	// Write to SQLite.
	sqlStore, sqlCleanup := newSQLiteStore(t)
	defer sqlCleanup()
	ctx := context.Background()
	sqlRow := pipelineRow("parity-pipe", specYAML)
	if err := sqlStore.SavePipeline(ctx, &sqlRow); err != nil {
		t.Fatalf("sqlite save: %v", err)
	}
	sqlLoaded, _ := sqlStore.GetPipeline(ctx, "parity-pipe")
	if sqlLoaded == nil {
		t.Fatal("sqlite load nil")
	}

	// Import into MySQL and verify identical spec_yaml + status.
	myStoreUntyped, myCleanup := newMySQLStore(t)
	defer myCleanup()
	myStore := myStoreUntyped.(*mysql.Store)
	// myCleanup already wiped tables at factory time.
	myRow := pipelineRow("parity-pipe", sqlLoaded.SpecYAML)
	if err := myStore.SavePipeline(ctx, &myRow); err != nil {
		t.Fatalf("mysql save: %v", err)
	}
	myLoaded, err := myStore.GetPipeline(ctx, "parity-pipe")
	if err != nil {
		t.Fatalf("mysql load: %v", err)
	}
	if myLoaded == nil {
		t.Fatal("mysql load nil")
	}
	if myLoaded.SpecYAML != sqlLoaded.SpecYAML {
		t.Errorf("spec_yaml drift:\n sqlite=%q\n mysql =%q", sqlLoaded.SpecYAML, myLoaded.SpecYAML)
	}
	if myLoaded.Status != sqlLoaded.Status {
		t.Errorf("status drift: sqlite=%q mysql=%q", sqlLoaded.Status, myLoaded.Status)
	}

	// Version numbering must be backend-independent and start at 1.
	mysqlTableWiper(t, myStore) // reset
	v, err := myStore.SavePipelineVersion(ctx, "parity-pipe", specYAML)
	if err != nil {
		t.Fatalf("mysql save version: %v", err)
	}
	if v != 1 {
		t.Errorf("mysql first version = %d, want 1", v)
	}
}

// pipelineRow builds a PipelineRow with the canonical conformance status.
func pipelineRow(name, specYAML string) storage.PipelineRow {
	return storage.PipelineRow{
		Name:     name,
		SpecYAML: specYAML,
		Status:   "loaded",
	}
}
