package transform

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func baseDBTConfig(tmp string) map[string]any {
	return map[string]any{
		"project_dir":      tmp,
		"model_name":       "transformed_orders",
		"source_schema":    "etl_staging",
		"source_table":     "orders_raw",
		"target_schema":    "etl_output",
		"target_table":     "transformed_orders",
		"adapter":          "postgres",
		"dsn":              "postgres://etl:secret@db.example:5432/warehouse?sslmode=disable",
		"threads":          2,
		"target":           "dev",
		"exec_timeout_sec": 30,
		"write_mode":       "replace",
	}
}

func TestNewDBTTransformValidation(t *testing.T) {
	tmp := t.TempDir()

	t.Run("missing project_dir", func(t *testing.T) {
		cfg := baseDBTConfig(tmp)
		delete(cfg, "project_dir")
		if _, err := NewDBTTransform(cfg); err == nil {
			t.Fatal("expected error for missing project_dir")
		}
	})

	t.Run("missing model_name", func(t *testing.T) {
		cfg := baseDBTConfig(tmp)
		delete(cfg, "model_name")
		if _, err := NewDBTTransform(cfg); err == nil {
			t.Fatal("expected error for missing model_name")
		}
	})

	t.Run("missing source_table", func(t *testing.T) {
		cfg := baseDBTConfig(tmp)
		delete(cfg, "source_table")
		if _, err := NewDBTTransform(cfg); err == nil {
			t.Fatal("expected error for missing source_table")
		}
	})

	t.Run("missing dsn for postgres", func(t *testing.T) {
		cfg := baseDBTConfig(tmp)
		delete(cfg, "dsn")
		if _, err := NewDBTTransform(cfg); err == nil {
			t.Fatal("expected error for missing dsn")
		}
	})

	t.Run("unsupported adapter", func(t *testing.T) {
		cfg := baseDBTConfig(tmp)
		cfg["adapter"] = "clickhouse"
		if _, err := NewDBTTransform(cfg); err == nil {
			t.Fatal("expected error for unsupported adapter")
		}
	})

	t.Run("duckdb requires path", func(t *testing.T) {
		cfg := baseDBTConfig(tmp)
		cfg["adapter"] = "duckdb"
		delete(cfg, "dsn")
		if _, err := NewDBTTransform(cfg); err == nil {
			t.Fatal("expected error for missing duckdb path")
		}
	})

	t.Run("happy path defaults", func(t *testing.T) {
		cfg := baseDBTConfig(tmp)
		tr, err := NewDBTTransform(cfg)
		if err != nil {
			t.Fatalf("NewDBTTransform: %v", err)
		}
		if tr.Name() != "dbt" {
			t.Fatalf("name = %q", tr.Name())
		}
		if tr.threads != 2 {
			t.Fatalf("threads = %d", tr.threads)
		}
		if tr.execTimeout != 30*time.Second {
			t.Fatalf("timeout = %s", tr.execTimeout)
		}
		if tr.targetTable != "transformed_orders" {
			t.Fatalf("target_table = %q", tr.targetTable)
		}
	})

	t.Run("auto detect duckdb from path", func(t *testing.T) {
		cfg := baseDBTConfig(tmp)
		delete(cfg, "adapter")
		delete(cfg, "dsn")
		cfg["path"] = "/tmp/warehouse.duckdb"
		tr, err := NewDBTTransform(cfg)
		if err != nil {
			t.Fatalf("NewDBTTransform: %v", err)
		}
		if tr.adapter != "duckdb" {
			t.Fatalf("adapter = %q, want duckdb", tr.adapter)
		}
	})
}

func TestDBTBuildCommandArgs(t *testing.T) {
	tmp := t.TempDir()
	cfg := baseDBTConfig(tmp)
	cfg["profiles_dir"] = "/tmp/profiles"
	cfg["full_refresh"] = true
	cfg["vars"] = map[string]any{"env": "test", "batch": 1}
	tr, err := NewDBTTransform(cfg)
	if err != nil {
		t.Fatal(err)
	}

	args := tr.BuildDBTArgs()
	joined := strings.Join(args, " ")
	wantParts := []string{
		"run",
		"--project-dir " + tmp,
		"--profiles-dir /tmp/profiles",
		"--target dev",
		"--select transformed_orders",
		"--threads 2",
		"--full-refresh",
		"--vars",
	}
	for _, p := range wantParts {
		if !strings.Contains(joined, p) {
			t.Fatalf("args missing %q\nfull: %s", p, joined)
		}
	}
	// vars should be stable-ordered
	if !strings.Contains(joined, "batch: 1") || !strings.Contains(joined, "env: test") {
		t.Fatalf("vars not rendered: %s", joined)
	}
}

func TestDBTBuildProfilesYMLPostgres(t *testing.T) {
	tmp := t.TempDir()
	// Seed a dbt_project.yml so profile name is picked up.
	if err := os.WriteFile(filepath.Join(tmp, "dbt_project.yml"), []byte("name: demo\nprofile: my_etl\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	cfg := baseDBTConfig(tmp)
	tr, err := NewDBTTransform(cfg)
	if err != nil {
		t.Fatal(err)
	}
	yml, err := tr.BuildProfilesYML()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"my_etl:",
		"type: postgres",
		`host: "db.example"`,
		`user: "etl"`,
		`password: "secret"`,
		"port: 5432",
		`dbname: "warehouse"`,
		`schema: "etl_staging"`,
		`sslmode: "disable"`,
		"threads: 2",
	} {
		if !strings.Contains(yml, want) {
			t.Fatalf("profiles.yml missing %q\n%s", want, yml)
		}
	}
}

func TestDBTBuildProfilesYMLDuckDB(t *testing.T) {
	tmp := t.TempDir()
	cfg := baseDBTConfig(tmp)
	cfg["adapter"] = "duckdb"
	delete(cfg, "dsn")
	cfg["path"] = "/data/warehouse.duckdb"
	tr, err := NewDBTTransform(cfg)
	if err != nil {
		t.Fatal(err)
	}
	yml, err := tr.BuildProfilesYML()
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		"type: duckdb",
		`path: "/data/warehouse.duckdb"`,
	} {
		if !strings.Contains(yml, want) {
			t.Fatalf("profiles.yml missing %q\n%s", want, yml)
		}
	}
}

func TestParsePostgresDSN(t *testing.T) {
	host, port, user, pass, db, ssl, err := parsePostgresDSN("postgres://alice:p%40ss@pg:15432/analytics?sslmode=require")
	if err != nil {
		t.Fatal(err)
	}
	if host != "pg" || port != 15432 || user != "alice" || pass != "p%40ss" || db != "analytics" || ssl != "require" {
		t.Fatalf("got host=%s port=%d user=%s pass=%s db=%s ssl=%s", host, port, user, pass, db, ssl)
	}

	host, port, user, pass, db, ssl, err = parsePostgresDSN("host=localhost user=u password=p dbname=d sslmode=disable port=5433")
	if err != nil {
		t.Fatal(err)
	}
	if host != "localhost" || port != 5433 || user != "u" || pass != "p" || db != "d" || ssl != "disable" {
		t.Fatalf("keyword dsn parse failed: host=%s port=%d user=%s pass=%s db=%s ssl=%s", host, port, user, pass, db, ssl)
	}
}

func TestDBTApplyBatchHappyPath(t *testing.T) {
	tmp := t.TempDir()
	cfg := baseDBTConfig(tmp)
	tr, err := NewDBTTransform(cfg)
	if err != nil {
		t.Fatal(err)
	}

	db, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
		sqlmock.MonitorPingsOption(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	tr.openDB = func(driver, dsn string) (*sql.DB, error) {
		if driver != "pgx" {
			t.Fatalf("driver = %q", driver)
		}
		return db, nil
	}
	var ranArgs []string
	tr.commandRunner = func(ctx context.Context, name string, args []string, env []string, dir string) (string, string, int, error) {
		ranArgs = args
		if name != "dbt" {
			t.Fatalf("binary = %q", name)
		}
		return "Done.", "", 0, nil
	}

	// Ping
	mock.ExpectPing()
	// CREATE TABLE IF NOT EXISTS
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS`).WillReturnResult(sqlmock.NewResult(0, 0))
	// DELETE (replace mode)
	mock.ExpectExec(`DELETE FROM`).WillReturnResult(sqlmock.NewResult(0, 2))
	// BEGIN + 2 inserts + COMMIT (columns sorted: amount, id)
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO`).WithArgs(10.5, 1).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec(`INSERT INTO`).WithArgs(20.0, 2).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	// SELECT * FROM target
	rows := sqlmock.NewRows([]string{"id", "amount", "doubled"}).
		AddRow(int64(1), 10.5, 21.0).
		AddRow(int64(2), 20.0, 40.0)
	mock.ExpectQuery(`SELECT \* FROM`).WillReturnRows(rows)

	in := []core.Record{
		{Operation: core.OpInsert, Data: map[string]any{"id": 1, "amount": 10.5}, Metadata: core.Metadata{Source: "mysql", Table: "orders", Key: "1"}},
		{Operation: core.OpInsert, Data: map[string]any{"id": 2, "amount": 20.0}, Metadata: core.Metadata{Source: "mysql", Table: "orders", Key: "2"}},
	}
	out, err := tr.ApplyBatch(context.Background(), in)
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("len(out)=%d", len(out))
	}
	if out[0].Data["doubled"] != 21.0 {
		t.Fatalf("doubled = %#v", out[0].Data["doubled"])
	}
	if out[0].Metadata.Source != "mysql" {
		t.Fatalf("lineage source = %q", out[0].Metadata.Source)
	}
	if out[0].Metadata.Key != "1" {
		t.Fatalf("lineage key = %q", out[0].Metadata.Key)
	}
	if !strings.Contains(strings.Join(ranArgs, " "), "--select transformed_orders") {
		t.Fatalf("dbt args = %v", ranArgs)
	}
	// profiles.yml should have been written
	if _, err := os.Stat(filepath.Join(tr.profilesDir, "profiles.yml")); err != nil {
		t.Fatalf("profiles.yml not written: %v", err)
	}
	metrics := tr.TransformMetrics()
	if metrics.Counters["dbt_runs"] != 1 || metrics.Counters["records_in"] != 2 || metrics.Counters["records_out"] != 2 {
		t.Fatalf("metrics = %#v", metrics.Counters)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
	_ = tr.Close()
}

func TestDBTRunNonZeroExit(t *testing.T) {
	tmp := t.TempDir()
	cfg := baseDBTConfig(tmp)
	tr, err := NewDBTTransform(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
		sqlmock.MonitorPingsOption(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tr.openDB = func(driver, dsn string) (*sql.DB, error) { return db, nil }
	tr.commandRunner = func(ctx context.Context, name string, args []string, env []string, dir string) (string, string, int, error) {
		return "Compilation Error", "model not found", 1, nil
	}
	mock.ExpectPing()
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO`).WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	_, err = tr.ApplyBatch(context.Background(), []core.Record{
		{Data: map[string]any{"id": 1}},
	})
	if err == nil {
		t.Fatal("expected dbt failure error")
	}
	if !strings.Contains(err.Error(), "exit=1") {
		t.Fatalf("error = %v", err)
	}
	if tr.TransformMetrics().Counters["dbt_failures"] != 1 {
		t.Fatalf("dbt_failures not incremented")
	}
}

func TestDBTRunTimeout(t *testing.T) {
	tmp := t.TempDir()
	cfg := baseDBTConfig(tmp)
	cfg["exec_timeout_sec"] = 1
	tr, err := NewDBTTransform(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
		sqlmock.MonitorPingsOption(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tr.openDB = func(driver, dsn string) (*sql.DB, error) { return db, nil }
	tr.commandRunner = func(ctx context.Context, name string, args []string, env []string, dir string) (string, string, int, error) {
		<-ctx.Done()
		return "", "", -1, ctx.Err()
	}
	mock.ExpectPing()
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec(`DELETE FROM`).WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO`).WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	_, err = tr.ApplyBatch(context.Background(), []core.Record{{Data: map[string]any{"id": 1}}})
	if err == nil || !strings.Contains(err.Error(), "timed out") {
		t.Fatalf("expected timeout error, got %v", err)
	}
}

func TestDBTRegisteredInRegistry(t *testing.T) {
	tmp := t.TempDir()
	tr, err := registry.BuildTransform("dbt", baseDBTConfig(tmp))
	if err != nil {
		t.Fatalf("BuildTransform(dbt): %v", err)
	}
	if tr.Name() != "dbt" {
		t.Fatalf("name = %q", tr.Name())
	}
	if _, ok := tr.(core.BatchTransform); !ok {
		t.Fatal("dbt transform should implement BatchTransform")
	}
}

func TestDBTApplyEmptyBatch(t *testing.T) {
	tmp := t.TempDir()
	tr, err := NewDBTTransform(baseDBTConfig(tmp))
	if err != nil {
		t.Fatal(err)
	}
	out, err := tr.ApplyBatch(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil {
		t.Fatalf("expected nil out, got %#v", out)
	}
}

func TestDBTWriteModeInsert(t *testing.T) {
	tmp := t.TempDir()
	cfg := baseDBTConfig(tmp)
	cfg["write_mode"] = "insert"
	tr, err := NewDBTTransform(cfg)
	if err != nil {
		t.Fatal(err)
	}
	db, mock, err := sqlmock.New(
		sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp),
		sqlmock.MonitorPingsOption(true),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	tr.openDB = func(driver, dsn string) (*sql.DB, error) { return db, nil }
	tr.commandRunner = func(ctx context.Context, name string, args []string, env []string, dir string) (string, string, int, error) {
		return "ok", "", 0, nil
	}
	mock.ExpectPing()
	mock.ExpectExec(`CREATE TABLE IF NOT EXISTS`).WillReturnResult(sqlmock.NewResult(0, 0))
	// no DELETE expected
	mock.ExpectBegin()
	mock.ExpectExec(`INSERT INTO`).WithArgs(9).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()
	mock.ExpectQuery(`SELECT \* FROM`).WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(int64(9)))

	out, err := tr.ApplyBatch(context.Background(), []core.Record{{Data: map[string]any{"id": 9}}})
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("len=%d", len(out))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}
