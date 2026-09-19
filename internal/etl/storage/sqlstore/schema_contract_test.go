package sqlstore

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// newContractTestStore opens a temp SQLite store with migrations applied.
func newContractTestStore(t *testing.T) *Store {
	t.Helper()
	dsn := filepath.Join(t.TempDir(), "contract.db") + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(wal)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	s := New(db, SQLiteDialect{})
	if err := s.migrate(); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return s
}

// CH-C2 (IT-5/T5.4): contract CRUD round-trips with fingerprint and column
// fidelity across save/load/latest/list/delete.
func TestSchemaContractStoreRoundTrip(t *testing.T) {
	s := newContractTestStore(t)
	ctx := context.Background()

	cols := []core.ColumnContract{
		{Name: "id", Type: "INT"},
		{Name: "name", Type: "VARCHAR(64)", Nullable: true},
	}
	c1 := core.NormalizeSchemaContract("pipe-a", 3, cols, "mysql_batch", "db1", "t1")
	if err := s.SaveSchemaContract(ctx, c1); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := s.LoadSchemaContract(ctx, "pipe-a", 3)
	if err != nil || got == nil {
		t.Fatalf("load: %v %v", got, err)
	}
	if got.Fingerprint != c1.Fingerprint || len(got.Columns) != 2 || got.Columns[1].Nullable != true {
		t.Fatalf("round-trip fidelity lost: %#v", got)
	}
	if got.SourceType != "mysql_batch" || got.Database != "db1" || got.Table != "t1" {
		t.Fatalf("origin labels lost: %#v", got)
	}

	// Missing version -> nil, nil.
	none, err := s.LoadSchemaContract(ctx, "pipe-a", 1)
	if err != nil || none != nil {
		t.Fatalf("missing version must return nil,nil: %v %v", none, err)
	}

	// Latest across versions.
	c2 := core.NormalizeSchemaContract("pipe-a", 7, append(cols, core.ColumnContract{Name: "x", Type: "INT"}), "mysql_batch", "db1", "t1")
	if err := s.SaveSchemaContract(ctx, c2); err != nil {
		t.Fatalf("save v7: %v", err)
	}
	latest, err := s.LoadLatestSchemaContract(ctx, "pipe-a")
	if err != nil || latest == nil || latest.SpecVersion != 7 || len(latest.Columns) != 3 {
		t.Fatalf("latest = %#v err=%v", latest, err)
	}

	// Upsert same version replaces.
	c2b := core.NormalizeSchemaContract("pipe-a", 7, cols, "mysql_batch", "db1", "t1")
	if err := s.SaveSchemaContract(ctx, c2b); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	after, _ := s.LoadSchemaContract(ctx, "pipe-a", 7)
	if len(after.Columns) != 2 {
		t.Fatalf("upsert must replace: %d columns", len(after.Columns))
	}

	// List oldest first.
	all, err := s.ListSchemaContracts(ctx, "pipe-a")
	if err != nil || len(all) != 2 || all[0].SpecVersion != 3 {
		t.Fatalf("list = %#v err=%v", all, err)
	}

	// Delete cascade.
	if err := s.DeleteSchemaContracts(ctx, "pipe-a"); err != nil {
		t.Fatalf("delete: %v", err)
	}
	empty, _ := s.ListSchemaContracts(ctx, "pipe-a")
	if len(empty) != 0 {
		t.Fatalf("delete must remove all contracts: %#v", empty)
	}
}

// Validation guards.
func TestSchemaContractStoreValidation(t *testing.T) {
	s := newContractTestStore(t)
	ctx := context.Background()
	if err := s.SaveSchemaContract(ctx, core.SchemaContract{Pipeline: "", SpecVersion: 1, Fingerprint: "f"}); err == nil {
		t.Fatal("empty pipeline must be rejected")
	}
	if err := s.SaveSchemaContract(ctx, core.SchemaContract{Pipeline: "p", SpecVersion: 1, Fingerprint: ""}); err == nil {
		t.Fatal("empty fingerprint must be rejected")
	}
}
