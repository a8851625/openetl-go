package sink

import (
	"context"
	"fmt"
	"os"
	"testing"

	"openetl-go/internal/etl/core"
)

// TestMySQLSinkUpsertIdempotency verifies that re-running an upsert sink with
// the same input produces no duplicates (PK-conflict upsert).
//
// Requires MYSQL_HOST env var to be set; otherwise skipped.
func TestMySQLSinkUpsertIdempotency(t *testing.T) {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		t.Skip("MYSQL_HOST not set; skipping integration idempotency test")
	}

	s, err := NewMySQLSink(map[string]any{
		"host":       host,
		"port":       atoiOr(os.Getenv("MYSQL_PORT"), 3306),
		"user":       getenvOr("MYSQL_USER", "root"),
		"password":   os.Getenv("MYSQL_PASSWORD"),
		"database":   getenvOr("MYSQL_DATABASE", "mysql"),
		"table":      "idempotency_test",
		"pk_columns": []interface{}{"id"},
		"batch_mode": "upsert",
	})
	if err != nil {
		t.Fatalf("NewMySQLSink: %v", err)
	}
	if err := s.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	// Reset table.
	if _, err := s.db.Exec("DROP TABLE IF EXISTS idempotency_test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("CREATE TABLE idempotency_test (id INT PRIMARY KEY, name VARCHAR(64), value INT)"); err != nil {
		t.Fatal(err)
	}

	recs := []core.Record{
		{Operation: core.OpInsert, Data: map[string]any{"id": 1, "name": "alice", "value": 100}},
		{Operation: core.OpInsert, Data: map[string]any{"id": 2, "name": "bob", "value": 200}},
		{Operation: core.OpInsert, Data: map[string]any{"id": 3, "name": "carol", "value": 300}},
	}

	// Write the same batch twice (simulating replay after crash).
	for i := 0; i < 2; i++ {
		if err := s.Write(context.Background(), recs); err != nil {
			t.Fatalf("Write %d: %v", i+1, err)
		}
	}

	// Verify row count = 3 (no duplicates).
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM idempotency_test").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 3 {
		t.Errorf("after replay, row count = %d, want 3 (duplicates leaked)", count)
	}

	// Verify data integrity (latest values retained).
	var name string
	var value int
	if err := s.db.QueryRow("SELECT name, value FROM idempotency_test WHERE id = 1").Scan(&name, &value); err != nil {
		t.Fatal(err)
	}
	if name != "alice" || value != 100 {
		t.Errorf("row id=1: name=%q value=%d, want alice/100", name, value)
	}
}

// TestMySQLSinkInsertIgnoreAvoidsDuplicates verifies the default insert mode
// uses INSERT IGNORE so replays don't fail or duplicate.
func TestMySQLSinkInsertIgnoreAvoidsDuplicates(t *testing.T) {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		t.Skip("MYSQL_HOST not set; skipping integration idempotency test")
	}

	s, err := NewMySQLSink(map[string]any{
		"host":       host,
		"port":       atoiOr(os.Getenv("MYSQL_PORT"), 3306),
		"user":       getenvOr("MYSQL_USER", "root"),
		"password":   os.Getenv("MYSQL_PASSWORD"),
		"database":   getenvOr("MYSQL_DATABASE", "mysql"),
		"table":      "ignore_test",
		"pk_columns": []interface{}{"id"},
	})
	if err != nil {
		t.Fatalf("NewMySQLSink: %v", err)
	}
	if err := s.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if _, err := s.db.Exec("DROP TABLE IF EXISTS ignore_test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("CREATE TABLE ignore_test (id INT PRIMARY KEY, val VARCHAR(64))"); err != nil {
		t.Fatal(err)
	}

	recs := []core.Record{
		{Operation: core.OpInsert, Data: map[string]any{"id": 1, "val": "first"}},
		{Operation: core.OpInsert, Data: map[string]any{"id": 2, "val": "second"}},
	}
	// Write twice.
	for i := 0; i < 2; i++ {
		if err := s.Write(context.Background(), recs); err != nil {
			t.Fatalf("Write %d: %v", i+1, err)
		}
	}

	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM ignore_test").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("count = %d, want 2 (INSERT IGNORE should not duplicate)", count)
	}
}

// TestMySQLSinkUpdateModifiesRow verifies UPDATE ops route through upsert.
func TestMySQLSinkUpdateModifiesRow(t *testing.T) {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		t.Skip("MYSQL_HOST not set; skipping integration idempotency test")
	}

	s, err := NewMySQLSink(map[string]any{
		"host":       host,
		"port":       atoiOr(os.Getenv("MYSQL_PORT"), 3306),
		"user":       getenvOr("MYSQL_USER", "root"),
		"password":   os.Getenv("MYSQL_PASSWORD"),
		"database":   getenvOr("MYSQL_DATABASE", "mysql"),
		"table":      "update_test",
		"pk_columns": []interface{}{"id"},
	})
	if err != nil {
		t.Fatalf("NewMySQLSink: %v", err)
	}
	if err := s.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if _, err := s.db.Exec("DROP TABLE IF EXISTS update_test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("CREATE TABLE update_test (id INT PRIMARY KEY, counter INT)"); err != nil {
		t.Fatal(err)
	}

	// Insert initial.
	if err := s.Write(context.Background(), []core.Record{
		{Operation: core.OpInsert, Data: map[string]any{"id": 1, "counter": 5}},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Update via upsert path.
	if err := s.Write(context.Background(), []core.Record{
		{Operation: core.OpUpdate, Data: map[string]any{"id": 1, "counter": 10}},
	}); err != nil {
		t.Fatalf("update: %v", err)
	}

	var counter int
	if err := s.db.QueryRow("SELECT counter FROM update_test WHERE id = 1").Scan(&counter); err != nil {
		t.Fatal(err)
	}
	if counter != 10 {
		t.Errorf("counter = %d, want 10 (update didn't take effect)", counter)
	}
	// Ensure only one row.
	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM update_test").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Errorf("count = %d, want 1 (update created duplicate)", count)
	}
}

// TestMySQLSinkDeleteRemovesRow verifies DELETE ops.
func TestMySQLSinkDeleteRemovesRow(t *testing.T) {
	host := os.Getenv("MYSQL_HOST")
	if host == "" {
		t.Skip("MYSQL_HOST not set; skipping integration idempotency test")
	}

	s, err := NewMySQLSink(map[string]any{
		"host":       host,
		"port":       atoiOr(os.Getenv("MYSQL_PORT"), 3306),
		"user":       getenvOr("MYSQL_USER", "root"),
		"password":   os.Getenv("MYSQL_PASSWORD"),
		"database":   getenvOr("MYSQL_DATABASE", "mysql"),
		"table":      "delete_test",
		"pk_columns": []interface{}{"id"},
	})
	if err != nil {
		t.Fatalf("NewMySQLSink: %v", err)
	}
	if err := s.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()

	if _, err := s.db.Exec("DROP TABLE IF EXISTS delete_test"); err != nil {
		t.Fatal(err)
	}
	if _, err := s.db.Exec("CREATE TABLE delete_test (id INT PRIMARY KEY, name VARCHAR(64))"); err != nil {
		t.Fatal(err)
	}

	// Insert 3 rows.
	if err := s.Write(context.Background(), []core.Record{
		{Operation: core.OpInsert, Data: map[string]any{"id": 1, "name": "a"}},
		{Operation: core.OpInsert, Data: map[string]any{"id": 2, "name": "b"}},
		{Operation: core.OpInsert, Data: map[string]any{"id": 3, "name": "c"}},
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// Delete row 2.
	if err := s.Write(context.Background(), []core.Record{
		{Operation: core.OpDelete, Data: map[string]any{"id": 2}},
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	var count int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM delete_test").Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 2 {
		t.Errorf("after delete, count = %d, want 2", count)
	}

	// Verify row 2 is gone.
	var exists int
	if err := s.db.QueryRow("SELECT COUNT(*) FROM delete_test WHERE id = 2").Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != 0 {
		t.Errorf("row id=2 still exists after delete (exists=%d)", exists)
	}
}

// guard to silence unused import warnings if some helpers are not used.
var _ = fmt.Sprintf
