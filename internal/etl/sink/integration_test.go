//go:build integration
// +build integration

package sink

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func envDefault(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// TestClickHouseSinkTypeInference verifies ClickHouse sink creates proper
// column types via auto_create, not all String as the old TEXT-only behavior.
// Requires: CLICKHOUSE_HOST env var or running clickhouse on localhost:9000.
func TestClickHouseSinkTypeInference(t *testing.T) {
	chHost := envDefault("CLICKHOUSE_HOST", "127.0.0.1")
	chPort := 9000
	_ = chPort

	sink, err := NewClickHouseSink(map[string]any{
		"host":        chHost,
		"port":        9000,
		"user":        envDefault("CLICKHOUSE_USER", "default"),
		"password":    envDefault("CLICKHOUSE_PASSWORD", "dzh123456"),
		"database":    "dzh3136_go",
		"table":       "test_etl_e2e",
		"auto_create": true,
	})
	if err != nil {
		t.Fatalf("NewClickHouseSink: %v", err)
	}
	defer sink.Close()

	ctx := context.Background()
	if err := sink.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Drop test table if it exists.
	_ = sink.execContext(ctx, "DROP TABLE IF EXISTS dzh3136_go.test_etl_e2e")

	// Write records — triggers auto_create.
	records := []core.Record{
		{
			Operation: core.OpInsert,
			Data: map[string]any{
				"id": 1, "name": "Alice", "age": int64(30),
				"score": 95.5, "is_active": true, "created_at": time.Now(),
			},
			Metadata: core.Metadata{Table: "test_etl_e2e"},
		},
		{
			Operation: core.OpInsert,
			Data: map[string]any{
				"id": 2, "name": "Bob", "age": int64(25),
				"score": 88.0, "is_active": false, "created_at": time.Now(),
			},
			Metadata: core.Metadata{Table: "test_etl_e2e"},
		},
	}

	if err := sink.Write(ctx, records); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify schema via system.columns.
	colRows, err := sink.conn.Query(ctx,
		"SELECT name, type FROM system.columns WHERE database='dzh3136_go' AND table='test_etl_e2e' ORDER BY position")
	if err != nil {
		t.Fatalf("Query schema: %v", err)
	}
	defer colRows.Close()

	columns := map[string]string{}
	for colRows.Next() {
		var name, typ string
		if err := colRows.Scan(&name, &typ); err != nil {
			t.Fatal(err)
		}
		columns[name] = typ
	}
	t.Logf("ClickHouse columns: %v", columns)

	// Column type assertions — verify not all String.
	checks := map[string]string{
		"age":        "Int64",
		"score":      "Float64",
		"name":       "String",
		"is_active":  "UInt8",
		"created_at": "DateTime64(3)",
		"id":         "Int64",
	}
	for col, wantType := range checks {
		got, ok := columns[col]
		if !ok {
			t.Errorf("column %q missing from schema", col)
		} else if got != wantType {
			t.Errorf("column %q: got %q, want %q", col, got, wantType)
		}
	}

	// Verify _version column exists for ReplacingMergeTree.
	if _, ok := columns["_version"]; !ok {
		t.Error("_version column missing from ClickHouse table")
	}

	// Cleanup
	_ = sink.execContext(ctx, "DROP TABLE IF EXISTS dzh3136_go.test_etl_e2e")
}

// TestMySQLSinkTypeInference verifies MySQL sink creates proper types.
// Requires: MYSQL_HOST env var.
func TestMySQLSinkTypeInference(t *testing.T) {
	mysqlHost := envDefault("MYSQL_HOST", "127.0.0.1")
	if os.Getenv("MYSQL_HOST") == "" {
		t.Skip("MYSQL_HOST not set, skipping MySQL integration test")
	}

	sink, err := NewMySQLSink(map[string]any{
		"host":        mysqlHost,
		"port":        3306,
		"user":        "root",
		"password":    "root123456",
		"database":    "dzh3136_go",
		"table":       "test_etl_types",
		"auto_create": true,
	})
	if err != nil {
		t.Fatalf("NewMySQLSink: %v", err)
	}
	defer sink.Close()

	ctx := context.Background()
	if err := sink.Open(ctx); err != nil {
		t.Fatalf("Open: %v", err)
	}

	sink.db.ExecContext(ctx, "DROP TABLE IF EXISTS test_etl_types")

	records := []core.Record{
		{
			Operation: core.OpInsert,
			Data: map[string]any{
				"id": 1, "name": "Test", "age": int64(30),
				"score": 95.5, "email": "test@example.com",
				"amount": 199.99, "created_at": time.Now(),
			},
			Metadata: core.Metadata{Table: "test_etl_types"},
		},
	}

	if err := sink.Write(ctx, records); err != nil {
		t.Fatalf("Write: %v", err)
	}

	// Verify column types.
	rows, err := sink.db.QueryContext(ctx,
		`SELECT column_name, column_type FROM information_schema.columns WHERE table_schema='dzh3136_go' AND table_name='test_etl_types' ORDER BY ordinal_position`)
	if err != nil {
		t.Fatalf("Query schema: %v", err)
	}
	defer rows.Close()

	columns := map[string]string{}
	for rows.Next() {
		var name, typ string
		if err := rows.Scan(&name, &typ); err != nil {
			t.Fatal(err)
		}
		columns[name] = typ
	}
	t.Logf("MySQL columns: %v", columns)

	// NOT all TEXT.
	textOnly := true
	for _, typ := range columns {
		if typ != "text" {
			textOnly = false
			break
		}
	}
	if textOnly {
		t.Error("All columns are TEXT — type inference not working")
	}

	// Cleanup
	sink.db.ExecContext(ctx, "DROP TABLE IF EXISTS test_etl_types")
}
