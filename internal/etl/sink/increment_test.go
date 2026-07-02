package sink

import (
	"strings"
	"testing"
)

func TestMySQLIncrementBuildSQL(t *testing.T) {
	s := &MySQLSink{
		name:             "mysql",
		table:            "stock",
		batchMode:        "increment",
		pkColumns:        []string{"sku"},
		incrementColumns: map[string]string{"qty": "qty"},
		insertChunkSize:  500,
	}
	stmt := s.buildBatchInsertStatement("stock", []string{"sku", "qty"}, 2, "increment")
	if !strings.Contains(stmt, "ON DUPLICATE KEY UPDATE") {
		t.Fatalf("missing ON DUPLICATE KEY UPDATE: %s", stmt)
	}
	if !strings.Contains(stmt, "`qty`=IFNULL(`qty`,0)+VALUES(`qty`)") {
		t.Fatalf("missing increment clause for qty: %s", stmt)
	}
	// sku (PK) should NOT appear in the update clause
	updStart := strings.Index(stmt, "ON DUPLICATE KEY UPDATE")
	updClause := stmt[updStart:]
	if strings.Contains(updClause, "`sku`=VALUES") {
		t.Fatalf("PK sku should not be in update clause: %s", updClause)
	}
}

func TestMySQLIncrementRequiresIncrementColumns(t *testing.T) {
	_, err := NewMySQLSink(map[string]any{
		"host":       "mysql",
		"user":       "u",
		"database":   "db",
		"table":      "t",
		"batch_mode": "increment",
		"pk_columns": []any{"id"},
	})
	if err == nil {
		t.Fatalf("expected error: increment requires increment_columns")
	}
}

func TestPostgresIncrementRequiresIncrementColumns(t *testing.T) {
	_, err := NewPostgresSink(map[string]any{
		"host":       "pg",
		"user":       "u",
		"database":   "db",
		"table":      "t",
		"batch_mode": "increment",
		"pk_columns": []any{"id"},
	})
	if err == nil {
		t.Fatalf("expected error: increment requires increment_columns")
	}
}
