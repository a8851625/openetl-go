package source

import (
	"context"
	"regexp"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// TestMySQLBatchSourceParsesDatabase verifies the `database` config field is
// parsed into MySQLBatchSource.database and threaded into the reader.
func TestMySQLBatchSourceParsesDatabase(t *testing.T) {
	s, err := NewMySQLBatchSource(map[string]any{
		"host":      "127.0.0.1",
		"port":      3306,
		"user":      "root",
		"password":  "x",
		"database":  "shop",
		"table":     "orders",
		"pk_column": "id",
	})
	if err != nil {
		t.Fatalf("NewMySQLBatchSource: %v", err)
	}
	if s.database != "shop" {
		t.Errorf("source.database = %q, want shop", s.database)
	}

	s2, err := NewMySQLBatchSource(map[string]any{
		"host": "127.0.0.1", "port": 3306, "user": "root", "password": "x",
		"table": "orders", "pk_column": "id",
	})
	if err != nil {
		t.Fatalf("NewMySQLBatchSource (no db): %v", err)
	}
	if s2.database != "" {
		t.Errorf("source.database = %q, want empty when omitted", s2.database)
	}
}

// TestMySQLBatchReaderFillsDatabaseMetadata verifies the new assignment
// `Metadata.Database = r.database` in ReadBatch: records emitted by mysql_batch
// carry the source database so downstream sinks (e.g. kafka sink
// topic_template={db}-{table}) route by table of origin. Uses go-sqlmock so no
// real MySQL is required.
func TestMySQLBatchReaderFillsDatabaseMetadata(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// mysql_batch keyset pagination on the non-custom-query path:
	//   SELECT * FROM <table> WHERE <pk> > ? ORDER BY <pk> LIMIT <n>
	// Real MySQL honors the LIMIT clause, so the mock returns a single row.
	rows := sqlmock.NewRows([]string{"id", "name"}).
		AddRow(1, "a")
	mock.ExpectQuery(regexp.QuoteMeta("SELECT * FROM orders WHERE id > ? ORDER BY id LIMIT 1")).
		WithArgs(int64(0)).
		WillReturnRows(rows)

	r := &mysqlBatchReader{
		db:       db,
		database: "shop",
		table:    "orders",
		pkCol:    "id",
		limit:    5000,
	}

	recs, err := r.ReadBatch(context.Background(), 1)
	if err != nil {
		t.Fatalf("ReadBatch: %v", err)
	}
	if len(recs) != 1 {
		t.Fatalf("records = %d, want 1", len(recs))
	}
	rec := recs[0]
	if rec.Metadata.Database != "shop" {
		t.Errorf("Metadata.Database = %q, want shop", rec.Metadata.Database)
	}
	if rec.Metadata.Table != "orders" {
		t.Errorf("Metadata.Table = %q, want orders", rec.Metadata.Table)
	}
	if rec.Operation != core.OpInsert {
		t.Errorf("Operation = %q, want INSERT", rec.Operation)
	}
	if got := rec.Data["id"]; got != int64(1) {
		t.Errorf("Data.id = %#v, want 1", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet sqlmock expectations: %v", err)
	}
}
