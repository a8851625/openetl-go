package source

import (
	"context"
	"encoding/json"
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

// TestMySQLBatchStringPKCursorAdvances is the BUG-1 regression: a varchar
// primary key must advance the cursor so `WHERE pk > ?` progresses instead
// of re-reading the whole table forever. Also verifies checkpoint
// serialization round-trips a string cursor and restores legacy numeric
// checkpoints unchanged.
func TestMySQLBatchStringPKCursorAdvances(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	r := &mysqlBatchReader{
		db:    db,
		pkCol: "request_id",
		limit: 2,
		table: "orders",
	}

	// First batch: two varchar rows, ascending. Second batch: empty -> done.
	q := regexp.QuoteMeta("SELECT * FROM orders WHERE request_id > ? ORDER BY request_id LIMIT 2")
	mock.ExpectQuery(q).WithArgs(int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"request_id", "v"}).
			AddRow("a-1", "x").AddRow("b-2", "y"))
	mock.ExpectQuery(q).WithArgs("b-2").
		WillReturnRows(sqlmock.NewRows([]string{"request_id", "v"}))

	recs, err := r.ReadBatch(context.Background(), 0)
	if err != nil {
		t.Fatalf("first batch: %v", err)
	}
	if len(recs) != 2 {
		t.Fatalf("first batch records = %d, want 2", len(recs))
	}
	if r.lastCursor != "b-2" {
		t.Fatalf("cursor after first batch = %#v, want \"b-2\" (string PK must advance)", r.lastCursor)
	}

	// Snapshot must serialize the string cursor in the new format.
	cp, err := r.Snapshot(context.Background())
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	var pos mysqlBatchPosition
	if err := json.Unmarshal(cp.Position, &pos); err != nil {
		t.Fatalf("unmarshal snapshot: %v", err)
	}
	if pos.LastCursor == nil || *pos.LastCursor != "b-2" {
		t.Fatalf("snapshot last_cursor = %#v, want \"b-2\"", pos.LastCursor)
	}

	// Second batch returns nothing: pipeline completes (done) instead of
	// looping forever.
	recs, err = r.ReadBatch(context.Background(), 0)
	if err != nil || len(recs) != 0 {
		t.Fatalf("second batch = (%d, %v), want (0, nil)", len(recs), err)
	}
	if !r.done {
		t.Fatal("reader not done after cursor passed all rows (infinite re-read)")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sqlmock expectations: %v", err)
	}
}

// TestMySQLBatchLegacyNumericCheckpointRestores verifies old numeric
// checkpoints ({"last_id":42}) still restore into a numeric cursor so the
// first query parameter stays 42 (BUG-1 acceptance: numeric behavior
// unchanged).
func TestMySQLBatchLegacyNumericCheckpointRestores(t *testing.T) {
	legacy := []byte(`{"last_id":42}`)
	var pos mysqlBatchPosition
	if err := json.Unmarshal(legacy, &pos); err != nil {
		t.Fatalf("unmarshal legacy: %v", err)
	}
	var cursor any
	if pos.LastCursor != nil {
		cursor = *pos.LastCursor
	} else if pos.LastID != 0 {
		cursor = pos.LastID
	}
	if cursor != int64(42) {
		t.Fatalf("legacy restore cursor = %#v, want int64(42)", cursor)
	}

	// New-format numeric checkpoint keeps last_id byte-compatibility.
	r := &mysqlBatchReader{pkCol: "id", lastCursor: cursor}
	cp, _ := r.Snapshot(context.Background())
	var out mysqlBatchPosition
	if err := json.Unmarshal(cp.Position, &out); err != nil {
		t.Fatalf("unmarshal numeric snapshot: %v", err)
	}
	if out.LastID != 42 || out.LastCursor != nil {
		t.Fatalf("numeric snapshot = %+v, want last_id=42 no cursor", out)
	}
}
