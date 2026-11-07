package sink

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/a8851625/openetl-go/internal/etl/core"
)

// metadataPKRecord builds a record that carries a complete declared identity
// (PrimaryKeyColumns + JSON-object Key) as a CDC connector would.
func metadataPKRecord(op core.OpType, table string, data map[string]any, pkCols []string) core.Record {
	key := make(map[string]any, len(pkCols))
	for _, column := range pkCols {
		key[column] = data[column]
	}
	keyJSON, _ := json.Marshal(key)
	return core.Record{
		Operation: op,
		Data:      data,
		Metadata: core.Metadata{
			SourceType:        core.SourceTypeMySQLCDC,
			Source:            "mysql_cdc",
			Database:          "app",
			Table:             table,
			BinlogFile:        "mysql-bin.000001",
			BinlogPos:         100,
			PrimaryKeyColumns: pkCols,
			Key:               string(keyJSON),
		},
	}
}

func TestMetadataPKDocumentID(t *testing.T) {
	rec := metadataPKRecord(core.OpInsert, "orders", map[string]any{"tenant_id": 7, "order_id": 42}, []string{"tenant_id", "order_id"})
	if got := metadataPKDocumentID(rec, []string{"tenant_id", "order_id"}); got != "7:42" {
		t.Fatalf("document id = %q, want 7:42", got)
	}
	// Deletes may only carry the key in the before image.
	del := core.Record{
		Operation: core.OpDelete,
		Data:      map[string]any{"other": 1},
		Before:    map[string]any{"order_id": 42},
	}
	if got := metadataPKDocumentID(del, []string{"order_id"}); got != "42" {
		t.Fatalf("delete document id = %q, want 42", got)
	}
	if got := metadataPKDocumentID(rec, nil); got != "" {
		t.Fatalf("empty columns must yield no id, got %q", got)
	}
}

func TestJDBCMetadataPKDrivesUpsertAndDelete(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := &JDBCSink{
		name:                  "jdbc",
		driver:                "pgx",
		table:                 "",
		database:              "app",
		schema:                "public",
		batchMode:             "upsert",
		insertChunkSize:       500,
		pkColumnsFromMetadata: true,
		db:                    db,
		schemaCache:           core.NewSchemaCache(),
	}

	mock.ExpectBegin()
	// One table with a composite key, one row per table, upsert path.
	mock.ExpectExec("INSERT INTO").
		WithArgs("x", int64(42), int64(7)). // columns are sorted: amount, order_id, tenant_id
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	rec := metadataPKRecord(core.OpInsert, "orders", map[string]any{"tenant_id": int64(7), "order_id": int64(42), "amount": "x"}, []string{"tenant_id", "order_id"})
	if err := s.Write(context.Background(), []core.Record{rec}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestJDBCMetadataPKRejectsIncompleteIdentity(t *testing.T) {
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	s := &JDBCSink{
		name:                  "jdbc",
		driver:                "pgx",
		database:              "app",
		batchMode:             "upsert",
		insertChunkSize:       500,
		pkColumnsFromMetadata: true,
		db:                    db,
		schemaCache:           core.NewSchemaCache(),
	}
	// No PrimaryKeyColumns / Key at all: the sink must refuse before any SQL.
	rec := core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1}, Metadata: core.Metadata{Table: "orders"}}
	err = s.Write(context.Background(), []core.Record{rec})
	if err == nil || !strings.Contains(err.Error(), "metadata-PK") {
		t.Fatalf("err = %v, want a metadata-PK rejection", err)
	}
}

func TestElasticsearchMetadataPKBuildsDocumentID(t *testing.T) {
	var bodies []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, string(body))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"errors":false,"items":[]}`))
	}))
	defer ts.Close()

	s, err := NewElasticsearchSink(map[string]any{
		"hosts":                    []any{ts.URL},
		"index":                    "orders",
		"pk_columns_from_metadata": true,
		"chunk_size":               10,
	})
	if err != nil {
		t.Fatalf("NewElasticsearchSink: %v", err)
	}
	s.client = ts.Client()

	first := metadataPKRecord(core.OpInsert, "orders", map[string]any{"tenant_id": int64(7), "order_id": int64(42), "amount": "a"}, []string{"tenant_id", "order_id"})
	second := metadataPKRecord(core.OpInsert, "orders", map[string]any{"tenant_id": int64(7), "order_id": int64(42), "amount": "b"}, []string{"tenant_id", "order_id"})
	second.Metadata.BinlogPos = 200 // a different position, same business row
	if err := s.Write(context.Background(), []core.Record{first, second}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(bodies) != 1 {
		t.Fatalf("requests = %d, want 1", len(bodies))
	}
	// Both actions must target the same _id derived from the business key, so a
	// later event rewrites the document instead of creating a duplicate even
	// though the source positions differ.
	ids := regexp.MustCompile(`"_id":"([^"]+)"`).FindAllStringSubmatch(bodies[0], -1)
	if len(ids) != 2 {
		t.Fatalf("expected two _id values, got %d: %s", len(ids), bodies[0])
	}
	if ids[0][1] != ids[1][1] {
		t.Fatalf("_id changed between events: %q != %q", ids[0][1], ids[1][1])
	}
	for _, part := range []string{"7", "42"} {
		if !strings.Contains(ids[0][1], part) {
			t.Fatalf("_id %q must be derived from the metadata primary key", ids[0][1])
		}
	}
}

func TestRedisMetadataPKBuildsKey(t *testing.T) {
	s := &RedisSink{keyPrefix: "app:", keyField: "id", pkColumnsFromMetadata: true}
	pkByTable := map[string][]string{"orders": {"tenant_id", "order_id"}}
	rec := metadataPKRecord(core.OpInsert, "orders", map[string]any{"tenant_id": int64(7), "order_id": int64(42)}, []string{"tenant_id", "order_id"})
	if got := s.makeKeyFor(rec, pkByTable); got != "app:7:42" {
		t.Fatalf("key = %q, want app:7:42", got)
	}
	// Missing key values must not silently produce a partial key.
	bad := core.Record{Operation: core.OpInsert, Data: map[string]any{}, Metadata: core.Metadata{Table: "orders"}}
	if got := s.makeKeyFor(bad, pkByTable); got != "" {
		t.Fatalf("key = %q, want empty for an incomplete key", got)
	}
}

func TestMaxComputeRejectsMetadataPK(t *testing.T) {
	_, err := NewMaxComputeSink(map[string]any{
		"endpoint":                 "http://odps.example.com",
		"project":                  "p",
		"table":                    "t",
		"access_key_id":            "ak",
		"access_key_secret":        "sk",
		"pk_columns_from_metadata": true,
	})
	if err == nil || !strings.Contains(err.Error(), "pk_columns_from_metadata") {
		t.Fatalf("err = %v, want an explicit pk_columns_from_metadata rejection", err)
	}
}
