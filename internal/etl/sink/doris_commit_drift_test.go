package sink

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/a8851625/openetl-go/internal/etl/core"
)

// ── A4: write acknowledgement / dedup token ─────────────────────────────

func TestDorisSinkCommitMetadataReportsLabelBoundary(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Status":"Success","StatusCode":200,"NumberTotalRows":1,"NumberLoadedRows":1}`))
	}))
	defer ts.Close()
	host, portText, _ := net.SplitHostPort(strings.TrimPrefix(ts.URL, "http://"))
	port, _ := strconv.Atoi(portText)

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := &DorisSink{
		name:             "doris",
		host:             host,
		httpPort:         port,
		database:         "ods",
		table:            "orders",
		batchMode:        "upsert",
		writeMode:        "stream_load",
		pkColumns:        []string{"order_id"},
		streamLoadScheme: "http",
		streamLoadFormat: "json",
		httpClient:       ts.Client(),
		insertChunkSize:  500,
		db:               db,
		schemaCache:      core.NewSchemaCache(),
		tableMetrics:     newTableMetricsSet(),
	}
	mock.ExpectQuery("SHOW CREATE TABLE").WillReturnRows(
		sqlmock.NewRows([]string{"Create Table"}).AddRow("CREATE TABLE `orders` (\n  `order_id` bigint NOT NULL\n) ENGINE=OLAP\nUNIQUE KEY(`order_id`)\nDISTRIBUTED BY HASH(`order_id`) BUCKETS 10"))

	rec := core.Record{
		Operation: core.OpInsert,
		Data:      map[string]any{"order_id": 1},
		Metadata:  core.Metadata{Source: "mysql_cdc", Database: "app", Table: "orders", BinlogFile: "mysql-bin.000001", BinlogPos: 10, SourceType: core.SourceTypeMySQLCDC},
	}
	if err := s.Write(context.Background(), []core.Record{rec}); err != nil {
		t.Fatalf("Write: %v", err)
	}

	meta, err := s.SinkCommitMetadata(context.Background())
	if err != nil {
		t.Fatalf("SinkCommitMetadata: %v", err)
	}
	if meta == nil {
		t.Fatal("expected a commit boundary after a successful write")
	}
	if meta["dedup_mode"] != "label" {
		t.Fatalf("dedup_mode = %v, want label", meta["dedup_mode"])
	}
	labels, ok := meta["stream_load_labels"].(map[string]any)
	if !ok || labels["orders"] == nil {
		t.Fatalf("stream_load_labels = %v, want a label for orders", meta["stream_load_labels"])
	}
	if !strings.HasPrefix(labels["orders"].(string), "etl_") {
		t.Fatalf("label = %v, want the etl_ deterministic prefix", labels["orders"])
	}
	if meta["row_count"] != 1 {
		t.Fatalf("row_count = %v, want 1", meta["row_count"])
	}
	if meta["batch_seq"] != uint64(1) {
		t.Fatalf("batch_seq = %v, want 1", meta["batch_seq"])
	}

	// A second, identical write must produce the same label (Doris dedups it).
	if err := s.Write(context.Background(), []core.Record{rec}); err != nil {
		t.Fatalf("second Write: %v", err)
	}
	meta2, err := s.SinkCommitMetadata(context.Background())
	if err != nil {
		t.Fatalf("SinkCommitMetadata after replay: %v", err)
	}
	labels2 := meta2["stream_load_labels"].(map[string]any)
	if labels2["orders"] != labels["orders"] {
		t.Fatalf("replayed batch label changed: %v != %v; Doris could not deduplicate", labels2["orders"], labels["orders"])
	}
}

func TestDorisSinkCommitMetadataEmptyAndFailed(t *testing.T) {
	s := &DorisSink{name: "doris", batchMode: "upsert", pkColumns: []string{"id"}}

	meta, err := s.SinkCommitMetadata(context.Background())
	if err != nil {
		t.Fatalf("SinkCommitMetadata before any write: %v", err)
	}
	if meta != nil {
		t.Fatalf("meta = %v, want nil before the first write", meta)
	}

	// A failed write must not leave a stale boundary behind.
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"Status":"Fail","StatusCode":500,"Message":"be down"}`))
	}))
	defer ts.Close()
	host, portText, _ := net.SplitHostPort(strings.TrimPrefix(ts.URL, "http://"))
	port, _ := strconv.Atoi(portText)
	s.host = host
	s.httpPort = port
	s.database = "ods"
	s.table = "orders"
	s.streamLoadScheme = "http"
	s.streamLoadFormat = "json"
	s.httpClient = ts.Client()
	s.insertChunkSize = 500
	s.schemaCache = core.NewSchemaCache()
	s.tableMetrics = newTableMetricsSet()

	rec := core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1}, Metadata: core.Metadata{Table: "orders"}}
	if err := s.Write(context.Background(), []core.Record{rec}); err == nil {
		t.Fatal("expected the write to fail")
	}
	meta, err = s.SinkCommitMetadata(context.Background())
	if err != nil {
		t.Fatalf("SinkCommitMetadata after failure: %v", err)
	}
	if meta != nil {
		t.Fatalf("meta = %v, want nil after a failed write", meta)
	}
}

// ── A5: schema_drift=sync type widening ─────────────────────────────────

func TestDorisTypeCompatibilityAndWidening(t *testing.T) {
	cases := []struct {
		existing, desired string
		compatible        bool
		widens            bool
	}{
		{"BIGINT", "BIGINT", true, true},
		{"BIGINT", "INT", true, true},  // existing is wider: no change needed
		{"INT", "BIGINT", false, true}, // widening
		{"VARCHAR(64)", "VARCHAR(32)", true, true},
		{"VARCHAR(32)", "VARCHAR(128)", false, true},
		{"BIGINT", "VARCHAR(255)", false, false}, // different family: refused
		{"DECIMAL(18,2)", "BIGINT", false, false},
		{"DOUBLE", "BIGINT", true, true},
		{"INT", "DECIMAL(20,0)", true, true}, // integers always fit a wide DECIMAL
		{"DECIMAL(18,2)", "DECIMAL(20,2)", false, true},
		{"DECIMAL(20,2)", "DECIMAL(18,2)", true, true},
		{"DOUBLE", "FLOAT", true, true},
	}
	for _, tc := range cases {
		if got := dorisTypeCompatible(tc.existing, tc.desired); got != tc.compatible {
			t.Errorf("dorisTypeCompatible(%s, %s) = %v, want %v", tc.existing, tc.desired, got, tc.compatible)
		}
		if got := dorisTypeWidens(tc.existing, tc.desired); got != tc.widens {
			t.Errorf("dorisTypeWidens(%s, %s) = %v, want %v", tc.existing, tc.desired, got, tc.widens)
		}
	}
}

func TestDorisSyncColumnTypesAltersWidenedColumn(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := &DorisSink{
		name:         "doris",
		database:     "ods",
		table:        "orders",
		batchMode:    "upsert",
		pkColumns:    []string{"order_id"},
		schemaDrift:  dorisSchemaDriftSync,
		db:           db,
		schemaCache:  core.NewSchemaCache(),
		tableMetrics: newTableMetricsSet(),
	}
	mock.ExpectQuery("SELECT column_name, column_type, is_nullable").
		WithArgs("ods", "orders").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "column_type", "is_nullable"}).
			AddRow("order_id", "bigint", "NO").
			AddRow("seq", "int", "YES"))
	// seq infers BIGINT while the target column is still INT: schema_drift=sync
	// widens it instead of letting the load truncate or fail.
	mock.ExpectExec("ALTER TABLE `ods`.`orders` MODIFY COLUMN `seq` BIGINT").
		WillReturnResult(sqlmock.NewResult(0, 0))

	records := []core.Record{{
		Operation: core.OpInsert,
		Data:      map[string]any{"order_id": int64(1), "seq": int64(9000000000)},
		Metadata:  core.Metadata{Table: "orders"},
	}}
	if err := s.syncColumnTypes(context.Background(), records, nil); err != nil {
		t.Fatalf("syncColumnTypes: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestDorisSyncColumnTypesRefusesNarrowing(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := &DorisSink{
		name:         "doris",
		database:     "ods",
		table:        "orders",
		batchMode:    "upsert",
		pkColumns:    []string{"order_id"},
		schemaDrift:  dorisSchemaDriftSync,
		db:           db,
		schemaCache:  core.NewSchemaCache(),
		tableMetrics: newTableMetricsSet(),
	}
	mock.ExpectQuery("SELECT column_name, column_type, is_nullable").
		WithArgs("ods", "orders").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "column_type", "is_nullable"}).
			AddRow("order_id", "bigint", "NO").
			AddRow("legacy_id", "varchar(64)", "YES"))

	// A VARCHAR target column cannot absorb an integer family change: Doris
	// can only widen inside a family, so this must be refused (not silently
	// truncated by the load).
	records := []core.Record{{
		Operation: core.OpInsert,
		Data:      map[string]any{"order_id": int64(1), "legacy_id": int64(7)},
		Metadata:  core.Metadata{Table: "orders"},
	}}
	err = s.syncColumnTypes(context.Background(), records, nil)
	if err == nil || !strings.Contains(err.Error(), "narrow") {
		t.Fatalf("err = %v, want a narrowing refusal", err)
	}
}
