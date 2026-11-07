package sink

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestDorisConfigParsing(t *testing.T) {
	s, err := NewDorisSink(map[string]any{
		"host":                     "doris.example.com",
		"port":                     9030,
		"http_port":                8030,
		"user":                     "etl",
		"password":                 "secret",
		"database":                 "warehouse",
		"table":                    "orders",
		"write_mode":               "stream_load",
		"batch_mode":               "upsert",
		"pk_columns":               []interface{}{"order_id"},
		"pk_columns_from_metadata": true,
		"stream_load_format":       "csv",
		"stream_load_timeout_sec":  60,
		"insert_chunk_size":        200,
		"auto_create":              true,
		"schema_drift":             "add_columns",
	})
	if err != nil {
		t.Fatalf("NewDorisSink: %v", err)
	}
	if s.host != "doris.example.com" {
		t.Errorf("host = %q", s.host)
	}
	if s.port != 9030 {
		t.Errorf("port = %d", s.port)
	}
	if s.httpPort != 8030 {
		t.Errorf("http_port = %d", s.httpPort)
	}
	if s.user != "etl" {
		t.Errorf("user = %q", s.user)
	}
	if s.writeMode != "stream_load" {
		t.Errorf("write_mode = %q", s.writeMode)
	}
	if s.batchMode != "upsert" {
		t.Errorf("batch_mode = %q", s.batchMode)
	}
	if len(s.pkColumns) != 1 || s.pkColumns[0] != "order_id" {
		t.Errorf("pk_columns = %v", s.pkColumns)
	}
	if !s.pkColumnsFromMetadata {
		t.Error("pk_columns_from_metadata = false, want true")
	}
	if s.streamLoadFormat != "csv" {
		t.Errorf("stream_load_format = %q", s.streamLoadFormat)
	}
	if s.autoCreate != true {
		t.Errorf("auto_create = %v", s.autoCreate)
	}
	if s.schemaDrift != "add_columns" {
		t.Errorf("schema_drift = %q", s.schemaDrift)
	}
}

func TestDorisConfigDefaults(t *testing.T) {
	s, err := NewDorisSink(map[string]any{
		"host":     "localhost",
		"database": "test",
		"table":    "t1",
	})
	if err != nil {
		t.Fatalf("NewDorisSink: %v", err)
	}
	if s.port != 9030 {
		t.Errorf("default port = %d, want 9030", s.port)
	}
	if s.httpPort != 8030 {
		t.Errorf("default http_port = %d, want 8030", s.httpPort)
	}
	if s.user != "root" {
		t.Errorf("default user = %q, want root", s.user)
	}
	if s.writeMode != "stream_load" {
		t.Errorf("default write_mode = %q", s.writeMode)
	}
	if s.streamLoadFormat != "json" {
		t.Errorf("default stream_load_format = %q", s.streamLoadFormat)
	}
	if s.insertChunkSize != 500 {
		t.Errorf("default insert_chunk_size = %d", s.insertChunkSize)
	}
	if s.schemaDrift != "ignore" {
		t.Errorf("default schema_drift = %q", s.schemaDrift)
	}
	if s.ddLPolicy != DDLPolicyReject {
		t.Errorf("default ddl_policy = %q, want reject", s.ddLPolicy)
	}
}

func TestDorisBuildInsertStatement(t *testing.T) {
	s := &DorisSink{name: "doris", table: "orders", insertChunkSize: 500}
	stmt := s.buildInsertStatement("orders", []string{"id", "name", "amount"}, 3)
	if !strings.Contains(stmt, "INSERT INTO") {
		t.Errorf("missing INSERT INTO: %s", stmt)
	}
	if !strings.Contains(stmt, "`orders`") {
		t.Errorf("missing table name: %s", stmt)
	}
	// 3 rows × 3 cols = 9 placeholders
	if got := strings.Count(stmt, "?"); got != 9 {
		t.Errorf("placeholder count = %d, want 9", got)
	}
	// 3 row groups
	if got := strings.Count(stmt, "),(") + 1; got != 3 {
		t.Errorf("VALUES rows = %d, want 3", got)
	}
}

func TestDorisBuildJSONBody(t *testing.T) {
	s := &DorisSink{name: "doris", table: "t"}
	recs := []core.Record{
		{Operation: core.OpInsert, Data: map[string]any{"id": 1, "name": "alice"}},
		{Operation: core.OpInsert, Data: map[string]any{"id": 2, "name": "bob"}},
	}
	body := s.buildJSONBody(recs)
	str := string(body)
	lines := strings.Split(strings.TrimSpace(str), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 JSON lines, got %d", len(lines))
	}
	if !strings.Contains(lines[0], `"id":1`) {
		t.Errorf("first line missing id:1: %s", lines[0])
	}
	if !strings.Contains(lines[1], `"name":"bob"`) {
		t.Errorf("second line missing name:bob: %s", lines[1])
	}
}

func TestDorisWriteCompactsUsingEnvelopeMetadataKey(t *testing.T) {
	var bodies []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read stream load body: %v", err)
		}
		bodies = append(bodies, string(body))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Status":"Success","StatusCode":200,"NumberTotalRows":1,"NumberLoadedRows":1}`))
	}))
	defer ts.Close()

	host, portText, err := net.SplitHostPort(strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse test server port: %v", err)
	}
	s := &DorisSink{
		host:                  host,
		httpPort:              port,
		database:              "ods",
		tableTemplate:         "ods_{table}",
		pkColumnsFromMetadata: true,
		streamLoadScheme:      "http",
		streamLoadFormat:      "json",
		httpClient:            ts.Client(),
		insertChunkSize:       500,
		streamLoadTimeout:     time.Second,
		schemaCache:           core.NewSchemaCache(),
		tableMetrics:          newTableMetricsSet(),
	}
	records := []core.Record{
		{Operation: core.OpUpdate, Before: map[string]any{"id": 1, "value": "before"}, Data: map[string]any{"id": 1, "value": "old"}, Metadata: core.Metadata{Table: "orders", Key: `{"id":1}`, PrimaryKeyColumns: []string{"id"}, FormatContractID: core.FormatContractOpenETLEnvelopeV1}},
		{Operation: core.OpUpdate, Before: map[string]any{"id": 1, "value": "old"}, Data: map[string]any{"id": 1, "value": "new"}, Metadata: core.Metadata{Table: "orders", Key: `{"id":1}`, PrimaryKeyColumns: []string{"id"}, FormatContractID: core.FormatContractOpenETLEnvelopeV1}},
	}
	if err := s.Write(context.Background(), records); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(bodies) != 1 {
		t.Fatalf("stream load requests = %d, want 1", len(bodies))
	}
	if strings.Count(strings.TrimSpace(bodies[0]), "\n") != 0 || !strings.Contains(bodies[0], `"value":"new"`) || strings.Contains(bodies[0], `"value":"old"`) {
		t.Fatalf("compacted stream load body = %q, want only final record", bodies[0])
	}
}

func TestDorisWriteCompactsMetadataKeyWithStaticTargetAndEmptySourceTable(t *testing.T) {
	var bodies []string
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read stream load body: %v", err)
		}
		bodies = append(bodies, string(body))
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Status":"Success","StatusCode":200,"NumberTotalRows":1,"NumberLoadedRows":1}`))
	}))
	defer ts.Close()

	host, portText, err := net.SplitHostPort(strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse test server port: %v", err)
	}
	s := &DorisSink{
		host:                  host,
		httpPort:              port,
		database:              "ods",
		table:                 "orders",
		pkColumnsFromMetadata: true,
		streamLoadScheme:      "http",
		streamLoadFormat:      "json",
		httpClient:            ts.Client(),
		insertChunkSize:       500,
		streamLoadTimeout:     time.Second,
		schemaCache:           core.NewSchemaCache(),
		tableMetrics:          newTableMetricsSet(),
	}
	records := []core.Record{
		{Operation: core.OpUpdate, Before: map[string]any{"order_id": 1, "value": "before"}, Data: map[string]any{"order_id": 1, "value": "old"}, Metadata: core.Metadata{Key: `{"order_id":1}`, PrimaryKeyColumns: []string{"order_id"}, FormatContractID: core.FormatContractOpenETLEnvelopeV1}},
		{Operation: core.OpUpdate, Before: map[string]any{"order_id": 1, "value": "old"}, Data: map[string]any{"order_id": 1, "value": "new"}, Metadata: core.Metadata{Key: `{"order_id":1}`, PrimaryKeyColumns: []string{"order_id"}, FormatContractID: core.FormatContractOpenETLEnvelopeV1}},
	}
	if err := s.Write(context.Background(), records); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if len(bodies) != 1 || strings.Contains(bodies[0], `"value":"old"`) || !strings.Contains(bodies[0], `"value":"new"`) {
		t.Fatalf("static-target compact body = %q, want only final record", bodies)
	}
}

func TestDorisBuildCSVBody(t *testing.T) {
	s := &DorisSink{name: "doris", table: "t", streamLoadFormat: "csv"}
	recs := []core.Record{
		{Operation: core.OpInsert, Data: map[string]any{"id": 1, "name": "alice"}},
		{Operation: core.OpInsert, Data: map[string]any{"id": 2, "name": "bob"}},
	}
	body := s.buildCSVBody(recs, "t")
	str := strings.TrimSpace(string(body))
	lines := strings.Split(str, "\n")
	if len(lines) != 2 {
		t.Fatalf("expected 2 CSV lines, got %d", len(lines))
	}
}

func TestDorisStreamLoadHeaders(t *testing.T) {
	tests := []struct {
		name   string
		format string
		check  func(t *testing.T, r *http.Request)
	}{
		{
			name:   "json-lines",
			format: "json",
			check: func(t *testing.T, r *http.Request) {
				if got := r.Header.Get("format"); got != "json" {
					t.Fatalf("format header = %q, want json", got)
				}
				if got := r.Header.Get("read_json_by_line"); got != "true" {
					t.Fatalf("read_json_by_line header = %q, want true", got)
				}
			},
		},
		{
			name:   "csv",
			format: "csv",
			check: func(t *testing.T, r *http.Request) {
				if got := r.Header.Get("format"); got != "csv" {
					t.Fatalf("format header = %q, want csv", got)
				}
				if got := r.Header.Get("columns"); got != "id,name" {
					t.Fatalf("columns header = %q, want id,name", got)
				}
				if got := r.Header.Get("column_separator"); got != "," {
					t.Fatalf("column_separator header = %q, want comma", got)
				}
			},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var checked bool
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				checked = true
				tc.check(t, r)
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(`{"Status":"Success","StatusCode":200,"NumberTotalRows":1,"NumberLoadedRows":1}`))
			}))
			defer ts.Close()

			host, portText, err := net.SplitHostPort(strings.TrimPrefix(ts.URL, "http://"))
			if err != nil {
				t.Fatalf("parse test server URL: %v", err)
			}
			port, err := strconv.Atoi(portText)
			if err != nil {
				t.Fatalf("parse test server port: %v", err)
			}
			s := &DorisSink{
				host:             host,
				httpPort:         port,
				database:         "db",
				table:            "orders",
				streamLoadScheme: "http",
				streamLoadFormat: tc.format,
				httpClient:       ts.Client(),
			}
			err = s.streamLoadOnce(context.Background(), "orders", []core.Record{
				{Operation: core.OpInsert, Data: map[string]any{"id": 1, "name": "alice"}},
			})
			if err != nil {
				t.Fatalf("streamLoadOnce: %v", err)
			}
			if !checked {
				t.Fatal("test server did not receive Stream Load request")
			}
		})
	}
}

func TestDorisInferType(t *testing.T) {
	cases := []struct {
		col  string
		val  any
		want string
	}{
		// Name-hinted columns (nil value still resolves via nameHint).
		{"id", nil, "BIGINT"},
		{"user_id", nil, "BIGINT"},
		{"amount", nil, "DECIMAL(18,2)"},
		{"price", nil, "DECIMAL(18,2)"},
		{"created_at", nil, "DATETIME"},
		{"is_active", nil, "BOOLEAN"},
		{"data", nil, "JSON"},
		{"metadata", nil, "JSON"},
		// Value-driven columns — pass a representative Go value so the typing
		// engine infers the correct Doris type. Without a value, the engine
		// can only fall back to STRING.
		{"count", int(42), "INT"},
		{"quantity", int32(10), "INT"},
		{"score", float64(4.5), "DOUBLE"},
		// "date" matches the temporal name hint and resolves to DATETIME in
		// the unified typing engine (Doris doesn't have a separate DATE hint).
		{"date", nil, "DATETIME"},
		// No hint + nil value → fallback string type.
		{"name", nil, "STRING"},
		{"description", nil, "STRING"},
	}
	for _, tc := range cases {
		t.Run(tc.col, func(t *testing.T) {
			got := inferDorisType(tc.col, tc.val)
			if got != tc.want {
				t.Errorf("inferDorisType(%q, %v) = %q, want %q", tc.col, tc.val, got, tc.want)
			}
		})
	}
}

func TestDorisResolveTable(t *testing.T) {
	s := &DorisSink{table: "default_table"}

	rec := core.Record{Metadata: core.Metadata{Table: "custom_table"}}
	got, err := s.resolveTable(rec)
	if err != nil {
		t.Fatalf("resolveTable: %v", err)
	}
	if got != "default_table" {
		t.Errorf("resolveTable with configured table = %q, want default_table", got)
	}

	s.table = ""
	got, err = s.resolveTable(rec)
	if err != nil {
		t.Fatalf("resolveTable fallback: %v", err)
	}
	if got != "custom_table" {
		t.Errorf("resolveTable without configured table = %q, want custom_table", got)
	}
}

func TestDorisCreateTableDDL(t *testing.T) {
	s := &DorisSink{
		table:       "orders",
		pkColumns:   []string{"order_id"},
		database:    "test",
		schemaCache: core.NewSchemaCache(),
	}

	ddl, err := s.buildCreateTableDDL("orders", []string{"order_id", "amount", "name"}, map[string]any{
		"order_id": int64(1001),
		"amount":   12.5,
		"name":     "alice",
	})
	if err != nil {
		t.Fatalf("buildCreateTableDDL: %v", err)
	}
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `orders`",
		"`order_id` BIGINT NOT NULL",
		"`amount` DECIMAL(18,2)",
		"`name` VARCHAR(255)",
		"UNIQUE KEY(`order_id`)",
		"DISTRIBUTED BY HASH(`order_id`)",
	} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("DDL missing %q:\n%s", want, ddl)
		}
	}
	if strings.Index(ddl, "`order_id` BIGINT NOT NULL") > strings.Index(ddl, "`amount` DECIMAL(18,2)") {
		t.Fatalf("Doris key column must be the table schema prefix:\n%s", ddl)
	}

	// Verify pkColumns are correctly used in table resolution
	got, err := s.resolveTable(core.Record{})
	if err != nil || got != "orders" {
		t.Errorf("table resolution = (%q,%v), want orders", got, err)
	}
}

func TestDorisCreateTableRequiresStableKey(t *testing.T) {
	s := &DorisSink{}
	_, err := s.buildCreateTableDDL("events", []string{"event_name", "payload"}, map[string]any{"payload": "{}"})
	if err == nil || !strings.Contains(err.Error(), "pk_columns is required") {
		t.Fatalf("buildCreateTableDDL error = %v, want pk_columns required", err)
	}
}

func TestDorisMetadataKeyColumnsFromEnvelope(t *testing.T) {
	s, err := NewDorisSink(map[string]any{
		"host":                     "fe",
		"database":                 "ods",
		"table_template":           "ods_{table}",
		"batch_mode":               "upsert",
		"pk_columns_from_metadata": true,
	})
	if err != nil {
		t.Fatalf("NewDorisSink: %v", err)
	}
	pkByTable, err := s.pkColumnsByTable([]core.Record{{
		Operation: core.OpInsert,
		Metadata: core.Metadata{
			Table: "orders", Key: `{"tenant_id":"t1","order_id":42}`,
			PrimaryKeyColumns: []string{"tenant_id", "order_id"},
		},
		Data: map[string]any{"tenant_id": "t1", "order_id": 42, "amount": 10},
	}})
	if err != nil {
		t.Fatalf("pkColumnsByTable: %v", err)
	}
	if got := pkByTable["orders"]; !sameIdentifierSet(got, []string{"order_id", "tenant_id"}) {
		t.Fatalf("metadata pk columns = %v, want [order_id tenant_id]", got)
	}
	ddl, err := s.buildCreateTableDDLWithPK("ods_orders", []string{"tenant_id", "order_id", "amount"}, map[string]any{
		"tenant_id": "t1",
		"order_id":  int64(42),
		"amount":    10,
	}, pkByTable["orders"])
	if err != nil {
		t.Fatalf("buildCreateTableDDLWithPK: %v", err)
	}
	if !strings.Contains(ddl, "UNIQUE KEY(`order_id`, `tenant_id`)") && !strings.Contains(ddl, "UNIQUE KEY(`tenant_id`, `order_id`)") {
		t.Fatalf("DDL missing metadata-derived composite key:\n%s", ddl)
	}
}

func TestDorisMetadataKeyColumnsFromSnapshotCDCSource(t *testing.T) {
	// mysql_snapshot_cdc emits a single-column JSON object key like {"id":123}
	// (see source.metadataKeyJSON). Doris pk_columns_from_metadata must derive
	// the key column name from it so the snapshot_cdc -> kafka -> doris path
	// auto-detects the PK without a static pk_columns config.
	s := &DorisSink{name: "doris", database: "ods", tableTemplate: "ods_cct_{table}", pkColumnsFromMetadata: true}
	pkByTable, err := s.pkColumnsByTable([]core.Record{{
		Operation: core.OpInsert,
		Metadata:  core.Metadata{Table: "address", Key: `{"address_id":12345}`, PrimaryKeyColumns: []string{"address_id"}},
		Data:      map[string]any{"address_id": int64(12345), "city": "sh"},
	}})
	if err != nil {
		t.Fatalf("pkColumnsByTable: %v", err)
	}
	if got := pkByTable["ods_cct_address"]; len(got) != 1 || got[0] != "address_id" {
		t.Fatalf("metadata pk columns for ods_cct_address = %v, want [address_id]", got)
	}
}

func TestDorisMetadataKeyColumnsRejectScalarEnvelopeKey(t *testing.T) {
	s := &DorisSink{name: "doris", database: "ods", tableTemplate: "ods_{table}", pkColumnsFromMetadata: true}
	_, err := s.pkColumnsByTable([]core.Record{{Operation: core.OpInsert, Data: map[string]any{"id": 42}, Metadata: core.Metadata{Table: "orders", Key: `"42"`, PrimaryKeyColumns: []string{"id"}}}})
	if err == nil || !strings.Contains(err.Error(), string(core.RecordIdentityReasonKeyNotObject)) {
		t.Fatalf("scalar metadata key error = %v, want key_not_object", err)
	}
}

func TestDorisValidateSchemaAllowsMetadataPKAutoCreate(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	s := &DorisSink{
		name:                  "doris",
		db:                    db,
		database:              "ods",
		table:                 "orders",
		batchMode:             "upsert",
		autoCreate:            true,
		pkColumnsFromMetadata: true,
		schemaCache:           core.NewSchemaCache(),
		tableMetrics:          newTableMetricsSet(),
	}
	expectation := mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?"))
	expectation.WithArgs("ods", "orders").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(0))

	err = s.ValidateSchema(context.Background(), core.SchemaInfo{Columns: []core.ColumnInfo{{Name: "order_id", DataType: "BIGINT"}}})
	if err != nil {
		t.Fatalf("ValidateSchema: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("schema expectation: %v", err)
	}
}

func TestDorisDeleteUsesEnvelopeMetadataKey(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	s := &DorisSink{
		name:                  "doris",
		db:                    db,
		database:              "ods",
		tableTemplate:         "ods_{table}",
		pkColumnsFromMetadata: true,
		insertChunkSize:       500,
		schemaCache:           core.NewSchemaCache(),
		tableMetrics:          newTableMetricsSet(),
	}
	expectation := mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `ods_orders` WHERE (`order_id`=? AND `tenant_id`=?)"))
	expectation.WithArgs(int64(42), "t1").WillReturnResult(sqlmock.NewResult(0, 1))

	err = s.Write(context.Background(), []core.Record{{
		Operation: core.OpDelete,
		Data: map[string]any{
			"tenant_id": "t1",
			"order_id":  int64(42),
		},
		Metadata: core.Metadata{
			Table: "orders", Key: `{"tenant_id":"t1","order_id":42}`,
			PrimaryKeyColumns: []string{"tenant_id", "order_id"},
		},
	}})
	if err != nil {
		t.Fatalf("Write delete: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("delete expectation: %v", err)
	}
}

func TestDorisMetadataPKKeyChangeWritesNewThenDeletesOld(t *testing.T) {
	var body string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		payload, readErr := io.ReadAll(r.Body)
		if readErr != nil {
			t.Errorf("read stream body: %v", readErr)
		}
		body = string(payload)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Status":"Success","StatusCode":200,"NumberTotalRows":1,"NumberLoadedRows":1}`))
	}))
	defer server.Close()
	host, portText, err := net.SplitHostPort(strings.TrimPrefix(server.URL, "http://"))
	if err != nil {
		t.Fatalf("parse HTTP server: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse HTTP port: %v", err)
	}
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()

	s := &DorisSink{
		name: "doris", host: host, httpPort: port, database: "ods", tableTemplate: "ods_{table}",
		writeMode: "stream_load", streamLoadScheme: "http", streamLoadFormat: "json", streamLoadTimeout: time.Second,
		pkColumnsFromMetadata: true, insertChunkSize: 500, db: db, httpClient: server.Client(),
		schemaCache: core.NewSchemaCache(), tableMetrics: newTableMetricsSet(),
	}
	mock.ExpectExec(regexp.QuoteMeta("DELETE FROM `ods_orders` WHERE (`id`=? AND `tenant_id`=?)")).
		WithArgs(int64(1), "acme").WillReturnResult(sqlmock.NewResult(0, 1))
	record := core.Record{
		Operation: core.OpUpdate,
		Before:    map[string]any{"tenant_id": "acme", "id": int64(1), "value": "old"},
		Data:      map[string]any{"tenant_id": "acme", "id": int64(2), "value": "new"},
		Metadata: core.Metadata{
			Table: "orders", Key: `{"tenant_id":"acme","id":1}`,
			PrimaryKeyColumns: []string{"tenant_id", "id"}, FormatContractID: core.FormatContractOpenETLEnvelopeV1,
		},
	}
	if err := s.Write(context.Background(), []core.Record{record}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	if !strings.Contains(body, `"id":2`) || strings.Contains(body, `"id":1`) {
		t.Fatalf("stream load body=%q, want only new-key live row", body)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("SQL expectations: %v", err)
	}
	stats := s.TableWriteStats()
	if len(stats) != 1 || stats[0].RowsWritten != 2 || stats[0].Errors != 0 {
		t.Fatalf("per-target stats=%+v, want live write + old-key delete", stats)
	}
}

func TestDorisCreateTableRejectsMissingPKColumn(t *testing.T) {
	s := &DorisSink{pkColumns: []string{"order_id"}}
	_, err := s.buildCreateTableDDL("events", []string{"event_name", "payload"}, map[string]any{"payload": "{}"})
	if err == nil || !strings.Contains(err.Error(), "pk_column") {
		t.Fatalf("buildCreateTableDDL error = %v, want missing pk_column error", err)
	}
}

func TestDorisCollectSchemaInputsUsesRepresentativeValues(t *testing.T) {
	s := &DorisSink{table: "orders"}
	cols, values, err := s.collectSchemaInputs([]core.Record{
		{Data: map[string]any{"id": nil, "amount": nil, "name": "first"}},
		{Data: map[string]any{"id": int64(42), "amount": 19.95, "name": "second"}},
	})
	if err != nil {
		t.Fatalf("collectSchemaInputs: %v", err)
	}
	if got := values["orders"]["id"]; got != int64(42) {
		t.Fatalf("id representative value = %#v, want int64(42)", got)
	}
	if got := values["orders"]["amount"]; got != 19.95 {
		t.Fatalf("amount representative value = %#v, want 19.95", got)
	}
	if len(cols["orders"]) != 3 {
		t.Fatalf("cols = %v, want 3 unique columns", cols["orders"])
	}
}

func TestDorisUniqueKeyParsingAndMatching(t *testing.T) {
	stmt := "CREATE TABLE `orders` (\n`tenant_id` BIGINT,\n`order_id` BIGINT\n) ENGINE=OLAP\nUNIQUE KEY(`tenant_id`, `order_id`)\nDISTRIBUTED BY HASH(`tenant_id`, `order_id`) BUCKETS 10"
	keys := parseDorisUniqueKeyColumns(stmt)
	if !sameIdentifierSet(keys, []string{"order_id", "tenant_id"}) {
		t.Fatalf("unique keys = %v", keys)
	}
	if sameIdentifierSet(keys, []string{"order_id"}) {
		t.Fatal("single-column pk unexpectedly matched composite unique key")
	}
}

func TestDorisStreamLoadErrorClassification(t *testing.T) {
	cases := []struct {
		name       string
		statusCode int
		message    string
		class      core.ErrorClass
	}{
		{"rate-limit", 429, "stream load failed (HTTP 429): retry later", core.ErrorClassTransient},
		{"server", 503, "stream load failed (HTTP 503): unavailable", core.ErrorClassTransient},
		{"auth", 401, "stream load failed (HTTP 401): unauthorized", core.ErrorClassAuth},
		{"schema", 400, "unknown column amount", core.ErrorClassSchema},
		{"data", 400, "too many filtered rows", core.ErrorClassData},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := classifyDorisStreamLoadError(tc.statusCode, tc.message)
			var classified core.ClassifiedError
			if !errors.As(err, &classified) {
				t.Fatalf("error %T is not classified", err)
			}
			if classified.Class != tc.class {
				t.Fatalf("class = %s, want %s", classified.Class, tc.class)
			}
		})
	}
}

func TestDorisApplyDDLPolicyOnlyAllowsSafeAddColumn(t *testing.T) {
	if err := validateDorisApplyDDL("ALTER TABLE customers ADD COLUMN age INT"); err != nil {
		t.Fatalf("safe ADD COLUMN rejected: %v", err)
	}
	for _, ddl := range []string{
		"DROP TABLE customers",
		"ALTER TABLE customers DROP COLUMN age",
		"ALTER TABLE customers MODIFY COLUMN age BIGINT",
		"CREATE TABLE t (id BIGINT)",
	} {
		if err := validateDorisApplyDDL(ddl); err == nil {
			t.Fatalf("DDL %q was allowed, want rejection", ddl)
		}
	}
}

func TestDorisImplementsSchemaManager(t *testing.T) {
	s, _ := NewDorisSink(map[string]any{
		"host":     "localhost",
		"database": "test",
		"table":    "t",
	})
	// Verify DorisSink implements core.SchemaManager
	var _ core.SchemaManager = s
}

func TestDorisImplementsSink(t *testing.T) {
	s, _ := NewDorisSink(map[string]any{
		"host":     "localhost",
		"database": "test",
		"table":    "t",
	})
	// Verify DorisSink implements core.Sink
	var _ core.Sink = s
}

func TestDorisImplementsSchemaValidator(t *testing.T) {
	s, _ := NewDorisSink(map[string]any{
		"host":     "localhost",
		"database": "test",
		"table":    "t",
	})
	var _ core.SchemaValidator = s
}

// ── IT-7 / T7.2: source_order config surface + table contract ───────────────

func TestDorisSourceOrderConfigValidation(t *testing.T) {
	base := func(extra map[string]any) map[string]any {
		cfg := map[string]any{
			"host":         "doris-fe",
			"database":     "ods",
			"table":        "orders",
			"batch_mode":   "upsert",
			"pk_columns":   []string{"order_id"},
			"version_mode": "source_order",
		}
		for k, v := range extra {
			cfg[k] = v
		}
		return cfg
	}

	t.Run("valid", func(t *testing.T) {
		s, err := NewDorisSink(base(nil))
		if err != nil {
			t.Fatalf("NewDorisSink: %v", err)
		}
		if !s.sourceOrderEnabled() {
			t.Fatal("sourceOrderEnabled = false, want true")
		}
		if got := s.effectiveSequenceColumn(); got != "_version" {
			t.Fatalf("sequence column = %q, want _version", got)
		}
		if got := s.effectiveSequenceType(); got != dorisSequenceTypeLargeInt {
			t.Fatalf("sequence type = %q, want largeint", got)
		}
		if got := s.dorisSequenceColumnDDLType(); got != "LARGEINT" {
			t.Fatalf("ddl type = %q, want LARGEINT", got)
		}
	})

	t.Run("off is default and lenient", func(t *testing.T) {
		s, err := NewDorisSink(map[string]any{"host": "h", "database": "d", "table": "t", "write_mode": "insert"})
		if err != nil {
			t.Fatalf("NewDorisSink: %v", err)
		}
		if s.sourceOrderEnabled() {
			t.Fatal("sourceOrderEnabled = true for default config, want false")
		}
	})

	t.Run("unknown version_mode", func(t *testing.T) {
		_, err := NewDorisSink(base(map[string]any{"version_mode": "ordered"}))
		if err == nil || !strings.Contains(err.Error(), "version_mode") {
			t.Fatalf("err = %v, want version_mode rejection", err)
		}
	})

	t.Run("write_mode insert rejected", func(t *testing.T) {
		_, err := NewDorisSink(base(map[string]any{"write_mode": "insert"}))
		if err == nil || !strings.Contains(err.Error(), "write_mode=stream_load") {
			t.Fatalf("err = %v, want write_mode rejection", err)
		}
	})

	t.Run("batch_mode insert rejected", func(t *testing.T) {
		_, err := NewDorisSink(base(map[string]any{"batch_mode": "insert"}))
		if err == nil || !strings.Contains(err.Error(), "batch_mode=upsert") {
			t.Fatalf("err = %v, want batch_mode rejection", err)
		}
	})

	t.Run("blank sequence_column rejected", func(t *testing.T) {
		_, err := NewDorisSink(base(map[string]any{"sequence_column": "   "}))
		if err == nil || !strings.Contains(err.Error(), "sequence_column must not be blank") {
			t.Fatalf("err = %v, want blank sequence_column rejection", err)
		}
	})

	t.Run("sequence_column colliding with pk rejected", func(t *testing.T) {
		_, err := NewDorisSink(base(map[string]any{"sequence_column": "ORDER_ID"}))
		if err == nil || !strings.Contains(err.Error(), "collides with pk_columns") {
			t.Fatalf("err = %v, want pk collision rejection", err)
		}
	})

	t.Run("reserved delete sign column rejected", func(t *testing.T) {
		_, err := NewDorisSink(base(map[string]any{"sequence_column": dorisDeleteSignColumn}))
		if err == nil || !strings.Contains(err.Error(), "reserved by Doris") {
			t.Fatalf("err = %v, want reserved column rejection", err)
		}
	})

	t.Run("unknown sequence_type", func(t *testing.T) {
		_, err := NewDorisSink(base(map[string]any{"sequence_type": "decimal"}))
		if err == nil || !strings.Contains(err.Error(), "sequence_type") {
			t.Fatalf("err = %v, want sequence_type rejection", err)
		}
	})
}

func TestDorisCreateTableDDLSourceOrder(t *testing.T) {
	values := map[string]any{"order_id": int64(1), "amount": 12.5}

	offSink := &DorisSink{table: "orders", pkColumns: []string{"order_id"}, database: "test", versionMode: dorisVersionModeOff}
	offDDL, err := offSink.buildCreateTableDDLWithPK("orders", []string{"order_id", "amount"}, values, []string{"order_id"})
	if err != nil {
		t.Fatalf("off DDL: %v", err)
	}
	for _, unwanted := range []string{"LARGEINT", "function_column.sequence_col", "enable_unique_key_merge_on_write"} {
		if strings.Contains(offDDL, unwanted) {
			t.Fatalf("version_mode=off DDL must stay unchanged, found %q:\n%s", unwanted, offDDL)
		}
	}
	// Byte-identical guarantee for the legacy path: the source_order work must
	// not perturb the DDL that existing pipelines already produce.
	wantOff := "CREATE TABLE IF NOT EXISTS `orders` (\n" +
		"  `order_id` BIGINT NOT NULL,\n" +
		"  `amount` DECIMAL(18,2)\n" +
		") ENGINE=OLAP\n" +
		"UNIQUE KEY(`order_id`)\n" +
		"DISTRIBUTED BY HASH(`order_id`) BUCKETS 10\n" +
		"PROPERTIES (\n" +
		"  \"replication_allocation\" = \"tag.location.default: 1\",\n" +
		"  \"light_schema_change\" = \"true\"\n" +
		")"
	if offDDL != wantOff {
		t.Fatalf("version_mode=off DDL changed:\ngot:  %q\nwant: %q", offDDL, wantOff)
	}

	onSink := &DorisSink{
		table:        "orders",
		pkColumns:    []string{"order_id"},
		database:     "test",
		versionMode:  dorisVersionModeSourceOrder,
		sequenceType: dorisSequenceTypeLargeInt,
	}
	onDDL, err := onSink.buildCreateTableDDLWithPK("orders", []string{"order_id", "amount"}, values, []string{"order_id"})
	if err != nil {
		t.Fatalf("source_order DDL: %v", err)
	}
	for _, want := range []string{
		"`_version` LARGEINT NOT NULL",
		`"enable_unique_key_merge_on_write" = "true"`,
		`"function_column.sequence_col" = "_version"`,
		"UNIQUE KEY(`order_id`)",
	} {
		if !strings.Contains(onDDL, want) {
			t.Fatalf("source_order DDL missing %q:\n%s", want, onDDL)
		}
	}
	// The sequence column must not become part of the key, and the key must
	// still be the schema prefix.
	if strings.Contains(onDDL, "UNIQUE KEY(`order_id`, `_version`)") {
		t.Fatalf("sequence column leaked into the UNIQUE KEY:\n%s", onDDL)
	}

	// sequence_type=bigint must emit BIGINT rather than LARGEINT.
	bigSink := &DorisSink{database: "test", versionMode: dorisVersionModeSourceOrder, sequenceType: dorisSequenceTypeBigInt}
	bigDDL, err := bigSink.buildCreateTableDDLWithPK("orders", []string{"order_id", "amount"}, values, []string{"order_id"})
	if err != nil {
		t.Fatalf("bigint DDL: %v", err)
	}
	if !strings.Contains(bigDDL, "`_version` BIGINT NOT NULL") {
		t.Fatalf("bigint DDL missing BIGINT sequence column:\n%s", bigDDL)
	}

	// A custom sequence column name and an explicit column in the record must
	// not be duplicated.
	custom := &DorisSink{database: "test", versionMode: dorisVersionModeSourceOrder, sequenceColumn: "src_version"}
	customDDL, err := custom.buildCreateTableDDLWithPK("orders", []string{"order_id", "amount", "src_version"}, values, []string{"order_id"})
	if err != nil {
		t.Fatalf("custom sequence DDL: %v", err)
	}
	if strings.Count(customDDL, "`src_version`") != 1 {
		t.Fatalf("custom sequence column emitted %d times, want 1:\n%s", strings.Count(customDDL, "`src_version`"), customDDL)
	}
	if !strings.Contains(customDDL, `"function_column.sequence_col" = "src_version"`) {
		t.Fatalf("custom sequence property missing:\n%s", customDDL)
	}
}

const dorisSourceOrderTableStmt = "CREATE TABLE `orders` (\n  `order_id` bigint NOT NULL,\n  `amount` decimal(18,2) NULL,\n  `_version` LARGEINT NOT NULL\n) ENGINE=OLAP\nUNIQUE KEY(`order_id`)\nDISTRIBUTED BY HASH(`order_id`) BUCKETS 10\nPROPERTIES (\n\"replication_allocation\" = \"tag.location.default: 1\",\n\"enable_unique_key_merge_on_write\" = \"true\",\n\"function_column.sequence_col\" = \"_version\"\n)"

func sourceOrderSink() *DorisSink {
	return &DorisSink{
		name:         "doris",
		database:     "ods",
		table:        "orders",
		batchMode:    "upsert",
		writeMode:    "stream_load",
		pkColumns:    []string{"order_id"},
		versionMode:  dorisVersionModeSourceOrder,
		sequenceType: dorisSequenceTypeLargeInt,
	}
}

func TestDorisValidateSourceOrderStatement(t *testing.T) {
	t.Run("satisfied", func(t *testing.T) {
		if err := sourceOrderSink().validateSourceOrderStatement(dorisSourceOrderTableStmt, "orders"); err != nil {
			t.Fatalf("validateSourceOrderStatement: %v", err)
		}
	})

	t.Run("missing merge-on-write", func(t *testing.T) {
		stmt := strings.Replace(dorisSourceOrderTableStmt, "\"enable_unique_key_merge_on_write\" = \"true\",\n", "\"enable_unique_key_merge_on_write\" = \"false\",\n", 1)
		err := sourceOrderSink().validateSourceOrderStatement(stmt, "orders")
		if err == nil || !strings.Contains(err.Error(), "enable_unique_key_merge_on_write") {
			t.Fatalf("err = %v, want merge-on-write rejection", err)
		}
		if !strings.Contains(err.Error(), "ALTER TABLE ... SET") {
			t.Fatalf("migration remedy should explain why ALTER does not work: %v", err)
		}
	})

	t.Run("missing sequence property", func(t *testing.T) {
		stmt := strings.Replace(dorisSourceOrderTableStmt, "\"function_column.sequence_col\" = \"_version\"", "\"light_schema_change\" = \"true\"", 1)
		err := sourceOrderSink().validateSourceOrderStatement(stmt, "orders")
		if err == nil || !strings.Contains(err.Error(), "function_column.sequence_col") {
			t.Fatalf("err = %v, want missing property rejection", err)
		}
	})

	t.Run("property points at another column", func(t *testing.T) {
		s := sourceOrderSink()
		s.sequenceColumn = "src_version"
		stmt := strings.Replace(dorisSourceOrderTableStmt, "`_version` LARGEINT NOT NULL", "`src_version` LARGEINT NOT NULL", 1)
		err := s.validateSourceOrderStatement(stmt, "orders")
		if err == nil || !strings.Contains(err.Error(), "function_column.sequence_col") {
			t.Fatalf("err = %v, want property mismatch rejection", err)
		}
	})

	t.Run("sequence column missing from schema", func(t *testing.T) {
		stmt := strings.Replace(dorisSourceOrderTableStmt, ",\n  `_version` LARGEINT NOT NULL", "", 1)
		err := sourceOrderSink().validateSourceOrderStatement(stmt, "orders")
		if err == nil || !strings.Contains(err.Error(), "not declared in the table schema") {
			t.Fatalf("err = %v, want missing column rejection", err)
		}
	})

	t.Run("sequence column inside the unique key", func(t *testing.T) {
		stmt := strings.Replace(dorisSourceOrderTableStmt, "UNIQUE KEY(`order_id`)", "UNIQUE KEY(`order_id`, `_version`)", 1)
		err := sourceOrderSink().validateSourceOrderStatement(stmt, "orders")
		if err == nil || !strings.Contains(err.Error(), "part of the UNIQUE KEY") {
			t.Fatalf("err = %v, want key collision rejection", err)
		}
	})

	t.Run("sequence column declared as bigint", func(t *testing.T) {
		stmt := strings.Replace(dorisSourceOrderTableStmt, "`_version` LARGEINT NOT NULL", "`_version` BIGINT NOT NULL", 1)
		err := sourceOrderSink().validateSourceOrderStatement(stmt, "orders")
		if err == nil || !strings.Contains(err.Error(), "silently wraps BIGINT") {
			t.Fatalf("err = %v, want BIGINT rejection for sequence_type=largeint", err)
		}
		// The same table is acceptable when bigint is explicitly requested.
		bigSink := sourceOrderSink()
		bigSink.sequenceType = dorisSequenceTypeBigInt
		if err := bigSink.validateSourceOrderStatement(stmt, "orders"); err != nil {
			t.Fatalf("bigint opt-in should accept a BIGINT sequence column: %v", err)
		}
	})

	t.Run("not a unique key table", func(t *testing.T) {
		stmt := strings.Replace(dorisSourceOrderTableStmt, "UNIQUE KEY(`order_id`)", "", 1)
		err := sourceOrderSink().validateSourceOrderStatement(stmt, "orders")
		if err == nil || !strings.Contains(err.Error(), "UNIQUE KEY table") {
			t.Fatalf("err = %v, want unique key rejection", err)
		}
	})

	t.Run("off mode skips the contract", func(t *testing.T) {
		s := sourceOrderSink()
		s.versionMode = dorisVersionModeOff
		if err := s.validateSourceOrderStatement("CREATE TABLE `x` (`id` int)", "x"); err != nil {
			t.Fatalf("off mode must not validate the source_order contract: %v", err)
		}
	})
}

func TestDorisValidateSchemaEnforcesSourceOrderContract(t *testing.T) {
	newSink := func(t *testing.T, db *sql.DB) *DorisSink {
		t.Helper()
		s := sourceOrderSink()
		s.db = db
		s.schemaCache = core.NewSchemaCache()
		s.tableMetrics = newTableMetricsSet()
		return s
	}
	schema := core.SchemaInfo{Columns: []core.ColumnInfo{
		{Name: "order_id", DataType: "BIGINT"},
		{Name: "amount", DataType: "DECIMAL"},
	}}

	t.Run("satisfied table passes", func(t *testing.T) {
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?")).
			WithArgs("ods", "orders").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
		mock.ExpectQuery(regexp.QuoteMeta("SELECT column_name, column_type, is_nullable FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")).
			WithArgs("ods", "orders").
			WillReturnRows(sqlmock.NewRows([]string{"column_name", "column_type", "is_nullable"}).
				AddRow("order_id", "bigint", "NO").
				AddRow("amount", "decimal(18,2)", "YES").
				AddRow("_version", "largeint", "NO"))
		mock.ExpectQuery(regexp.QuoteMeta("SHOW CREATE TABLE `orders`")).
			WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("orders", dorisSourceOrderTableStmt))

		if err := newSink(t, db).ValidateSchema(context.Background(), schema); err != nil {
			t.Fatalf("ValidateSchema: %v", err)
		}
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Fatalf("expectations: %v", err)
		}
	})

	t.Run("legacy table without sequence contract fails closed", func(t *testing.T) {
		legacy := "CREATE TABLE `orders` (\n  `order_id` bigint NOT NULL,\n  `amount` decimal(18,2) NULL\n) ENGINE=OLAP\nUNIQUE KEY(`order_id`)\nDISTRIBUTED BY HASH(`order_id`) BUCKETS 10\nPROPERTIES (\n\"replication_allocation\" = \"tag.location.default: 1\",\n\"enable_unique_key_merge_on_write\" = \"true\"\n)"
		db, mock, err := sqlmock.New()
		if err != nil {
			t.Fatalf("sqlmock.New: %v", err)
		}
		defer db.Close()
		mock.ExpectQuery(regexp.QuoteMeta("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?")).
			WithArgs("ods", "orders").WillReturnRows(sqlmock.NewRows([]string{"COUNT(*)"}).AddRow(1))
		mock.ExpectQuery(regexp.QuoteMeta("SELECT column_name, column_type, is_nullable FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position")).
			WithArgs("ods", "orders").
			WillReturnRows(sqlmock.NewRows([]string{"column_name", "column_type", "is_nullable"}).
				AddRow("order_id", "bigint", "NO").
				AddRow("amount", "decimal(18,2)", "YES"))
		mock.ExpectQuery(regexp.QuoteMeta("SHOW CREATE TABLE `orders`")).
			WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("orders", legacy))

		err = newSink(t, db).ValidateSchema(context.Background(), schema)
		if err == nil || !strings.Contains(err.Error(), "function_column.sequence_col") {
			t.Fatalf("err = %v, want legacy table rejection", err)
		}
		if !strings.Contains(err.Error(), "Migrate by") {
			t.Fatalf("err = %v, want migration guidance", err)
		}
	})
}

func TestDorisParseTablePropertiesAndColumnType(t *testing.T) {
	props := parseDorisTableProperties(dorisSourceOrderTableStmt)
	if props["function_column.sequence_col"] != "_version" {
		t.Fatalf("sequence property = %q", props["function_column.sequence_col"])
	}
	if !strings.EqualFold(props["enable_unique_key_merge_on_write"], "true") {
		t.Fatalf("mow property = %q", props["enable_unique_key_merge_on_write"])
	}
	if got, ok := parseDorisColumnType(dorisSourceOrderTableStmt, "_version"); !ok || got != "LARGEINT" {
		t.Fatalf("parseDorisColumnType(_version) = (%q,%v), want LARGEINT", got, ok)
	}
	if got, ok := parseDorisColumnType(dorisSourceOrderTableStmt, "amount"); !ok || got != "DECIMAL(18,2)" {
		t.Fatalf("parseDorisColumnType(amount) = (%q,%v), want DECIMAL(18,2)", got, ok)
	}
	if _, ok := parseDorisColumnType(dorisSourceOrderTableStmt, "missing"); ok {
		t.Fatal("parseDorisColumnType(missing) reported a type")
	}
}

func TestDorisWriteEnforcesSourceOrderTableContract(t *testing.T) {
	legacyStmt := "CREATE TABLE `ods_orders` (\n  `id` bigint NOT NULL,\n  `value` varchar(64) NULL\n) ENGINE=OLAP\nUNIQUE KEY(`id`)\nDISTRIBUTED BY HASH(`id`) BUCKETS 10\nPROPERTIES (\n\"replication_allocation\" = \"tag.location.default: 1\",\n\"enable_unique_key_merge_on_write\" = \"true\"\n)"

	var streamLoads int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		streamLoads++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"Status":"Success","StatusCode":200,"NumberTotalRows":1,"NumberLoadedRows":1}`))
	}))
	defer ts.Close()
	host, portText, err := net.SplitHostPort(strings.TrimPrefix(ts.URL, "http://"))
	if err != nil {
		t.Fatalf("parse test server URL: %v", err)
	}
	port, err := strconv.Atoi(portText)
	if err != nil {
		t.Fatalf("parse test server port: %v", err)
	}

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectQuery(regexp.QuoteMeta("SHOW CREATE TABLE `ods_orders`")).
		WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("ods_orders", legacyStmt))

	s := &DorisSink{
		host:              host,
		httpPort:          port,
		database:          "ods",
		tableTemplate:     "ods_{table}",
		pkColumns:         []string{"id"},
		versionMode:       dorisVersionModeSourceOrder,
		sequenceType:      dorisSequenceTypeLargeInt,
		streamLoadScheme:  "http",
		streamLoadFormat:  "json",
		httpClient:        ts.Client(),
		insertChunkSize:   500,
		streamLoadTimeout: time.Second,
		schemaCache:       core.NewSchemaCache(),
		tableMetrics:      newTableMetricsSet(),
		db:                db,
	}
	err = s.Write(context.Background(), []core.Record{{
		Operation: core.OpUpdate,
		Data:      map[string]any{"id": 1, "value": "v"},
		Metadata:  core.Metadata{Table: "orders"},
	}})
	if err == nil || !strings.Contains(err.Error(), "function_column.sequence_col") {
		t.Fatalf("Write err = %v, want source_order contract rejection", err)
	}
	if streamLoads != 0 {
		t.Fatalf("stream load requests = %d, want 0 (must fail before writing)", streamLoads)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
}

func TestDorisEnsureSourceOrderTablesCachesSuccess(t *testing.T) {
	stmt := strings.Replace(dorisSourceOrderTableStmt, "`orders`", "`ods_orders`", -1)
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	// Exactly one SHOW CREATE TABLE for two batches: the success is cached.
	mock.ExpectQuery(regexp.QuoteMeta("SHOW CREATE TABLE `ods_orders`")).
		WillReturnRows(sqlmock.NewRows([]string{"Table", "Create Table"}).AddRow("ods_orders", stmt))

	s := sourceOrderSink()
	s.db = db
	s.tableTemplate = "ods_{table}"
	s.table = ""
	s.tableMetrics = newTableMetricsSet()
	records := []core.Record{{Operation: core.OpUpdate, Data: map[string]any{"order_id": 1}, Metadata: core.Metadata{Table: "orders"}}}
	for i := 0; i < 2; i++ {
		if err := s.ensureSourceOrderTables(context.Background(), records); err != nil {
			t.Fatalf("ensureSourceOrderTables pass %d: %v", i, err)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("expectations: %v", err)
	}
	s.versionMode = dorisVersionModeOff
	if err := s.ensureSourceOrderTables(context.Background(), records); err != nil {
		t.Fatalf("off mode must not query: %v", err)
	}
}
