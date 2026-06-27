package sink

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestDorisConfigParsing(t *testing.T) {
	s, err := NewDorisSink(map[string]any{
		"host":                    "doris.example.com",
		"port":                    9030,
		"http_port":               8030,
		"user":                    "etl",
		"password":                "secret",
		"database":                "warehouse",
		"table":                   "orders",
		"write_mode":              "stream_load",
		"batch_mode":              "upsert",
		"pk_columns":              []interface{}{"order_id"},
		"stream_load_format":      "csv",
		"stream_load_timeout_sec": 60,
		"insert_chunk_size":       200,
		"auto_create":             true,
		"schema_drift":            "add_columns",
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
	if got := s.resolveTable(rec); got != "default_table" {
		t.Errorf("resolveTable with configured table = %q, want default_table", got)
	}

	s.table = ""
	if got := s.resolveTable(rec); got != "custom_table" {
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
	if s.resolveTable(core.Record{}) != "orders" {
		t.Errorf("table resolution failed")
	}
}

func TestDorisCreateTableRequiresStableKey(t *testing.T) {
	s := &DorisSink{}
	_, err := s.buildCreateTableDDL("events", []string{"event_name", "payload"}, map[string]any{"payload": "{}"})
	if err == nil || !strings.Contains(err.Error(), "pk_columns is required") {
		t.Fatalf("buildCreateTableDDL error = %v, want pk_columns required", err)
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
	cols, values := s.collectSchemaInputs([]core.Record{
		{Data: map[string]any{"id": nil, "amount": nil, "name": "first"}},
		{Data: map[string]any{"id": int64(42), "amount": 19.95, "name": "second"}},
	})
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
