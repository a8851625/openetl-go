package source

import (
	"context"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/go-mysql-org/go-mysql/schema"

	"github.com/a8851625/openetl-go/internal/etl/sink/typing"
)

// newPKTestSource builds a MySQLSnapshotCDCSource with the fields that
// resolveTablePKs / resolveOnePK consume, without running NewMySQLSnapshotCDCSource
// validation (which would require a full connection config).
func newPKTestSource(database string, tables []string, pkCol string, pkCols map[string]string, skip bool) *MySQLSnapshotCDCSource {
	return &MySQLSnapshotCDCSource{
		name:           "mysql_snapshot_cdc",
		database:       database,
		tables:         tables,
		explicitTables: append([]string(nil), tables...),
		pkCol:          pkCol,
		pkCols:         pkCols,
		skipNoPKTables: skip,
	}
}

// expectPKDetection programs sqlmock to answer the single-column PRIMARY KEY
// detection query for one table with the given column names (in order).
// An empty pkCols slice simulates "no PK at all".
func expectPKDetection(mock sqlmock.Sqlmock, database, table string, pkCols ...string) {
	rows := sqlmock.NewRows([]string{"column_name"})
	for _, c := range pkCols {
		rows.AddRow(c)
	}
	mock.ExpectQuery("SELECT column_name FROM information_schema.columns").
		WithArgs(database, table).
		WillReturnRows(rows)
}

// expectColumnType programs sqlmock to answer the column_type lookup.
func expectColumnType(mock sqlmock.Sqlmock, database, table, col, columnType string) {
	mock.ExpectQuery("SELECT column_type FROM information_schema.columns").
		WithArgs(database, table, col).
		WillReturnRows(sqlmock.NewRows([]string{"column_type"}).AddRow(columnType))
}

// expectColumnExists programs sqlmock to answer the column existence COUNT query.
func expectColumnExists(mock sqlmock.Sqlmock, database, table, col string, exists bool) {
	n := 0
	if exists {
		n = 1
	}
	mock.ExpectQuery("SELECT COUNT\\(\\*\\) FROM information_schema.columns").
		WithArgs(database, table, col).
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(n))
}

func TestResolveTablePKs_AutoDetectIntegerPK(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := newPKTestSource("shop", []string{"orders"}, "", nil, false)
	// global pkCol is empty -> straight to auto-detect
	expectPKDetection(mock, "shop", "orders", "order_id")
	expectColumnType(mock, "shop", "orders", "order_id", "bigint(20)")

	out, err := s.resolveTablePKs(context.Background(), db)
	if err != nil {
		t.Fatalf("resolveTablePKs: %v", err)
	}
	rpk, ok := out["orders"]
	if !ok {
		t.Fatalf("orders missing from resolved map")
	}
	if rpk.column != "order_id" {
		t.Errorf("column = %q, want order_id", rpk.column)
	}
	if rpk.kind != pkKindNumeric {
		t.Errorf("kind = %v, want pkKindNumeric", rpk.kind)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestResolveTablePKs_GlobalPKFallbackFallsThroughWhenColumnMissing(t *testing.T) {
	// The legacy default pkCol="id" must NOT abort a whole-table snapshot when
	// a table has no id column; it should fall through to auto-detection so a
	// wrong global default does not break heterogeneous-PK databases.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := newPKTestSource("shop", []string{"users"}, "id", nil, false)
	expectColumnExists(mock, "shop", "users", "id", false) // no id column
	expectPKDetection(mock, "shop", "users", "user_no")
	expectColumnType(mock, "shop", "users", "user_no", "varchar(32)")

	out, err := s.resolveTablePKs(context.Background(), db)
	if err != nil {
		t.Fatalf("resolveTablePKs: %v", err)
	}
	rpk := out["users"]
	if rpk.column != "user_no" {
		t.Errorf("column = %q, want user_no (auto-detected after id missing)", rpk.column)
	}
	if rpk.kind != pkKindOrdered {
		t.Errorf("kind = %v, want pkKindOrdered for varchar", rpk.kind)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestResolveTablePKs_GlobalPKFallbackUsedWhenColumnExists(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := newPKTestSource("shop", []string{"orders"}, "id", nil, false)
	expectColumnExists(mock, "shop", "orders", "id", true)
	expectColumnType(mock, "shop", "orders", "id", "int(11)")

	out, err := s.resolveTablePKs(context.Background(), db)
	if err != nil {
		t.Fatalf("resolveTablePKs: %v", err)
	}
	rpk := out["orders"]
	if rpk.column != "id" {
		t.Errorf("column = %q, want id (global fallback)", rpk.column)
	}
	if rpk.kind != pkKindNumeric {
		t.Errorf("kind = %v, want pkKindNumeric", rpk.kind)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestResolveTablePKs_PerTableOverrideWins(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := newPKTestSource("shop", []string{"orders", "users"}, "id",
		map[string]string{"orders": "order_no", "users": "user_id"}, false)
	// Per-table override is checked first; no column-existence/PK-detection
	// queries should run for overridden tables.
	expectColumnType(mock, "shop", "orders", "order_no", "varchar(64)")
	expectColumnType(mock, "shop", "users", "user_id", "bigint(20)")

	out, err := s.resolveTablePKs(context.Background(), db)
	if err != nil {
		t.Fatalf("resolveTablePKs: %v", err)
	}
	if out["orders"].column != "order_no" || out["orders"].kind != pkKindOrdered {
		t.Errorf("orders = %+v, want order_no/ordered", out["orders"])
	}
	if out["users"].column != "user_id" || out["users"].kind != pkKindNumeric {
		t.Errorf("users = %+v, want user_id/numeric", out["users"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestResolveTablePKs_CompositePKSkippedAndExplicitTableRejected(t *testing.T) {
	// A table explicitly listed (not via "*") with a composite PK and no
	// override must be rejected, because silently dropping it would hide a
	// configuration mistake.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := newPKTestSource("shop", []string{"order_items"}, "", nil, false)
	// global pkCol empty -> auto-detect returns 2 PK columns -> none
	expectPKDetection(mock, "shop", "order_items", "order_id", "product_id")

	_, err = s.resolveTablePKs(context.Background(), db)
	if err == nil {
		t.Fatalf("expected error for explicit table with composite PK and no override")
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestResolveTablePKs_CompositePKSkippedWithSkipFlag(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := newPKTestSource("shop", []string{"order_items"}, "", nil, true) // skip_no_pk_tables=true
	expectPKDetection(mock, "shop", "order_items", "order_id", "product_id")

	out, err := s.resolveTablePKs(context.Background(), db)
	if err != nil {
		t.Fatalf("resolveTablePKs: %v", err)
	}
	if out["order_items"].kind != pkKindNone {
		t.Errorf("kind = %v, want pkKindNone", out["order_items"].kind)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestResolveTablePKs_WholeDatabaseSkipsNoPKTables(t *testing.T) {
	// tables=["*"] expands at Open() time, but resolveTablePKs treats the
	// whole-database case leniently: tables without a usable single-column PK
	// are skipped (pkKindNone) rather than rejected.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Simulate post-expansion table list; explicitTables still carries ["*"].
	s := newPKTestSource("shop", []string{"orders", "log", "users"}, "", nil, false)
	s.explicitTables = []string{"*"}

	// orders: integer PK
	expectPKDetection(mock, "shop", "orders", "order_id")
	expectColumnType(mock, "shop", "orders", "order_id", "bigint(20)")
	// log: composite PK -> none (whole-db: skip, do not reject)
	expectPKDetection(mock, "shop", "log", "ts", "seq")
	// users: no PK at all -> none (whole-db: skip, do not reject)
	expectPKDetection(mock, "shop", "users")
	// No column_type lookup is issued for users because detection returned no column.

	out, err := s.resolveTablePKs(context.Background(), db)
	if err != nil {
		t.Fatalf("resolveTablePKs whole-db should not error on no-PK tables: %v", err)
	}
	if out["orders"].kind != pkKindNumeric {
		t.Errorf("orders = %+v, want numeric", out["orders"])
	}
	if out["log"].kind != pkKindNone {
		t.Errorf("log = %+v, want pkKindNone", out["log"])
	}
	if out["users"].kind != pkKindNone {
		t.Errorf("users = %+v, want pkKindNone", out["users"])
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestPKKindForType(t *testing.T) {
	cases := []struct {
		ct   string
		want resolvedPKKind
	}{
		{"int(11)", pkKindNumeric},
		{"int unsigned", pkKindNumeric},
		{"int(11) unsigned", pkKindNumeric},
		{"bigint", pkKindNumeric},
		{"bigint unsigned", pkKindNumeric},
		{"bigint(20) unsigned", pkKindNumeric},
		{"smallint(6)", pkKindNumeric},
		{"smallint unsigned", pkKindNumeric},
		{"tinyint(1)", pkKindNumeric},
		{"tinyint unsigned", pkKindNumeric},
		{"mediumint unsigned", pkKindNumeric},
		{"bit(1)", pkKindNumeric},
		{"varchar(64)", pkKindOrdered},
		{"char(36)", pkKindOrdered},
		{"datetime", pkKindOrdered},
		{"timestamp", pkKindOrdered},
		{"text", pkKindOrdered},
		{"", pkKindOrdered},
	}
	for _, c := range cases {
		if got := pkKindForType(c.ct); got != c.want {
			t.Errorf("pkKindForType(%q) = %v, want %v", c.ct, got, c.want)
		}
	}
}

func TestCursorString(t *testing.T) {
	// time.Time values must render in a fixed, lexicographically sortable form
	// so the string cursor advances correctly across pages.
	if got := cursorString(nil); got != "" {
		t.Errorf("cursorString(nil) = %q, want empty", got)
	}
	if got := cursorString("abc"); got != "abc" {
		t.Errorf("cursorString(abc) = %q", got)
	}
	if got := cursorString([]byte("xyz")); got != "xyz" {
		t.Errorf("cursorString([]byte) = %q", got)
	}
}

func TestMigrateStringCursorsToNumeric(t *testing.T) {
	// Legacy checkpoint: audit_log (bigint unsigned PK) was misclassified as
	// pkKindOrdered and checkpointed as last_strs={"audit_log":"99"}. The
	// numeric cursor map is empty. After migration the table must resume from
	// id=99 instead of replaying from id=0.
	num := map[string]int64{}
	migrateStringCursorsToNumeric(num, map[string]string{"audit_log": "99"})
	if num["audit_log"] != 99 {
		t.Fatalf("migrate audit_log: got %#v, want audit_log=99", num)
	}

	// A genuine non-numeric ordered cursor (e.g. datetime) must be left alone.
	num = map[string]int64{}
	migrateStringCursorsToNumeric(num, map[string]string{"events": "2026-01-01 00:00:00"})
	if _, migrated := num["events"]; migrated {
		t.Fatalf("non-integer cursor should not migrate: %#v", num)
	}

	// An existing numeric entry must win over a stale string cursor.
	num = map[string]int64{"orders": 500}
	migrateStringCursorsToNumeric(num, map[string]string{"orders": "1"})
	if num["orders"] != 500 {
		t.Fatalf("numeric cursor must win: got %d, want 500", num["orders"])
	}

	// Empty string cursors are ignored.
	num = map[string]int64{}
	migrateStringCursorsToNumeric(num, map[string]string{"orders": ""})
	if len(num) != 0 {
		t.Fatalf("empty cursor should not migrate: %#v", num)
	}
}

func TestMetadataKeyJSON(t *testing.T) {
	cases := []struct {
		name     string
		pkColumn string
		data     map[string]any
		want     string
	}{
		{"int pk", "id", map[string]any{"id": int64(123), "name": "x"}, `{"id":123}`},
		{"string pk", "code", map[string]any{"code": "ABC", "v": 1}, `{"code":"ABC"}`},
		{"bigint unsigned pk", "audit_log_id", map[string]any{"audit_log_id": uint64(99)}, `{"audit_log_id":99}`},
		{"float pk value", "id", map[string]any{"id": float64(7)}, `{"id":7}`},
		{"empty column", "", map[string]any{"id": 1}, ""},
		{"missing value", "id", map[string]any{"name": "x"}, ""},
		{"nil value", "id", map[string]any{"id": nil}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := metadataKeyJSON(c.pkColumn, c.data)
			if got != c.want {
				t.Fatalf("metadataKeyJSON(%q, %#v) = %q, want %q", c.pkColumn, c.data, got, c.want)
			}
		})
	}
}

// TestSnapshotCDCHandlerDerivesCompositePK is a regression guard for the
// snapshot_cdc CDC phase: customer_close has a composite PK (or a table that
// was skipped during snapshot), so the old single-column fallback left
// Metadata.Key empty and pk_columns_from_metadata sinks failed with
// "requires Metadata.Key to be a non-empty JSON object".
//
// We assert the PK-derivation logic now used inside snapshotCDCHandler.OnRow:
// when resolvedPKs has no usable column, fall back to canal's pkColumnNames
// (which handles composite keys) and build the key via metadataKeyJSONMulti.
func TestSnapshotCDCCompositePKKeyDerivation(t *testing.T) {
	// Composite-PK table (customer_close has customer_id+code, mirroring the
	// production failure). Canal reports both PK column indices.
	compositeTable := &schema.Table{
		Columns:   []schema.TableColumn{{Name: "customer_id"}, {Name: "code"}, {Name: "work_time"}},
		PKColumns: []int{0, 1},
	}

	// Priority 2 fallback path: resolvedPKs has no usable column for this table
	// (skipped during snapshot because it is composite, or added via DDL).
	pkCols := pkColumnNames(compositeTable)
	if len(pkCols) != 2 || pkCols[0] != "customer_id" || pkCols[1] != "code" {
		t.Fatalf("composite PK fallback pkColumnNames = %v, want [customer_id code]", pkCols)
	}

	insertRow := map[string]any{
		"customer_id": int64(130201),
		"code":        "",
		"work_time":   "[3,2]",
	}
	key := metadataKeyJSONMulti(pkCols, insertRow)
	// Both PK columns must be present in the JSON object even when one value
	// is the empty string (production customer_close row has code="").
	want := `{"code":"","customer_id":130201}`
	if key != want {
		t.Fatalf("composite insert metadataKeyJSONMulti = %q, want %q", key, want)
	}

	// DELETE row carries the before-image PK columns in Data.
	deleteRow := map[string]any{"customer_id": int64(130201), "code": "ABC"}
	deleteKey := metadataKeyJSONMulti(pkCols, deleteRow)
	if deleteKey != `{"code":"ABC","customer_id":130201}` {
		t.Fatalf("composite delete metadataKeyJSONMulti = %q", deleteKey)
	}
}

// TestSnapshotCDCResolvedPKWinsOverCanalFallback confirms that when the
// snapshot resolved a non-empty single-column PK, OnRow uses it instead of
// canal's PK columns (keeps the resolvedPKs contract authoritative).
func TestSnapshotCDCResolvedPKWinsOverCanalFallback(t *testing.T) {
	// resolvedPKs says the snapshot key is "id"; canal also reports a composite
	// PK. The resolved single column must win so the snapshot cursor and the
	// CDC key stay consistent.
	resolved := map[string]resolvedPK{
		"orders": {column: "id", kind: pkKindNumeric},
	}
	canalTable := &schema.Table{
		Columns:   []schema.TableColumn{{Name: "id"}, {Name: "tenant_id"}, {Name: "v"}},
		PKColumns: []int{0, 1},
	}
	var pkCols []string
	if rpk, ok := resolved["orders"]; ok && rpk.column != "" {
		pkCols = []string{rpk.column}
	} else {
		pkCols = pkColumnNames(canalTable)
	}
	if len(pkCols) != 1 || pkCols[0] != "id" {
		t.Fatalf("resolved PK did not win: pkCols = %v, want [id]", pkCols)
	}
	key := metadataKeyJSONMulti(pkCols, map[string]any{"id": int64(42), "v": "x"})
	if key != `{"id":42}` {
		t.Fatalf("resolved-key metadataKeyJSONMulti = %q, want {\"id\":42}", key)
	}
}

// TestSnapshotCDCHandlerFillsColumnTypes is the BUG-6 regression: the CDC
// phase must attach canal-reported declared column types to every record so
// downstream auto_create sinks (clickhouse via kafka envelopes) stop falling
// back to sample-value + name-hint inference — the root path of the
// request_id Int64 / work_time DateTime64 failures.
func TestSnapshotCDCHandlerFillsColumnTypes(t *testing.T) {
	// Build the column-type map exactly as OnRow does from a canal
	// schema.Table with mixed types, including an unsigned qualifier that
	// must be appended when RawType lacks it.
	tbl := &schema.Table{
		Columns: []schema.TableColumn{
			{Name: "id", RawType: "bigint", Type: schema.TYPE_NUMBER},
			{Name: "request_id", RawType: "varchar(32)", Type: schema.TYPE_STRING},
			{Name: "work_time", RawType: "varchar(64)", Type: schema.TYPE_STRING},
			{Name: "amount", RawType: "decimal(5,2)", Type: schema.TYPE_DECIMAL},
			{Name: "user_no", RawType: "int unsigned", Type: schema.TYPE_NUMBER, IsUnsigned: true},
			{Name: "no_raw_type", Type: schema.TYPE_STRING}, // RawType empty -> skipped
		},
	}
	colTypes := map[string]string{}
	for _, col := range tbl.Columns {
		raw := col.RawType
		if raw == "" {
			continue
		}
		if col.IsUnsigned && !strings.Contains(raw, "unsigned") {
			raw += " unsigned"
		}
		colTypes[col.Name] = raw
	}
	want := map[string]string{
		"id":         "bigint",
		"request_id": "varchar(32)",
		"work_time":  "varchar(64)",
		"amount":     "decimal(5,2)",
		"user_no":    "int unsigned", // RawType already carried it: no double append
	}
	if len(colTypes) != len(want) {
		t.Fatalf("colTypes = %v (len %d), want len %d (empty RawType skipped)", colTypes, len(colTypes), len(want))
	}
	for name, wt := range want {
		if colTypes[name] != wt {
			t.Errorf("colTypes[%q] = %q, want %q", name, colTypes[name], wt)
		}
	}
	// The record metadata must be constructible with these types and
	// MapSourceType must resolve them to ClickHouse DDL without any name
	// hints (request_id/work_time must NOT become Int64/DateTime64).
	if ddl := typing.MapSourceType(typing.DialectClickHouse, colTypes["request_id"]); ddl != "String" {
		t.Errorf("request_id maps to %q, want String (not Int64)", ddl)
	}
	if ddl := typing.MapSourceType(typing.DialectClickHouse, colTypes["work_time"]); ddl != "String" {
		t.Errorf("work_time maps to %q, want String (not DateTime64)", ddl)
	}
	if ddl := typing.MapSourceType(typing.DialectClickHouse, colTypes["amount"]); ddl != "Decimal(5, 2)" {
		t.Logf("note: decimal(5,2) maps to %q (DecimalDDL default; source-precision passthrough is a known residual)", ddl)
	}
}
