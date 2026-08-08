package source

import (
	"context"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
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
