package server

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

// withMySQLCDCPreflightMock swaps the mysql_cdc/mysql_snapshot_cdc preflight
// connection factory for a sqlmock handle so the snapshot-coverage probe can
// be unit-tested without a live MySQL.
func withMySQLCDCPreflightMock(t *testing.T) (sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	previous := openMySQLCDCPreflightDB
	openMySQLCDCPreflightDB = func(driverName, dataSourceName string) (*sql.DB, error) {
		if driverName != "mysql" {
			t.Fatalf("driverName = %q, want mysql", driverName)
		}
		return db, nil
	}
	return mock, func() {
		openMySQLCDCPreflightDB = previous
		_ = db.Close()
	}
}

// expectCDCBase mocks the shared mysql preflight probes (binlog settings,
// grants, server_id, table existence) that run before the coverage probe.
func expectCDCBase(mock sqlmock.Sqlmock) {
	mock.ExpectPing()
	mock.ExpectQuery(`SELECT @@binlog_format`).WillReturnRows(
		sqlmock.NewRows([]string{"@@binlog_format"}).AddRow("ROW"))
	mock.ExpectQuery(`SELECT @@binlog_row_image`).WillReturnRows(
		sqlmock.NewRows([]string{"@@binlog_row_image"}).AddRow("FULL"))
	// SHOW GRANTS fallback (either form is fine; make both succeed benignly).
	mock.ExpectQuery(`SHOW GRANTS`).WillReturnRows(
		sqlmock.NewRows([]string{"grants"}).AddRow("GRANT ALL ON *.* TO 'sync'@'%'"))
	mock.ExpectQuery(`SELECT @@server_id`).WillReturnRows(
		sqlmock.NewRows([]string{"@@server_id"}).AddRow(1))
}

// expectCoverageSingleColumnPK: information_schema.columns PRI detection
// returns exactly one column -> numeric/ordered single-column cursor.
func expectCoverageSingleColumnPK(mock sqlmock.Sqlmock, database, table, column, columnType string) {
	mock.ExpectQuery(`SELECT column_name FROM information_schema\.columns`).
		WithArgs(database, table).
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}).AddRow(column))
	mock.ExpectQuery(`SELECT column_type FROM information_schema\.columns`).
		WithArgs(database, table, column).
		WillReturnRows(sqlmock.NewRows([]string{"column_type"}).AddRow(columnType))
}

// expectCoverageCompositePK: PRI detection returns two columns (so composite
// detection query is skipped) plus per-column type checks.
func expectCoverageCompositePK(mock sqlmock.Sqlmock, database, table string, cols []string) {
	mock.ExpectQuery(`SELECT column_name FROM information_schema\.columns`).
		WithArgs(database, table).
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}).
			AddRow(cols[0]).
			AddRow(cols[1]))
	mock.ExpectQuery(`SELECT kcu\.column_name FROM information_schema\.key_column_usage`).
		WithArgs(database, table).
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}).
			AddRow(cols[0]).
			AddRow(cols[1]))
	for _, col := range cols {
		mock.ExpectQuery(`SELECT is_nullable, data_type FROM information_schema\.columns`).
			WithArgs(database, table, col).
			WillReturnRows(sqlmock.NewRows([]string{"is_nullable", "data_type"}).
				AddRow("NO", "int"))
	}
}

// expectCoverageNoPK: no PRI columns and no composite constraint -> none.
func expectCoverageNoPK(mock sqlmock.Sqlmock, database, table string) {
	mock.ExpectQuery(`SELECT column_name FROM information_schema\.columns`).
		WithArgs(database, table).
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
	mock.ExpectQuery(`SELECT kcu\.column_name FROM information_schema\.key_column_usage`).
		WithArgs(database, table).
		WillReturnRows(sqlmock.NewRows([]string{"column_name"}))
}

func snapshotCoverageSpec(t *testing.T, tables []string) *pipeline.Spec {
	spec := pipeline.Spec{
		Name: "snapshot-coverage-preflight",
		Source: pipeline.SourceSpec{
			Type: "mysql_snapshot_cdc",
			Config: map[string]any{
				"host":     "mysql",
				"port":     3306,
				"user":     "sync",
				"password": "secret",
				"database": "shop",
				"tables":   tables,
				"limit":    2,
			},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir()},
		},
	}
	pipeline.ApplyDefaults(&spec)
	return &spec
}

func TestRunPreflightSnapshotCoverageReportsCompositeCursor(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	mock, cleanup := withMySQLCDCPreflightMock(t)
	defer cleanup()
	expectCDCBase(mock)
	// table existence (orders exists)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM information_schema\.tables`).
		WithArgs("shop", "orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	expectCoverageCompositePK(mock, "shop", "orders", []string{"order_id", "product_id"})

	result := s.RunPreflight(context.Background(), snapshotCoverageSpec(t, []string{"orders"}))
	if !result.Passed {
		t.Fatalf("RunPreflight passed = false, issues = %#v", result.Issues)
	}
	var coverageField *PreflightFieldIssue
	for i := range result.FieldIssues {
		if result.FieldIssues[i].Check == "snapshot_coverage" {
			coverageField = &result.FieldIssues[i]
		}
	}
	if coverageField == nil {
		t.Fatalf("no snapshot_coverage field issue: %#v", result.FieldIssues)
	}
	if !strings.Contains(coverageField.Message, "1 composite") {
		t.Fatalf("coverage message = %q, want composite cursor count", coverageField.Message)
	}
	if !strings.Contains(coverageField.Remediation, `"kind":"composite"`) || !strings.Contains(coverageField.Remediation, `"key":["order_id","product_id"]`) {
		t.Fatalf("coverage payload = %q, want composite key JSON", coverageField.Remediation)
	}
	if preflightIssuesContain(result, "snapshot-coverage") {
		t.Fatalf("composite table must not raise snapshot-coverage WARN: %#v", result.Issues)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestRunPreflightSnapshotCoverageWarnsOnNoPKTable(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	mock, cleanup := withMySQLCDCPreflightMock(t)
	defer cleanup()
	expectCDCBase(mock)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM information_schema\.tables`).
		WithArgs("shop", "audit_log").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	expectCoverageNoPK(mock, "shop", "audit_log")

	result := s.RunPreflight(context.Background(), snapshotCoverageSpec(t, []string{"audit_log"}))
	// The skip itself is a warning, not a pass-blocker (CDC still covers it).
	if !result.Passed {
		t.Fatalf("RunPreflight passed = false, issues = %#v", result.Issues)
	}
	if !preflightIssuesContain(result, "snapshot-coverage") {
		t.Fatalf("issues = %#v, want snapshot-coverage WARN for no-PK table", result.Issues)
	}
	var coverageField *PreflightFieldIssue
	for i := range result.FieldIssues {
		if result.FieldIssues[i].Check == "snapshot_coverage" {
			coverageField = &result.FieldIssues[i]
		}
	}
	if coverageField == nil {
		t.Fatalf("no snapshot_coverage field issue: %#v", result.FieldIssues)
	}
	if !strings.Contains(coverageField.Message, "1 skipped") || !strings.Contains(coverageField.Message, "audit_log") {
		t.Fatalf("coverage message = %q, want skip list naming audit_log", coverageField.Message)
	}
	if !strings.Contains(coverageField.Remediation, `"kind":"none"`) {
		t.Fatalf("coverage payload = %q, want kind none entry", coverageField.Remediation)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestRunPreflightSnapshotCoverageSingleColumnNumeric(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	mock, cleanup := withMySQLCDCPreflightMock(t)
	defer cleanup()
	expectCDCBase(mock)
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM information_schema\.tables`).
		WithArgs("shop", "users").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	expectCoverageSingleColumnPK(mock, "shop", "users", "id", "bigint(20) unsigned")

	result := s.RunPreflight(context.Background(), snapshotCoverageSpec(t, []string{"users"}))
	if !result.Passed {
		t.Fatalf("RunPreflight passed = false, issues = %#v", result.Issues)
	}
	var coverageField *PreflightFieldIssue
	for i := range result.FieldIssues {
		if result.FieldIssues[i].Check == "snapshot_coverage" {
			coverageField = &result.FieldIssues[i]
		}
	}
	if coverageField == nil {
		t.Fatalf("no snapshot_coverage field issue: %#v", result.FieldIssues)
	}
	if !strings.Contains(coverageField.Message, "1 numeric") {
		t.Fatalf("coverage message = %q, want numeric cursor count", coverageField.Message)
	}
	if strings.Contains(coverageField.Message, "skipped") {
		// message format: "0 skipped" is fine; only non-empty skip lists fail
		if !strings.Contains(coverageField.Message, "0 skipped") {
			t.Fatalf("coverage message = %q, unexpected skips", coverageField.Message)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}
