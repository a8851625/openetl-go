package server

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/IBM/sarama"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	_ "github.com/a8851625/openetl-go/internal/etl/sink"
	_ "github.com/a8851625/openetl-go/internal/etl/source"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

const (
	testSchemaPreflightSource = "test_schema_preflight_source"
	testPlainPreflightSource  = "test_plain_preflight_source"
	testSchemaPreflightSink   = "test_schema_preflight_sink"
)

func init() {
	registry.RegisterSource(testSchemaPreflightSource, func(config map[string]any) (core.Source, error) {
		if err := configuredError(config, "build_error"); err != nil {
			return nil, err
		}
		return &schemaPreflightSource{
			schema: core.SchemaInfo{Columns: []core.ColumnInfo{
				{Name: "id", DataType: "INT", Nullable: false},
				{Name: "name", DataType: "VARCHAR(255)", Nullable: true},
			}},
			describeErr: configuredError(config, "describe_error"),
		}, nil
	})
	registry.RegisterSource(testPlainPreflightSource, func(config map[string]any) (core.Source, error) {
		if err := configuredError(config, "build_error"); err != nil {
			return nil, err
		}
		return plainPreflightSource{}, nil
	})
	registry.RegisterSink(testSchemaPreflightSink, func(config map[string]any) (core.Sink, error) {
		if err := configuredError(config, "build_error"); err != nil {
			return nil, err
		}
		return &schemaPreflightSink{
			openErr:     configuredError(config, "open_error"),
			validateErr: configuredError(config, "validation_error"),
		}, nil
	})
}

func newTestHTTPServer(t *testing.T) (*Server, *httptest.Server) {
	t.Helper()

	dir := t.TempDir()
	store, err := sqlite.New(filepath.Join(dir, "etl.db"))
	if err != nil {
		t.Fatalf("NewStore: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	s, err := NewServer(store, dir)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	mux := http.NewServeMux()
	s.RegisterHTTPRoutes(mux)
	return s, httptest.NewServer(mux)
}

func mustPipelineJSON(t *testing.T, spec pipeline.Spec) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{"spec": spec})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return body
}

func mustPipelineUpdateJSON(t *testing.T, spec pipeline.Spec) []byte {
	t.Helper()
	return mustPipelineUpdateJSONWithID(t, "", spec)
}

func mustPipelineUpdateJSONWithID(t *testing.T, id string, spec pipeline.Spec) []byte {
	t.Helper()
	body, err := json.Marshal(map[string]any{"id": id, "spec": spec, "reset_checkpoint": false})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}
	return body
}

type schemaPreflightSource struct {
	schema      core.SchemaInfo
	describeErr error
}

func (s *schemaPreflightSource) Name() string { return testSchemaPreflightSource }
func (s *schemaPreflightSource) Open(context.Context, *core.Checkpoint) (core.RecordReader, error) {
	return nil, nil
}
func (s *schemaPreflightSource) Describe(context.Context) (core.SchemaInfo, error) {
	if s.describeErr != nil {
		return core.SchemaInfo{}, s.describeErr
	}
	return s.schema, nil
}

type plainPreflightSource struct{}

func (s plainPreflightSource) Name() string { return testPlainPreflightSource }
func (s plainPreflightSource) Open(context.Context, *core.Checkpoint) (core.RecordReader, error) {
	return nil, nil
}

type schemaPreflightSink struct {
	openErr     error
	validateErr error
}

func (s *schemaPreflightSink) Name() string { return testSchemaPreflightSink }
func (s *schemaPreflightSink) Open(context.Context) error {
	return s.openErr
}
func (s *schemaPreflightSink) Write(context.Context, []core.Record) error {
	return nil
}
func (s *schemaPreflightSink) Close() error { return nil }
func (s *schemaPreflightSink) ValidateSchema(context.Context, core.SchemaInfo) error {
	return s.validateErr
}

func configuredError(config map[string]any, key string) error {
	if msg, ok := config[key].(string); ok && msg != "" {
		return errors.New(msg)
	}
	return nil
}

func warningsContain(raw any, needle string) bool {
	warnings, ok := raw.([]any)
	if !ok {
		return false
	}
	for _, warning := range warnings {
		if strings.Contains(fmt.Sprint(warning), needle) {
			return true
		}
	}
	return false
}

func preflightIssuesContain(result *PreflightResult, check string) bool {
	if result == nil {
		return false
	}
	for _, issue := range result.Issues {
		if issue.Check == check {
			return true
		}
	}
	return false
}

func preflightGuidanceContain(result *PreflightResult, code string) bool {
	if result == nil {
		return false
	}
	for _, guidance := range result.Guidance {
		if guidance.Code == code {
			return true
		}
	}
	return false
}

func preflightReadiness(result *PreflightResult, kind, typ string) (PreflightConnectorReadiness, bool) {
	if result == nil {
		return PreflightConnectorReadiness{}, false
	}
	for _, readiness := range result.Readiness {
		if readiness.Kind == kind && readiness.Type == typ {
			return readiness, true
		}
	}
	return PreflightConnectorReadiness{}, false
}

func preflightRecommendation(result *PreflightResult, path string) (PreflightRecommendation, bool) {
	if result == nil {
		return PreflightRecommendation{}, false
	}
	for _, recommendation := range result.Recommendations {
		if recommendation.Path == path {
			return recommendation, true
		}
	}
	return PreflightRecommendation{}, false
}

func preflightFieldIssueContain(result *PreflightResult, field, check string) bool {
	if result == nil {
		return false
	}
	for _, issue := range result.FieldIssues {
		if issue.Field == field && issue.Check == check {
			return true
		}
	}
	return false
}

func withMySQLBatchPreflightMock(t *testing.T) (sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	previous := openMySQLBatchPreflightDB
	openMySQLBatchPreflightDB = func(driverName, dataSourceName string) (*sql.DB, error) {
		if driverName != "mysql" {
			t.Fatalf("driverName = %q, want mysql", driverName)
		}
		return db, nil
	}
	return mock, func() {
		openMySQLBatchPreflightDB = previous
		_ = db.Close()
	}
}

func withPostgresCDCPreflightMock(t *testing.T) (sqlmock.Sqlmock, func()) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(true))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	previous := openPostgresCDCPreflightDB
	openPostgresCDCPreflightDB = func(driverName, dataSourceName string) (*sql.DB, error) {
		if driverName != "pgx" {
			t.Fatalf("driverName = %q, want pgx", driverName)
		}
		return db, nil
	}
	return mock, func() {
		openPostgresCDCPreflightDB = previous
		_ = db.Close()
	}
}

func TestRunPreflightChecksMySQLBatchTableAndColumns(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	mock, cleanup := withMySQLBatchPreflightMock(t)
	defer cleanup()
	mock.ExpectPing()
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM information_schema\.tables`).
		WithArgs("shop", "orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM information_schema\.columns`).
		WithArgs("shop", "orders", "id").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM information_schema\.columns`).
		WithArgs("shop", "orders", "amount").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))

	spec := pipeline.Spec{
		Name: "mysql-batch-table-preflight",
		Source: pipeline.SourceSpec{
			Type: "mysql_batch",
			Config: map[string]any{
				"host":     "mysql",
				"port":     3306,
				"user":     "sync",
				"password": "secret",
				"database": "shop",
				"table":    "orders",
				"columns":  []any{"amount"},
			},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir()},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if !result.Passed {
		t.Fatalf("RunPreflight passed = false, issues = %#v", result.Issues)
	}
	for _, check := range []string{"mysql-batch-table", "mysql-batch-column"} {
		if preflightIssuesContain(result, check) {
			t.Fatalf("issues = %#v, did not expect %s", result.Issues, check)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestRunPreflightBlocksMySQLBatchQueryMissingCursorColumn(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	mock, cleanup := withMySQLBatchPreflightMock(t)
	defer cleanup()
	mock.ExpectPing()
	mock.ExpectQuery(`SELECT \* FROM \(SELECT amount FROM orders\) AS openetl_preflight_probe LIMIT 0`).
		WillReturnRows(sqlmock.NewRows([]string{"amount"}))

	spec := pipeline.Spec{
		Name: "mysql-batch-query-preflight",
		Source: pipeline.SourceSpec{
			Type: "mysql_batch",
			Config: map[string]any{
				"host":          "mysql",
				"port":          3306,
				"user":          "sync",
				"password":      "secret",
				"database":      "shop",
				"query":         "SELECT amount FROM orders",
				"cursor_column": "id",
			},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir()},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	if !preflightIssuesContain(result, "mysql-batch-cursor-column") {
		t.Fatalf("issues = %#v, want mysql-batch-cursor-column", result.Issues)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestRunPreflightChecksPostgresCDCPrerequisites(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	mock, cleanup := withPostgresCDCPreflightMock(t)
	defer cleanup()
	mock.ExpectPing()
	mock.ExpectQuery(`SHOW wal_level`).
		WillReturnRows(sqlmock.NewRows([]string{"wal_level"}).AddRow("logical"))
	mock.ExpectQuery(`SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname = current_user`).
		WillReturnRows(sqlmock.NewRows([]string{"can_replicate"}).AddRow(true))
	mock.ExpectQuery(`SELECT COUNT\(\*\) FROM information_schema\.tables`).
		WithArgs("public", "orders").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1))
	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM pg_publication WHERE pubname='etl_pub'\)`).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM pg_replication_slots WHERE slot_name=\$1\)`).
		WithArgs("etl_slot").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT database FROM pg_replication_slots WHERE slot_name=\$1`).
		WithArgs("etl_slot").
		WillReturnRows(sqlmock.NewRows([]string{"database"}).AddRow("app"))

	spec := pipeline.Spec{
		Name: "postgres-cdc-preflight",
		Source: pipeline.SourceSpec{
			Type: "postgres_cdc",
			Config: map[string]any{
				"host":      "postgres",
				"port":      5432,
				"user":      "sync",
				"password":  "secret",
				"database":  "app",
				"slot_name": "etl_slot",
				"tables":    []any{"orders"},
			},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir()},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if !result.Passed {
		t.Fatalf("RunPreflight passed = false, issues = %#v", result.Issues)
	}
	for _, check := range []string{"postgres-cdc-wal-level", "postgres-cdc-replication-role", "postgres-cdc-table", "postgres-cdc-publication", "postgres-cdc-slot"} {
		if preflightIssuesContain(result, check) {
			t.Fatalf("issues = %#v, did not expect %s", result.Issues, check)
		}
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestRunPreflightBlocksPostgresCDCNonLogicalWalLevel(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()
	mock, cleanup := withPostgresCDCPreflightMock(t)
	defer cleanup()
	mock.ExpectPing()
	mock.ExpectQuery(`SHOW wal_level`).
		WillReturnRows(sqlmock.NewRows([]string{"wal_level"}).AddRow("replica"))
	mock.ExpectQuery(`SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname = current_user`).
		WillReturnRows(sqlmock.NewRows([]string{"can_replicate"}).AddRow(true))
	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM pg_publication WHERE pubname='etl_pub'\)`).
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	mock.ExpectQuery(`SELECT EXISTS \(SELECT 1 FROM pg_replication_slots WHERE slot_name=\$1\)`).
		WithArgs("etl_slot").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))

	spec := pipeline.Spec{
		Name: "postgres-cdc-wal-preflight",
		Source: pipeline.SourceSpec{
			Type: "postgres_cdc",
			Config: map[string]any{
				"host":     "postgres",
				"user":     "sync",
				"password": "secret",
				"database": "app",
			},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir()},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	if !preflightIssuesContain(result, "postgres-cdc-wal-level") {
		t.Fatalf("issues = %#v, want postgres-cdc-wal-level", result.Issues)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}

func TestRunPreflightBlocksInvalidPostgresCDCSourceConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "invalid-postgres-cdc-source-config",
		Source: pipeline.SourceSpec{
			Type: "postgres_cdc",
			Config: map[string]any{
				"port":            0,
				"slot_name":       "bad slot",
				"sslmode":         "tls",
				"enable_snapshot": true,
				"tables":          []string{"", ".orders"},
			},
		},
		Sink: pipeline.SinkSpec{
			Type:   testSchemaPreflightSink,
			Config: map[string]any{},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	for _, check := range []string{
		"postgres-cdc-source-required-config",
		"postgres-cdc-source-port",
		"postgres-cdc-source-slot-name",
		"postgres-cdc-source-sslmode",
		"postgres-cdc-source-table",
	} {
		if !preflightIssuesContain(result, check) {
			t.Fatalf("issues = %#v, want %s", result.Issues, check)
		}
	}
	for _, item := range []struct {
		field string
		check string
	}{
		{field: "source.config.host", check: "postgres-cdc-source-required-config"},
		{field: "source.config.user", check: "postgres-cdc-source-required-config"},
		{field: "source.config.database", check: "postgres-cdc-source-required-config"},
		{field: "source.config.port", check: "postgres-cdc-source-port"},
		{field: "source.config.slot_name", check: "postgres-cdc-source-slot-name"},
		{field: "source.config.sslmode", check: "postgres-cdc-source-sslmode"},
		{field: "source.config.tables", check: "postgres-cdc-source-table"},
	} {
		if !preflightFieldIssueContain(result, item.field, item.check) {
			t.Fatalf("field issues = %#v, want %s/%s", result.FieldIssues, item.field, item.check)
		}
	}
}

func TestRunPreflightBlocksPostgresCDCSnapshotWithoutTables(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "invalid-postgres-cdc-snapshot-config",
		Source: pipeline.SourceSpec{
			Type: "postgres_cdc",
			Config: map[string]any{
				"host":            "postgres",
				"user":            "sync",
				"database":        "app",
				"enable_snapshot": true,
			},
		},
		Sink: pipeline.SinkSpec{
			Type:   testSchemaPreflightSink,
			Config: map[string]any{},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	if !preflightIssuesContain(result, "postgres-cdc-source-snapshot-tables") {
		t.Fatalf("issues = %#v, want postgres-cdc-source-snapshot-tables", result.Issues)
	}
	if !preflightFieldIssueContain(result, "source.config.tables", "postgres-cdc-source-snapshot-tables") {
		t.Fatalf("field issues = %#v, want source.config.tables", result.FieldIssues)
	}
}

func TestRunPreflightBlocksInvalidMySQLCDCSourceConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "invalid-mysql-cdc-source-config",
		Source: pipeline.SourceSpec{
			Type: "mysql_cdc",
			Config: map[string]any{
				"port":           0,
				"server_id":      0,
				"server_id_base": 0,
				"shard_total":    2,
				"shard_index":    2,
				"start_from":     "file:mysql-bin.000001:4",
			},
		},
		Sink: pipeline.SinkSpec{
			Type:   testSchemaPreflightSink,
			Config: map[string]any{},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	for _, check := range []string{
		"mysql-cdc-source-required-config",
		"mysql-cdc-source-tables",
		"mysql-cdc-source-port",
		"mysql-cdc-source-server-id",
		"mysql-cdc-source-server-id-base",
		"mysql-cdc-source-shard",
		"mysql-cdc-source-start-from",
	} {
		if !preflightIssuesContain(result, check) {
			t.Fatalf("issues = %#v, want %s", result.Issues, check)
		}
	}
	for _, item := range []struct {
		field string
		check string
	}{
		{field: "source.config.host", check: "mysql-cdc-source-required-config"},
		{field: "source.config.user", check: "mysql-cdc-source-required-config"},
		{field: "source.config.database", check: "mysql-cdc-source-required-config"},
		{field: "source.config.tables", check: "mysql-cdc-source-tables"},
		{field: "source.config.port", check: "mysql-cdc-source-port"},
		{field: "source.config.server_id", check: "mysql-cdc-source-server-id"},
		{field: "source.config.server_id_base", check: "mysql-cdc-source-server-id-base"},
		{field: "source.config.shard_index", check: "mysql-cdc-source-shard"},
		{field: "source.config.start_from", check: "mysql-cdc-source-start-from"},
	} {
		if !preflightFieldIssueContain(result, item.field, item.check) {
			t.Fatalf("field issues = %#v, want %s/%s", result.FieldIssues, item.field, item.check)
		}
	}
}

func TestRunPreflightBlocksInvalidMySQLSnapshotCDCSourceConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "invalid-mysql-snapshot-cdc-source-config",
		Source: pipeline.SourceSpec{
			Type: "mysql_snapshot_cdc",
			Config: map[string]any{
				"host":      "mysql",
				"user":      "sync",
				"database":  "app",
				"pk_column": "",
				"limit":     0,
			},
		},
		Sink: pipeline.SinkSpec{
			Type:   testSchemaPreflightSink,
			Config: map[string]any{},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	for _, check := range []string{
		"mysql-snapshot-cdc-source-tables",
		"mysql-snapshot-cdc-source-pk-column",
		"mysql-snapshot-cdc-source-limit",
	} {
		if !preflightIssuesContain(result, check) {
			t.Fatalf("issues = %#v, want %s", result.Issues, check)
		}
	}
	for _, item := range []struct {
		field string
		check string
	}{
		{field: "source.config.tables", check: "mysql-snapshot-cdc-source-tables"},
		{field: "source.config.pk_column", check: "mysql-snapshot-cdc-source-pk-column"},
		{field: "source.config.limit", check: "mysql-snapshot-cdc-source-limit"},
	} {
		if !preflightFieldIssueContain(result, item.field, item.check) {
			t.Fatalf("field issues = %#v, want %s/%s", result.FieldIssues, item.field, item.check)
		}
	}
}

func TestRunPreflightBlocksMissingFileSourcePath(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "file-source-missing-path",
		Source: pipeline.SourceSpec{
			Type:   "file",
			Config: map[string]any{"format": "json"},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir()},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	if !preflightIssuesContain(result, "file-source-path") {
		t.Fatalf("issues = %#v, want file-source-path", result.Issues)
	}
}

func TestRunPreflightBlocksMalformedJSONFileSource(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	path := filepath.Join(t.TempDir(), "bad.jsonl")
	if err := os.WriteFile(path, []byte("{bad-json}\n"), 0o600); err != nil {
		t.Fatalf("write sample file: %v", err)
	}
	spec := pipeline.Spec{
		Name: "file-source-malformed-json",
		Source: pipeline.SourceSpec{
			Type:   "file",
			Config: map[string]any{"path": path, "format": "json"},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir()},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	if !preflightIssuesContain(result, "file-source-sample") {
		t.Fatalf("issues = %#v, want file-source-sample", result.Issues)
	}
}

func TestRunPreflightChecksHTTPSourceSampleRequest(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret-token" {
			t.Fatalf("Authorization = %q, want bearer token", got)
		}
		if got := r.URL.Query().Get("page"); got != "1" {
			t.Fatalf("page = %q, want 1", got)
		}
		if got := r.URL.Query().Get("size"); got != "2" {
			t.Fatalf("size = %q, want 2", got)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":[{"id":1,"name":"Alice"}]}`))
	}))
	defer api.Close()

	spec := pipeline.Spec{
		Name: "http-source-sample-preflight",
		Source: pipeline.SourceSpec{
			Type: "http",
			Config: map[string]any{
				"url":        api.URL + "/items",
				"auth_type":  "bearer",
				"auth_token": "secret-token",
				"page_param": "page",
				"size_param": "size",
				"page_size":  2,
				"result_key": "data",
			},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir()},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if !result.Passed {
		t.Fatalf("RunPreflight passed = false, issues = %#v", result.Issues)
	}
	if preflightIssuesContain(result, "http-source-sample") || preflightIssuesContain(result, "http-source-empty") {
		t.Fatalf("issues = %#v, did not expect http source sample issues", result.Issues)
	}
}

func TestRunPreflightBlocksHTTPSourceNonJSONResponse(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte("not json"))
	}))
	defer api.Close()

	spec := pipeline.Spec{
		Name: "http-source-non-json-preflight",
		Source: pipeline.SourceSpec{
			Type:   "http",
			Config: map[string]any{"url": api.URL + "/items"},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir()},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	if !preflightIssuesContain(result, "http-source-sample") {
		t.Fatalf("issues = %#v, want http-source-sample", result.Issues)
	}
}

func TestRunPreflightChecksKafkaSourceTopic(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(broker.BrokerID()).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader("orders.events", 0, broker.BrokerID()).
			SetLeader("orders.events", 1, broker.BrokerID()),
		"DescribeConfigsRequest": sarama.NewMockDescribeConfigsResponse(t),
	})

	spec := pipeline.Spec{
		Name: "kafka-source-topic-preflight",
		Source: pipeline.SourceSpec{
			Type: "kafka",
			Config: map[string]any{
				"brokers":  []string{broker.Addr()},
				"topic":    "orders.events",
				"group_id": "kafka-source-topic-preflight",
			},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir()},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if !result.Passed {
		t.Fatalf("RunPreflight passed = false, issues = %#v", result.Issues)
	}
	if preflightIssuesContain(result, "kafka-source-topic") {
		t.Fatalf("issues = %#v, did not expect kafka-source-topic", result.Issues)
	}
}

func TestRunPreflightBlocksMissingKafkaSourceTopic(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(broker.BrokerID()).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader("orders.events", 0, broker.BrokerID()),
		"DescribeConfigsRequest": sarama.NewMockDescribeConfigsResponse(t),
	})

	spec := pipeline.Spec{
		Name: "kafka-source-missing-topic-preflight",
		Source: pipeline.SourceSpec{
			Type: "kafka",
			Config: map[string]any{
				"brokers":  []string{broker.Addr()},
				"topic":    "orders.missing",
				"group_id": "kafka-source-missing-topic-preflight",
			},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir()},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	if !preflightIssuesContain(result, "kafka-source-topic") {
		t.Fatalf("issues = %#v, want kafka-source-topic", result.Issues)
	}
}

func TestRunPreflightReportsSinkReachabilityWarning(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "sink-reachability-warning",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: testSchemaPreflightSink,
			Config: map[string]any{
				"open_error": "dial tcp 127.0.0.1:1: connect: connection refused",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if !result.Passed {
		t.Fatalf("RunPreflight passed = false, issues = %#v", result.Issues)
	}
	if result.Summary != "1 warning(s) found" {
		t.Fatalf("summary = %q, want warning summary", result.Summary)
	}
	if len(result.Issues) != 1 {
		t.Fatalf("issues = %#v, want one sink-reachable warning", result.Issues)
	}
	issue := result.Issues[0]
	if issue.Level != "warning" || issue.Check != "sink-reachable" {
		t.Fatalf("issue = %#v, want sink-reachable warning", issue)
	}
	if !strings.Contains(issue.Message, "connection refused") {
		t.Fatalf("issue message = %q, want connection error", issue.Message)
	}
	if !preflightGuidanceContain(result, "delivery-at-least-once") {
		t.Fatalf("guidance = %#v, want delivery-at-least-once", result.Guidance)
	}
	if !preflightGuidanceContain(result, "dlq-replay") {
		t.Fatalf("guidance = %#v, want dlq-replay", result.Guidance)
	}
}

func TestRunPreflightGuidesAppendOnlyReplayRisk(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "append-only-replay-guidance",
		Source: pipeline.SourceSpec{
			Type:   "kafka",
			Config: map[string]any{"brokers": []string{"127.0.0.1:9092"}, "topic": "orders", "group_id": "test"},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir()},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if !result.Passed {
		t.Fatalf("RunPreflight passed = false, issues = %#v", result.Issues)
	}
	for _, code := range []string{"checkpoint-bounds-replay", "append-only-sink-replay"} {
		if !preflightGuidanceContain(result, code) {
			t.Fatalf("guidance = %#v, want %s", result.Guidance, code)
		}
	}
	kafkaReadiness, ok := preflightReadiness(result, "source", "kafka")
	if !ok {
		t.Fatalf("readiness = %#v, want source kafka readiness", result.Readiness)
	}
	if kafkaReadiness.Maturity != "production" || kafkaReadiness.Status == "" {
		t.Fatalf("kafka readiness = %#v, want production status", kafkaReadiness)
	}
	fileReadiness, ok := preflightReadiness(result, "sink", "file_sink")
	if !ok {
		t.Fatalf("readiness = %#v, want sink file_sink readiness", result.Readiness)
	}
	if fileReadiness.Maturity != "production" {
		t.Fatalf("file_sink readiness = %#v, want production maturity", fileReadiness)
	}
	if !preflightGuidanceContain(result, "readiness-source-kafka-schema_introspection") {
		t.Fatalf("guidance = %#v, want kafka schema readiness guidance", result.Guidance)
	}
	if !preflightGuidanceContain(result, "readiness-sink-file_sink-replay_absorption") {
		t.Fatalf("guidance = %#v, want file sink replay readiness guidance", result.Guidance)
	}
	for _, path := range []string{"batch_size", "checkpoint_interval_sec", "dlq.enable"} {
		if _, ok := preflightRecommendation(result, path); !ok {
			t.Fatalf("recommendations = %#v, want %s", result.Recommendations, path)
		}
	}
	prefix, ok := preflightRecommendation(result, "sink.config.prefix")
	if !ok || prefix.Value != "append-only-replay-guidance_" || prefix.Safety != "safe" {
		t.Fatalf("prefix recommendation = %#v, want safe file prefix", prefix)
	}
}

func TestRunPreflightRecommendsS3OutputPrefix(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "Kafka Orders To S3",
		Source: pipeline.SourceSpec{
			Type:   "kafka",
			Config: map[string]any{"brokers": []string{"127.0.0.1:9092"}, "topic": "orders", "group_id": "test"},
		},
		Sink: pipeline.SinkSpec{
			Type:   "s3",
			Config: map[string]any{"endpoint": "http://127.0.0.1:1", "bucket": "openetl", "output_dir": t.TempDir(), "format": "jsonl"},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if !result.Passed {
		t.Fatalf("RunPreflight passed = false, issues = %#v", result.Issues)
	}
	prefix, ok := preflightRecommendation(result, "sink.config.prefix")
	if !ok || prefix.Value != "kafka-orders-to-s3/" || prefix.Safety != "safe" {
		t.Fatalf("prefix recommendation = %#v, want safe s3 prefix", prefix)
	}
}

func TestRunPreflightBlocksS3WithoutEndpoint(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "s3-missing-endpoint",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type:   "s3",
			Config: map[string]any{"bucket": "openetl", "format": "jsonl"},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	if !preflightIssuesContain(result, "s3-sink-endpoint") {
		t.Fatalf("issues = %#v, want s3-sink-endpoint", result.Issues)
	}
	if !preflightFieldIssueContain(result, "sink.config.endpoint", "s3-sink-endpoint") {
		t.Fatalf("field issues = %#v, want sink.config.endpoint s3-sink-endpoint", result.FieldIssues)
	}
}

func TestRunPreflightBlocksUnsupportedFileSinkFormat(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "unsupported-file-sink-format",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir(), "format": "xml"},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	if !preflightIssuesContain(result, "sink-config-format") {
		t.Fatalf("issues = %#v, want sink-config-format", result.Issues)
	}
	if len(result.FieldIssues) == 0 {
		t.Fatalf("field issues = %#v, want sink.config.format", result.FieldIssues)
	}
	if got := result.FieldIssues[0]; got.Field != "sink.config.format" || got.Check != "sink-config-format" {
		t.Fatalf("field issue = %#v, want sink.config.format sink-config-format", got)
	}
}

func TestRunPreflightRecommendsKafkaSinkReplayConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "kafka-sink-recommendations",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{"key_column": "order_id"},
		},
		Sink: pipeline.SinkSpec{
			Type: "kafka",
			Config: map[string]any{
				"brokers": []any{"127.0.0.1:1"},
				"topic":   "ods.orders",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if !result.Passed {
		t.Fatalf("RunPreflight passed = false, issues = %#v", result.Issues)
	}
	key, ok := preflightRecommendation(result, "sink.config.key_column")
	if !ok || key.Value != "order_id" || key.Safety != "review" {
		t.Fatalf("key_column recommendation = %#v, want review order_id", key)
	}
	autoCreate, ok := preflightRecommendation(result, "sink.config.auto_create_topic")
	if !ok || autoCreate.Value != true || autoCreate.Safety != "review" {
		t.Fatalf("auto_create_topic recommendation = %#v, want review true", autoCreate)
	}
}

func TestRunPreflightChecksKafkaSinkTopic(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(broker.BrokerID()).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader("ods.orders", 0, broker.BrokerID()).
			SetLeader("ods.orders", 1, broker.BrokerID()),
		"DescribeConfigsRequest": sarama.NewMockDescribeConfigsResponse(t),
	})

	spec := pipeline.Spec{
		Name: "kafka-sink-topic-preflight",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "kafka",
			Config: map[string]any{
				"brokers": []string{broker.Addr()},
				"topic":   "ods.orders",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if !result.Passed {
		t.Fatalf("RunPreflight passed = false, issues = %#v", result.Issues)
	}
	if preflightIssuesContain(result, "kafka-sink-topic-metadata") {
		t.Fatalf("issues = %#v, did not expect kafka-sink-topic-metadata", result.Issues)
	}
}

func TestRunPreflightBlocksMissingKafkaSinkTopic(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	broker := sarama.NewMockBroker(t, 1)
	defer broker.Close()
	broker.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetController(broker.BrokerID()).
			SetBroker(broker.Addr(), broker.BrokerID()).
			SetLeader("ods.existing", 0, broker.BrokerID()),
		"DescribeConfigsRequest": sarama.NewMockDescribeConfigsResponse(t),
	})

	spec := pipeline.Spec{
		Name: "kafka-sink-missing-topic-preflight",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "kafka",
			Config: map[string]any{
				"brokers": []string{broker.Addr()},
				"topic":   "ods.missing",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	if !preflightIssuesContain(result, "kafka-sink-topic-metadata") {
		t.Fatalf("issues = %#v, want kafka-sink-topic-metadata", result.Issues)
	}
	if !preflightFieldIssueContain(result, "sink.config.topic", "kafka-sink-topic-metadata") {
		t.Fatalf("field issues = %#v, want sink.config.topic kafka-sink-topic-metadata", result.FieldIssues)
	}
}

func TestRunPreflightBlocksInvalidKafkaSinkConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "invalid-kafka-sink-config",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "kafka",
			Config: map[string]any{
				"brokers":          []string{"127.0.0.1:1"},
				"compression":      "brotli",
				"retry_backoff_ms": -1,
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	for _, check := range []string{"kafka-sink-topic", "kafka-sink-compression", "kafka-sink-retry"} {
		if !preflightIssuesContain(result, check) {
			t.Fatalf("issues = %#v, want %s", result.Issues, check)
		}
	}
	for _, item := range []struct {
		field string
		check string
	}{
		{field: "sink.config.topic", check: "kafka-sink-topic"},
		{field: "sink.config.compression", check: "kafka-sink-compression"},
		{field: "sink.config.retry_backoff_ms", check: "kafka-sink-retry"},
	} {
		if !preflightFieldIssueContain(result, item.field, item.check) {
			t.Fatalf("field issues = %#v, want %s/%s", result.FieldIssues, item.field, item.check)
		}
	}
}

func TestRunPreflightBlocksInvalidElasticsearchSinkConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "invalid-elasticsearch-sink-config",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "elasticsearch",
			Config: map[string]any{
				"hosts":         []string{"  "},
				"chunk_size":    0,
				"max_retries":   -1,
				"retry_base_ms": -1,
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	for _, check := range []string{"elasticsearch-sink-hosts", "elasticsearch-sink-index", "elasticsearch-sink-bulk", "elasticsearch-sink-retry"} {
		if !preflightIssuesContain(result, check) {
			t.Fatalf("issues = %#v, want %s", result.Issues, check)
		}
	}
	for _, item := range []struct {
		field string
		check string
	}{
		{field: "sink.config.hosts", check: "elasticsearch-sink-hosts"},
		{field: "sink.config.index", check: "elasticsearch-sink-index"},
		{field: "sink.config.chunk_size", check: "elasticsearch-sink-bulk"},
		{field: "sink.config.max_retries", check: "elasticsearch-sink-retry"},
		{field: "sink.config.retry_base_ms", check: "elasticsearch-sink-retry"},
	} {
		if !preflightFieldIssueContain(result, item.field, item.check) {
			t.Fatalf("field issues = %#v, want %s/%s", result.FieldIssues, item.field, item.check)
		}
	}
}

func TestRunPreflightBlocksInvalidMySQLSinkConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "invalid-mysql-sink-config",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "mysql",
			Config: map[string]any{
				"port":              70000,
				"batch_mode":        "upsert",
				"schema_drift":      "sync",
				"ddl_policy":        "fail",
				"insert_chunk_size": 0,
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	for _, check := range []string{
		"mysql-sink-required-config",
		"mysql-sink-port",
		"mysql-sink-upsert-keys",
		"mysql-sink-schema-drift",
		"mysql-sink-ddl-policy",
		"mysql-sink-insert-chunk-size",
	} {
		if !preflightIssuesContain(result, check) {
			t.Fatalf("issues = %#v, want %s", result.Issues, check)
		}
	}
	for _, item := range []struct {
		field string
		check string
	}{
		{field: "sink.config.host", check: "mysql-sink-required-config"},
		{field: "sink.config.user", check: "mysql-sink-required-config"},
		{field: "sink.config.database", check: "mysql-sink-required-config"},
		{field: "sink.config.table", check: "mysql-sink-required-config"},
		{field: "sink.config.port", check: "mysql-sink-port"},
		{field: "sink.config.pk_columns", check: "mysql-sink-upsert-keys"},
		{field: "sink.config.schema_drift", check: "mysql-sink-schema-drift"},
		{field: "sink.config.ddl_policy", check: "mysql-sink-ddl-policy"},
		{field: "sink.config.insert_chunk_size", check: "mysql-sink-insert-chunk-size"},
	} {
		if !preflightFieldIssueContain(result, item.field, item.check) {
			t.Fatalf("field issues = %#v, want %s/%s", result.FieldIssues, item.field, item.check)
		}
	}
}

func TestRunPreflightBlocksInvalidPostgresSinkConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "invalid-postgres-sink-config",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "postgres",
			Config: map[string]any{
				"host":       "postgres",
				"user":       "etl",
				"database":   "warehouse",
				"table":      "orders",
				"batch_mode": "merge",
				"sslmode":    "tls",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	for _, check := range []string{"postgres-sink-batch-mode", "postgres-sink-sslmode"} {
		if !preflightIssuesContain(result, check) {
			t.Fatalf("issues = %#v, want %s", result.Issues, check)
		}
	}
	for _, item := range []struct {
		field string
		check string
	}{
		{field: "sink.config.batch_mode", check: "postgres-sink-batch-mode"},
		{field: "sink.config.sslmode", check: "postgres-sink-sslmode"},
	} {
		if !preflightFieldIssueContain(result, item.field, item.check) {
			t.Fatalf("field issues = %#v, want %s/%s", result.FieldIssues, item.field, item.check)
		}
	}
}

func TestRunPreflightBlocksInvalidClickHouseSinkConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "invalid-clickhouse-sink-config",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "clickhouse",
			Config: map[string]any{
				"port":                  0,
				"protocol":              "grpc",
				"schema_drift":          "replace",
				"ddl_policy":            "fail",
				"source_dialect":        "oracle",
				"optimize_interval_sec": -1,
				"compression":           "brotli",
				"version_column":        "",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	for _, check := range []string{
		"clickhouse-sink-required-config",
		"clickhouse-sink-port",
		"clickhouse-sink-protocol",
		"clickhouse-sink-schema-drift",
		"clickhouse-sink-ddl-policy",
		"clickhouse-sink-source-dialect",
		"clickhouse-sink-optimize-interval",
		"clickhouse-sink-compression",
		"clickhouse-sink-version-column",
	} {
		if !preflightIssuesContain(result, check) {
			t.Fatalf("issues = %#v, want %s", result.Issues, check)
		}
	}
	for _, item := range []struct {
		field string
		check string
	}{
		{field: "sink.config.host", check: "clickhouse-sink-required-config"},
		{field: "sink.config.database", check: "clickhouse-sink-required-config"},
		{field: "sink.config.port", check: "clickhouse-sink-port"},
		{field: "sink.config.protocol", check: "clickhouse-sink-protocol"},
		{field: "sink.config.schema_drift", check: "clickhouse-sink-schema-drift"},
		{field: "sink.config.ddl_policy", check: "clickhouse-sink-ddl-policy"},
		{field: "sink.config.source_dialect", check: "clickhouse-sink-source-dialect"},
		{field: "sink.config.optimize_interval_sec", check: "clickhouse-sink-optimize-interval"},
		{field: "sink.config.compression", check: "clickhouse-sink-compression"},
		{field: "sink.config.version_column", check: "clickhouse-sink-version-column"},
	} {
		if !preflightFieldIssueContain(result, item.field, item.check) {
			t.Fatalf("field issues = %#v, want %s/%s", result.FieldIssues, item.field, item.check)
		}
	}
}

func TestRunPreflightBlocksInvalidDorisSinkConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "invalid-doris-sink-config",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "doris",
			Config: map[string]any{
				"port":                    0,
				"http_port":               70000,
				"write_mode":              "copy",
				"batch_mode":              "upsert",
				"stream_load_format":      "parquet",
				"stream_load_scheme":      "ftp",
				"stream_load_timeout_sec": 0,
				"insert_chunk_size":       0,
				"schema_drift":            "sync",
				"ddl_policy":              "fail",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	for _, check := range []string{
		"doris-sink-required-config",
		"doris-sink-port",
		"doris-sink-http-port",
		"doris-sink-write-mode",
		"doris-sink-upsert-keys",
		"doris-sink-stream-load-format",
		"doris-sink-stream-load-scheme",
		"doris-sink-stream-load-timeout",
		"doris-sink-insert-chunk-size",
		"doris-sink-schema-drift",
		"doris-sink-ddl-policy",
	} {
		if !preflightIssuesContain(result, check) {
			t.Fatalf("issues = %#v, want %s", result.Issues, check)
		}
	}
	for _, item := range []struct {
		field string
		check string
	}{
		{field: "sink.config.host", check: "doris-sink-required-config"},
		{field: "sink.config.database", check: "doris-sink-required-config"},
		{field: "sink.config.table", check: "doris-sink-required-config"},
		{field: "sink.config.port", check: "doris-sink-port"},
		{field: "sink.config.http_port", check: "doris-sink-http-port"},
		{field: "sink.config.write_mode", check: "doris-sink-write-mode"},
		{field: "sink.config.pk_columns", check: "doris-sink-upsert-keys"},
		{field: "sink.config.stream_load_format", check: "doris-sink-stream-load-format"},
		{field: "sink.config.stream_load_scheme", check: "doris-sink-stream-load-scheme"},
		{field: "sink.config.stream_load_timeout_sec", check: "doris-sink-stream-load-timeout"},
		{field: "sink.config.insert_chunk_size", check: "doris-sink-insert-chunk-size"},
		{field: "sink.config.schema_drift", check: "doris-sink-schema-drift"},
		{field: "sink.config.ddl_policy", check: "doris-sink-ddl-policy"},
	} {
		if !preflightFieldIssueContain(result, item.field, item.check) {
			t.Fatalf("field issues = %#v, want %s/%s", result.FieldIssues, item.field, item.check)
		}
	}
}

func TestRunPreflightBlocksInvalidMaxComputeSinkConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "invalid-maxcompute-sink-config",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "maxcompute",
			Config: map[string]any{
				"endpoint":          "ftp://service.cn-hangzhou.maxcompute.aliyun.com/api",
				"project":           "warehouse",
				"table":             "ods_events",
				"access_key_id":     "ak",
				"write_mode":        "merge",
				"batch_size":        0,
				"max_retries":       -1,
				"retry_base_ms":     0,
				"partition":         map[string]any{"": "bad", "dt": "2026-06-26"},
				"partition_fields":  []string{"", "dt"},
				"columns":           map[string]any{"": "STRING", "payload": "ARRAY<STRING>"},
				"tunnel_endpoint":   "not-a-url",
				"auto_create_table": true,
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, issues = %#v", result.Issues)
	}
	for _, check := range []string{
		"maxcompute-sink-required-config",
		"maxcompute-sink-endpoint",
		"maxcompute-sink-write-mode",
		"maxcompute-sink-batch-size",
		"maxcompute-sink-retry",
		"maxcompute-sink-partition",
		"maxcompute-sink-partition-fields",
		"maxcompute-sink-partition-conflict",
		"maxcompute-sink-columns",
	} {
		if !preflightIssuesContain(result, check) {
			t.Fatalf("issues = %#v, want %s", result.Issues, check)
		}
	}
	for _, item := range []struct {
		field string
		check string
	}{
		{field: "sink.config.access_key_secret", check: "maxcompute-sink-required-config"},
		{field: "sink.config.endpoint", check: "maxcompute-sink-endpoint"},
		{field: "sink.config.tunnel_endpoint", check: "maxcompute-sink-endpoint"},
		{field: "sink.config.write_mode", check: "maxcompute-sink-write-mode"},
		{field: "sink.config.batch_size", check: "maxcompute-sink-batch-size"},
		{field: "sink.config.max_retries", check: "maxcompute-sink-retry"},
		{field: "sink.config.retry_base_ms", check: "maxcompute-sink-retry"},
		{field: "sink.config.partition", check: "maxcompute-sink-partition"},
		{field: "sink.config.partition_fields", check: "maxcompute-sink-partition-fields"},
		{field: "sink.config.partition_fields", check: "maxcompute-sink-partition-conflict"},
		{field: "sink.config.columns", check: "maxcompute-sink-columns"},
	} {
		if !preflightFieldIssueContain(result, item.field, item.check) {
			t.Fatalf("field issues = %#v, want %s/%s", result.FieldIssues, item.field, item.check)
		}
	}
}

func TestRunPreflightRecommendsRelationalReplaySafeSinkConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "relational-replay-recommendations",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{"pk_column": "order_id"},
		},
		Sink: pipeline.SinkSpec{
			Type: "mysql",
			Config: map[string]any{
				"host":     "127.0.0.1",
				"port":     1,
				"user":     "sync",
				"password": "secret",
				"database": "warehouse",
				"table":    "orders",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if !result.Passed {
		t.Fatalf("RunPreflight passed = false, issues = %#v", result.Issues)
	}
	mode, ok := preflightRecommendation(result, "sink.config.batch_mode")
	if !ok || mode.Value != "upsert" || mode.Safety != "review" {
		t.Fatalf("batch_mode recommendation = %#v, want review upsert", mode)
	}
	keys, ok := preflightRecommendation(result, "sink.config.pk_columns")
	if !ok {
		t.Fatalf("recommendations = %#v, want sink.config.pk_columns", result.Recommendations)
	}
	keyValues, ok := keys.Value.([]string)
	if !ok || len(keyValues) != 1 || keyValues[0] != "order_id" {
		t.Fatalf("pk_columns recommendation = %#v, want [order_id]", keys)
	}
}

func TestRunPreflightInfersSchemaFromKafkaSampleHint(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "kafka-sample-schema-hint",
		Source: pipeline.SourceSpec{
			Type: "kafka",
			Config: map[string]any{
				"brokers":  []any{"127.0.0.1:9092"},
				"topic":    "orders",
				"group_id": "test",
				"sample": map[string]any{
					"operation": "INSERT",
					"data": map[string]any{
						"id":   1,
						"name": "Alice",
					},
				},
			},
		},
		Sink: pipeline.SinkSpec{
			Type: testSchemaPreflightSink,
			Config: map[string]any{
				"validation_error": "missing target columns [name]",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, want schema compatibility error")
	}
	if !preflightGuidanceContain(result, "schema-fallback-inferred") {
		t.Fatalf("guidance = %#v, want schema-fallback-inferred", result.Guidance)
	}
	if !preflightIssuesContain(result, "schema-compatibility") {
		t.Fatalf("issues = %#v, want schema-compatibility", result.Issues)
	}
	if len(result.FieldIssues) != 1 || result.FieldIssues[0].Field != "name" {
		t.Fatalf("field issues = %#v, want missing name from inferred sample schema", result.FieldIssues)
	}
}

func TestRunPreflightRejectsInvalidExplicitSchemaHint(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "invalid-schema-hint",
		Source: pipeline.SourceSpec{
			Type: "http",
			Config: map[string]any{
				"url": "http://127.0.0.1:1/items",
				"schema": []any{
					map[string]any{"data_type": "BIGINT"},
				},
			},
		},
		Sink: pipeline.SinkSpec{
			Type:   testSchemaPreflightSink,
			Config: map[string]any{},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, want schema-infer error")
	}
	if !preflightIssuesContain(result, "schema-infer") {
		t.Fatalf("issues = %#v, want schema-infer", result.Issues)
	}
}

func TestRunPreflightInfersFileSchemaForDDLPreview(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	path := filepath.Join(t.TempDir(), "orders.jsonl")
	if err := os.WriteFile(path, []byte(`{"id":1,"name":"Alice","dt":"20260630"}`+"\n"), 0o600); err != nil {
		t.Fatalf("write sample file: %v", err)
	}
	spec := pipeline.Spec{
		Name: "file-schema-ddl-preview",
		Source: pipeline.SourceSpec{
			Type: "file",
			Config: map[string]any{
				"path":   path,
				"format": "json",
			},
		},
		Sink: pipeline.SinkSpec{
			Type: "maxcompute",
			Config: map[string]any{
				"endpoint":          "http://127.0.0.1:1/api",
				"project":           "warehouse",
				"table":             "ods_orders",
				"access_key_id":     "ak",
				"access_key_secret": "secret",
				"partition_fields":  []any{"dt"},
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if !preflightGuidanceContain(result, "schema-fallback-inferred") {
		t.Fatalf("guidance = %#v, want schema-fallback-inferred", result.Guidance)
	}
	if result.DDLPreview == nil {
		t.Fatalf("DDLPreview = nil, want preview from file sample")
	}
	if result.DDLPreview.Dialect != "maxcompute" || result.DDLPreview.Table != "warehouse.ods_orders" {
		t.Fatalf("DDLPreview = %#v, want maxcompute warehouse.ods_orders", result.DDLPreview)
	}
	stmt := strings.Join(result.DDLPreview.Statements, "\n")
	for _, want := range []string{"`id`", "`name`", "PARTITIONED BY", "`dt`"} {
		if !strings.Contains(stmt, want) {
			t.Fatalf("DDL preview statement = %q, missing %q", stmt, want)
		}
	}
}

func TestRunPreflightBlocksRuntimeStateWithoutRedis(t *testing.T) {
	t.Setenv("ETL_STATE_REDIS_ADDR", "")
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "state-preflight",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type:   testSchemaPreflightSink,
			Config: map[string]any{},
		},
		Transforms: []pipeline.TransformSpec{{
			Type: "deduplicate",
			Config: map[string]any{
				"keys":          []any{"id"},
				"state_backend": "redis",
			},
		}},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, want Redis state/cache error")
	}
	if !preflightIssuesContain(result, "redis-state-cache") {
		t.Fatalf("RunPreflight issues = %#v, want redis-state-cache", result.Issues)
	}
}

func TestRunPreflightBlocksInvalidLookupQueryConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "lookup-query-config-preflight",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type:   testSchemaPreflightSink,
			Config: map[string]any{},
		},
		Transforms: []pipeline.TransformSpec{{
			Type: "lookup",
			Config: map[string]any{
				"mode":            "query",
				"dsn":             "user:pass@tcp(mysql:3306)/app",
				"query":           "SELECT id, tier FROM dim_users",
				"join_key":        "user_id",
				"fields":          []any{"tier"},
				"timeout_seconds": 0,
			},
		}},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, want transform config error")
	}
	if !preflightIssuesContain(result, "transform-config") {
		t.Fatalf("RunPreflight issues = %#v, want transform-config", result.Issues)
	}
	if !preflightFieldIssueContain(result, "transforms[0].config.query", "transform-config") {
		t.Fatalf("RunPreflight field issues = %#v, want transforms[0].config.query", result.FieldIssues)
	}
	if !preflightFieldIssueContain(result, "transforms[0].config.timeout_seconds", "transform-config") {
		t.Fatalf("RunPreflight field issues = %#v, want transforms[0].config.timeout_seconds", result.FieldIssues)
	}
}

func TestCreatePipelineReturnsPreflightWarningsWithoutBlocking(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "sink-reachability-create-warning",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: testSchemaPreflightSink,
			Config: map[string]any{
				"open_error": "temporary sink outage",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	resp, err := http.Post(ts.URL+"/api/v2/pipelines", "application/json", bytes.NewReader(mustPipelineJSON(t, spec)))
	if err != nil {
		t.Fatalf("POST /api/v2/pipelines: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, _ := body["preflight_valid"].(bool); !got {
		t.Fatalf("preflight_valid = %v, want true", got)
	}
	if !warningsContain(body["preflight_warnings"], "sink-reachable") {
		t.Fatalf("preflight_warnings = %#v, want sink-reachable warning", body["preflight_warnings"])
	}
	id, _ := body["id"].(string)
	if id == "" {
		t.Fatalf("create response id is empty: %#v", body)
	}
	if _, exists := s.pipelines[id]; !exists {
		t.Fatalf("pipeline %q should be created when preflight has warnings only", spec.Name)
	}
}

func TestCreatePipelineRejectsPreflightErrors(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "p5-14-create",
		Source: pipeline.SourceSpec{
			Type: "mysql_cdc",
			Config: map[string]any{
				"host":     "127.0.0.1",
				"port":     1,
				"user":     "root",
				"database": "db",
				"tables":   []string{"customers"},
			},
		},
		Sink: pipeline.SinkSpec{
			Type: "mysql",
			Config: map[string]any{
				"host":     "127.0.0.1",
				"port":     3306,
				"user":     "root",
				"database": "target",
				"table":    "customers",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	resp, err := http.Post(ts.URL+"/api/v2/pipelines", "application/json", bytes.NewReader(mustPipelineJSON(t, spec)))
	if err != nil {
		t.Fatalf("POST /api/v2/pipelines: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, _ := body["preflight_valid"].(bool); got {
		t.Fatalf("preflight_valid = %v, want false", got)
	}
	if refs := s.pipelineNameRefs[spec.Name]; len(refs) > 0 {
		t.Fatalf("pipeline %q should not be created when preflight fails", spec.Name)
	}
}

func TestCreatePipelineRejectsSchemaPreflightErrors(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "schema-preflight-create",
		Source: pipeline.SourceSpec{
			Type:   testSchemaPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: testSchemaPreflightSink,
			Config: map[string]any{
				"validation_error": "missing target columns [name]",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	resp, err := http.Post(ts.URL+"/api/v2/pipelines", "application/json", bytes.NewReader(mustPipelineJSON(t, spec)))
	if err != nil {
		t.Fatalf("POST /api/v2/pipelines: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}

	var body map[string]any
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if got, _ := body["preflight_valid"].(bool); got {
		t.Fatalf("preflight_valid = %v, want false", got)
	}
	if !warningsContain(body["preflight_warnings"], "schema-compatibility") {
		t.Fatalf("preflight_warnings = %#v, want schema-compatibility issue", body["preflight_warnings"])
	}
	if refs := s.pipelineNameRefs[spec.Name]; len(refs) > 0 {
		t.Fatalf("pipeline %q should not be created when schema preflight fails", spec.Name)
	}
}

func TestRunPreflightSkipsSchemaCompatibilityWhenUnsupported(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "schema-preflight-skip",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: testSchemaPreflightSink,
			Config: map[string]any{
				"validation_error": "should not be called",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if !result.Passed {
		t.Fatalf("RunPreflight passed = false, issues = %#v", result.Issues)
	}
	for _, issue := range result.Issues {
		if issue.Check == "schema-compatibility" {
			t.Fatalf("unexpected schema compatibility issue: %#v", issue)
		}
	}
	if !preflightGuidanceContain(result, "schema-source-introspection-unavailable") {
		t.Fatalf("guidance = %#v, want schema-source-introspection-unavailable", result.Guidance)
	}
}

func TestRunPreflightReturnsFieldIssuesForSchemaCompatibility(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "schema-field-issues",
		Source: pipeline.SourceSpec{
			Type:   testSchemaPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: testSchemaPreflightSink,
			Config: map[string]any{
				"validation_error": "schema validation failed for target: missing target columns [name]; incompatible target column types [id source=INT target=VARCHAR(255)]",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, want false")
	}
	if len(result.FieldIssues) != 2 {
		t.Fatalf("field issues = %#v, want missing + type issues", result.FieldIssues)
	}
	byField := map[string]PreflightFieldIssue{}
	for _, issue := range result.FieldIssues {
		byField[issue.Field] = issue
	}
	if got := byField["name"]; got.Check != "schema-field-missing" || got.SourceType != "VARCHAR(255)" {
		t.Fatalf("name field issue = %#v, want missing with source type", got)
	}
	if got := byField["id"]; got.Check != "schema-field-type" || got.SourceType != "INT" || got.TargetType != "VARCHAR(255)" {
		t.Fatalf("id field issue = %#v, want type mismatch", got)
	}
	rec, ok := preflightRecommendation(result, "transforms")
	if !ok {
		t.Fatalf("recommendations = %#v, want transforms type_convert recommendation", result.Recommendations)
	}
	transforms, ok := rec.Value.([]pipeline.TransformSpec)
	if !ok || len(transforms) != 1 || transforms[0].Type != "type_convert" {
		t.Fatalf("transforms recommendation = %#v, want one type_convert transform", rec.Value)
	}
	conversions, ok := transforms[0].Config["conversions"].(map[string]string)
	if !ok || conversions["id"] != "string" {
		t.Fatalf("type_convert conversions = %#v, want id -> string", transforms[0].Config["conversions"])
	}
}

func TestSchemaFieldRecommendationsSuggestSchemaDriftForSupportedSinks(t *testing.T) {
	result := &PreflightResult{FieldIssues: []PreflightFieldIssue{{
		Level:      "error",
		Field:      "name",
		Check:      "schema-field-missing",
		SourceType: "VARCHAR(255)",
	}}}
	spec := &pipeline.Spec{
		Name: "schema-drift-recommendation",
		Sink: pipeline.SinkSpec{
			Type:   "mysql",
			Config: map[string]any{"schema_drift": "ignore"},
		},
	}

	addSchemaFieldRecommendations(spec, result)
	rec, ok := preflightRecommendation(result, "sink.config.schema_drift")
	if !ok || rec.Value != "add_columns" || rec.Safety != "review" {
		t.Fatalf("schema_drift recommendation = %#v, want review add_columns", rec)
	}
}

func TestRunPreflightReturnsElasticsearchMappingFieldIssues(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	es := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_cluster/health":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"status":"green"}`))
		case "/orders/_mapping":
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"orders":{"mappings":{"properties":{"id":{"type":"long"},"phone":{"type":"long"},"name":{"type":"keyword"}}}}}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":"not found"}`))
		}
	}))
	defer es.Close()

	spec := pipeline.Spec{
		Name: "es-mapping-preflight",
		Source: pipeline.SourceSpec{
			Type: "kafka",
			Config: map[string]any{
				"brokers":  []any{"127.0.0.1:9092"},
				"topic":    "orders",
				"group_id": "test",
				"sample": map[string]any{
					"data": map[string]any{
						"id":    1,
						"phone": "not-a-number",
						"name":  "Alice",
					},
				},
			},
		},
		Sink: pipeline.SinkSpec{
			Type: "elasticsearch",
			Config: map[string]any{
				"host":  es.URL,
				"index": "orders",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, want ES mapping error")
	}
	if !preflightIssuesContain(result, "schema-compatibility") {
		t.Fatalf("issues = %#v, want schema-compatibility", result.Issues)
	}
	if !preflightGuidanceContain(result, "schema-fallback-inferred") {
		t.Fatalf("guidance = %#v, want schema-fallback-inferred", result.Guidance)
	}
	if len(result.FieldIssues) != 1 {
		t.Fatalf("field issues = %#v, want one phone type issue", result.FieldIssues)
	}
	if got := result.FieldIssues[0]; got.Field != "phone" || got.SourceType != "string" || got.TargetType != "long" {
		t.Fatalf("field issue = %#v, want phone string->long", got)
	}
}

func TestRunPreflightRejectsFailedMaxComputeRemotePreflight(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "maxcompute-remote-preflight-failed",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "maxcompute",
			Config: map[string]any{
				"endpoint":          "http://127.0.0.1:1/api",
				"project":           "warehouse",
				"table":             "ods_events",
				"access_key_id":     "ak",
				"access_key_secret": "secret",
				"partition":         map[string]any{"dt": "2026-06-26"},
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, want false")
	}
	found := false
	for _, issue := range result.Issues {
		if issue.Check == "maxcompute-preflight" && issue.Level == "error" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("RunPreflight issues = %#v, want maxcompute-preflight error", result.Issues)
	}
}

func TestRunPreflightReturnsMaxComputeDDLPreviewAndPartitionFieldIssue(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "maxcompute-preflight-preview",
		Source: pipeline.SourceSpec{
			Type:   testSchemaPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "maxcompute",
			Config: map[string]any{
				"endpoint":          "http://127.0.0.1:1/api",
				"project":           "warehouse",
				"table":             "ods_events",
				"access_key_id":     "ak",
				"access_key_secret": "secret",
				"columns": map[string]any{
					"id":   "BIGINT",
					"name": "STRING",
				},
				"partition_fields": []any{"dt"},
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if result.Passed {
		t.Fatalf("RunPreflight passed = true, want writer/schema errors")
	}
	foundWriter := false
	foundSchema := false
	for _, issue := range result.Issues {
		switch issue.Check {
		case "maxcompute-preflight":
			foundWriter = true
		case "schema-compatibility":
			foundSchema = true
		}
	}
	if !foundWriter || !foundSchema {
		t.Fatalf("issues = %#v, want maxcompute writer and schema compatibility errors", result.Issues)
	}
	if len(result.FieldIssues) != 1 {
		t.Fatalf("field issues = %#v, want partition field issue", result.FieldIssues)
	}
	if got := result.FieldIssues[0]; got.Check != "schema-partition-field-missing" || got.Field != "dt" {
		t.Fatalf("field issue = %#v, want missing partition dt", got)
	}
	if result.DDLPreview == nil {
		t.Fatal("DDLPreview = nil, want MaxCompute preview")
	}
	if result.DDLPreview.Dialect != "maxcompute" || result.DDLPreview.Table != "warehouse.ods_events" {
		t.Fatalf("DDLPreview = %#v, want maxcompute warehouse.ods_events", result.DDLPreview)
	}
	if len(result.DDLPreview.Statements) != 1 {
		t.Fatalf("DDLPreview statements = %#v, want one statement", result.DDLPreview.Statements)
	}
	stmt := result.DDLPreview.Statements[0]
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS `warehouse`.`ods_events`",
		"`id` BIGINT",
		"`name` STRING",
		"PARTITIONED BY",
		"`dt` STRING",
	} {
		if !strings.Contains(stmt, want) {
			t.Fatalf("DDL preview statement = %q, missing %q", stmt, want)
		}
	}
}

func TestUpdatePipelineRejectsPreflightErrorsWithoutReplacingRunner(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	original := pipeline.Spec{
		Name: "p5-14-update",
		Source: pipeline.SourceSpec{
			Type:   "file",
			Config: map[string]any{"path": filepath.Join(t.TempDir(), "missing.jsonl"), "format": "json"},
		},
		Sink: pipeline.SinkSpec{
			Type:   "file_sink",
			Config: map[string]any{"output_dir": t.TempDir()},
		},
	}
	pipeline.ApplyDefaults(&original)

	runner, err := s.newRunner(&original)
	if err != nil {
		t.Fatalf("newRunner(original): %v", err)
	}
	originalID := newPipelineInstanceID()
	s.registerPipelineLocked(originalID, original.Name, runner, &original, nil)

	badUpdate := pipeline.Spec{
		Name: "p5-14-update",
		Source: pipeline.SourceSpec{
			Type: "mysql_cdc",
			Config: map[string]any{
				"host":     "127.0.0.1",
				"port":     1,
				"user":     "root",
				"database": "db",
				"tables":   []string{"customers"},
			},
		},
		Sink: pipeline.SinkSpec{
			Type: "mysql",
			Config: map[string]any{
				"host":     "127.0.0.1",
				"port":     3306,
				"user":     "root",
				"database": "target",
				"table":    "customers",
			},
		},
	}
	pipeline.ApplyDefaults(&badUpdate)

	req, err := http.NewRequest(http.MethodPut, ts.URL+"/api/v2/pipelines", bytes.NewReader(mustPipelineUpdateJSONWithID(t, originalID, badUpdate)))
	if err != nil {
		t.Fatalf("new PUT request: %v", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("PUT /api/v2/pipelines: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusBadRequest)
	}

	if got := s.pipelines[originalID]; got != runner {
		t.Fatalf("runner replaced on failed update")
	}
	if got := s.specs[originalID]; got != &original {
		t.Fatalf("spec replaced on failed update")
	}
}

// TestRunPreflightSkipsTableAndPKForDebeziumCDC verifies that when a Debezium
// CDC transform is present and the MySQL sink uses auto_create, preflight does
// not require static sink.config.table or sink.config.pk_columns because both
// are derived from Debezium record metadata at runtime.
func TestRunPreflightSkipsTableAndPKForDebeziumCDC(t *testing.T) {
	s, _ := newTestHTTPServer(t)

	spec := pipeline.Spec{
		Name: "debezium-cdc-to-mysql",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Transforms: []pipeline.TransformSpec{
			{Type: "debezium_cdc"},
		},
		Sink: pipeline.SinkSpec{
			Type: "mysql",
			Config: map[string]any{
				"host":                     "localhost",
				"user":                     "root",
				"password":                 "",
				"database":                 "target",
				"port":                     3306,
				"batch_mode":               "upsert",
				"auto_create":              true,
				"pk_columns_from_metadata": true,
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)

	// sink.config.table must not be required: the table is derived per record.
	if preflightIssuesContain(result, "mysql-sink-required-config") {
		t.Fatalf("issues should not contain mysql-sink-required-config for Debezium CDC pipeline: %#v", result.Issues)
	}
	if preflightFieldIssueContain(result, "sink.config.table", "mysql-sink-required-config") {
		t.Fatalf("field issues should not require sink.config.table for Debezium CDC pipeline: %#v", result.FieldIssues)
	}
	// sink.config.pk_columns must not be required: keys come from Debezium metadata.
	if preflightIssuesContain(result, "mysql-sink-upsert-keys") {
		t.Fatalf("issues should not contain mysql-sink-upsert-keys for Debezium CDC pipeline: %#v", result.Issues)
	}
	if preflightFieldIssueContain(result, "sink.config.pk_columns", "mysql-sink-upsert-keys") {
		t.Fatalf("field issues should not require sink.config.pk_columns for Debezium CDC pipeline: %#v", result.FieldIssues)
	}
	// pk_columns recommendation should be suppressed when keys are derived from metadata.
	for _, rec := range result.Recommendations {
		if rec.Path == "sink.config.pk_columns" {
			t.Fatalf("pk_columns recommendation should be suppressed for Debezium CDC pipeline: %#v", result.Recommendations)
		}
	}
}

// TestRunPreflightStillRequiresTableWithoutDebeziumCDC is a regression guard:
// without a debezium_cdc transform, the static table requirement still applies.
func TestRunPreflightStillRequiresTableWithoutDebeziumCDC(t *testing.T) {
	s, _ := newTestHTTPServer(t)

	spec := pipeline.Spec{
		Name: "plain-to-mysql",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "mysql",
			Config: map[string]any{
				"host":       "localhost",
				"user":       "root",
				"database":   "target",
				"port":       3306,
				"batch_mode": "upsert",
			},
		},
	}
	pipeline.ApplyDefaults(&spec)

	result := s.RunPreflight(context.Background(), &spec)
	if !preflightIssuesContain(result, "mysql-sink-required-config") {
		t.Fatalf("issues should contain mysql-sink-required-config without Debezium CDC: %#v", result.Issues)
	}
	if !preflightFieldIssueContain(result, "sink.config.table", "mysql-sink-required-config") {
		t.Fatalf("field issues should require sink.config.table without Debezium CDC: %#v", result.FieldIssues)
	}
	if !preflightIssuesContain(result, "mysql-sink-upsert-keys") {
		t.Fatalf("issues should contain mysql-sink-upsert-keys without Debezium CDC: %#v", result.Issues)
	}
}
