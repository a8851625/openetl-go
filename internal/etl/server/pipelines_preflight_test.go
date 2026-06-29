package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

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
}

func TestRunPreflightRejectsExperimentalMaxComputeWriter(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "maxcompute-writer-disabled",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "maxcompute",
			Config: map[string]any{
				"endpoint":          "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
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
		if issue.Check == "maxcompute-writer" && issue.Level == "error" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("RunPreflight issues = %#v, want maxcompute-writer error", result.Issues)
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
				"endpoint":          "https://service.cn-hangzhou.maxcompute.aliyun.com/api",
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
		case "maxcompute-writer":
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
