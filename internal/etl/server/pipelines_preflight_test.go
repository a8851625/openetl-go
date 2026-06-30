package server

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
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
	if !preflightGuidanceContain(result, "dlq-linear-replay") {
		t.Fatalf("guidance = %#v, want dlq-linear-replay", result.Guidance)
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
			Config: map[string]any{"bucket": "openetl", "output_dir": t.TempDir(), "format": "jsonl"},
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

func TestRunPreflightRecommendsKafkaSinkReplayConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "kafka-sink-recommendations",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
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
	if !ok || key.Value != "id" || key.Safety != "review" {
		t.Fatalf("key_column recommendation = %#v, want review id", key)
	}
	autoCreate, ok := preflightRecommendation(result, "sink.config.auto_create_topic")
	if !ok || autoCreate.Value != true || autoCreate.Safety != "review" {
		t.Fatalf("auto_create_topic recommendation = %#v, want review true", autoCreate)
	}
}

func TestRunPreflightRecommendsRelationalReplaySafeSinkConfig(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "relational-replay-recommendations",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
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
	if !ok || len(keyValues) != 1 || keyValues[0] != "id" {
		t.Fatalf("pk_columns recommendation = %#v, want [id]", keys)
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
