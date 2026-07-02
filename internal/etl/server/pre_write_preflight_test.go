package server

import (
	"context"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

func TestPreflightPreWriteDeleteRequiresCondition(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "pw-delete-no-cond",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "mysql",
			Config: map[string]any{
				"host":     "mysql",
				"user":     "etl",
				"database": "warehouse",
				"table":    "orders",
				"pre_write": map[string]any{
					"action": "delete",
				},
			},
		},
	}
	pipeline.ApplyDefaults(&spec)
	result := s.RunPreflight(context.Background(), &spec)
	if !preflightIssuesContain(result, "mysql-sink-pre-write-condition") {
		t.Fatalf("expected mysql-sink-pre-write-condition issue, got %#v", result.Issues)
	}
}

func TestPreflightPreWriteTruncateBatchOK(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "pw-truncate-batch",
		Source: pipeline.SourceSpec{
			Type:   testPlainPreflightSource,
			Config: map[string]any{},
		},
		Sink: pipeline.SinkSpec{
			Type: "mysql",
			Config: map[string]any{
				"host":     "mysql",
				"user":     "etl",
				"database": "warehouse",
				"table":    "orders",
				"pre_write": map[string]any{
					"action": "truncate",
				},
			},
		},
	}
	pipeline.ApplyDefaults(&spec)
	result := s.RunPreflight(context.Background(), &spec)
	// Should NOT contain a static validation error for truncate without condition
	for _, issue := range result.Issues {
		if issue.Check == "mysql-sink-pre-write-condition" {
			t.Fatalf("truncate should not require condition, got issue: %#v", issue)
		}
	}
}

func TestPreflightPreWriteTruncateCDCStreamingError(t *testing.T) {
	s, ts := newTestHTTPServer(t)
	defer ts.Close()

	spec := pipeline.Spec{
		Name: "pw-truncate-cdc",
		Source: pipeline.SourceSpec{
			Type: "mysql_cdc",
			Config: map[string]any{
				"host":     "mysql",
				"user":     "etl",
				"password": "pw",
				"server_id": 1234,
				"database": "src",
			},
		},
		Sink: pipeline.SinkSpec{
			Type: "mysql",
			Config: map[string]any{
				"host":     "mysql",
				"user":     "etl",
				"database": "warehouse",
				"table":    "orders",
				"pre_write": map[string]any{
					"action": "truncate",
				},
			},
		},
	}
	pipeline.ApplyDefaults(&spec)
	result := s.RunPreflight(context.Background(), &spec)
	found := false
	for _, g := range result.Guidance {
		if g.Code == "pre-write-idempotency" && g.Level == "error" {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected error-level pre-write-idempotency guidance for CDC+truncate, got %#v", result.Guidance)
	}
}

// Test that the static preflight schema descriptor for MySQL sink includes pre_write field.
func TestPreflightMySQLSchemaHasPreWrite(t *testing.T) {
	schema := configSchema()
	sinks, ok := schema["sinks"].(map[string][]ConfigField)
	if !ok {
		t.Fatalf("sinks schema has unexpected type: %T", schema["sinks"])
	}
	mysqlSink, ok := sinks["mysql"]
	if !ok {
		t.Fatalf("mysql sink schema missing")
	}
	found := false
	for _, f := range mysqlSink {
		if f.Name == "pre_write" {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("mysql sink schema missing pre_write field")
	}
}

// Ensure Describe method is available (used by introspection)
var _ = (*core.SchemaCache)(nil)
