package server

import (
	"context"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

func TestMetadataIdentityPreflightRejectsIncapableSourceWithFieldIssue(t *testing.T) {
	server, httpServer := newTestHTTPServer(t)
	defer httpServer.Close()
	spec := &pipeline.Spec{
		Name:   "file-metadata-pk",
		Source: pipeline.SourceSpec{Type: "file", Config: map[string]any{"path": "/missing", "format": "json"}},
		Sink: pipeline.SinkSpec{Type: "postgres", Config: map[string]any{
			"host": "127.0.0.1", "user": "etl", "database": "target", "batch_mode": "upsert", "pk_columns_from_metadata": true,
		}},
	}
	result := server.RunPreflight(context.Background(), spec)
	if result.Passed || !preflightFieldIssueContain(result, "source.type", "metadata-identity-capability") {
		t.Fatalf("preflight = %#v, want source.type metadata identity error", result)
	}
}

func TestMetadataIdentityPreflightAcceptsCanalCapability(t *testing.T) {
	spec := &pipeline.Spec{
		Source: pipeline.SourceSpec{Type: "kafka", Config: map[string]any{"format": "canal_json"}},
		Sink:   pipeline.SinkSpec{Type: "clickhouse", Config: map[string]any{"pk_columns_from_metadata": true}},
	}
	result := &PreflightResult{Passed: true}
	checkMetadataIdentityCompatibility(spec, result)
	if preflightIssuesContain(result, "metadata-identity-capability") || !result.Passed {
		t.Fatalf("Canal capability rejected: %#v", result)
	}
}

func TestSinkDerivesPKFromMetadataIncludesAdvertisedSinks(t *testing.T) {
	for _, sinkType := range []string{"mysql", "postgres", "postgresql", "clickhouse", "doris"} {
		spec := &pipeline.Spec{Sink: pipeline.SinkSpec{Type: sinkType, Config: map[string]any{"pk_columns_from_metadata": true}}}
		if !sinkDerivesPKFromMetadata(spec) {
			t.Fatalf("sink %s advertises pk_columns_from_metadata but helper returned false", sinkType)
		}
	}
}

func TestMetadataPKTargetPreflightRequiresResolvableTableAndDatabaseTemplate(t *testing.T) {
	tests := []struct {
		name      string
		spec      *pipeline.Spec
		wantField string
		wantCheck string
	}{
		{
			name: "unknown source has no dynamic table",
			spec: &pipeline.Spec{
				Source: pipeline.SourceSpec{Type: "custom", Config: map[string]any{}},
				Sink: pipeline.SinkSpec{Type: "postgres", Config: map[string]any{
					"database": "target", "pk_columns_from_metadata": true,
				}},
			},
			wantField: "sink.config.table", wantCheck: "metadata-pk-target-table",
		},
		{
			name: "legacy envelope database placeholder is not provable",
			spec: &pipeline.Spec{
				Source: pipeline.SourceSpec{Type: "kafka", Config: map[string]any{"format": "envelope"}},
				Sink: pipeline.SinkSpec{Type: "clickhouse", Config: map[string]any{
					"database": "target", "table_template": "{db}_{table}", "pk_columns_from_metadata": true,
				}},
			},
			wantField: "sink.config.table_template", wantCheck: "metadata-pk-target-database",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := &PreflightResult{Passed: true}
			checkMetadataPKTargetCompatibility(test.spec, result)
			if result.Passed || !preflightFieldIssueContain(result, test.wantField, test.wantCheck) {
				t.Fatalf("result=%#v, want %s/%s", result, test.wantField, test.wantCheck)
			}
		})
	}
}

func TestMetadataPKTargetPreflightAcceptsDynamicCDCAndFixedTarget(t *testing.T) {
	for _, spec := range []*pipeline.Spec{
		{
			Source: pipeline.SourceSpec{Type: "kafka", Config: map[string]any{"format": "canal_json"}},
			Sink: pipeline.SinkSpec{Type: "clickhouse", Config: map[string]any{
				"database": "target", "pk_columns_from_metadata": true,
			}},
		},
		{
			Source: pipeline.SourceSpec{Type: "kafka", Config: map[string]any{"format": "canal_json"}},
			Sink: pipeline.SinkSpec{Type: "doris", Config: map[string]any{
				"database": "target", "table_template": "{db}_{table}", "pk_columns_from_metadata": true,
			}},
		},
		{
			Source: pipeline.SourceSpec{Type: "custom", Config: map[string]any{}},
			Sink: pipeline.SinkSpec{Type: "mysql", Config: map[string]any{
				"database": "target", "table": "orders", "pk_columns_from_metadata": true,
			}},
		},
	} {
		result := &PreflightResult{Passed: true}
		checkMetadataPKTargetCompatibility(spec, result)
		if !result.Passed || len(result.FieldIssues) != 0 {
			t.Fatalf("valid target rejected: %#v", result)
		}
	}
}

func TestMutableSinkPreflightRequiresExplicitTargetKey(t *testing.T) {
	for _, sinkType := range []string{"mysql", "postgres", "doris", "clickhouse"} {
		t.Run(sinkType, func(t *testing.T) {
			spec := &pipeline.Spec{
				Source: pipeline.SourceSpec{Type: "kafka", Config: map[string]any{"format": "canal_json"}},
				Sink: pipeline.SinkSpec{Type: sinkType, Config: map[string]any{
					"host": "target", "user": "etl", "database": "target", "table": "orders",
				}},
			}
			result := &PreflightResult{Passed: true}
			if sinkType == "doris" {
				checkDorisSinkConfig(spec, result)
			} else if sinkType == "clickhouse" {
				checkClickHouseSinkConfig(spec, result)
			} else {
				checkRelationalSinkConfig(spec, result)
			}
			found := false
			for _, issue := range result.FieldIssues {
				if issue.Field == "sink.config.pk_columns" {
					found = true
				}
			}
			if result.Passed || !found {
				t.Fatalf("result=%#v, want actionable target key error", result)
			}
		})
	}
}
