package server

import (
	"context"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

func TestRunPreflightTreatsTargetContractFailureAsBlocking(t *testing.T) {
	server, httpServer := newTestHTTPServer(t)
	defer httpServer.Close()
	spec := pipeline.Spec{
		Name:   "target-contract-preflight",
		Source: pipeline.SourceSpec{Type: testPlainPreflightSource, Config: map[string]any{}},
		Sink: pipeline.SinkSpec{Type: testSchemaPreflightSink, Config: map[string]any{
			"target_contract_error": "legacy Int64 version table",
		}},
	}
	result := server.RunPreflight(context.Background(), &spec)
	if result.Passed || !preflightIssuesContain(result, "sink-target-contract") {
		t.Fatalf("result = %#v, want blocking target contract issue", result)
	}
}

func TestClickHousePreflightRejectsSourceOrderWithoutCapability(t *testing.T) {
	for _, sourceType := range []string{"file", "http", "rest_source", "redis", "demo", "custom_source"} {
		t.Run(sourceType, func(t *testing.T) {
			spec := clickHouseOrderPreflightSpec(sourceType, nil, nil)
			result := &PreflightResult{Passed: true}
			checkClickHouseSinkConfig(spec, result)
			if result.Passed || !preflightIssuesContain(result, "clickhouse-source-order-capability") {
				t.Fatalf("result = %#v, want source-order capability error", result)
			}
			if !preflightFieldIssueContain(result, "source.type", "clickhouse-source-order-capability") {
				t.Fatalf("field issues = %#v, want source.type", result.FieldIssues)
			}
			if !strings.Contains(result.Issues[len(result.Issues)-1].Remediation, "metadata.source_type") {
				t.Fatalf("remediation = %q, want missing metadata fields", result.Issues[len(result.Issues)-1].Remediation)
			}
		})
	}
}

func TestClickHousePreflightRejectsDerivedWindowSourceOrder(t *testing.T) {
	spec := clickHouseOrderPreflightSpec("kafka", nil, nil)
	spec.Transforms = []pipeline.TransformSpec{
		{Type: "normalize_envelope"},
		{Type: "window"},
	}
	result := &PreflightResult{Passed: true}
	checkClickHouseSinkConfig(spec, result)
	if result.Passed || !preflightFieldIssueContain(result, "transforms[1].type", "clickhouse-source-order-transform") {
		t.Fatalf("result = %#v, want derived-window source-order error", result)
	}
}

func TestClickHousePreflightAppendIsExplicitlyInsertOnly(t *testing.T) {
	fileSpec := clickHouseOrderPreflightSpec("file", nil, map[string]any{"version_mode": "append"})
	fileResult := &PreflightResult{Passed: true}
	checkClickHouseSinkConfig(fileSpec, fileResult)
	if !fileResult.Passed || preflightIssuesContain(fileResult, "clickhouse-source-order-capability") {
		t.Fatalf("file append result = %#v, want accepted INSERT-only combination", fileResult)
	}

	for _, test := range []struct {
		name       string
		sourceType string
		sourceCfg  map[string]any
	}{
		{name: "mysql_cdc", sourceType: "mysql_cdc"},
		{name: "snapshot_cdc", sourceType: "mysql_snapshot_cdc"},
		{name: "postgres_cdc", sourceType: "postgres_cdc"},
		{name: "kafka_envelope", sourceType: "kafka", sourceCfg: map[string]any{"format": "envelope"}},
		{name: "kafka_canal", sourceType: "kafka", sourceCfg: map[string]any{"format": "canal_json"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			spec := clickHouseOrderPreflightSpec(test.sourceType, test.sourceCfg, map[string]any{"version_mode": "append"})
			result := &PreflightResult{Passed: true}
			checkClickHouseSinkConfig(spec, result)
			if result.Passed || !preflightIssuesContain(result, "clickhouse-append-mutable-source") {
				t.Fatalf("result = %#v, want INSERT-only error", result)
			}
		})
	}

	aggregateSpec := clickHouseOrderPreflightSpec("kafka", nil, map[string]any{"version_mode": "append"})
	aggregateSpec.Transforms = []pipeline.TransformSpec{
		{Type: "normalize_envelope"},
		{Type: "window"},
	}
	aggregateResult := &PreflightResult{Passed: true}
	checkClickHouseSinkConfig(aggregateSpec, aggregateResult)
	if !aggregateResult.Passed || preflightIssuesContain(aggregateResult, "clickhouse-append-mutable-source") {
		t.Fatalf("window aggregate append result = %#v, want accepted INSERT-only derived records", aggregateResult)
	}
}

func TestClickHousePreflightRejectsPostgresSnapshotWithoutOrder(t *testing.T) {
	spec := clickHouseOrderPreflightSpec("postgres_cdc", map[string]any{"enable_snapshot": true}, nil)
	result := &PreflightResult{Passed: true}
	checkClickHouseSinkConfig(spec, result)
	if result.Passed || !preflightFieldIssueContain(result, "source.config.enable_snapshot", "clickhouse-source-order-capability") {
		t.Fatalf("result = %#v, want PostgreSQL snapshot source-order error", result)
	}
}

func TestClickHousePreflightRejectsOrderedTextCursor(t *testing.T) {
	spec := clickHouseOrderPreflightSpec("mysql_batch", map[string]any{"cursor_column": "customer_code"}, nil)
	result := &PreflightResult{Passed: true}
	checkClickHouseSourceOrderSchema(spec, core.SchemaInfo{Columns: []core.ColumnInfo{
		{Name: "customer_code", DataType: "varchar(64)"},
	}}, result)
	if result.Passed || !preflightFieldIssueContain(result, "source.config.cursor_column", "clickhouse-source-order-cursor") {
		t.Fatalf("result = %#v, want text batch cursor error", result)
	}
	if !strings.Contains(result.Issues[0].Message, "metadata.cursor_kind=ordered") {
		t.Fatalf("message = %q", result.Issues[0].Message)
	}
}

func TestClickHousePreflightAcceptsSnapshotTextCursorWithBinlogHandoff(t *testing.T) {
	spec := clickHouseOrderPreflightSpec("mysql_snapshot_cdc", map[string]any{"pk_column": "customer_code"}, nil)
	result := &PreflightResult{Passed: true}
	checkClickHouseSourceOrderSchema(spec, core.SchemaInfo{Columns: []core.ColumnInfo{
		{Name: "customer_code", DataType: "varchar(64)"},
	}}, result)
	if !result.Passed || len(result.Issues) != 0 {
		t.Fatalf("result = %#v, snapshot ordering must use the captured binlog handoff rather than hashing/rejecting its text pagination cursor", result)
	}
}

func TestClickHousePreflightAcceptsIntegerCursor(t *testing.T) {
	spec := clickHouseOrderPreflightSpec("mysql_batch", map[string]any{"cursor_column": "id"}, nil)
	result := &PreflightResult{Passed: true}
	checkClickHouseSourceOrderSchema(spec, core.SchemaInfo{Columns: []core.ColumnInfo{{Name: "id", DataType: "BIGINT UNSIGNED"}}}, result)
	if !result.Passed || len(result.Issues) != 0 {
		t.Fatalf("result = %#v, want integer cursor accepted", result)
	}
}

func TestClickHouseDDLPreviewMatchesVersionMode(t *testing.T) {
	columns := []core.ColumnInfo{{Name: "id", DataType: "BIGINT"}, {Name: "name", DataType: "VARCHAR(64)"}}
	sourceOrder := createClickHouseDDLPreview("warehouse.orders", columns, map[string]any{"pk_columns": []any{"id"}})
	for _, want := range []string{"`_version` UInt64", "`_is_deleted` UInt8", "ReplacingMergeTree(`_version`, `_is_deleted`)"} {
		if !strings.Contains(sourceOrder, want) {
			t.Fatalf("source-order preview missing %q:\n%s", want, sourceOrder)
		}
	}
	if strings.Contains(sourceOrder, "_version` Int64") {
		t.Fatalf("source-order preview retained legacy Int64:\n%s", sourceOrder)
	}

	appendDDL := createClickHouseDDLPreview("warehouse.events", columns, map[string]any{"version_mode": "append", "pk_columns": []any{"id"}})
	if !strings.Contains(appendDDL, "ENGINE = MergeTree") || strings.Contains(appendDDL, "_version") || strings.Contains(appendDDL, "_is_deleted") {
		t.Fatalf("append preview = %s", appendDDL)
	}
}

func TestClickHousePreflightRejectsInvalidVersionColumns(t *testing.T) {
	tests := []struct {
		name   string
		config map[string]any
		check  string
	}{
		{name: "mode", config: map[string]any{"version_mode": "clock"}, check: "clickhouse-sink-version-mode"},
		{name: "delete_empty", config: map[string]any{"delete_column": ""}, check: "clickhouse-sink-delete-column"},
		{name: "same_columns", config: map[string]any{"version_column": "order", "delete_column": "order"}, check: "clickhouse-sink-order-columns"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			spec := clickHouseOrderPreflightSpec("mysql_cdc", nil, test.config)
			result := &PreflightResult{Passed: true}
			checkClickHouseSinkConfig(spec, result)
			if result.Passed || !preflightIssuesContain(result, test.check) {
				t.Fatalf("result = %#v, want %s", result, test.check)
			}
		})
	}
}

func clickHouseOrderPreflightSpec(sourceType string, sourceConfig, sinkConfig map[string]any) *pipeline.Spec {
	if sourceConfig == nil {
		sourceConfig = map[string]any{}
	}
	config := map[string]any{"host": "clickhouse", "database": "warehouse", "table": "orders"}
	for key, value := range sinkConfig {
		config[key] = value
	}
	return &pipeline.Spec{
		Name:   "clickhouse-order-preflight",
		Source: pipeline.SourceSpec{Type: sourceType, Config: sourceConfig},
		Sink:   pipeline.SinkSpec{Type: "clickhouse", Config: config},
	}
}
