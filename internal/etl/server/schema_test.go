package server

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPluginSchemaIncludesImplementedConfigFields(t *testing.T) {
	schema := configSchema()

	sources, ok := schema["sources"].(map[string][]ConfigField)
	if !ok {
		t.Fatalf("sources schema has type %T", schema["sources"])
	}
	sinks, ok := schema["sinks"].(map[string][]ConfigField)
	if !ok {
		t.Fatalf("sinks schema has type %T", schema["sinks"])
	}
	transforms, ok := schema["transforms"].(map[string][]ConfigField)
	if !ok {
		t.Fatalf("transforms schema has type %T", schema["transforms"])
	}

	assertFields(t, sources, "mysql_batch", "query", "cursor_column", "shard_index", "shard_total")
	assertFields(t, sources, "mysql_cdc", "server_id_base", "shard_index", "shard_total", "start_from")
	assertFields(t, sources, "mysql_snapshot_cdc", "tables", "consistent_snapshot_lock", "server_id_base")
	assertFields(t, sources, "http", "body", "auth_type", "auth_user", "auth_pass", "max_retries", "retry_base_ms")
	assertFields(t, sources, "kafka", "initial_offset", "sasl_user", "sasl_password", "tls")
	assertFields(t, sources, "redis", "pattern", "count")
	assertFields(t, sources, "demo", "interval_ms", "count", "fields")

	assertFields(t, sinks, "mysql", "auto_create", "schema_drift", "insert_chunk_size", "ddl_policy")
	assertFields(t, sinks, "postgres", "sslmode", "auto_create", "schema_drift", "insert_chunk_size", "ddl_policy")
	assertFields(t, sinks, "postgresql", "sslmode", "auto_create", "schema_drift", "insert_chunk_size", "ddl_policy")
	assertFields(t, sinks, "clickhouse", "source_dialect", "ddl_policy", "async_insert", "ttl")
	assertFields(t, sinks, "kafka", "auto_create_topic", "retry_backoff_ms")
	assertFields(t, sinks, "elasticsearch", "host", "chunk_size", "retry_base_ms")
	assertFields(t, sinks, "es", "host", "chunk_size", "retry_base_ms")
	assertFields(t, sinks, "doris", "allow_mixed_cdc_non_atomic", "ddl_policy")
	assertFields(t, sinks, "jdbc", "schema", "tls_ca_cert", "allow_unsupported_driver", "ddl_policy")
	assertFields(t, sinks, "maxcompute", "endpoint", "project", "table", "access_key_id", "access_key_secret", "columns", "partition", "partition_fields", "write_mode")
	assertFields(t, sinks, "odps", "endpoint", "project", "table", "access_key_id", "access_key_secret", "columns", "partition", "partition_fields", "write_mode")
	assertFields(t, sinks, "redis", "allow_non_idempotent_list", "pipeline_chunk_size")
	assertFields(t, sinks, "file_sink", "output_dir", "path")

	assertFields(t, transforms, "router", "field", "routes", "default")
	assertFields(t, transforms, "rate_limiter", "rps", "burst")
	assertFields(t, transforms, "enricher", "timeout_seconds", "cache_ttl_seconds", "on_error")
	assertFields(t, transforms, "lookup", "dsn", "query", "fields", "on_miss", "on_refresh_error", "state_backend", "state_ttl_seconds")
	assertFields(t, transforms, "normalize_envelope", "keep_metadata")
	assertFields(t, transforms, "debezium_envelope", "keep_metadata")
	assertFields(t, transforms, "project", "fields", "mappings", "constants", "time_formats", "keep_unmapped")
	assertFields(t, transforms, "select_fields", "fields", "mappings", "constants", "time_formats", "keep_unmapped")
	assertFields(t, transforms, "flat_map", "language", "script", "code", "on_error", "timeout_ms")
	assertFields(t, transforms, "udtf", "language", "script", "code", "on_error", "timeout_ms")
	assertFields(t, transforms, "debezium_cdc", "keep_metadata", "skip_tombstone", "target_table_template", "table_mapping")
	assertFields(t, transforms, "cdc_policy", "include_databases", "exclude_databases", "include_tables", "exclude_tables", "skip_delete", "skip_snapshot", "skip_tombstone", "dangerous_ddl", "ddl_allowlist", "ddl_denylist")
	assertFields(t, transforms, "ddl_guard", "include_databases", "exclude_databases", "include_tables", "exclude_tables", "skip_delete", "skip_snapshot", "skip_tombstone", "dangerous_ddl", "ddl_allowlist", "ddl_denylist")
	assertFields(t, transforms, "window", "window_size_seconds", "allowed_lateness_seconds", "aggregates", "state_backend", "state_ttl_seconds")
	assertFields(t, transforms, "deduplicate", "keys", "window_size", "state_backend", "state_ttl_seconds")
	assertFields(t, transforms, "join", "on_miss", "state_backend", "state_ttl_seconds")
	assertFields(t, transforms, "javascript", "script", "code", "timeout_ms")
	assertFields(t, transforms, "js", "script", "code", "timeout_ms")
}

func TestWindowSchemaOnlyExposesImplementedWindowTypes(t *testing.T) {
	schema := configSchema()
	transforms := schema["transforms"].(map[string][]ConfigField)

	fields := transforms["window"]
	for _, field := range fields {
		if field.Name != "window_type" {
			continue
		}
		if len(field.Enum) != 1 || field.Enum[0] != "tumbling" {
			t.Fatalf("window_type enum = %#v, want only tumbling", field.Enum)
		}
		return
	}
	t.Fatal("window schema missing window_type")
}

func TestPluginMetadataUsesEvidenceDrivenMaturity(t *testing.T) {
	metadata := pluginMetadata()
	for kind, rawGroup := range metadata {
		group, ok := rawGroup.(map[string]any)
		if !ok {
			t.Fatalf("metadata[%s] has type %T", kind, rawGroup)
		}
		for name, rawInfo := range group {
			info, ok := rawInfo.(map[string]any)
			if !ok {
				t.Fatalf("metadata[%s][%s] has type %T", kind, name, rawInfo)
			}
			maturity, _ := info["maturity"].(string)
			switch maturity {
			case "production", "beta", "experimental", "dev-only":
			default:
				t.Fatalf("metadata[%s][%s] maturity = %q, want production|beta|experimental|dev-only", kind, name, maturity)
			}
			if maturity == "stable" {
				t.Fatalf("metadata[%s][%s] still uses stable maturity", kind, name)
			}
		}
	}
}

func TestConnectorMaturityLevelsAreExplicit(t *testing.T) {
	want := []string{"production", "beta", "experimental", "dev-only"}
	if len(connectorMaturityLevels) != len(want) {
		t.Fatalf("connectorMaturityLevels = %#v, want %#v", connectorMaturityLevels, want)
	}
	for i := range want {
		if connectorMaturityLevels[i] != want[i] {
			t.Fatalf("connectorMaturityLevels = %#v, want %#v", connectorMaturityLevels, want)
		}
	}
	if got := normalizeConnectorMaturity("stable"); got != "experimental" {
		t.Fatalf("normalizeConnectorMaturity(stable) = %q, want experimental", got)
	}
}

func TestPublicDocsDoNotUseLegacyStableMaturity(t *testing.T) {
	for _, path := range []string{
		filepath.Join("..", "..", "..", "docs", "etl-api.md"),
		filepath.Join("..", "..", "..", "docs", "etl-api.zh.md"),
	} {
		body, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", path, err)
		}
		if strings.Contains(string(body), `"maturity": "stable"`) {
			t.Fatalf("%s still documents legacy maturity stable", path)
		}
	}
}

func TestConnectorDescriptorsMergeRegistrySchemaAndMetadata(t *testing.T) {
	descriptors := connectorDescriptors()
	if len(descriptors) == 0 {
		t.Fatal("connectorDescriptors returned no descriptors")
	}

	kafka := findDescriptor(descriptors, "source", "kafka")
	if kafka == nil {
		t.Fatal("missing kafka source descriptor")
	}
	if kafka.Version != "v1" || kafka.Maturity != "beta" || !kafka.Registered {
		t.Fatalf("unexpected kafka descriptor metadata: %#v", kafka)
	}
	if !contains(kafka.Required, "brokers") || !contains(kafka.Required, "topic") {
		t.Fatalf("kafka required fields = %#v", kafka.Required)
	}
	if !contains(kafka.SecretFields, "sasl_password") {
		t.Fatalf("kafka secret fields = %#v", kafka.SecretFields)
	}
	if !contains(kafka.SupportedSchedules, "streaming") || kafka.DefaultSchedule != "streaming" {
		t.Fatalf("kafka schedules = %#v default=%q, want streaming only", kafka.SupportedSchedules, kafka.DefaultSchedule)
	}

	mysqlBatch := findDescriptor(descriptors, "source", "mysql_batch")
	if mysqlBatch == nil {
		t.Fatal("missing mysql_batch source descriptor")
	}
	if !contains(mysqlBatch.Capabilities, "schema_descriptor") {
		t.Fatalf("mysql_batch capabilities = %#v, want schema_descriptor", mysqlBatch.Capabilities)
	}
	for _, schedule := range []string{"once", "cron", "periodic", "dependency"} {
		if !contains(mysqlBatch.SupportedSchedules, schedule) {
			t.Fatalf("mysql_batch supported_schedules = %#v, want %s", mysqlBatch.SupportedSchedules, schedule)
		}
	}
	if mysqlBatch.DefaultSchedule != "once" {
		t.Fatalf("mysql_batch default_schedule = %q, want once", mysqlBatch.DefaultSchedule)
	}
	mysqlCDC := findDescriptor(descriptors, "source", "mysql_cdc")
	if mysqlCDC == nil {
		t.Fatal("missing mysql_cdc source descriptor")
	}
	if !contains(mysqlCDC.Capabilities, "schema_descriptor_single_table") {
		t.Fatalf("mysql_cdc capabilities = %#v, want schema_descriptor_single_table", mysqlCDC.Capabilities)
	}
	if len(mysqlCDC.SupportedSchedules) != 1 || mysqlCDC.SupportedSchedules[0] != "streaming" || mysqlCDC.DefaultSchedule != "streaming" {
		t.Fatalf("mysql_cdc schedules = %#v default=%q, want streaming only", mysqlCDC.SupportedSchedules, mysqlCDC.DefaultSchedule)
	}

	clickhouse := findDescriptor(descriptors, "sink", "clickhouse")
	if clickhouse == nil {
		t.Fatal("missing clickhouse sink descriptor")
	}
	if !contains(clickhouse.Capabilities, "schema_drift") || !contains(clickhouse.Capabilities, "schema_validator") || clickhouse.Maturity != "beta" {
		t.Fatalf("unexpected clickhouse descriptor: %#v", clickhouse)
	}
	maxcompute := findDescriptor(descriptors, "sink", "maxcompute")
	if maxcompute == nil {
		t.Fatal("missing maxcompute sink descriptor")
	}
	if maxcompute.Maturity != "experimental" || !maxcompute.Registered || !contains(maxcompute.Capabilities, "partition_preflight") || !contains(maxcompute.SecretFields, "access_key_secret") {
		t.Fatalf("unexpected maxcompute descriptor: %#v", maxcompute)
	}
	odps := findDescriptor(descriptors, "sink", "odps")
	if odps == nil {
		t.Fatal("missing odps sink descriptor")
	}
	if odps.Maturity != "experimental" || !odps.Registered || !contains(odps.Capabilities, "schema_validator") {
		t.Fatalf("unexpected odps descriptor: %#v", odps)
	}

	normalize := findDescriptor(descriptors, "transform", "normalize_envelope")
	if normalize == nil {
		t.Fatal("missing normalize_envelope transform descriptor")
	}
	if normalize.Maturity != "beta" || len(normalize.Fields) == 0 {
		t.Fatalf("unexpected normalize descriptor: %#v", normalize)
	}
	project := findDescriptor(descriptors, "transform", "project")
	if project == nil {
		t.Fatal("missing project transform descriptor")
	}
	if project.Maturity != "beta" || !contains(project.Capabilities, "projection") || !contains(project.Capabilities, "time_format") || len(project.Fields) == 0 {
		t.Fatalf("unexpected project descriptor: %#v", project)
	}
	selectFields := findDescriptor(descriptors, "transform", "select_fields")
	if selectFields == nil {
		t.Fatal("missing select_fields transform descriptor")
	}
	if selectFields.Maturity != "beta" || !contains(selectFields.Capabilities, "schema_mapping") || len(selectFields.Fields) == 0 {
		t.Fatalf("unexpected select_fields descriptor: %#v", selectFields)
	}
	flatMap := findDescriptor(descriptors, "transform", "flat_map")
	if flatMap == nil {
		t.Fatal("missing flat_map transform descriptor")
	}
	if flatMap.Maturity != "beta" || !contains(flatMap.Capabilities, "one_to_many") || !contains(flatMap.Capabilities, "transform_metrics") || len(flatMap.Fields) == 0 {
		t.Fatalf("unexpected flat_map descriptor: %#v", flatMap)
	}
	udtf := findDescriptor(descriptors, "transform", "udtf")
	if udtf == nil {
		t.Fatal("missing udtf transform descriptor")
	}
	if udtf.Maturity != "beta" || !contains(udtf.Capabilities, "record_lineage") || len(udtf.Fields) == 0 {
		t.Fatalf("unexpected udtf descriptor: %#v", udtf)
	}
	tsTransform := findDescriptor(descriptors, "transform", "ts")
	if tsTransform == nil {
		t.Fatal("missing ts transform descriptor")
	}
	if tsTransform.Maturity != "experimental" || !contains(tsTransform.Capabilities, "one_to_many") || len(tsTransform.Fields) == 0 {
		t.Fatalf("unexpected ts descriptor: %#v", tsTransform)
	}
	jsTransform := findDescriptor(descriptors, "transform", "javascript")
	if jsTransform == nil {
		t.Fatal("missing javascript transform descriptor")
	}
	if jsTransform.Maturity != "experimental" || !contains(jsTransform.Capabilities, "one_to_many") || len(jsTransform.Fields) == 0 {
		t.Fatalf("unexpected javascript descriptor: %#v", jsTransform)
	}
	debeziumCDC := findDescriptor(descriptors, "transform", "debezium_cdc")
	if debeziumCDC == nil {
		t.Fatal("missing debezium_cdc transform descriptor")
	}
	if debeziumCDC.Maturity != "beta" || !contains(debeziumCDC.Capabilities, "table_mapping") || !contains(debeziumCDC.Capabilities, "cdc_metadata") || len(debeziumCDC.Fields) == 0 {
		t.Fatalf("unexpected debezium_cdc descriptor: %#v", debeziumCDC)
	}
	cdcPolicy := findDescriptor(descriptors, "transform", "cdc_policy")
	if cdcPolicy == nil {
		t.Fatal("missing cdc_policy transform descriptor")
	}
	if cdcPolicy.Maturity != "beta" || !contains(cdcPolicy.Capabilities, "ddl_guard") || !contains(cdcPolicy.Capabilities, "transform_metrics") || len(cdcPolicy.Fields) == 0 {
		t.Fatalf("unexpected cdc_policy descriptor: %#v", cdcPolicy)
	}
}

func TestConnectorDescriptorsEndpointIncludesMaturityLevels(t *testing.T) {
	_, ts := newTestHTTPServer(t)
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/api/v2/connectors/descriptors")
	if err != nil {
		t.Fatalf("GET descriptors: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", resp.StatusCode, http.StatusOK)
	}
	var body struct {
		Version        string                `json:"version"`
		MaturityLevels []string              `json:"maturity_levels"`
		Descriptors    []ConnectorDescriptor `json:"descriptors"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		t.Fatalf("decode descriptors response: %v", err)
	}
	if body.Version != "v1" || len(body.Descriptors) == 0 {
		t.Fatalf("unexpected descriptor response: %#v", body)
	}
	if len(body.MaturityLevels) != len(connectorMaturityLevels) {
		t.Fatalf("maturity_levels = %#v, want %#v", body.MaturityLevels, connectorMaturityLevels)
	}
	for i := range connectorMaturityLevels {
		if body.MaturityLevels[i] != connectorMaturityLevels[i] {
			t.Fatalf("maturity_levels = %#v, want %#v", body.MaturityLevels, connectorMaturityLevels)
		}
	}
}

func TestCompileWithExtismJSDisablesNpxFallbackByDefault(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "plugin.ts")
	if err := os.WriteFile(srcFile, []byte("export function transform() {}"), 0644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	t.Setenv("PATH", "")
	t.Setenv("OPENETL_ALLOW_NPX_PLUGIN_COMPILE", "")

	_, err := compileWithExtismJS(tmpDir, srcFile, "safe-plugin")
	if err == nil {
		t.Fatal("compileWithExtismJS() = nil error, want missing extism-js error")
	}
	if strings.Contains(err.Error(), "npx --yes") {
		t.Fatalf("compileWithExtismJS() error mentions npx fallback execution: %v", err)
	}
}

func TestCompileWithExtismJSRequiresExplicitNpxPackage(t *testing.T) {
	tmpDir := t.TempDir()
	srcFile := filepath.Join(tmpDir, "plugin.ts")
	if err := os.WriteFile(srcFile, []byte("export function transform() {}"), 0644); err != nil {
		t.Fatalf("write source: %v", err)
	}
	binDir := filepath.Join(tmpDir, "bin")
	if err := os.MkdirAll(binDir, 0755); err != nil {
		t.Fatalf("mkdir bin: %v", err)
	}
	if err := os.WriteFile(filepath.Join(binDir, "npx"), []byte("#!/bin/sh\nexit 99\n"), 0755); err != nil {
		t.Fatalf("write fake npx: %v", err)
	}
	t.Setenv("PATH", binDir)
	t.Setenv("OPENETL_ALLOW_NPX_PLUGIN_COMPILE", "true")
	t.Setenv("OPENETL_EXTISM_JS_PKG", "")

	_, err := compileWithExtismJS(tmpDir, srcFile, "safe-plugin")
	if err == nil {
		t.Fatal("compileWithExtismJS() = nil error, want missing package error")
	}
	if !strings.Contains(err.Error(), "OPENETL_EXTISM_JS_PKG") {
		t.Fatalf("compileWithExtismJS() error = %v, want explicit package guidance", err)
	}
}

func findDescriptor(items []ConnectorDescriptor, kind, typ string) *ConnectorDescriptor {
	for i := range items {
		if items[i].Kind == kind && items[i].Type == typ {
			return &items[i]
		}
	}
	return nil
}

func contains(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func assertFields(t *testing.T, schemas map[string][]ConfigField, plugin string, names ...string) {
	t.Helper()

	fields, ok := schemas[plugin]
	if !ok {
		t.Fatalf("schema for %q is missing", plugin)
	}

	seen := make(map[string]bool, len(fields))
	for _, field := range fields {
		seen[field.Name] = true
	}
	for _, name := range names {
		if !seen[name] {
			t.Fatalf("schema for %q missing field %q", plugin, name)
		}
	}
}
