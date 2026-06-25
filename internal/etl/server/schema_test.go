package server

import (
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
	assertFields(t, sinks, "redis", "allow_non_idempotent_list", "pipeline_chunk_size")
	assertFields(t, sinks, "file_sink", "output_dir", "path")

	assertFields(t, transforms, "router", "field", "routes", "default")
	assertFields(t, transforms, "rate_limiter", "rps", "burst")
	assertFields(t, transforms, "enricher", "timeout_seconds", "cache_ttl_seconds", "on_error")
	assertFields(t, transforms, "lookup", "dsn", "query", "fields", "on_miss", "on_refresh_error", "state_backend", "state_ttl_seconds")
	assertFields(t, transforms, "normalize_envelope", "keep_metadata")
	assertFields(t, transforms, "debezium_envelope", "keep_metadata")
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

	clickhouse := findDescriptor(descriptors, "sink", "clickhouse")
	if clickhouse == nil {
		t.Fatal("missing clickhouse sink descriptor")
	}
	if !contains(clickhouse.Capabilities, "schema_drift") || clickhouse.Maturity != "beta" {
		t.Fatalf("unexpected clickhouse descriptor: %#v", clickhouse)
	}

	normalize := findDescriptor(descriptors, "transform", "normalize_envelope")
	if normalize == nil {
		t.Fatal("missing normalize_envelope transform descriptor")
	}
	if normalize.Maturity != "beta" || len(normalize.Fields) == 0 {
		t.Fatalf("unexpected normalize descriptor: %#v", normalize)
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
