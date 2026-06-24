package server

import "testing"

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
	assertFields(t, transforms, "window", "window_size_seconds", "allowed_lateness_seconds", "aggregates")
	assertFields(t, transforms, "join", "on_miss")
	assertFields(t, transforms, "javascript", "script", "code", "timeout_ms")
	assertFields(t, transforms, "js", "script", "code", "timeout_ms")
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
