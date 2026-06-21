package server

type FieldType string

const (
	FieldString      FieldType = "string"
	FieldInt         FieldType = "int"
	FieldBool        FieldType = "bool"
	FieldFloat       FieldType = "float"
	FieldStringArray FieldType = "string_array"
	FieldMap         FieldType = "map"
)

type ConfigField struct {
	Name        string    `json:"name"`
	Type        FieldType `json:"type"`
	Required    bool      `json:"required"`
	Default     any       `json:"default,omitempty"`
	Description string    `json:"description"`
	Secret      bool      `json:"secret"`
	Example     any       `json:"example,omitempty"`
	Enum        []string  `json:"enum,omitempty"`
}

func configSchema() map[string]any {
	return map[string]any{
		"sources":    sourceConfigSchemas(),
		"sinks":      sinkConfigSchemas(),
		"transforms": transformConfigSchemas(),
	}
}

func sourceConfigSchemas() map[string][]ConfigField {
	return map[string][]ConfigField{
		"file": {
			{Name: "path", Type: FieldString, Required: true, Description: "File path inside the container", Example: "/app/data/customers.jsonl"},
			{Name: "format", Type: FieldString, Required: false, Default: "csv", Description: "File format: csv or json", Enum: []string{"csv", "json"}},
			{Name: "delimiter", Type: FieldString, Required: false, Default: ",", Description: "CSV delimiter"},
			{Name: "has_header", Type: FieldBool, Required: false, Default: true, Description: "Whether CSV first row contains column names"},
		},
		"http": {
			{Name: "url", Type: FieldString, Required: true, Description: "Base URL to fetch data from", Example: "http://api.example.com/items"},
			{Name: "method", Type: FieldString, Required: false, Default: "GET", Description: "HTTP method"},
			{Name: "headers", Type: FieldMap, Required: false, Description: "Request headers"},
			{Name: "pagination", Type: FieldString, Required: false, Description: "Pagination type", Enum: []string{"page"}},
			{Name: "page_param", Type: FieldString, Required: false, Description: "Query parameter for page number"},
			{Name: "size_param", Type: FieldString, Required: false, Description: "Query parameter for page size"},
			{Name: "page_size", Type: FieldInt, Required: false, Default: 100, Description: "Page size"},
			{Name: "max_pages", Type: FieldInt, Required: false, Default: 100, Description: "Maximum pages to read"},
			{Name: "result_key", Type: FieldString, Required: false, Description: "JSON key for result array (auto-detected if empty)"},
			{Name: "auth_token", Type: FieldString, Required: false, Description: "Bearer token for auth", Secret: true},
		},
		"mysql_batch": {
			{Name: "host", Type: FieldString, Required: true, Description: "MySQL host"},
			{Name: "port", Type: FieldInt, Required: false, Default: 3306, Description: "MySQL port"},
			{Name: "user", Type: FieldString, Required: true, Description: "MySQL user"},
			{Name: "password", Type: FieldString, Required: false, Description: "MySQL password", Secret: true},
			{Name: "database", Type: FieldString, Required: true, Description: "Source database name"},
			{Name: "table", Type: FieldString, Required: false, Description: "Source table name (required if no query)"},
			{Name: "query", Type: FieldString, Required: false, Description: "Custom SQL query (overrides table, supports JOIN). Pagination via OFFSET/LIMIT.", Example: "SELECT u.id, u.name, o.amount FROM users u JOIN orders o ON u.id = o.user_id"},
			{Name: "pk_column", Type: FieldString, Required: false, Default: "id", Description: "Primary key column for pagination"},
			{Name: "limit", Type: FieldInt, Required: false, Default: 5000, Description: "Rows per query page"},
			{Name: "columns", Type: FieldStringArray, Required: false, Description: "Specific columns to SELECT (default: *)"},
		},
		"mysql_cdc": {
			{Name: "host", Type: FieldString, Required: true, Description: "MySQL host"},
			{Name: "port", Type: FieldInt, Required: false, Default: 3306, Description: "MySQL port"},
			{Name: "user", Type: FieldString, Required: true, Description: "MySQL user"},
			{Name: "password", Type: FieldString, Required: false, Description: "MySQL password", Secret: true},
			{Name: "database", Type: FieldString, Required: true, Description: "Source database name"},
			{Name: "tables", Type: FieldStringArray, Required: true, Description: "Tables to watch for changes"},
			{Name: "server_id", Type: FieldInt, Required: false, Default: 1001, Description: "Unique replication server ID"},
			{Name: "enable_gtid", Type: FieldBool, Required: false, Default: false, Description: "Enable GTID-based replication for HA failover"},
		},
		"mysql_snapshot_cdc": {
			{Name: "host", Type: FieldString, Required: true, Description: "MySQL host"},
			{Name: "port", Type: FieldInt, Required: false, Default: 3306, Description: "MySQL port"},
			{Name: "user", Type: FieldString, Required: true, Description: "MySQL user"},
			{Name: "password", Type: FieldString, Required: false, Description: "MySQL password", Secret: true},
			{Name: "database", Type: FieldString, Required: true, Description: "Source database name"},
			{Name: "table", Type: FieldString, Required: true, Description: "Source table name"},
			{Name: "pk_column", Type: FieldString, Required: false, Default: "id", Description: "Primary key column for snapshot pagination"},
			{Name: "limit", Type: FieldInt, Required: false, Default: 1000, Description: "Rows per snapshot query page"},
			{Name: "server_id", Type: FieldInt, Required: false, Default: 1101, Description: "Unique replication server ID"},
		},
		"kafka": {
			{Name: "brokers", Type: FieldStringArray, Required: true, Description: "Kafka broker addresses", Example: []string{"localhost:9092"}},
			{Name: "topic", Type: FieldString, Required: true, Description: "Kafka topic to consume"},
			{Name: "group_id", Type: FieldString, Required: false, Default: "etl-consumer", Description: "Consumer group ID"},
			{Name: "format", Type: FieldString, Required: false, Default: "json", Description: "Message format", Enum: []string{"json", "text"}},
			{Name: "key_column", Type: FieldString, Required: false, Description: "Column name for message key"},
			{Name: "value_column", Type: FieldString, Required: false, Description: "Column name for raw message value"},
		},
		"postgres_cdc": {
			{Name: "host", Type: FieldString, Required: true, Description: "PostgreSQL host"},
			{Name: "port", Type: FieldInt, Required: false, Default: 5432, Description: "PostgreSQL port"},
			{Name: "user", Type: FieldString, Required: true, Description: "PostgreSQL user"},
			{Name: "password", Type: FieldString, Required: false, Description: "PostgreSQL password", Secret: true},
			{Name: "database", Type: FieldString, Required: true, Description: "Source database name"},
			{Name: "slot_name", Type: FieldString, Required: false, Default: "etl_slot", Description: "Logical replication slot name"},
			{Name: "tables", Type: FieldStringArray, Required: false, Description: "Tables to watch"},
			{Name: "sslmode", Type: FieldString, Required: false, Default: "prefer", Description: "SSL mode (disable/prefer/require/verify-full)"},
			{Name: "enable_snapshot", Type: FieldBool, Required: false, Default: false, Description: "Perform initial full-table snapshot before CDC"},
		},
	}
}

func sinkConfigSchemas() map[string][]ConfigField {
	return map[string][]ConfigField{
		"file_sink": {
			{Name: "output_dir", Type: FieldString, Required: true, Description: "Output directory path"},
			{Name: "format", Type: FieldString, Required: false, Default: "json", Description: "Output format", Enum: []string{"json", "jsonl", "csv", "parquet"}},
			{Name: "prefix", Type: FieldString, Required: false, Description: "File name prefix"},
			{Name: "max_retries", Type: FieldInt, Required: false, Default: 3, Description: "Retry attempts for S3-compatible upload mode"},
			{Name: "retry_base_ms", Type: FieldInt, Required: false, Default: 500, Description: "Base retry delay in milliseconds"},
		},
		"s3": {
			{Name: "endpoint", Type: FieldString, Required: false, Description: "S3-compatible endpoint URL (e.g., MinIO)"},
			{Name: "region", Type: FieldString, Required: false, Description: "S3 region"},
			{Name: "bucket", Type: FieldString, Required: true, Description: "S3 bucket name"},
			{Name: "access_key", Type: FieldString, Required: false, Description: "Access key", Secret: true},
			{Name: "secret_key", Type: FieldString, Required: false, Description: "Secret key", Secret: true},
			{Name: "output_dir", Type: FieldString, Required: false, Default: "/tmp/etl-output", Description: "Local fallback directory"},
			{Name: "format", Type: FieldString, Required: false, Default: "json", Description: "Output format", Enum: []string{"json", "jsonl", "csv", "parquet"}},
			{Name: "prefix", Type: FieldString, Required: false, Description: "Object key prefix"},
			{Name: "max_retries", Type: FieldInt, Required: false, Default: 3, Description: "Retry attempts for S3 upload"},
			{Name: "retry_base_ms", Type: FieldInt, Required: false, Default: 500, Description: "Base retry delay in milliseconds"},
		},
		"mysql": {
			{Name: "host", Type: FieldString, Required: true, Description: "MySQL host"},
			{Name: "port", Type: FieldInt, Required: false, Default: 3306, Description: "MySQL port"},
			{Name: "user", Type: FieldString, Required: true, Description: "MySQL user"},
			{Name: "password", Type: FieldString, Required: false, Description: "MySQL password", Secret: true},
			{Name: "database", Type: FieldString, Required: true, Description: "Target database name"},
			{Name: "table", Type: FieldString, Required: true, Description: "Target table name"},
			{Name: "batch_mode", Type: FieldString, Required: false, Default: "insert", Description: "Write mode", Enum: []string{"insert", "upsert"}},
			{Name: "pk_columns", Type: FieldStringArray, Required: false, Description: "Primary key columns for upsert mode"},
			{Name: "tls", Type: FieldBool, Required: false, Default: false, Description: "Enable TLS for MySQL connection"},
			{Name: "tls_skip_verify", Type: FieldBool, Required: false, Default: false, Description: "Skip TLS certificate verification"},
		},
		"clickhouse": {
			{Name: "host", Type: FieldString, Required: true, Description: "ClickHouse host"},
			{Name: "port", Type: FieldInt, Required: false, Default: 9000, Description: "ClickHouse port (9000=native, 8123=http)"},
			{Name: "protocol", Type: FieldString, Required: false, Default: "native", Description: "Connection protocol", Enum: []string{"native", "http"}},
			{Name: "user", Type: FieldString, Required: false, Default: "default", Description: "ClickHouse user"},
			{Name: "password", Type: FieldString, Required: false, Description: "ClickHouse password", Secret: true},
			{Name: "database", Type: FieldString, Required: true, Description: "Target database name"},
			{Name: "table", Type: FieldString, Required: false, Description: "Target table name (empty = use source table name dynamically)"},
			{Name: "pk_columns", Type: FieldStringArray, Required: false, Description: "Primary key columns for ORDER BY, DELETE, and UPDATE conditions"},
			{Name: "version_column", Type: FieldString, Required: false, Default: "_version", Description: "Version column for ReplacingMergeTree deduplication"},
			{Name: "auto_create", Type: FieldBool, Required: false, Default: false, Description: "Auto-create target table if missing (ReplacingMergeTree engine)"},
			{Name: "schema_drift", Type: FieldString, Required: false, Default: "ignore", Description: "Schema drift handling mode", Enum: []string{"ignore", "fail", "add_columns", "sync"}},
			{Name: "optimize_interval_sec", Type: FieldInt, Required: false, Default: 0, Description: "Periodic OPTIMIZE TABLE FINAL interval (0 = disabled)"},
			{Name: "use_final", Type: FieldBool, Required: false, Default: false, Description: "Append FINAL to internal queries for deduplicated reads"},
			{Name: "tls", Type: FieldBool, Required: false, Default: false, Description: "Enable TLS for connection (required by ClickHouse Cloud)"},
			{Name: "tls_skip_verify", Type: FieldBool, Required: false, Default: false, Description: "Skip TLS certificate verification"},
			{Name: "compression", Type: FieldString, Required: false, Default: "LZ4", Description: "Compression method", Enum: []string{"LZ4", "ZSTD"}},
			{Name: "async_insert", Type: FieldBool, Required: false, Default: false, Description: "Enable ClickHouse async_insert for lower-latency writes"},
			{Name: "async_insert_wait", Type: FieldBool, Required: false, Default: true, Description: "Wait for async insert to complete before returning"},
			{Name: "ttl", Type: FieldString, Required: false, Description: "TTL expression for auto-created tables (e.g. 'toDateTime(created_at) + INTERVAL 30 DAY')"},
		},
		"postgres": {
			{Name: "host", Type: FieldString, Required: true, Description: "PostgreSQL host"},
			{Name: "port", Type: FieldInt, Required: false, Default: 5432, Description: "PostgreSQL port"},
			{Name: "user", Type: FieldString, Required: true, Description: "PostgreSQL user"},
			{Name: "password", Type: FieldString, Required: false, Description: "PostgreSQL password", Secret: true},
			{Name: "database", Type: FieldString, Required: true, Description: "Target database name"},
			{Name: "schema", Type: FieldString, Required: false, Default: "public", Description: "Target schema name"},
			{Name: "table", Type: FieldString, Required: true, Description: "Target table name"},
			{Name: "batch_mode", Type: FieldString, Required: false, Default: "insert", Description: "Write mode", Enum: []string{"insert", "upsert"}},
			{Name: "pk_columns", Type: FieldStringArray, Required: false, Description: "Primary key columns for upsert mode"},
		},
		"kafka": {
			{Name: "brokers", Type: FieldStringArray, Required: true, Description: "Kafka broker addresses"},
			{Name: "topic", Type: FieldString, Required: true, Description: "Kafka topic to produce to"},
			{Name: "key_column", Type: FieldString, Required: false, Description: "Column for message key"},
			{Name: "compression", Type: FieldString, Required: false, Default: "none", Description: "Compression codec", Enum: []string{"none", "gzip", "snappy", "lz4", "zstd"}},
			{Name: "sasl_user", Type: FieldString, Required: false, Description: "SASL username"},
			{Name: "sasl_password", Type: FieldString, Required: false, Description: "SASL password", Secret: true},
			{Name: "sasl_mechanism", Type: FieldString, Required: false, Description: "SASL mechanism", Enum: []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"}},
			{Name: "tls", Type: FieldBool, Required: false, Default: false, Description: "Enable TLS for broker connection"},
			{Name: "tls_skip_verify", Type: FieldBool, Required: false, Default: false, Description: "Skip TLS certificate verification"},
		},
		"elasticsearch": {
			{Name: "hosts", Type: FieldStringArray, Required: true, Description: "Elasticsearch host URLs", Example: []string{"http://localhost:9200"}},
			{Name: "username", Type: FieldString, Required: false, Description: "ES username", Secret: true},
			{Name: "password", Type: FieldString, Required: false, Description: "ES password", Secret: true},
			{Name: "index", Type: FieldString, Required: true, Description: "Target index name"},
			{Name: "id_column", Type: FieldString, Required: false, Default: "id", Description: "Column for document ID (enables upsert)"},
			{Name: "chunk_size", Type: FieldInt, Required: false, Default: 500, Description: "Records per bulk request"},
			{Name: "max_retries", Type: FieldInt, Required: false, Default: 3, Description: "Retry attempts for bulk writes"},
			{Name: "retry_base_ms", Type: FieldInt, Required: false, Default: 500, Description: "Base retry delay in milliseconds"},
			{Name: "tls_skip_verify", Type: FieldBool, Required: false, Default: false, Description: "Skip TLS certificate verification"},
		},
		"doris": {
			{Name: "host", Type: FieldString, Required: true, Description: "Doris FE host"},
			{Name: "port", Type: FieldInt, Required: false, Default: 9030, Description: "Doris MySQL protocol port"},
			{Name: "http_port", Type: FieldInt, Required: false, Default: 8030, Description: "Doris Stream Load HTTP port"},
			{Name: "user", Type: FieldString, Required: false, Default: "root", Description: "Doris user"},
			{Name: "password", Type: FieldString, Required: false, Description: "Doris password", Secret: true},
			{Name: "database", Type: FieldString, Required: true, Description: "Target database name"},
			{Name: "table", Type: FieldString, Required: true, Description: "Target table name"},
			{Name: "write_mode", Type: FieldString, Required: false, Default: "stream_load", Description: "Write method", Enum: []string{"stream_load", "insert"}},
			{Name: "batch_mode", Type: FieldString, Required: false, Default: "insert", Description: "Write mode", Enum: []string{"insert", "upsert"}},
			{Name: "pk_columns", Type: FieldStringArray, Required: false, Description: "Key columns for DELETE and auto-create UNIQUE KEY model"},
			{Name: "stream_load_format", Type: FieldString, Required: false, Default: "json", Description: "Stream Load payload format", Enum: []string{"json", "csv"}},
			{Name: "stream_load_scheme", Type: FieldString, Required: false, Default: "http", Description: "Stream Load scheme", Enum: []string{"http", "https"}},
			{Name: "https", Type: FieldBool, Required: false, Default: false, Description: "Shortcut to use HTTPS for Stream Load"},
			{Name: "tls_skip_verify", Type: FieldBool, Required: false, Default: false, Description: "Skip TLS certificate verification"},
			{Name: "stream_load_timeout_sec", Type: FieldInt, Required: false, Default: 30, Description: "Stream Load HTTP timeout in seconds"},
			{Name: "insert_chunk_size", Type: FieldInt, Required: false, Default: 500, Description: "Rows per INSERT statement"},
			{Name: "auto_create", Type: FieldBool, Required: false, Default: false, Description: "Auto-create target table if missing"},
			{Name: "schema_drift", Type: FieldString, Required: false, Default: "ignore", Description: "Schema drift handling", Enum: []string{"ignore", "fail", "add_columns"}},
		},
		"jdbc": {
			{Name: "dsn", Type: FieldString, Required: true, Description: "JDBC connection string (e.g. mysql://user:pass@tcp(host:3306)/db)", Example: "mysql://user:pass@tcp(localhost:3306)/mydb"},
			{Name: "driver", Type: FieldString, Required: false, Description: "Database driver name (auto-detected from DSN if empty)"},
			{Name: "table", Type: FieldString, Required: true, Description: "Target table name"},
			{Name: "batch_mode", Type: FieldString, Required: false, Default: "insert", Description: "Write mode", Enum: []string{"insert", "upsert"}},
			{Name: "pk_columns", Type: FieldStringArray, Required: false, Description: "Primary key columns for upsert mode"},
			{Name: "insert_chunk_size", Type: FieldInt, Required: false, Default: 500, Description: "Rows per INSERT statement"},
			{Name: "auto_create", Type: FieldBool, Required: false, Default: false, Description: "Auto-create target table if missing"},
			{Name: "schema_drift", Type: FieldString, Required: false, Default: "ignore", Description: "Schema drift handling", Enum: []string{"ignore", "fail", "add_columns"}},
		},
		"redis": {
			{Name: "host", Type: FieldString, Required: false, Default: "localhost", Description: "Redis host"},
			{Name: "port", Type: FieldInt, Required: false, Default: 6379, Description: "Redis port"},
			{Name: "password", Type: FieldString, Required: false, Description: "Redis password", Secret: true},
			{Name: "db", Type: FieldInt, Required: false, Default: 0, Description: "Redis database index"},
			{Name: "key_field", Type: FieldString, Required: false, Default: "id", Description: "Record field used as Redis key"},
			{Name: "key_prefix", Type: FieldString, Required: false, Description: "Prefix for Redis keys"},
			{Name: "data_type", Type: FieldString, Required: false, Default: "hash", Description: "Redis data structure", Enum: []string{"hash", "string", "list"}},
			{Name: "ttl_seconds", Type: FieldInt, Required: false, Default: 0, Description: "Key TTL in seconds (0 = no expiry)"},
			{Name: "value_field", Type: FieldString, Required: false, Description: "Field to store for string/list modes"},
			{Name: "tls", Type: FieldBool, Required: false, Default: false, Description: "Enable TLS for Redis connection"},
			{Name: "tls_skip_verify", Type: FieldBool, Required: false, Default: false, Description: "Skip TLS certificate verification"},
			{Name: "max_retries", Type: FieldInt, Required: false, Default: 3, Description: "Redis client retry count"},
		},
	}
}

func transformConfigSchemas() map[string][]ConfigField {
	return map[string][]ConfigField{
		"identity": {},
		"rename": {
			{Name: "mappings", Type: FieldMap, Required: true, Description: "Map of old_name → new_name", Example: map[string]string{"old_name": "new_name"}},
		},
		"drop_field": {
			{Name: "fields", Type: FieldStringArray, Required: true, Description: "Field names to remove", Example: []string{"password_hash"}},
		},
		"add_field": {
			{Name: "field", Type: FieldString, Required: true, Description: "Field name to add"},
			{Name: "value", Type: FieldString, Required: true, Description: "Field value (supports {{now}}, {{ts}})"},
		},
		"type_convert": {
			{Name: "conversions", Type: FieldMap, Required: true, Description: "Map of field → target type", Example: map[string]string{"id": "int", "amount": "float"}},
		},
		"filter": {
			{Name: "expression", Type: FieldString, Required: true, Description: "Filter expression"},
		},
		"lua": {
			{Name: "script", Type: FieldString, Required: true, Description: "Lua script code"},
		},
		"ts": {
			{Name: "script", Type: FieldString, Required: true, Description: "TypeScript/JavaScript function, e.g: transform(record) { record.data.x = 1; return record; }"},
		},
		// ── Advanced DAG node transforms ──
		"router": {
			{Name: "rules", Type: FieldStringArray, Required: false, Description: "Routing rules as JSON: [{field, operator, value, route}]"},
		},
		"fanout": {}, // no config — pure 1-to-N broadcast marker
		"tap": {
			{Name: "log_every", Type: FieldInt, Required: false, Default: 100, Description: "Log every N records"},
			{Name: "alert_on_lag_ms", Type: FieldInt, Required: false, Default: 0, Description: "Alert if processing latency exceeds N ms (0=off)"},
		},
		"rate_limiter": {
			{Name: "rate", Type: FieldInt, Required: true, Default: 100, Description: "Max records per second (token bucket rate)"},
			{Name: "burst", Type: FieldInt, Required: false, Default: 100, Description: "Burst capacity (bucket size)"},
		},
		"enricher": {
			{Name: "mode", Type: FieldString, Required: true, Default: "http", Description: "Enrichment mode", Enum: []string{"http", "sql"}},
			{Name: "url", Type: FieldString, Required: false, Description: "HTTP API URL (mode=http). Use {{field}} for template substitution", Example: "http://user-svc/api/users/{{user_id}}"},
			{Name: "dsn", Type: FieldString, Required: false, Description: "Database DSN (mode=sql)", Example: "user:pass@tcp(host:3306)/db"},
			{Name: "query", Type: FieldString, Required: false, Description: "SQL query (mode=sql). Use ? as placeholder."},
			{Name: "params", Type: FieldStringArray, Required: false, Description: "Field names for SQL query params (mode=sql)"},
			{Name: "target_field", Type: FieldString, Required: false, Default: "enriched", Description: "Field name to store enrichment result"},
			{Name: "cache_ttl_sec", Type: FieldInt, Required: false, Default: 300, Description: "Cache TTL in seconds (0=off)"},
		},
		"lookup": {
			{Name: "dsn", Type: FieldString, Required: true, Description: "DSN for dimension database", Example: "user:pass@tcp(host:3306)/db"},
			{Name: "query", Type: FieldString, Required: true, Description: "SQL query to load dimension table", Example: "SELECT id, name FROM dim_customers"},
			{Name: "join_key", Type: FieldString, Required: false, Default: "id", Description: "Field in source record to join on"},
			{Name: "dim_key", Type: FieldString, Required: false, Default: "id", Description: "Column in dimension table to match"},
			{Name: "fields", Type: FieldStringArray, Required: true, Description: "Dimension columns to copy into the record", Example: []string{"name", "tier"}},
			{Name: "refresh_interval_sec", Type: FieldInt, Required: false, Default: 300, Description: "Refresh dimension table every N seconds (0=load once)"},
		},
		"window": {
			{Name: "window_sec", Type: FieldInt, Required: true, Default: 60, Description: "Tumbling window size in seconds"},
			{Name: "group_by", Type: FieldStringArray, Required: false, Description: "Group-by fields"},
			{Name: "aggregates", Type: FieldString, Required: true, Description: "Aggregations as JSON: {field: func}. funcs: count, sum, avg, min, max", Example: "{\"amount\": \"sum\", \"*\": \"count\"}"},
		},
		"deduplicate": {
			{Name: "keys", Type: FieldStringArray, Required: true, Description: "Fields forming the dedup key", Example: []string{"order_id"}},
			{Name: "window_size", Type: FieldInt, Required: false, Default: 10000, Description: "Dedup window size (max records to remember)"},
		},
		"validate": {
			{Name: "required_fields", Type: FieldStringArray, Required: false, Description: "Fields that must be present and non-null"},
			{Name: "rules", Type: FieldString, Required: false, Description: "Validation rules as JSON array: [{field, type, value}]", Example: "[{\"field\":\"email\",\"type\":\"regex\",\"value\":\"@\"}]"},
			{Name: "on_failure", Type: FieldString, Required: false, Default: "dlq", Description: "Action on validation failure", Enum: []string{"dlq", "drop", "error"}},
			{Name: "fail_immediate", Type: FieldBool, Required: false, Default: false, Description: "Stop on first failure instead of collecting all"},
		},
		"join": {
			{Name: "join_type", Type: FieldString, Required: false, Default: "inner", Description: "Join type", Enum: []string{"inner", "left", "right"}},
			{Name: "join_key", Type: FieldString, Required: true, Description: "Field name to join on"},
			{Name: "join_window_sec", Type: FieldInt, Required: false, Default: 60, Description: "How long to keep records in join state (seconds)"},
			{Name: "join_fields", Type: FieldStringArray, Required: true, Description: "Fields to copy from matched record"},
			{Name: "join_prefix", Type: FieldString, Required: false, Default: "joined_", Description: "Prefix for joined fields"},
			{Name: "where", Type: FieldString, Required: false, Description: "Optional filter expression for join side"},
		},
	}
}
