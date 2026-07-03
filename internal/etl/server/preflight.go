package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/sha512"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/go-sql-driver/mysql"
	"github.com/gogf/gf/v2/frame/g"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	etlsink "github.com/a8851625/openetl-go/internal/etl/sink"
)

// PreflightIssue describes a single check failure with remediation.
type PreflightIssue struct {
	Level       string `json:"level"`       // "error" or "warning"
	Check       string `json:"check"`       // which check produced this
	Message     string `json:"message"`     // what went wrong
	Remediation string `json:"remediation"` // how to fix it
}

// PreflightFieldIssue describes a schema-level problem tied to one field.
type PreflightFieldIssue struct {
	Level       string `json:"level"`
	Field       string `json:"field"`
	Check       string `json:"check"`
	SourceType  string `json:"source_type,omitempty"`
	TargetType  string `json:"target_type,omitempty"`
	Message     string `json:"message"`
	Remediation string `json:"remediation"`
}

// PreflightDDLPreview is an informational target DDL preview built from source
// schema metadata. It is not executed by preflight.
type PreflightDDLPreview struct {
	Dialect    string   `json:"dialect"`
	Table      string   `json:"table"`
	Statements []string `json:"statements"`
	Warnings   []string `json:"warnings,omitempty"`
}

// PreflightGuidance is non-blocking operational guidance for first-run
// readiness. It explains replay, DLQ, and schema boundaries that are not
// necessarily misconfigurations.
type PreflightGuidance struct {
	Level    string `json:"level"`    // "info" or "warning"
	Category string `json:"category"` // "delivery", "dlq", "schema", ...
	Code     string `json:"code"`
	Message  string `json:"message"`
	Action   string `json:"action,omitempty"`
}

// PreflightConnectorReadiness carries source/sink readiness gates into
// validate/preflight responses so first-run users see connector evidence and
// gaps before starting a pipeline.
type PreflightConnectorReadiness struct {
	Kind     string                   `json:"kind"`
	Type     string                   `json:"type"`
	Maturity string                   `json:"maturity"`
	Status   string                   `json:"status"`
	Summary  string                   `json:"summary"`
	Gates    []ConnectorReadinessGate `json:"gates,omitempty"`
}

// PreflightRecommendation is an optional, operator-reviewed config patch that
// can make a first pipeline safer to start.
type PreflightRecommendation struct {
	Path   string `json:"path"`
	Value  any    `json:"value"`
	Reason string `json:"reason"`
	Safety string `json:"safety,omitempty"` // safe or review
}

// PreflightResult is the outcome of running all preflight checks.
type PreflightResult struct {
	Passed          bool                          `json:"passed"`
	Issues          []PreflightIssue              `json:"issues,omitempty"`
	FieldIssues     []PreflightFieldIssue         `json:"field_issues,omitempty"`
	DDLPreview      *PreflightDDLPreview          `json:"ddl_preview,omitempty"`
	Guidance        []PreflightGuidance           `json:"guidance,omitempty"`
	Readiness       []PreflightConnectorReadiness `json:"readiness,omitempty"`
	Recommendations []PreflightRecommendation     `json:"recommendations,omitempty"`
	Summary         string                        `json:"summary"`
}

// RunPreflight validates a pipeline spec's source and sink connectivity
// before starting. It checks:
//   - MySQL binlog format and permissions
//   - ClickHouse / target reachability
//   - Source/sink schema compatibility when both connectors support it
//   - Source table existence (best-effort)
//   - Common misconfiguration patterns
//
// Returns nil if all checks pass. Errors are returned as PreflightIssue
// entries (never fatal — partial checks are allowed).
func (s *Server) RunPreflight(ctx context.Context, spec *pipeline.Spec) *PreflightResult {
	result := &PreflightResult{Passed: true}

	s.checkRuntimeStateRequirements(spec, result)
	s.checkTransformConfigRequirements(spec, result)

	// Source checks
	if s.checkStaticSourceConfig(spec, result) {
		s.checkMySQLCDC(ctx, spec, result)
		s.checkMySQLBatchSource(ctx, spec, result)
		s.checkPostgresCDCSource(ctx, spec, result)
		s.checkFileSource(ctx, spec, result)
		s.checkHTTPSource(ctx, spec, result)
		s.checkKafkaSource(ctx, spec, result)
	}

	// Sink checks
	s.checkStaticSinkConfig(spec, result)
	s.checkKafkaSink(ctx, spec, result)
	if spec == nil || strings.ToLower(spec.Sink.Type) != "kafka" {
		if sink, ok := s.checkSinkReachable(ctx, spec, result); ok {
			defer func() { _ = sink.Close() }()
			s.checkSchemaCompatibility(ctx, spec, sink, result)
		}
	}

	addConnectorReadiness(spec, result)
	addOperationalGuidance(spec, result)
	addFirstRunRecommendations(spec, result)

	if len(result.Issues) == 0 {
		result.Summary = "all checks passed"
	} else if !result.Passed {
		result.Summary = fmt.Sprintf("%d issue(s) found", len(result.Issues))
	} else {
		result.Summary = fmt.Sprintf("%d warning(s) found", len(result.Issues))
	}
	return result
}

func (s *Server) checkRuntimeStateRequirements(spec *pipeline.Spec, result *PreflightResult) {
	for _, problem := range pipeline.ValidateRuntimeStateRequirements(spec) {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "redis-state-cache",
			Message:     problem,
			Remediation: "configure etl.state.redis.addr / ETL_STATE_REDIS_ADDR for runtime state/cache, or remove state/cache fields; do not use SQLite/MySQL/PostgreSQL as cache backends",
		})
		result.Passed = false
	}
}

func (s *Server) checkTransformConfigRequirements(spec *pipeline.Spec, result *PreflightResult) {
	for _, problem := range pipeline.ValidateTransformConfigRequirements(spec) {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "transform-config",
			Message:     problem,
			Remediation: "fix the transform config before starting; async lookup/enricher I/O controls must have safe positive bounds and Redis-backed caches must configure Redis",
		})
		if field := transformProblemField(problem); field != "" {
			result.FieldIssues = append(result.FieldIssues, PreflightFieldIssue{
				Level:       "error",
				Field:       field,
				Check:       "transform-config",
				Message:     problem,
				Remediation: "fix this transform field before starting the pipeline",
			})
		}
		result.Passed = false
	}
}

func transformProblemField(problem string) string {
	idx := strings.Index(problem, ".")
	if !strings.HasPrefix(problem, "transforms[") || idx < 0 {
		return ""
	}
	field := problem[:idx]
	rest := problem[idx+1:]
	for i, r := range rest {
		if r == ' ' || r == '=' {
			return field + ".config." + rest[:i]
		}
	}
	if rest == "" {
		return ""
	}
	return field + ".config." + rest
}

// ── Source: static config checks ─────────────────────────────────────

func (s *Server) checkStaticSourceConfig(spec *pipeline.Spec, result *PreflightResult) bool {
	if spec == nil || result == nil {
		return true
	}
	before := len(result.Issues)
	switch strings.ToLower(spec.Source.Type) {
	case "mysql_cdc":
		checkMySQLCDCSourceConfig(spec, result)
	case "mysql_snapshot_cdc":
		checkMySQLSnapshotCDCSourceConfig(spec, result)
	case "postgres_cdc":
		checkPostgresCDCSourceConfig(spec, result)
	}
	return len(result.Issues) == before
}

func checkMySQLCDCSourceConfig(spec *pipeline.Spec, result *PreflightResult) {
	cfg := spec.Source.Config
	for _, field := range []string{"host", "user", "database"} {
		if strings.TrimSpace(stringField(cfg, field, "")) == "" {
			addStaticFieldError(result, "mysql-cdc-source-required-config", "source.config."+field,
				fmt.Sprintf("mysql_cdc source requires source.config.%s", field),
				fmt.Sprintf("set source.config.%s before starting the CDC pipeline", field))
		}
	}
	if len(trimmedStringSlice(stringSliceField(cfg, "tables"))) == 0 {
		addStaticFieldError(result, "mysql-cdc-source-tables", "source.config.tables",
			"mysql_cdc source requires source.config.tables",
			"set source.config.tables to one or more MySQL tables to watch")
	}
	checkMySQLCDCCommonStaticConfig("mysql-cdc", cfg, result)
	if startFrom := strings.TrimSpace(stringField(cfg, "start_from", "")); startFrom != "" && !isSupportedMySQLCDCStartFrom(startFrom) {
		addStaticFieldError(result, "mysql-cdc-source-start-from", "source.config.start_from",
			fmt.Sprintf("mysql_cdc source start_from %q is invalid", startFrom),
			"set start_from to timestamp, binlog:<file>:<pos>, or gtid:<set>")
	}
}

func checkMySQLSnapshotCDCSourceConfig(spec *pipeline.Spec, result *PreflightResult) {
	cfg := spec.Source.Config
	for _, field := range []string{"host", "user", "database"} {
		if strings.TrimSpace(stringField(cfg, field, "")) == "" {
			addStaticFieldError(result, "mysql-snapshot-cdc-source-required-config", "source.config."+field,
				fmt.Sprintf("mysql_snapshot_cdc source requires source.config.%s", field),
				fmt.Sprintf("set source.config.%s before starting the snapshot+CDC pipeline", field))
		}
	}
	if strings.TrimSpace(stringField(cfg, "table", "")) == "" && len(trimmedStringSlice(stringSliceField(cfg, "tables"))) == 0 {
		addStaticFieldError(result, "mysql-snapshot-cdc-source-tables", "source.config.tables",
			"mysql_snapshot_cdc source requires source.config.table or source.config.tables",
			"set source.config.table for one table or source.config.tables for multi-table snapshot+CDC")
	}
	if strings.TrimSpace(stringField(cfg, "pk_column", "id")) == "" {
		addStaticFieldError(result, "mysql-snapshot-cdc-source-pk-column", "source.config.pk_column",
			"mysql_snapshot_cdc source pk_column cannot be empty",
			"remove source.config.pk_column to use id, or set a non-empty primary key/cursor column")
	}
	if limit := intField(cfg, "limit", 1000); limit <= 0 {
		addStaticFieldError(result, "mysql-snapshot-cdc-source-limit", "source.config.limit",
			fmt.Sprintf("mysql_snapshot_cdc source limit must be > 0, got %d", limit),
			"set source.config.limit to a positive snapshot page size")
	}
	checkMySQLCDCCommonStaticConfig("mysql-snapshot-cdc", cfg, result)
}

func checkMySQLCDCCommonStaticConfig(prefix string, cfg map[string]any, result *PreflightResult) {
	if port := intField(cfg, "port", 3306); port <= 0 || port > 65535 {
		addStaticFieldError(result, prefix+"-source-port", "source.config.port",
			fmt.Sprintf("MySQL CDC source port must be between 1 and 65535, got %d", port),
			"set source.config.port to the MySQL listener port, usually 3306")
	}
	serverID := intField(cfg, "server_id", 0)
	if _, ok := cfg["server_id"]; ok && (serverID <= 0 || serverID > 4294967295) {
		addStaticFieldError(result, prefix+"-source-server-id", "source.config.server_id",
			fmt.Sprintf("MySQL CDC source server_id must be between 1 and 4294967295, got %d", serverID),
			"remove source.config.server_id to auto-derive one, or set a unique positive replication server ID")
	}
	serverIDBase := intField(cfg, "server_id_base", 0)
	if _, ok := cfg["server_id_base"]; ok && serverIDBase <= 0 {
		addStaticFieldError(result, prefix+"-source-server-id-base", "source.config.server_id_base",
			fmt.Sprintf("MySQL CDC source server_id_base must be > 0, got %d", serverIDBase),
			"set source.config.server_id_base to a positive base ID when using shard_total")
	}
	if shardTotal := intField(cfg, "shard_total", 0); shardTotal > 0 {
		shardIndex := intField(cfg, "shard_index", 0)
		if shardTotal < 2 || shardIndex < 0 || shardIndex >= shardTotal {
			addStaticFieldError(result, prefix+"-source-shard", "source.config.shard_index",
				fmt.Sprintf("MySQL CDC source shard settings are invalid: shard_index=%d shard_total=%d", shardIndex, shardTotal),
				"set shard_total >= 2 and shard_index between 0 and shard_total-1, or remove both fields")
		}
	}
}

func isSupportedMySQLCDCStartFrom(startFrom string) bool {
	startFrom = strings.TrimSpace(startFrom)
	if startFrom == "" || startFrom == "timestamp" {
		return true
	}
	if strings.HasPrefix(startFrom, "gtid:") && strings.TrimSpace(strings.TrimPrefix(startFrom, "gtid:")) != "" {
		return true
	}
	if strings.HasPrefix(startFrom, "binlog:") {
		parts := strings.Split(startFrom, ":")
		return len(parts) == 3 && strings.TrimSpace(parts[1]) != "" && strings.TrimSpace(parts[2]) != ""
	}
	return false
}

func trimmedStringSlice(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}

func checkPostgresCDCSourceConfig(spec *pipeline.Spec, result *PreflightResult) {
	cfg := spec.Source.Config
	for _, field := range []string{"host", "user", "database"} {
		if strings.TrimSpace(stringField(cfg, field, "")) == "" {
			addStaticFieldError(result, "postgres-cdc-source-required-config", "source.config."+field,
				fmt.Sprintf("postgres_cdc source requires source.config.%s", field),
				fmt.Sprintf("set source.config.%s before starting the PostgreSQL CDC pipeline", field))
		}
	}
	if port := intField(cfg, "port", 5432); port <= 0 || port > 65535 {
		addStaticFieldError(result, "postgres-cdc-source-port", "source.config.port",
			fmt.Sprintf("postgres_cdc source port must be between 1 and 65535, got %d", port),
			"set source.config.port to the PostgreSQL listener port, usually 5432")
	}
	slotName := strings.TrimSpace(stringField(cfg, "slot_name", "etl_slot"))
	if slotName == "" {
		addStaticFieldError(result, "postgres-cdc-source-slot-name", "source.config.slot_name",
			"postgres_cdc source slot_name cannot be empty",
			"remove source.config.slot_name to use etl_slot, or set a stable logical replication slot name")
	} else if strings.ContainsAny(slotName, " \t\r\n") {
		addStaticFieldError(result, "postgres-cdc-source-slot-name", "source.config.slot_name",
			fmt.Sprintf("postgres_cdc source slot_name %q cannot contain whitespace", slotName),
			"set source.config.slot_name to a PostgreSQL replication slot identifier without whitespace")
	}
	sslmode := strings.ToLower(strings.TrimSpace(stringField(cfg, "sslmode", "prefer")))
	if sslmode == "" {
		sslmode = "prefer"
	}
	if !isSupportedPostgresSSLMode(sslmode) {
		addStaticFieldError(result, "postgres-cdc-source-sslmode", "source.config.sslmode",
			fmt.Sprintf("postgres_cdc source sslmode %q is invalid", sslmode),
			"set source.config.sslmode to disable, allow, prefer, require, verify-ca, or verify-full")
	}
	tables := trimmedStringSlice(stringSliceField(cfg, "tables"))
	if boolField(cfg, "enable_snapshot", false) && len(tables) == 0 {
		addStaticFieldError(result, "postgres-cdc-source-snapshot-tables", "source.config.tables",
			"postgres_cdc source enable_snapshot requires source.config.tables",
			"set source.config.tables so the initial snapshot has a bounded table list")
	}
	for _, table := range stringSliceField(cfg, "tables") {
		table = strings.TrimSpace(table)
		if table == "" {
			addStaticFieldError(result, "postgres-cdc-source-table", "source.config.tables",
				"postgres_cdc source tables contains an empty table name",
				"remove empty entries from source.config.tables")
			continue
		}
		if strings.Contains(table, ".") {
			parts := strings.SplitN(table, ".", 2)
			if strings.TrimSpace(parts[0]) == "" || strings.TrimSpace(parts[1]) == "" {
				addStaticFieldError(result, "postgres-cdc-source-table", "source.config.tables",
					fmt.Sprintf("postgres_cdc source table %q is invalid", table),
					"use table names like orders or schema-qualified names like public.orders")
			}
		}
	}
}

// ── Sink: static config checks ───────────────────────────────────────

func (s *Server) checkStaticSinkConfig(spec *pipeline.Spec, result *PreflightResult) {
	if spec == nil || result == nil {
		return
	}
	switch strings.ToLower(spec.Sink.Type) {
	case "file_sink", "s3":
		checkFileLikeSinkConfig(spec, result)
		if strings.ToLower(spec.Sink.Type) == "s3" {
			checkS3SinkConfig(spec, result)
		}
	case "mysql", "postgres", "postgresql":
		checkRelationalSinkConfig(spec, result)
	case "clickhouse":
		checkClickHouseSinkConfig(spec, result)
	case "doris":
		checkDorisSinkConfig(spec, result)
	case "maxcompute", "odps":
		checkMaxComputeSinkConfig(spec, result)
	case "kafka":
		checkKafkaSinkConfig(spec, result)
	case "elasticsearch", "es":
		checkElasticsearchSinkConfig(spec, result)
	}
}

func checkFileLikeSinkConfig(spec *pipeline.Spec, result *PreflightResult) {
	format := strings.ToLower(strings.TrimSpace(stringField(spec.Sink.Config, "format", "json")))
	if format == "" {
		format = "json"
	}
	if !isSupportedFileLikeSinkFormat(format) {
		msg := fmt.Sprintf("sink %q format %q is not supported", spec.Sink.Type, format)
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "sink-config-format",
			Message:     msg,
			Remediation: "set sink.config.format to json, jsonl, csv, or parquet; unsupported formats can otherwise produce unreadable or empty output",
		})
		result.FieldIssues = append(result.FieldIssues, PreflightFieldIssue{
			Level:       "error",
			Field:       "sink.config.format",
			Check:       "sink-config-format",
			Message:     msg,
			Remediation: "choose one of: json, jsonl, csv, parquet",
		})
		result.Passed = false
	}
	if maxRetries := intField(spec.Sink.Config, "max_retries", 3); maxRetries < 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "sink-config-retry",
			Message:     fmt.Sprintf("sink %q max_retries must be >= 0, got %d", spec.Sink.Type, maxRetries),
			Remediation: "set sink.config.max_retries to 0 or a positive retry count",
		})
		result.FieldIssues = append(result.FieldIssues, PreflightFieldIssue{
			Level:       "error",
			Field:       "sink.config.max_retries",
			Check:       "sink-config-retry",
			Message:     "max_retries must be >= 0",
			Remediation: "set sink.config.max_retries to 0 or a positive retry count",
		})
		result.Passed = false
	}
	if retryBase := intField(spec.Sink.Config, "retry_base_ms", 500); retryBase < 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "sink-config-retry",
			Message:     fmt.Sprintf("sink %q retry_base_ms must be >= 0, got %d", spec.Sink.Type, retryBase),
			Remediation: "set sink.config.retry_base_ms to 0 or a positive backoff in milliseconds",
		})
		result.FieldIssues = append(result.FieldIssues, PreflightFieldIssue{
			Level:       "error",
			Field:       "sink.config.retry_base_ms",
			Check:       "sink-config-retry",
			Message:     "retry_base_ms must be >= 0",
			Remediation: "set sink.config.retry_base_ms to 0 or a positive backoff in milliseconds",
		})
		result.Passed = false
	}
}

func isSupportedFileLikeSinkFormat(format string) bool {
	switch format {
	case "json", "jsonl", "csv", "parquet":
		return true
	default:
		return false
	}
}

func checkS3SinkConfig(spec *pipeline.Spec, result *PreflightResult) {
	if strings.TrimSpace(stringField(spec.Sink.Config, "endpoint", "")) == "" {
		addStaticSinkFieldError(result, "s3-sink-endpoint", "sink.config.endpoint",
			"s3 sink requires sink.config.endpoint",
			"set sink.config.endpoint to the S3-compatible endpoint reachable from the ETL process, for example http://minio:9000")
	}
	if strings.TrimSpace(stringField(spec.Sink.Config, "bucket", "")) == "" {
		addStaticSinkFieldError(result, "s3-sink-bucket", "sink.config.bucket",
			"s3 sink requires sink.config.bucket",
			"set sink.config.bucket to the target S3 bucket name")
	}
}

func checkKafkaSinkConfig(spec *pipeline.Spec, result *PreflightResult) {
	if len(stringSliceField(spec.Sink.Config, "brokers")) == 0 {
		msg := "kafka sink requires sink.config.brokers"
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "kafka-sink-brokers",
			Message:     msg,
			Remediation: "set sink.config.brokers to the Kafka bootstrap broker addresses reachable from the ETL process",
		})
		result.FieldIssues = append(result.FieldIssues, PreflightFieldIssue{
			Level:       "error",
			Field:       "sink.config.brokers",
			Check:       "kafka-sink-brokers",
			Message:     msg,
			Remediation: "provide at least one Kafka broker address",
		})
		result.Passed = false
	}
	if strings.TrimSpace(stringField(spec.Sink.Config, "topic", "")) == "" {
		msg := "kafka sink requires sink.config.topic"
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "kafka-sink-topic",
			Message:     msg,
			Remediation: "set sink.config.topic to the Kafka topic that should receive records",
		})
		result.FieldIssues = append(result.FieldIssues, PreflightFieldIssue{
			Level:       "error",
			Field:       "sink.config.topic",
			Check:       "kafka-sink-topic",
			Message:     msg,
			Remediation: "provide a non-empty Kafka topic name",
		})
		result.Passed = false
	}
	compression := strings.ToLower(strings.TrimSpace(stringField(spec.Sink.Config, "compression", "none")))
	if compression == "" {
		compression = "none"
	}
	if !isSupportedKafkaCompression(compression) {
		msg := fmt.Sprintf("kafka sink compression %q is not supported", compression)
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "kafka-sink-compression",
			Message:     msg,
			Remediation: "set sink.config.compression to none, gzip, snappy, lz4, or zstd",
		})
		result.FieldIssues = append(result.FieldIssues, PreflightFieldIssue{
			Level:       "error",
			Field:       "sink.config.compression",
			Check:       "kafka-sink-compression",
			Message:     msg,
			Remediation: "choose one of: none, gzip, snappy, lz4, zstd",
		})
		result.Passed = false
	}
	if retryBackoff := intField(spec.Sink.Config, "retry_backoff_ms", 0); retryBackoff < 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "kafka-sink-retry",
			Message:     fmt.Sprintf("kafka sink retry_backoff_ms must be >= 0, got %d", retryBackoff),
			Remediation: "set sink.config.retry_backoff_ms to 0 or a positive backoff in milliseconds",
		})
		result.FieldIssues = append(result.FieldIssues, PreflightFieldIssue{
			Level:       "error",
			Field:       "sink.config.retry_backoff_ms",
			Check:       "kafka-sink-retry",
			Message:     "retry_backoff_ms must be >= 0",
			Remediation: "set sink.config.retry_backoff_ms to 0 or a positive backoff in milliseconds",
		})
		result.Passed = false
	}
}

func (s *Server) checkKafkaSink(ctx context.Context, spec *pipeline.Spec, result *PreflightResult) {
	if spec == nil || strings.ToLower(spec.Sink.Type) != "kafka" {
		return
	}
	cfg := spec.Sink.Config
	brokers := stringSliceField(cfg, "brokers")
	topic := strings.TrimSpace(stringField(cfg, "topic", ""))
	if len(brokers) == 0 || topic == "" {
		return
	}

	adminConfig, err := kafkaPreflightAdminConfig(cfg)
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "kafka-sink-config",
			Message:     fmt.Sprintf("invalid kafka sink client config: %v", err),
			Remediation: "verify sink.config.sasl_* / tls fields match the Kafka cluster requirements",
		})
		result.Passed = false
		return
	}

	admin, err := sarama.NewClusterAdmin(brokers, adminConfig)
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "warning",
			Check:       "kafka-sink-reachable",
			Message:     fmt.Sprintf("cannot connect to Kafka brokers %v: %v", brokers, err),
			Remediation: "verify Kafka brokers, network routing, TLS/SASL settings, and firewall rules before starting the pipeline",
		})
		return
	}
	defer func() { _ = admin.Close() }()

	topics, err := admin.ListTopics()
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "warning",
			Check:       "kafka-sink-metadata",
			Message:     fmt.Sprintf("cannot list Kafka topics from brokers %v: %v", brokers, err),
			Remediation: "grant the Kafka principal metadata/list permissions or verify broker compatibility before production rollout",
		})
		return
	}
	detail, ok := topics[topic]
	if !ok {
		if boolField(cfg, "auto_create_topic", false) {
			result.Issues = append(result.Issues, PreflightIssue{
				Level:       "warning",
				Check:       "kafka-sink-topic-auto-create",
				Message:     fmt.Sprintf("kafka topic %q was not found and will be created at sink open", topic),
				Remediation: "verify the Kafka principal has CreateTopics permission, or create the topic explicitly before production",
			})
			return
		}
		addStaticSinkFieldError(result, "kafka-sink-topic-metadata", "sink.config.topic",
			fmt.Sprintf("kafka topic %q was not found in broker metadata", topic),
			"create the Kafka topic before starting the pipeline, set sink.config.auto_create_topic=true for first-run smoke tests, or update sink.config.topic")
		return
	}
	if detail.NumPartitions == 0 {
		addStaticSinkFieldError(result, "kafka-sink-topic-partitions", "sink.config.topic",
			fmt.Sprintf("kafka topic %q has no partitions", topic),
			"create at least one partition for the target topic before starting the pipeline")
		return
	}
	g.Log().Debugf(ctx, "Kafka sink preflight passed: brokers=%v topic=%s partitions=%d", brokers, topic, detail.NumPartitions)
}

func isSupportedKafkaCompression(compression string) bool {
	switch compression {
	case "none", "gzip", "snappy", "lz4", "zstd":
		return true
	default:
		return false
	}
}

// hasDebeziumCDCTransform reports whether the pipeline carries a debezium_cdc
// transform. Such pipelines derive the target table and primary key columns
// from Debezium record metadata at runtime, so static sink.config.table and
// sink.config.pk_columns requirements are relaxed.
func hasDebeziumCDCTransform(spec *pipeline.Spec) bool {
	if spec == nil {
		return false
	}
	for _, t := range spec.Transforms {
		if strings.EqualFold(strings.TrimSpace(t.Type), "debezium_cdc") {
			return true
		}
	}
	return false
}

// sinkDerivesTableFromMetadata reports whether the sink is expected to derive
// its target table from per-record metadata (e.g. Debezium CDC multi-table
// sync). This requires auto_create so missing target tables can be created on
// the fly.
func sinkDerivesTableFromMetadata(spec *pipeline.Spec) bool {
	return hasDebeziumCDCTransform(spec) && boolField(spec.Sink.Config, "auto_create", false)
}

// sinkDerivesPKFromMetadata reports whether the sink is expected to derive
// primary key columns from per-record metadata (e.g. Debezium key payload)
// instead of a static sink.config.pk_columns list.
func sinkDerivesPKFromMetadata(spec *pipeline.Spec) bool {
	if boolField(spec.Sink.Config, "pk_columns_from_metadata", false) {
		return true
	}
	return hasDebeziumCDCTransform(spec)
}

func checkRelationalSinkConfig(spec *pipeline.Spec, result *PreflightResult) {
	sinkType := strings.ToLower(spec.Sink.Type)
	cfg := spec.Sink.Config
	label := sinkType
	if label == "postgresql" {
		label = "postgres"
	}
	// Debezium CDC pipelines derive the target table from record metadata when
	// auto_create is enabled, so the static table requirement is relaxed.
	requireFields := []string{"host", "user", "database"}
	if !sinkDerivesTableFromMetadata(spec) {
		requireFields = append(requireFields, "table")
	}
	for _, field := range requireFields {
		if strings.TrimSpace(stringField(cfg, field, "")) == "" {
			check := label + "-sink-required-config"
			addStaticSinkFieldError(result, check, "sink.config."+field,
				fmt.Sprintf("%s sink requires sink.config.%s", sinkType, field),
				fmt.Sprintf("set sink.config.%s before starting the pipeline", field))
		}
	}
	defaultPort := 3306
	if sinkType == "postgres" || sinkType == "postgresql" {
		defaultPort = 5432
	}
	if port := intField(cfg, "port", defaultPort); port <= 0 || port > 65535 {
		addStaticSinkFieldError(result, label+"-sink-port", "sink.config.port",
			fmt.Sprintf("%s sink port must be between 1 and 65535, got %d", sinkType, port),
			"set sink.config.port to a valid database listener port")
	}
	batchMode := strings.ToLower(strings.TrimSpace(stringField(cfg, "batch_mode", "insert")))
	if batchMode == "" {
		batchMode = "insert"
	}
	if batchMode != "insert" && batchMode != "upsert" && batchMode != "increment" {
		addStaticSinkFieldError(result, label+"-sink-batch-mode", "sink.config.batch_mode",
			fmt.Sprintf("%s sink batch_mode %q is not supported", sinkType, batchMode),
			"set sink.config.batch_mode to insert, upsert, or increment")
	}
	// Primary keys may be derived per-table from Debezium key metadata
	// (pk_columns_from_metadata) instead of a static list.
	pkFromMetadata := sinkDerivesPKFromMetadata(spec)
	if batchMode == "upsert" && !pkFromMetadata && len(stringSliceField(cfg, "pk_columns")) == 0 {
		addStaticSinkFieldError(result, label+"-sink-upsert-keys", "sink.config.pk_columns",
			fmt.Sprintf("%s sink upsert mode requires sink.config.pk_columns", sinkType),
			"set sink.config.pk_columns to stable business key columns before relying on replay absorption, or set pk_columns_from_metadata: true for CDC multi-table sync")
	}
	if batchMode == "increment" {
		if !pkFromMetadata && len(stringSliceField(cfg, "pk_columns")) == 0 {
			addStaticSinkFieldError(result, label+"-sink-increment-keys", "sink.config.pk_columns",
				fmt.Sprintf("%s sink increment mode requires sink.config.pk_columns", sinkType),
				"set sink.config.pk_columns to identify the row being accumulated")
		}
		if len(stringMapField(cfg, "increment_columns")) == 0 {
			addStaticSinkFieldError(result, label+"-sink-increment-columns", "sink.config.increment_columns",
				fmt.Sprintf("%s sink increment mode requires sink.config.increment_columns", sinkType),
				"set sink.config.increment_columns to {target_col: source_field} for accumulator columns")
		}
	}
	if schemaDrift := strings.ToLower(strings.TrimSpace(stringField(cfg, "schema_drift", "ignore"))); schemaDrift != "" && !isSupportedRelationalSchemaDrift(schemaDrift) {
		addStaticSinkFieldError(result, label+"-sink-schema-drift", "sink.config.schema_drift",
			fmt.Sprintf("%s sink schema_drift %q is not supported", sinkType, schemaDrift),
			"set sink.config.schema_drift to ignore, fail, or add_columns")
	}
	if ddlPolicy := strings.ToLower(strings.TrimSpace(stringField(cfg, "ddl_policy", "reject"))); ddlPolicy != "" && !isSupportedDDLPolicy(ddlPolicy) {
		addStaticSinkFieldError(result, label+"-sink-ddl-policy", "sink.config.ddl_policy",
			fmt.Sprintf("%s sink ddl_policy %q is not supported", sinkType, ddlPolicy),
			"set sink.config.ddl_policy to reject, ignore, or apply")
	}
	if chunkSize := intField(cfg, "insert_chunk_size", 500); chunkSize <= 0 {
		addStaticSinkFieldError(result, label+"-sink-insert-chunk-size", "sink.config.insert_chunk_size",
			fmt.Sprintf("%s sink insert_chunk_size must be > 0, got %d", sinkType, chunkSize),
			"set sink.config.insert_chunk_size to a positive row count such as 500")
	}
	// pre_write validation (MySQL/PostgreSQL sink)
	if rawPW, ok := cfg["pre_write"]; ok && rawPW != nil {
		pwMap, ok := rawPW.(map[string]any)
		if !ok {
			addStaticSinkFieldError(result, label+"-sink-pre-write", "sink.config.pre_write",
				fmt.Sprintf("%s sink pre_write must be a map, got %T", sinkType, rawPW),
				"set sink.config.pre_write to a map with action: delete|truncate|truncate_partition")
		} else {
			action := strings.ToLower(strings.TrimSpace(stringField(pwMap, "action", "")))
			switch action {
			case "delete", "truncate", "truncate_partition":
			case "":
				addStaticSinkFieldError(result, label+"-sink-pre-write-action", "sink.config.pre_write.action",
					fmt.Sprintf("%s sink pre_write.action is required", sinkType),
					"set sink.config.pre_write.action to delete, truncate, or truncate_partition")
			default:
				addStaticSinkFieldError(result, label+"-sink-pre-write-action", "sink.config.pre_write.action",
					fmt.Sprintf("%s sink pre_write.action %q is not supported", sinkType, action),
					"set sink.config.pre_write.action to delete, truncate, or truncate_partition")
			}
			cond := strings.TrimSpace(stringField(pwMap, "condition", ""))
			if (action == "delete" || action == "truncate_partition") && cond == "" {
				addStaticSinkFieldError(result, label+"-sink-pre-write-condition", "sink.config.pre_write.condition",
					fmt.Sprintf("%s sink pre_write.action=%s requires a non-empty condition", sinkType, action),
					"set sink.config.pre_write.condition to a WHERE clause (use action=truncate to wipe the whole table)")
			}
		}
	}
	if sinkType == "postgres" || sinkType == "postgresql" {
		sslmode := strings.ToLower(strings.TrimSpace(stringField(cfg, "sslmode", "prefer")))
		if sslmode == "" {
			sslmode = "prefer"
		}
		if !isSupportedPostgresSSLMode(sslmode) {
			addStaticSinkFieldError(result, "postgres-sink-sslmode", "sink.config.sslmode",
				fmt.Sprintf("postgres sink sslmode %q is invalid", sslmode),
				"set sink.config.sslmode to disable, allow, prefer, require, verify-ca, or verify-full")
		}
	}
}

func addStaticSinkFieldError(result *PreflightResult, check, field, message, remediation string) {
	addStaticFieldError(result, check, field, message, remediation)
}

func addStaticFieldError(result *PreflightResult, check, field, message, remediation string) {
	result.Issues = append(result.Issues, PreflightIssue{
		Level:       "error",
		Check:       check,
		Message:     message,
		Remediation: remediation,
	})
	result.FieldIssues = append(result.FieldIssues, PreflightFieldIssue{
		Level:       "error",
		Field:       field,
		Check:       check,
		Message:     message,
		Remediation: remediation,
	})
	result.Passed = false
}

func isSupportedRelationalSchemaDrift(value string) bool {
	switch value {
	case "ignore", "fail", "add_columns":
		return true
	default:
		return false
	}
}

func isSupportedDDLPolicy(value string) bool {
	switch value {
	case "reject", "ignore", "apply":
		return true
	default:
		return false
	}
}

func isSupportedPostgresSSLMode(value string) bool {
	switch value {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
		return true
	default:
		return false
	}
}

func checkClickHouseSinkConfig(spec *pipeline.Spec, result *PreflightResult) {
	cfg := spec.Sink.Config
	for _, field := range []string{"host", "database"} {
		if strings.TrimSpace(stringField(cfg, field, "")) == "" {
			addStaticSinkFieldError(result, "clickhouse-sink-required-config", "sink.config."+field,
				fmt.Sprintf("clickhouse sink requires sink.config.%s", field),
				fmt.Sprintf("set sink.config.%s before starting the pipeline", field))
		}
	}
	if port := intField(cfg, "port", 9000); port <= 0 || port > 65535 {
		addStaticSinkFieldError(result, "clickhouse-sink-port", "sink.config.port",
			fmt.Sprintf("clickhouse sink port must be between 1 and 65535, got %d", port),
			"set sink.config.port to 9000 for native protocol or 8123 for HTTP")
	}
	protocol := strings.ToLower(strings.TrimSpace(stringField(cfg, "protocol", "native")))
	if protocol == "" {
		protocol = "native"
	}
	if protocol != "native" && protocol != "http" {
		addStaticSinkFieldError(result, "clickhouse-sink-protocol", "sink.config.protocol",
			fmt.Sprintf("clickhouse sink protocol %q is not supported", protocol),
			"set sink.config.protocol to native or http")
	}
	if schemaDrift := strings.ToLower(strings.TrimSpace(stringField(cfg, "schema_drift", "ignore"))); schemaDrift != "" && !isSupportedClickHouseSchemaDrift(schemaDrift) {
		addStaticSinkFieldError(result, "clickhouse-sink-schema-drift", "sink.config.schema_drift",
			fmt.Sprintf("clickhouse sink schema_drift %q is not supported", schemaDrift),
			"set sink.config.schema_drift to ignore, fail, add_columns, or sync")
	}
	if ddlPolicy := strings.ToLower(strings.TrimSpace(stringField(cfg, "ddl_policy", "apply"))); ddlPolicy != "" && !isSupportedDDLPolicy(ddlPolicy) {
		addStaticSinkFieldError(result, "clickhouse-sink-ddl-policy", "sink.config.ddl_policy",
			fmt.Sprintf("clickhouse sink ddl_policy %q is not supported", ddlPolicy),
			"set sink.config.ddl_policy to reject, ignore, or apply")
	}
	if sourceDialect := strings.ToLower(strings.TrimSpace(stringField(cfg, "source_dialect", ""))); sourceDialect != "" && !isSupportedClickHouseSourceDialect(sourceDialect) {
		addStaticSinkFieldError(result, "clickhouse-sink-source-dialect", "sink.config.source_dialect",
			fmt.Sprintf("clickhouse sink source_dialect %q is not supported", sourceDialect),
			"set sink.config.source_dialect to mysql, postgres, postgresql, or clickhouse")
	}
	if optimizeInterval := intField(cfg, "optimize_interval_sec", 0); optimizeInterval < 0 {
		addStaticSinkFieldError(result, "clickhouse-sink-optimize-interval", "sink.config.optimize_interval_sec",
			fmt.Sprintf("clickhouse sink optimize_interval_sec must be >= 0, got %d", optimizeInterval),
			"set sink.config.optimize_interval_sec to 0 to disable periodic OPTIMIZE, or a positive interval in seconds")
	}
	compression := strings.ToUpper(strings.TrimSpace(stringField(cfg, "compression", "LZ4")))
	if compression == "" {
		compression = "LZ4"
	}
	if compression != "LZ4" && compression != "ZSTD" {
		addStaticSinkFieldError(result, "clickhouse-sink-compression", "sink.config.compression",
			fmt.Sprintf("clickhouse sink compression %q is not supported", compression),
			"set sink.config.compression to LZ4 or ZSTD")
	}
	if _, ok := cfg["version_column"]; ok && strings.TrimSpace(stringField(cfg, "version_column", "")) == "" {
		addStaticSinkFieldError(result, "clickhouse-sink-version-column", "sink.config.version_column",
			"clickhouse sink version_column cannot be empty when configured",
			"remove sink.config.version_column to use _version, or set a non-empty version column")
	}
}

func isSupportedClickHouseSchemaDrift(value string) bool {
	switch value {
	case "ignore", "fail", "add_columns", "sync":
		return true
	default:
		return false
	}
}

func isSupportedClickHouseSourceDialect(value string) bool {
	switch value {
	case "mysql", "postgres", "postgresql", "clickhouse":
		return true
	default:
		return false
	}
}

func checkDorisSinkConfig(spec *pipeline.Spec, result *PreflightResult) {
	cfg := spec.Sink.Config
	requireFields := []string{"host", "database"}
	if !sinkDerivesTableFromMetadata(spec) {
		requireFields = append(requireFields, "table")
	}
	for _, field := range requireFields {
		if strings.TrimSpace(stringField(cfg, field, "")) == "" {
			addStaticSinkFieldError(result, "doris-sink-required-config", "sink.config."+field,
				fmt.Sprintf("doris sink requires sink.config.%s", field),
				fmt.Sprintf("set sink.config.%s before starting the pipeline", field))
		}
	}
	if port := intField(cfg, "port", 9030); port <= 0 || port > 65535 {
		addStaticSinkFieldError(result, "doris-sink-port", "sink.config.port",
			fmt.Sprintf("doris sink port must be between 1 and 65535, got %d", port),
			"set sink.config.port to the Doris FE MySQL protocol port, usually 9030")
	}
	if httpPort := intField(cfg, "http_port", 8030); httpPort <= 0 || httpPort > 65535 {
		addStaticSinkFieldError(result, "doris-sink-http-port", "sink.config.http_port",
			fmt.Sprintf("doris sink http_port must be between 1 and 65535, got %d", httpPort),
			"set sink.config.http_port to the Doris FE Stream Load HTTP port, usually 8030")
	}
	writeMode := strings.ToLower(strings.TrimSpace(stringField(cfg, "write_mode", "stream_load")))
	if writeMode == "" {
		writeMode = "stream_load"
	}
	if writeMode != "stream_load" && writeMode != "insert" {
		addStaticSinkFieldError(result, "doris-sink-write-mode", "sink.config.write_mode",
			fmt.Sprintf("doris sink write_mode %q is not supported", writeMode),
			"set sink.config.write_mode to stream_load or insert")
	}
	batchMode := strings.ToLower(strings.TrimSpace(stringField(cfg, "batch_mode", "insert")))
	if batchMode == "" {
		batchMode = "insert"
	}
	if batchMode != "insert" && batchMode != "upsert" {
		addStaticSinkFieldError(result, "doris-sink-batch-mode", "sink.config.batch_mode",
			fmt.Sprintf("doris sink batch_mode %q is not supported", batchMode),
			"set sink.config.batch_mode to insert or upsert")
	}
	if batchMode == "upsert" && !sinkDerivesPKFromMetadata(spec) && len(stringSliceField(cfg, "pk_columns")) == 0 {
		addStaticSinkFieldError(result, "doris-sink-upsert-keys", "sink.config.pk_columns",
			"doris sink upsert mode requires sink.config.pk_columns for a stable UNIQUE KEY model",
			"set sink.config.pk_columns to stable business key columns before relying on replay absorption, or set pk_columns_from_metadata: true for CDC multi-table sync")
	}
	format := strings.ToLower(strings.TrimSpace(stringField(cfg, "stream_load_format", "json")))
	if format == "" {
		format = "json"
	}
	if format != "json" && format != "csv" {
		addStaticSinkFieldError(result, "doris-sink-stream-load-format", "sink.config.stream_load_format",
			fmt.Sprintf("doris sink stream_load_format %q is not supported", format),
			"set sink.config.stream_load_format to json or csv")
	}
	scheme := strings.ToLower(strings.TrimSpace(stringField(cfg, "stream_load_scheme", "http")))
	if scheme == "" {
		scheme = "http"
	}
	if scheme != "http" && scheme != "https" {
		addStaticSinkFieldError(result, "doris-sink-stream-load-scheme", "sink.config.stream_load_scheme",
			fmt.Sprintf("doris sink stream_load_scheme %q is not supported", scheme),
			"set sink.config.stream_load_scheme to http or https")
	}
	if timeout := intField(cfg, "stream_load_timeout_sec", 30); timeout <= 0 {
		addStaticSinkFieldError(result, "doris-sink-stream-load-timeout", "sink.config.stream_load_timeout_sec",
			fmt.Sprintf("doris sink stream_load_timeout_sec must be > 0, got %d", timeout),
			"set sink.config.stream_load_timeout_sec to a positive timeout in seconds")
	}
	if chunkSize := intField(cfg, "insert_chunk_size", 500); chunkSize <= 0 {
		addStaticSinkFieldError(result, "doris-sink-insert-chunk-size", "sink.config.insert_chunk_size",
			fmt.Sprintf("doris sink insert_chunk_size must be > 0, got %d", chunkSize),
			"set sink.config.insert_chunk_size to a positive row count such as 500")
	}
	if schemaDrift := strings.ToLower(strings.TrimSpace(stringField(cfg, "schema_drift", "ignore"))); schemaDrift != "" && !isSupportedRelationalSchemaDrift(schemaDrift) {
		addStaticSinkFieldError(result, "doris-sink-schema-drift", "sink.config.schema_drift",
			fmt.Sprintf("doris sink schema_drift %q is not supported", schemaDrift),
			"set sink.config.schema_drift to ignore, fail, or add_columns")
	}
	if ddlPolicy := strings.ToLower(strings.TrimSpace(stringField(cfg, "ddl_policy", "reject"))); ddlPolicy != "" && !isSupportedDDLPolicy(ddlPolicy) {
		addStaticSinkFieldError(result, "doris-sink-ddl-policy", "sink.config.ddl_policy",
			fmt.Sprintf("doris sink ddl_policy %q is not supported", ddlPolicy),
			"set sink.config.ddl_policy to reject, ignore, or apply")
	}
}

func checkMaxComputeSinkConfig(spec *pipeline.Spec, result *PreflightResult) {
	cfg := spec.Sink.Config
	for _, field := range []string{"endpoint", "project", "table", "access_key_id", "access_key_secret"} {
		if strings.TrimSpace(stringField(cfg, field, "")) == "" {
			addStaticSinkFieldError(result, "maxcompute-sink-required-config", "sink.config."+field,
				fmt.Sprintf("%s sink requires sink.config.%s", spec.Sink.Type, field),
				fmt.Sprintf("set sink.config.%s before starting the pipeline", field))
		}
	}
	for _, field := range []string{"endpoint", "tunnel_endpoint"} {
		raw := strings.TrimSpace(stringField(cfg, field, ""))
		if raw == "" {
			continue
		}
		if parsed, err := url.Parse(raw); err != nil || parsed.Scheme == "" || parsed.Host == "" || (parsed.Scheme != "http" && parsed.Scheme != "https") {
			addStaticSinkFieldError(result, "maxcompute-sink-endpoint", "sink.config."+field,
				fmt.Sprintf("%s sink %s %q is not a valid HTTP(S) URL", spec.Sink.Type, field, raw),
				fmt.Sprintf("set sink.config.%s to a reachable MaxCompute HTTP(S) endpoint URL", field))
		}
	}
	writeMode := strings.ToLower(strings.TrimSpace(stringField(cfg, "write_mode", "append")))
	if writeMode == "" {
		writeMode = "append"
	}
	if writeMode != "append" && writeMode != "partition_overwrite" {
		addStaticSinkFieldError(result, "maxcompute-sink-write-mode", "sink.config.write_mode",
			fmt.Sprintf("%s sink write_mode %q is not supported", spec.Sink.Type, writeMode),
			"set sink.config.write_mode to append or partition_overwrite")
	}
	if batchSize := intField(cfg, "batch_size", 500); batchSize <= 0 {
		addStaticSinkFieldError(result, "maxcompute-sink-batch-size", "sink.config.batch_size",
			fmt.Sprintf("%s sink batch_size must be > 0, got %d", spec.Sink.Type, batchSize),
			"set sink.config.batch_size to a positive row count such as 500")
	}
	if maxRetries := intField(cfg, "max_retries", 3); maxRetries < 0 {
		addStaticSinkFieldError(result, "maxcompute-sink-retry", "sink.config.max_retries",
			fmt.Sprintf("%s sink max_retries must be >= 0, got %d", spec.Sink.Type, maxRetries),
			"set sink.config.max_retries to 0 or a positive retry count")
	}
	if retryBase := intField(cfg, "retry_base_ms", 500); retryBase <= 0 {
		addStaticSinkFieldError(result, "maxcompute-sink-retry", "sink.config.retry_base_ms",
			fmt.Sprintf("%s sink retry_base_ms must be > 0, got %d", spec.Sink.Type, retryBase),
			"set sink.config.retry_base_ms to a positive backoff in milliseconds")
	}

	partitions := stringMapField(cfg, "partition")
	partitionFields := stringSliceField(cfg, "partition_fields")
	if len(partitions) == 0 && len(partitionFields) == 0 {
		addStaticSinkFieldError(result, "maxcompute-sink-partition", "sink.config.partition",
			fmt.Sprintf("%s sink requires sink.config.partition or sink.config.partition_fields", spec.Sink.Type),
			"configure a static partition map or dynamic partition_fields for the MaxCompute partitioned table path")
	}
	partitionKeys := map[string]bool{}
	for key := range partitions {
		trimmed := strings.TrimSpace(key)
		if trimmed == "" {
			addStaticSinkFieldError(result, "maxcompute-sink-partition", "sink.config.partition",
				fmt.Sprintf("%s sink partition contains an empty key", spec.Sink.Type),
				"remove empty partition keys or set a valid MaxCompute partition column name")
			continue
		}
		partitionKeys[strings.ToLower(trimmed)] = true
	}
	for _, field := range partitionFields {
		trimmed := strings.TrimSpace(field)
		if trimmed == "" {
			addStaticSinkFieldError(result, "maxcompute-sink-partition-fields", "sink.config.partition_fields",
				fmt.Sprintf("%s sink partition_fields contains an empty field", spec.Sink.Type),
				"remove empty dynamic partition field names")
			continue
		}
		if partitionKeys[strings.ToLower(trimmed)] {
			addStaticSinkFieldError(result, "maxcompute-sink-partition-conflict", "sink.config.partition_fields",
				fmt.Sprintf("%s sink partition field %q is configured as both static and dynamic", spec.Sink.Type, trimmed),
				"use either sink.config.partition for a static value or sink.config.partition_fields for a record-derived value, not both")
		}
	}
	for name, typ := range stringMapField(cfg, "columns") {
		if strings.TrimSpace(name) == "" {
			addStaticSinkFieldError(result, "maxcompute-sink-columns", "sink.config.columns",
				fmt.Sprintf("%s sink columns contains an empty column name", spec.Sink.Type),
				"remove empty column names from sink.config.columns")
			continue
		}
		if !isSupportedMaxComputeConfigType(typ) {
			addStaticSinkFieldError(result, "maxcompute-sink-columns", "sink.config.columns",
				fmt.Sprintf("%s sink column %q uses unsupported type %q", spec.Sink.Type, name, typ),
				"set MaxCompute column types to STRING, BIGINT, DOUBLE, DECIMAL, BOOLEAN, DATETIME, or TIMESTAMP-compatible types")
		}
	}
}

func isSupportedMaxComputeConfigType(typ string) bool {
	base := strings.ToUpper(strings.TrimSpace(typ))
	if base == "" {
		return false
	}
	if idx := strings.IndexAny(base, "( "); idx >= 0 {
		base = base[:idx]
	}
	switch base {
	case "STRING", "VARCHAR", "CHAR", "BIGINT", "INT", "INTEGER", "DOUBLE", "FLOAT", "DECIMAL", "BOOLEAN", "BOOL", "DATETIME", "TIMESTAMP":
		return true
	default:
		return false
	}
}

func checkElasticsearchSinkConfig(spec *pipeline.Spec, result *PreflightResult) {
	if len(elasticsearchSinkHosts(spec.Sink.Config)) == 0 {
		msg := "elasticsearch sink requires sink.config.hosts or sink.config.host"
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "elasticsearch-sink-hosts",
			Message:     msg,
			Remediation: "set sink.config.hosts to one or more Elasticsearch/OpenSearch HTTP URLs reachable from the ETL process",
		})
		result.FieldIssues = append(result.FieldIssues, PreflightFieldIssue{
			Level:       "error",
			Field:       "sink.config.hosts",
			Check:       "elasticsearch-sink-hosts",
			Message:     msg,
			Remediation: "provide at least one host URL, for example http://opensearch:9200",
		})
		result.Passed = false
	}
	if strings.TrimSpace(stringField(spec.Sink.Config, "index", "")) == "" {
		msg := "elasticsearch sink requires sink.config.index"
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "elasticsearch-sink-index",
			Message:     msg,
			Remediation: "set sink.config.index to the target index name so replay and mapping preflight use a stable destination",
		})
		result.FieldIssues = append(result.FieldIssues, PreflightFieldIssue{
			Level:       "error",
			Field:       "sink.config.index",
			Check:       "elasticsearch-sink-index",
			Message:     msg,
			Remediation: "provide a non-empty Elasticsearch/OpenSearch index name",
		})
		result.Passed = false
	}
	if chunkSize := intField(spec.Sink.Config, "chunk_size", 500); chunkSize <= 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "elasticsearch-sink-bulk",
			Message:     fmt.Sprintf("elasticsearch sink chunk_size must be > 0, got %d", chunkSize),
			Remediation: "set sink.config.chunk_size to a positive bulk request size such as 500",
		})
		result.FieldIssues = append(result.FieldIssues, PreflightFieldIssue{
			Level:       "error",
			Field:       "sink.config.chunk_size",
			Check:       "elasticsearch-sink-bulk",
			Message:     "chunk_size must be > 0",
			Remediation: "set sink.config.chunk_size to a positive bulk request size",
		})
		result.Passed = false
	}
	if maxRetries := intField(spec.Sink.Config, "max_retries", 3); maxRetries < 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "elasticsearch-sink-retry",
			Message:     fmt.Sprintf("elasticsearch sink max_retries must be >= 0, got %d", maxRetries),
			Remediation: "set sink.config.max_retries to 0 or a positive retry count",
		})
		result.FieldIssues = append(result.FieldIssues, PreflightFieldIssue{
			Level:       "error",
			Field:       "sink.config.max_retries",
			Check:       "elasticsearch-sink-retry",
			Message:     "max_retries must be >= 0",
			Remediation: "set sink.config.max_retries to 0 or a positive retry count",
		})
		result.Passed = false
	}
	if retryBase := intField(spec.Sink.Config, "retry_base_ms", 500); retryBase < 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "elasticsearch-sink-retry",
			Message:     fmt.Sprintf("elasticsearch sink retry_base_ms must be >= 0, got %d", retryBase),
			Remediation: "set sink.config.retry_base_ms to 0 or a positive backoff in milliseconds",
		})
		result.FieldIssues = append(result.FieldIssues, PreflightFieldIssue{
			Level:       "error",
			Field:       "sink.config.retry_base_ms",
			Check:       "elasticsearch-sink-retry",
			Message:     "retry_base_ms must be >= 0",
			Remediation: "set sink.config.retry_base_ms to 0 or a positive backoff in milliseconds",
		})
		result.Passed = false
	}
}

func elasticsearchSinkHosts(cfg map[string]any) []string {
	var hosts []string
	for _, host := range stringSliceField(cfg, "hosts") {
		host = strings.TrimSpace(host)
		if host != "" {
			hosts = append(hosts, host)
		}
	}
	if host := strings.TrimSpace(stringField(cfg, "host", "")); host != "" {
		hosts = append(hosts, host)
	}
	return hosts
}

// ── Source: MySQL CDC checks ─────────────────────────────────────────

func (s *Server) checkMySQLCDC(ctx context.Context, spec *pipeline.Spec, result *PreflightResult) {
	if spec.Source.Type != "mysql_cdc" && spec.Source.Type != "mysql_snapshot_cdc" {
		return
	}

	cfg := spec.Source.Config
	host := stringField(cfg, "host", "localhost")
	port := intField(cfg, "port", 3306)
	user := stringField(cfg, "user", "root")
	password := stringField(cfg, "password", "")

	// Connect to MySQL.
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/?timeout=5s&readTimeout=5s", user, password, host, port)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-connect",
			Message:     fmt.Sprintf("cannot connect to MySQL at %s:%d: %v", host, port, err),
			Remediation: "verify MySQL is running and credentials in source.config are correct",
		})
		result.Passed = false
		return
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-connect",
			Message:     fmt.Sprintf("cannot ping MySQL at %s:%d: %v", host, port, err),
			Remediation: "verify MySQL host/port are reachable from the ETL process",
		})
		result.Passed = false
		return
	}

	// Check binlog format.
	var binlogFormat string
	_ = db.QueryRowContext(ctx, "SELECT @@binlog_format").Scan(&binlogFormat)
	if strings.ToUpper(binlogFormat) != "ROW" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-binlog-format",
			Message:     fmt.Sprintf("binlog_format is %q, must be ROW", binlogFormat),
			Remediation: "run: SET GLOBAL binlog_format = 'ROW'; and restart the MySQL server. Or add --binlog-format=ROW to mysqld.",
		})
		result.Passed = false
	}

	// Check binlog row image.
	var binlogRowImage string
	_ = db.QueryRowContext(ctx, "SELECT @@binlog_row_image").Scan(&binlogRowImage)
	if strings.ToUpper(binlogRowImage) != "FULL" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-binlog-row-image",
			Message:     fmt.Sprintf("binlog_row_image is %q, must be FULL", binlogRowImage),
			Remediation: "run: SET GLOBAL binlog_row_image = 'FULL'; and restart the MySQL server.",
		})
		result.Passed = false
	}

	// Check replication grants.
	grants := checkMySQLGrants(ctx, db, user, host)
	for _, g := range grants {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-replication-grant",
			Message:     fmt.Sprintf("missing grant: %s", g),
			Remediation: fmt.Sprintf("run: GRANT %s ON *.* TO '%s'@'%%'; FLUSH PRIVILEGES;", g, user),
		})
		result.Passed = false
	}

	// Check server_id uniqueness (warning only — may be fine in single-instance).
	var serverID int
	_ = db.QueryRowContext(ctx, "SELECT @@server_id").Scan(&serverID)
	cfgServerID := intField(cfg, "server_id", 1001)
	if serverID == cfgServerID {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "warning",
			Check:       "mysql-server-id",
			Message:     fmt.Sprintf("source MySQL server_id (%d) matches the configured server_id (%d); this is expected for single-instance but may cause issues with multiple replicas", serverID, cfgServerID),
			Remediation: "if running multiple ETL instances, set a unique server_id for each in source.config.server_id",
		})
	}

	// Check source tables exist (best-effort, non-blocking).
	database := stringField(cfg, "database", "")
	if database != "" {
		tables := stringSliceField(cfg, "tables")
		for _, table := range tables {
			var count int
			err := db.QueryRowContext(ctx,
				"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?", database, table,
			).Scan(&count)
			if err != nil {
				continue // best-effort
			}
			if count == 0 {
				result.Issues = append(result.Issues, PreflightIssue{
					Level:       "warning",
					Check:       "source-table-exists",
					Message:     fmt.Sprintf("source table %s.%s not found in MySQL", database, table),
					Remediation: fmt.Sprintf("create the table %s.%s in MySQL, or remove it from source.config.tables", database, table),
				})
			}
		}
	}

	if len(result.Issues) == 0 {
		g.Log().Infof(ctx, "MySQL preflight passed: binlog_format=ROW, binlog_row_image=FULL, grants OK")
	}
}

// ── Source: MySQL batch checks ───────────────────────────────────────

var openMySQLBatchPreflightDB = sql.Open

func (s *Server) checkMySQLBatchSource(ctx context.Context, spec *pipeline.Spec, result *PreflightResult) {
	if spec == nil || strings.ToLower(spec.Source.Type) != "mysql_batch" {
		return
	}
	cfg := spec.Source.Config
	if _, err := registry.BuildSource(spec.Source.Type, cfg); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-batch-source-config",
			Message:     fmt.Sprintf("mysql_batch source configuration error: %v", err),
			Remediation: "fix source.config table/query, pk_column, cursor_column, columns, and shard settings before starting",
		})
		result.Passed = false
		return
	}

	host := strings.TrimSpace(stringField(cfg, "host", ""))
	user := strings.TrimSpace(stringField(cfg, "user", ""))
	database := strings.TrimSpace(stringField(cfg, "database", ""))
	table := strings.TrimSpace(stringField(cfg, "table", ""))
	customQuery := strings.TrimSpace(stringField(cfg, "query", ""))
	if host == "" || user == "" || database == "" {
		missing := []string{}
		if host == "" {
			missing = append(missing, "host")
		}
		if user == "" {
			missing = append(missing, "user")
		}
		if database == "" {
			missing = append(missing, "database")
		}
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-batch-required-config",
			Message:     fmt.Sprintf("mysql_batch source is missing required config: %s", strings.Join(missing, ", ")),
			Remediation: "set source.config.host, user, and database to the MySQL database that should be read",
		})
		result.Passed = false
		return
	}
	if customQuery == "" && table == "" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-batch-source-target",
			Message:     "mysql_batch source requires source.config.table or source.config.query",
			Remediation: "set source.config.table for simple table reads, or source.config.query plus cursor_column for custom SQL/JOIN reads",
		})
		result.Passed = false
		return
	}
	if limit := intField(cfg, "limit", 5000); limit <= 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-batch-limit",
			Message:     fmt.Sprintf("mysql_batch source limit must be positive, got %d", limit),
			Remediation: "set source.config.limit to a positive page size such as 1000 or 5000",
		})
		result.Passed = false
	}
	if shardTotal := intField(cfg, "shard_total", 0); shardTotal > 0 {
		shardIndex := intField(cfg, "shard_index", 0)
		if shardTotal < 2 || shardIndex < 0 || shardIndex >= shardTotal {
			result.Issues = append(result.Issues, PreflightIssue{
				Level:       "error",
				Check:       "mysql-batch-shard",
				Message:     fmt.Sprintf("mysql_batch shard settings are invalid: shard_index=%d shard_total=%d", shardIndex, shardTotal),
				Remediation: "set shard_total >= 2 and shard_index between 0 and shard_total-1, or remove both fields for a single reader",
			})
			result.Passed = false
		}
	}
	if !result.Passed {
		return
	}

	port := intField(cfg, "port", 3306)
	password := stringField(cfg, "password", "")
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&loc=Local&timeout=5s&readTimeout=5s",
		user, password, host, port, database)
	db, err := openMySQLBatchPreflightDB("mysql", dsn)
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-batch-connect",
			Message:     fmt.Sprintf("cannot configure MySQL connection for mysql_batch at %s:%d/%s: %v", host, port, database, err),
			Remediation: "verify MySQL connection fields and driver-compatible DSN settings",
		})
		result.Passed = false
		return
	}
	defer db.Close()

	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(probeCtx); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-batch-connect",
			Message:     fmt.Sprintf("cannot ping MySQL for mysql_batch at %s:%d/%s: %v", host, port, database, err),
			Remediation: "verify the MySQL host/port, credentials, database name, network path, and grants from the ETL process",
		})
		result.Passed = false
		return
	}

	if customQuery != "" {
		checkMySQLBatchCustomQuery(probeCtx, db, cfg, customQuery, result)
		return
	}
	checkMySQLBatchTable(probeCtx, db, cfg, database, table, result)
}

func checkMySQLBatchTable(ctx context.Context, db *sql.DB, cfg map[string]any, database, table string, result *PreflightResult) {
	schemaName, tableName := mysqlBatchTableTarget(database, table)
	var count int
	if err := db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=? AND table_name=?",
		schemaName, tableName,
	).Scan(&count); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-batch-table",
			Message:     fmt.Sprintf("cannot verify source table %s.%s: %v", schemaName, tableName, err),
			Remediation: "grant SELECT on information_schema.tables or verify source.config.database/table",
		})
		result.Passed = false
		return
	}
	if count == 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-batch-table",
			Message:     fmt.Sprintf("source table %s.%s was not found in MySQL", schemaName, tableName),
			Remediation: "create the table, pick an existing table from connection context, or update source.config.database/table",
		})
		result.Passed = false
		return
	}

	requiredColumns := []string{stringField(cfg, "pk_column", "id")}
	requiredColumns = append(requiredColumns, stringSliceField(cfg, "columns")...)
	checkMySQLBatchColumns(ctx, db, schemaName, tableName, requiredColumns, "mysql-batch-column", result)
}

func checkMySQLBatchCustomQuery(ctx context.Context, db *sql.DB, cfg map[string]any, customQuery string, result *PreflightResult) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM (%s) AS openetl_preflight_probe LIMIT 0", customQuery))
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-batch-query",
			Message:     fmt.Sprintf("mysql_batch custom query cannot be described: %v", err),
			Remediation: "fix source.config.query SQL, referenced tables, aliases, and SELECT privileges before starting",
		})
		result.Passed = false
		return
	}
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-batch-query",
			Message:     fmt.Sprintf("mysql_batch custom query columns cannot be read: %v", err),
			Remediation: "verify the custom query returns a normal tabular result set",
		})
		result.Passed = false
		return
	}
	if len(columns) == 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-batch-query",
			Message:     "mysql_batch custom query has no result columns",
			Remediation: "select the fields that should be emitted by the pipeline",
		})
		result.Passed = false
		return
	}
	cursorColumn := stringField(cfg, "cursor_column", stringField(cfg, "pk_column", "id"))
	if !containsStringFold(columns, cursorColumn) {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "mysql-batch-cursor-column",
			Message:     fmt.Sprintf("mysql_batch custom query result does not include cursor column %q", cursorColumn),
			Remediation: "include the cursor column in SELECT output, alias it to match source.config.cursor_column, or update cursor_column/pk_column",
		})
		result.Passed = false
	}
}

func checkMySQLBatchColumns(ctx context.Context, db *sql.DB, database, table string, requiredColumns []string, check string, result *PreflightResult) {
	seen := map[string]struct{}{}
	for _, col := range requiredColumns {
		col = strings.TrimSpace(col)
		if col == "" {
			continue
		}
		key := strings.ToLower(col)
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		var count int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.columns WHERE table_schema=? AND table_name=? AND column_name=?",
			database, table, col,
		).Scan(&count); err != nil {
			result.Issues = append(result.Issues, PreflightIssue{
				Level:       "error",
				Check:       check,
				Message:     fmt.Sprintf("cannot verify source column %s.%s.%s: %v", database, table, col, err),
				Remediation: "grant SELECT on information_schema.columns or verify source.config.columns/pk_column",
			})
			result.Passed = false
			continue
		}
		if count == 0 {
			result.Issues = append(result.Issues, PreflightIssue{
				Level:       "error",
				Check:       check,
				Message:     fmt.Sprintf("source column %s.%s.%s was not found in MySQL", database, table, col),
				Remediation: "update source.config.columns/pk_column to existing columns, or adjust the source table schema",
			})
			result.Passed = false
		}
	}
}

func mysqlBatchTableTarget(database, table string) (string, string) {
	if strings.Contains(table, ".") {
		parts := strings.SplitN(table, ".", 2)
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	return database, table
}

func containsStringFold(values []string, needle string) bool {
	needle = strings.ToLower(strings.TrimSpace(needle))
	for _, value := range values {
		if strings.ToLower(strings.TrimSpace(value)) == needle {
			return true
		}
	}
	return false
}

// ── Source: PostgreSQL CDC checks ────────────────────────────────────

var openPostgresCDCPreflightDB = sql.Open

func (s *Server) checkPostgresCDCSource(ctx context.Context, spec *pipeline.Spec, result *PreflightResult) {
	if spec == nil || strings.ToLower(spec.Source.Type) != "postgres_cdc" {
		return
	}
	cfg := spec.Source.Config
	if _, err := registry.BuildSource(spec.Source.Type, cfg); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "postgres-cdc-source-config",
			Message:     fmt.Sprintf("postgres_cdc source configuration error: %v", err),
			Remediation: "fix source.config.host, user, database, slot_name, sslmode, and tables before starting",
		})
		result.Passed = false
		return
	}

	host := strings.TrimSpace(stringField(cfg, "host", ""))
	user := strings.TrimSpace(stringField(cfg, "user", ""))
	database := strings.TrimSpace(stringField(cfg, "database", ""))
	if host == "" || user == "" || database == "" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "postgres-cdc-required-config",
			Message:     "postgres_cdc requires source.config.host, user, and database",
			Remediation: "set source.config.host, user, and database to the PostgreSQL database that should be replicated",
		})
		result.Passed = false
		return
	}

	sslmode := strings.ToLower(strings.TrimSpace(stringField(cfg, "sslmode", "prefer")))
	if sslmode == "" {
		sslmode = "prefer"
	}
	switch sslmode {
	case "disable", "allow", "prefer", "require", "verify-ca", "verify-full":
	default:
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "postgres-cdc-sslmode",
			Message:     fmt.Sprintf("postgres_cdc sslmode %q is invalid", sslmode),
			Remediation: "set source.config.sslmode to disable, allow, prefer, require, verify-ca, or verify-full",
		})
		result.Passed = false
		return
	}
	slotName := strings.TrimSpace(stringField(cfg, "slot_name", "etl_slot"))
	if slotName == "" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "postgres-cdc-slot-name",
			Message:     "postgres_cdc slot_name cannot be empty",
			Remediation: "set source.config.slot_name to a stable logical replication slot name",
		})
		result.Passed = false
		return
	}

	port := intField(cfg, "port", 5432)
	password := stringField(cfg, "password", "")
	connStr := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s", user, password, host, port, database, sslmode)
	db, err := openPostgresCDCPreflightDB("pgx", connStr)
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "postgres-cdc-connect",
			Message:     fmt.Sprintf("cannot configure PostgreSQL connection for postgres_cdc at %s:%d/%s: %v", host, port, database, err),
			Remediation: "verify PostgreSQL connection fields and driver-compatible DSN settings",
		})
		result.Passed = false
		return
	}
	defer db.Close()

	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(probeCtx); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "postgres-cdc-connect",
			Message:     fmt.Sprintf("cannot ping PostgreSQL for postgres_cdc at %s:%d/%s: %v", host, port, database, err),
			Remediation: "verify PostgreSQL host/port, credentials, database name, network path, and pg_hba.conf from the ETL process",
		})
		result.Passed = false
		return
	}

	checkPostgresCDCWalLevel(probeCtx, db, result)
	checkPostgresCDCReplicationRole(probeCtx, db, result)
	checkPostgresCDCTables(probeCtx, db, stringSliceField(cfg, "tables"), result)
	checkPostgresCDCPublication(probeCtx, db, cfg, result)
	checkPostgresCDCSlot(probeCtx, db, slotName, database, result)
}

func checkPostgresCDCWalLevel(ctx context.Context, db *sql.DB, result *PreflightResult) {
	var walLevel string
	if err := db.QueryRowContext(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "postgres-cdc-wal-level",
			Message:     fmt.Sprintf("cannot read PostgreSQL wal_level: %v", err),
			Remediation: "grant permission to read server settings or verify the PostgreSQL connection",
		})
		result.Passed = false
		return
	}
	if strings.ToLower(strings.TrimSpace(walLevel)) != "logical" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "postgres-cdc-wal-level",
			Message:     fmt.Sprintf("PostgreSQL wal_level is %q, must be logical for postgres_cdc", walLevel),
			Remediation: "set wal_level=logical in postgresql.conf and restart PostgreSQL before starting CDC",
		})
		result.Passed = false
	}
}

func checkPostgresCDCReplicationRole(ctx context.Context, db *sql.DB, result *PreflightResult) {
	var canReplicate bool
	if err := db.QueryRowContext(ctx, "SELECT rolsuper OR rolreplication FROM pg_roles WHERE rolname = current_user").Scan(&canReplicate); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "postgres-cdc-replication-role",
			Message:     fmt.Sprintf("cannot verify PostgreSQL replication role: %v", err),
			Remediation: "grant access to pg_roles or verify the PostgreSQL user before starting CDC",
		})
		result.Passed = false
		return
	}
	if !canReplicate {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "postgres-cdc-replication-role",
			Message:     "PostgreSQL user does not have replication privilege",
			Remediation: "run ALTER ROLE <user> WITH REPLICATION, or use a role with replication/superuser privilege",
		})
		result.Passed = false
	}
}

func checkPostgresCDCTables(ctx context.Context, db *sql.DB, tables []string, result *PreflightResult) {
	for _, table := range tables {
		schemaName, tableName := postgresCDCTableTarget(table)
		if schemaName == "" || tableName == "" {
			result.Issues = append(result.Issues, PreflightIssue{
				Level:       "error",
				Check:       "postgres-cdc-table",
				Message:     fmt.Sprintf("postgres_cdc table %q is invalid", table),
				Remediation: "use table names like orders or schema-qualified names like public.orders",
			})
			result.Passed = false
			continue
		}
		var count int
		if err := db.QueryRowContext(ctx,
			"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema=$1 AND table_name=$2",
			schemaName, tableName,
		).Scan(&count); err != nil {
			result.Issues = append(result.Issues, PreflightIssue{
				Level:       "error",
				Check:       "postgres-cdc-table",
				Message:     fmt.Sprintf("cannot verify PostgreSQL source table %s.%s: %v", schemaName, tableName, err),
				Remediation: "grant access to information_schema.tables or verify source.config.tables",
			})
			result.Passed = false
			continue
		}
		if count == 0 {
			result.Issues = append(result.Issues, PreflightIssue{
				Level:       "error",
				Check:       "postgres-cdc-table",
				Message:     fmt.Sprintf("PostgreSQL source table %s.%s was not found", schemaName, tableName),
				Remediation: "create the table, pick an existing table from connection context, or update source.config.tables",
			})
			result.Passed = false
		}
	}
}

func checkPostgresCDCPublication(ctx context.Context, db *sql.DB, cfg map[string]any, result *PreflightResult) {
	var exists bool
	if err := db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname='etl_pub')").Scan(&exists); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "postgres-cdc-publication",
			Message:     fmt.Sprintf("cannot verify PostgreSQL publication etl_pub: %v", err),
			Remediation: "grant access to pg_publication or create publication etl_pub manually",
		})
		result.Passed = false
		return
	}
	if exists {
		return
	}
	var canCreate bool
	if err := db.QueryRowContext(ctx, "SELECT has_database_privilege(current_database(), 'CREATE')").Scan(&canCreate); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "warning",
			Check:       "postgres-cdc-publication",
			Message:     fmt.Sprintf("publication etl_pub does not exist and CREATE privilege could not be verified: %v", err),
			Remediation: "create publication etl_pub manually or grant CREATE on the database to the CDC user",
		})
		return
	}
	if !canCreate {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "postgres-cdc-publication",
			Message:     "publication etl_pub does not exist and the PostgreSQL user lacks database CREATE privilege",
			Remediation: postgresCDCPublicationRemediation(stringSliceField(cfg, "tables")),
		})
		result.Passed = false
	}
}

func checkPostgresCDCSlot(ctx context.Context, db *sql.DB, slotName, database string, result *PreflightResult) {
	var exists bool
	var slotDB sql.NullString
	if err := db.QueryRowContext(ctx, "SELECT EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name=$1)", slotName).Scan(&exists); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "postgres-cdc-slot",
			Message:     fmt.Sprintf("cannot verify PostgreSQL replication slot %q: %v", slotName, err),
			Remediation: "grant access to pg_replication_slots or verify source.config.slot_name",
		})
		result.Passed = false
		return
	}
	if !exists {
		return
	}
	if err := db.QueryRowContext(ctx, "SELECT database FROM pg_replication_slots WHERE slot_name=$1", slotName).Scan(&slotDB); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "warning",
			Check:       "postgres-cdc-slot",
			Message:     fmt.Sprintf("replication slot %q exists but its database could not be verified: %v", slotName, err),
			Remediation: "verify the existing slot belongs to the configured database and uses pgoutput",
		})
		return
	}
	if slotDB.Valid && slotDB.String != "" && slotDB.String != database {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "postgres-cdc-slot",
			Message:     fmt.Sprintf("replication slot %q belongs to database %q, not %q", slotName, slotDB.String, database),
			Remediation: "use a slot owned by the configured database, drop/recreate the slot, or change source.config.slot_name",
		})
		result.Passed = false
	}
}

func postgresCDCTableTarget(table string) (string, string) {
	table = strings.TrimSpace(table)
	if table == "" {
		return "", ""
	}
	if strings.Contains(table, ".") {
		parts := strings.SplitN(table, ".", 2)
		return strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
	}
	return "public", table
}

func postgresCDCPublicationRemediation(tables []string) string {
	if len(tables) == 0 {
		return "run CREATE PUBLICATION etl_pub FOR ALL TABLES, or grant CREATE on the database to the CDC user"
	}
	return "run CREATE PUBLICATION etl_pub FOR TABLE <tables>, or grant CREATE on the database to the CDC user"
}

// ── Source: HTTP checks ───────────────────────────────────────────────

func (s *Server) checkHTTPSource(ctx context.Context, spec *pipeline.Spec, result *PreflightResult) {
	if spec == nil || strings.ToLower(spec.Source.Type) != "http" {
		return
	}
	cfg := spec.Source.Config
	if _, err := registry.BuildSource(spec.Source.Type, cfg); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "http-source-config",
			Message:     fmt.Sprintf("http source configuration error: %v", err),
			Remediation: "fix source.config.url, method, pagination, auth, retry, and shard settings before starting",
		})
		result.Passed = false
		return
	}

	rawURL := strings.TrimSpace(stringField(cfg, "url", ""))
	if rawURL == "" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "http-source-url",
			Message:     "http source requires source.config.url",
			Remediation: "set source.config.url to the API endpoint that returns JSON records",
		})
		result.Passed = false
		return
	}
	parsedURL, err := url.Parse(rawURL)
	if err != nil || parsedURL.Scheme == "" || parsedURL.Host == "" || (parsedURL.Scheme != "http" && parsedURL.Scheme != "https") {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "http-source-url",
			Message:     fmt.Sprintf("http source url %q is invalid", rawURL),
			Remediation: "use an absolute http:// or https:// URL reachable from the ETL process",
		})
		result.Passed = false
		return
	}

	method := strings.ToUpper(strings.TrimSpace(stringField(cfg, "method", "GET")))
	if method == "" {
		method = "GET"
	}
	switch method {
	case http.MethodGet, http.MethodPost, http.MethodPut, http.MethodPatch:
	default:
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "http-source-method",
			Message:     fmt.Sprintf("http source method %q is not supported by preflight", method),
			Remediation: "use GET, POST, PUT, or PATCH for HTTP source reads",
		})
		result.Passed = false
		return
	}
	if pagination := strings.TrimSpace(stringField(cfg, "pagination", "")); pagination != "" && pagination != "page" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "http-source-pagination",
			Message:     fmt.Sprintf("http source pagination %q is unsupported", pagination),
			Remediation: "set source.config.pagination to page, or remove it for default page pagination",
		})
		result.Passed = false
		return
	}
	if pageSize := intField(cfg, "page_size", 100); pageSize <= 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "http-source-page-size",
			Message:     fmt.Sprintf("http source page_size must be positive, got %d", pageSize),
			Remediation: "set source.config.page_size to a positive page size such as 100",
		})
		result.Passed = false
		return
	}
	if maxPages := intField(cfg, "max_pages", 0); maxPages < 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "http-source-max-pages",
			Message:     fmt.Sprintf("http source max_pages must be >= 0, got %d", maxPages),
			Remediation: "set source.config.max_pages to 0 for no explicit cap, or a positive page limit",
		})
		result.Passed = false
		return
	}
	if maxRetries := intField(cfg, "max_retries", 3); maxRetries < 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "http-source-retry",
			Message:     fmt.Sprintf("http source max_retries must be >= 0, got %d", maxRetries),
			Remediation: "set source.config.max_retries to 0 or a positive retry count",
		})
		result.Passed = false
		return
	}
	if retryBase := intField(cfg, "retry_base_ms", 500); retryBase < 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "http-source-retry",
			Message:     fmt.Sprintf("http source retry_base_ms must be >= 0, got %d", retryBase),
			Remediation: "set source.config.retry_base_ms to 0 or a positive backoff in milliseconds",
		})
		result.Passed = false
		return
	}
	if shardTotal := intField(cfg, "shard_total", 0); shardTotal > 0 {
		shardIndex := intField(cfg, "shard_index", 0)
		if shardTotal < 2 || shardIndex < 0 || shardIndex >= shardTotal {
			result.Issues = append(result.Issues, PreflightIssue{
				Level:       "error",
				Check:       "http-source-shard",
				Message:     fmt.Sprintf("http source shard settings are invalid: shard_index=%d shard_total=%d", shardIndex, shardTotal),
				Remediation: "set shard_total >= 2 and shard_index between 0 and shard_total-1, or remove both fields for a single reader",
			})
			result.Passed = false
			return
		}
	}

	probeURL := applyHTTPPreflightPageParams(rawURL, cfg)
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := probeHTTPSourceSample(probeCtx, probeURL, method, cfg, result); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "http-source-sample",
			Message:     fmt.Sprintf("http source sample request failed: %v", err),
			Remediation: "verify URL, method, headers/auth, body, pagination parameters, result_key, and that the endpoint returns JSON object records",
		})
		result.Passed = false
	}
}

func applyHTTPPreflightPageParams(rawURL string, cfg map[string]any) string {
	pageParam := strings.TrimSpace(stringField(cfg, "page_param", ""))
	sizeParam := strings.TrimSpace(stringField(cfg, "size_param", ""))
	if pageParam == "" && sizeParam == "" {
		return rawURL
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}
	values := parsed.Query()
	if pageParam != "" {
		page := 1
		if shardTotal := intField(cfg, "shard_total", 0); shardTotal > 1 {
			page = intField(cfg, "shard_index", 0) + 1
		}
		values.Set(pageParam, fmt.Sprintf("%d", page))
	}
	if sizeParam != "" {
		values.Set(sizeParam, fmt.Sprintf("%d", intField(cfg, "page_size", 100)))
	}
	parsed.RawQuery = values.Encode()
	return parsed.String()
}

func probeHTTPSourceSample(ctx context.Context, requestURL, method string, cfg map[string]any, result *PreflightResult) error {
	var body io.Reader
	if rawBody := stringField(cfg, "body", ""); rawBody != "" {
		body = bytes.NewReader([]byte(rawBody))
	}
	req, err := http.NewRequestWithContext(ctx, method, requestURL, body)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	for k, v := range headerMapField(cfg, "headers") {
		req.Header.Set(k, v)
	}
	switch strings.ToLower(strings.TrimSpace(stringField(cfg, "auth_type", ""))) {
	case "", "bearer":
		if token := stringField(cfg, "auth_token", ""); token != "" {
			req.Header.Set("Authorization", "Bearer "+token)
		}
	case "basic":
		user := stringField(cfg, "auth_user", "")
		if user == "" {
			return fmt.Errorf("basic auth requires auth_user")
		}
		req.SetBasicAuth(user, stringField(cfg, "auth_pass", ""))
	default:
		return fmt.Errorf("unsupported auth_type %q", stringField(cfg, "auth_type", ""))
	}

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("request %s: %w", requestURL, err)
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("read response: %w", err)
	}
	if resp.StatusCode >= 400 {
		return fmt.Errorf("http status %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	items, err := extractHTTPPreflightItems(respBody, stringField(cfg, "result_key", ""))
	if err != nil {
		return err
	}
	objectCount := 0
	for _, item := range items {
		if _, ok := item.(map[string]any); ok {
			objectCount++
		}
	}
	if len(items) == 0 || objectCount == 0 {
		level := "warning"
		if strings.TrimSpace(stringField(cfg, "result_key", "")) != "" {
			level = "error"
			result.Passed = false
		}
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       level,
			Check:       "http-source-empty",
			Message:     "http source sample response did not contain JSON object records",
			Remediation: "verify result_key points to an array of objects, or return a top-level array/object list under data/items/results/records/list",
		})
	}
	return nil
}

func extractHTTPPreflightItems(body []byte, resultKey string) ([]any, error) {
	var raw any
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("json decode: %w", err)
	}
	if arr, ok := raw.([]any); ok {
		return arr, nil
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("response JSON must be an object or array")
	}
	if resultKey != "" {
		cur := any(m)
		for _, part := range strings.Split(resultKey, ".") {
			cm, ok := cur.(map[string]any)
			if !ok {
				return nil, nil
			}
			cur, ok = cm[part]
			if !ok {
				return nil, nil
			}
		}
		if arr, ok := cur.([]any); ok {
			return arr, nil
		}
		return nil, nil
	}
	for _, k := range []string{"data", "items", "results", "records", "list"} {
		if arr, ok := m[k].([]any); ok {
			return arr, nil
		}
	}
	return []any{m}, nil
}

// ── Source: file checks ───────────────────────────────────────────────

func (s *Server) checkFileSource(ctx context.Context, spec *pipeline.Spec, result *PreflightResult) {
	if spec == nil || strings.ToLower(spec.Source.Type) != "file" {
		return
	}
	cfg := spec.Source.Config
	if _, err := registry.BuildSource(spec.Source.Type, cfg); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "file-source-config",
			Message:     fmt.Sprintf("file source configuration error: %v", err),
			Remediation: "fix source.config.path, format, delimiter, has_header, and batch_size before starting",
		})
		result.Passed = false
		return
	}

	path := strings.TrimSpace(stringField(cfg, "path", ""))
	if path == "" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "file-source-path",
			Message:     "file source requires source.config.path",
			Remediation: "set source.config.path to a local file path mounted into the ETL process or container",
		})
		result.Passed = false
		return
	}
	format := strings.ToLower(strings.TrimSpace(stringField(cfg, "format", "csv")))
	if format == "" {
		format = "csv"
	}
	if format != "csv" && format != "json" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "file-source-format",
			Message:     fmt.Sprintf("unsupported file source format %q", format),
			Remediation: "set source.config.format to csv or json; json means newline-delimited JSON objects",
		})
		result.Passed = false
		return
	}
	if batchSize := intField(cfg, "batch_size", 1000); batchSize <= 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "file-source-batch-size",
			Message:     fmt.Sprintf("file source batch_size must be positive, got %d", batchSize),
			Remediation: "set source.config.batch_size to a positive value such as 1000",
		})
		result.Passed = false
		return
	}
	if delimiter := stringField(cfg, "delimiter", ","); format == "csv" && delimiter == "" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "file-source-delimiter",
			Message:     "file source csv delimiter cannot be empty",
			Remediation: "set source.config.delimiter to a one-character delimiter such as comma or tab",
		})
		result.Passed = false
		return
	}

	info, err := os.Stat(path)
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "file-source-readable",
			Message:     fmt.Sprintf("file source path %q is not readable: %v", path, err),
			Remediation: "verify the file exists and is mounted/readable from the ETL process or container",
		})
		result.Passed = false
		return
	}
	if info.IsDir() {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "file-source-readable",
			Message:     fmt.Sprintf("file source path %q is a directory, not a file", path),
			Remediation: "set source.config.path to a single CSV or JSON Lines file",
		})
		result.Passed = false
		return
	}

	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	cols, sample, err := introspectFileSource(probeCtx, cfg)
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "file-source-sample",
			Message:     fmt.Sprintf("file source sample could not be read as %s: %v", format, err),
			Remediation: "verify source.config.format, delimiter, has_header, and that JSON files are newline-delimited JSON objects",
		})
		result.Passed = false
		return
	}
	if len(cols) == 0 || len(sample) == 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "warning",
			Check:       "file-source-empty",
			Message:     fmt.Sprintf("file source path %q did not produce sample records during preflight", path),
			Remediation: "verify the file has at least one data row before expecting the pipeline to write records",
		})
	}
}

// ── Source: Kafka metadata checks ─────────────────────────────────────

func (s *Server) checkKafkaSource(ctx context.Context, spec *pipeline.Spec, result *PreflightResult) {
	if spec == nil || strings.ToLower(spec.Source.Type) != "kafka" {
		return
	}
	cfg := spec.Source.Config
	brokers := stringSliceField(cfg, "brokers")
	if len(brokers) == 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "kafka-source-brokers",
			Message:     "kafka source has no configured brokers",
			Remediation: "set source.config.brokers to the Kafka bootstrap broker addresses reachable from the ETL process",
		})
		result.Passed = false
		return
	}

	topic := strings.TrimSpace(stringField(cfg, "topic", ""))
	if topic == "" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "kafka-source-topic",
			Message:     "kafka source has no configured topic",
			Remediation: "set source.config.topic to the Kafka topic that should be consumed",
		})
		result.Passed = false
		return
	}

	groupID := strings.TrimSpace(stringField(cfg, "group_id", ""))
	if groupID == "" || groupID == "etl-consumer" {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "warning",
			Check:       "kafka-source-group",
			Message:     "kafka source is using the default consumer group",
			Remediation: "set source.config.group_id to a pipeline-specific value so multiple jobs do not share offsets accidentally",
		})
	}

	adminConfig, err := kafkaPreflightAdminConfig(cfg)
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "kafka-source-config",
			Message:     fmt.Sprintf("invalid kafka source client config: %v", err),
			Remediation: "verify source.config.sasl_* / tls / initial_offset fields match the Kafka cluster requirements",
		})
		result.Passed = false
		return
	}

	admin, err := sarama.NewClusterAdmin(brokers, adminConfig)
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "warning",
			Check:       "kafka-source-reachable",
			Message:     fmt.Sprintf("cannot connect to Kafka brokers %v: %v", brokers, err),
			Remediation: "verify Kafka brokers, network routing, TLS/SASL settings, and firewall rules before starting the pipeline",
		})
		return
	}
	defer func() { _ = admin.Close() }()

	topics, err := admin.ListTopics()
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "warning",
			Check:       "kafka-source-metadata",
			Message:     fmt.Sprintf("cannot list Kafka topics from brokers %v: %v", brokers, err),
			Remediation: "grant the Kafka principal metadata/list permissions or verify broker compatibility",
		})
		return
	}
	detail, ok := topics[topic]
	if !ok {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "kafka-source-topic",
			Message:     fmt.Sprintf("kafka topic %q was not found in broker metadata", topic),
			Remediation: "create the Kafka topic before starting the pipeline, or update source.config.topic to an existing topic",
		})
		result.Passed = false
		return
	}
	if detail.NumPartitions == 0 {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "kafka-source-topic-partitions",
			Message:     fmt.Sprintf("kafka topic %q has no partitions", topic),
			Remediation: "create at least one partition for the source topic before starting the pipeline",
		})
		result.Passed = false
	}
	g.Log().Debugf(ctx, "Kafka source preflight passed: brokers=%v topic=%s partitions=%d", brokers, topic, detail.NumPartitions)
}

func kafkaPreflightAdminConfig(cfg map[string]any) (*sarama.Config, error) {
	adminConfig := sarama.NewConfig()
	adminConfig.Version = sarama.V1_1_0_0
	adminConfig.ApiVersionsRequest = false
	adminConfig.Net.DialTimeout = 2 * time.Second
	adminConfig.Net.ReadTimeout = 2 * time.Second
	adminConfig.Net.WriteTimeout = 2 * time.Second

	if initialOffset := strings.ToLower(strings.TrimSpace(stringField(cfg, "initial_offset", ""))); initialOffset != "" && initialOffset != "oldest" && initialOffset != "newest" {
		return nil, fmt.Errorf("initial_offset must be oldest or newest")
	}
	if stringField(cfg, "sasl_user", "") != "" {
		adminConfig.Net.SASL.Enable = true
		adminConfig.Net.SASL.User = stringField(cfg, "sasl_user", "")
		adminConfig.Net.SASL.Password = stringField(cfg, "sasl_password", "")
		switch strings.ToLower(stringField(cfg, "sasl_mechanism", "")) {
		case "scram-sha-256":
			adminConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return etlsink.NewSCRAMClient(sha256.New)
			}
			adminConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "scram-sha-512":
			adminConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return etlsink.NewSCRAMClient(sha512.New)
			}
			adminConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			adminConfig.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		}
	}
	if boolField(cfg, "tls", false) {
		adminConfig.Net.TLS.Enable = true
		adminConfig.Net.TLS.Config = &tls.Config{InsecureSkipVerify: boolField(cfg, "tls_skip_verify", false)}
	}
	if err := adminConfig.Validate(); err != nil {
		return nil, err
	}
	return adminConfig, nil
}

// ── Sink: reachability check ─────────────────────────────────────────

func (s *Server) checkSinkReachable(ctx context.Context, spec *pipeline.Spec, result *PreflightResult) (core.Sink, bool) {
	// Build the sink to validate config parseability.
	sink, err := registry.BuildSink(spec.Sink.Type, spec.Sink.Config)
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "sink-config",
			Message:     fmt.Sprintf("sink %q configuration error: %v", spec.Sink.Type, err),
			Remediation: "fix the sink config in the pipeline spec",
		})
		result.Passed = false
		return nil, false
	}

	// Test actual sink reachability with a short timeout so preflight
	// doesn't block indefinitely when the target is unreachable (P4-11, SV-2).
	// The timeout is kept short because this runs synchronously during spec
	// validation; a longer probe belongs in the dedicated connection-test endpoint.
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	if err := sink.Open(probeCtx); err != nil {
		level := "warning"
		check := "sink-reachable"
		remediation := "verify the sink target is running and network is accessible from the ETL process"
		if isMaxComputeSinkType(spec.Sink.Type) {
			level = "error"
			check = "maxcompute-preflight"
			remediation = "verify MaxCompute endpoint/project/table/partition permissions, access keys, tunnel endpoint, and network reachability from the ETL process"
			result.Passed = false
		}
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       level,
			Check:       check,
			Message:     fmt.Sprintf("sink %q is not reachable: %v", spec.Sink.Type, err),
			Remediation: remediation,
		})
		if isMaxComputeSinkType(spec.Sink.Type) {
			// Remote MaxCompute preflight may fail because credentials/network
			// are unavailable in local validation, but the local schema and
			// partition contract is still useful preflight evidence.
			return sink, true
		}
		// Generic reachability failure is a warning because the target may
		// become available later; build-disabled experimental sinks are errors.
		return nil, false
	}
	return sink, true
}

func isMaxComputeSinkType(t string) bool {
	return t == "maxcompute" || t == "odps"
}

// ── Source/Sink schema compatibility ────────────────────────────────

func (s *Server) checkSchemaCompatibility(ctx context.Context, spec *pipeline.Spec, sink core.Sink, result *PreflightResult) {
	validator, ok := sink.(core.SchemaValidator)
	if !ok {
		if schemaPreflightMatters(spec.Sink.Type) {
			addPreflightGuidance(result, PreflightGuidance{
				Level:    "warning",
				Category: "schema",
				Code:     "schema-validator-unavailable",
				Message:  fmt.Sprintf("sink %q does not expose schema validation in preflight", spec.Sink.Type),
				Action:   "verify the target table, field types, and write mode manually before production use",
			})
		}
		return
	}

	source, err := registry.BuildSource(spec.Source.Type, spec.Source.Config)
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "source-config",
			Message:     fmt.Sprintf("source %q configuration error: %v", spec.Source.Type, err),
			Remediation: "fix the source config in the pipeline spec",
		})
		result.Passed = false
		return
	}
	descriptor, ok := source.(core.SchemaDescriptor)
	if !ok {
		schema, origin, inferred, err := inferPreflightSchema(ctx, spec)
		if err != nil {
			result.Issues = append(result.Issues, PreflightIssue{
				Level:       "error",
				Check:       "schema-infer",
				Message:     fmt.Sprintf("source %q fallback schema inference failed: %v", spec.Source.Type, err),
				Remediation: "fix source.config.schema/source.config.sample, or remove the invalid explicit schema hint",
			})
			result.Passed = false
			return
		}
		if !inferred {
			addPreflightGuidance(result, PreflightGuidance{
				Level:    "warning",
				Category: "schema",
				Code:     "schema-source-introspection-unavailable",
				Message:  fmt.Sprintf("source %q does not expose schema introspection for sink %q preflight", spec.Source.Type, spec.Sink.Type),
				Action:   "provide source.config.schema or source.config.sample, run transform dry-run with a real sample, and verify the target schema before starting",
			})
			return
		}
		addPreflightGuidance(result, PreflightGuidance{
			Level:    "info",
			Category: "schema",
			Code:     "schema-fallback-inferred",
			Message:  fmt.Sprintf("preflight inferred source schema from %s for source %q", origin, spec.Source.Type),
			Action:   "review the generated DDL preview and field issues because sample-based schema can miss optional or late-arriving fields",
		})
		result.DDLPreview = buildPreflightDDLPreview(spec, schema)
		validatePreflightSchema(ctx, spec, validator, schema, result)
		return
	}

	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	schema, err := descriptor.Describe(probeCtx)
	if err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "schema-describe",
			Message:     fmt.Sprintf("source %q schema description failed: %v", spec.Source.Type, err),
			Remediation: "verify the source table/query is reachable and the source credentials can read schema metadata",
		})
		result.Passed = false
		return
	}
	if len(schema.Columns) == 0 {
		return
	}

	result.DDLPreview = buildPreflightDDLPreview(spec, schema)
	validatePreflightSchema(probeCtx, spec, validator, schema, result)
}

func validatePreflightSchema(ctx context.Context, spec *pipeline.Spec, validator core.SchemaValidator, schema core.SchemaInfo, result *PreflightResult) {
	if err := validator.ValidateSchema(ctx, schema); err != nil {
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       "error",
			Check:       "schema-compatibility",
			Message:     fmt.Sprintf("source %q is not compatible with sink %q: %v", spec.Source.Type, spec.Sink.Type, err),
			Remediation: "update the target schema, enable auto_create/schema_drift when supported, or add a transform/type_convert step",
		})
		result.FieldIssues = append(result.FieldIssues, parseSchemaFieldIssues(err.Error(), schema)...)
		result.Passed = false
	}
}

func inferPreflightSchema(ctx context.Context, spec *pipeline.Spec) (core.SchemaInfo, string, bool, error) {
	if spec == nil {
		return core.SchemaInfo{}, "", false, nil
	}
	cfg := spec.Source.Config
	if raw, exists := cfg["schema"]; exists && isStructuredSchemaHint(raw) {
		cols, err := columnsFromSchemaHint(raw)
		if err != nil {
			return core.SchemaInfo{}, "source.config.schema", false, err
		}
		if len(cols) == 0 {
			return core.SchemaInfo{}, "source.config.schema", false, fmt.Errorf("source.config.schema has no columns")
		}
		return core.SchemaInfo{Columns: cols}, "source.config.schema", true, nil
	}
	if raw, exists := cfg["fields"]; exists && sourceFieldsAreSchemaHints(spec.Source.Type) {
		cols, err := columnsFromSchemaHint(raw)
		if err != nil {
			return core.SchemaInfo{}, "source.config.fields", false, err
		}
		if len(cols) > 0 {
			return core.SchemaInfo{Columns: cols}, "source.config.fields", true, nil
		}
	}
	for _, key := range []string{"sample", "sample_record", "sample_records"} {
		raw, exists := cfg[key]
		if !exists {
			continue
		}
		sample, err := samplesFromSchemaHint(raw)
		if err != nil {
			return core.SchemaInfo{}, "source.config." + key, false, err
		}
		cols := columnMetadataToCore(schemaFromSamples(sample))
		if len(cols) == 0 {
			return core.SchemaInfo{}, "source.config." + key, false, fmt.Errorf("sample has no data fields")
		}
		return core.SchemaInfo{Columns: cols}, "source.config." + key, true, nil
	}
	if strings.EqualFold(spec.Source.Type, "file") {
		probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		cols, _, err := introspectFileSource(probeCtx, cfg)
		if err == nil && len(cols) > 0 {
			return core.SchemaInfo{Columns: columnMetadataToCore(cols)}, "file sample", true, nil
		}
	}
	return core.SchemaInfo{}, "", false, nil
}

func isStructuredSchemaHint(raw any) bool {
	switch v := raw.(type) {
	case []any, []string, map[string]any:
		return true
	case string:
		trimmed := strings.TrimSpace(v)
		return strings.HasPrefix(trimmed, "{") || strings.HasPrefix(trimmed, "[")
	default:
		return false
	}
}

func sourceFieldsAreSchemaHints(sourceType string) bool {
	switch strings.ToLower(sourceType) {
	case "demo", "file", "http", "kafka":
		return true
	default:
		return false
	}
}

func columnsFromSchemaHint(raw any) ([]core.ColumnInfo, error) {
	raw, err := decodeJSONStringHint(raw)
	if err != nil {
		return nil, err
	}
	var cols []core.ColumnInfo
	switch v := raw.(type) {
	case []any:
		for _, item := range v {
			col, ok, err := columnFromSchemaHintItem(item)
			if err != nil {
				return nil, err
			}
			if ok {
				cols = append(cols, col)
			}
		}
	case map[string]any:
		if _, hasName := v["name"]; hasName {
			col, ok, err := columnFromSchemaHintItem(v)
			if err != nil {
				return nil, err
			}
			if ok {
				cols = append(cols, col)
			}
			break
		}
		names := make([]string, 0, len(v))
		for name := range v {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			cols = append(cols, core.ColumnInfo{Name: name, DataType: fmt.Sprint(v[name]), Nullable: true})
		}
	case []string:
		for _, name := range v {
			if strings.TrimSpace(name) != "" {
				cols = append(cols, core.ColumnInfo{Name: strings.TrimSpace(name), DataType: "string", Nullable: true})
			}
		}
	default:
		return nil, fmt.Errorf("unsupported schema hint type %T", raw)
	}
	return cols, nil
}

func columnFromSchemaHintItem(raw any) (core.ColumnInfo, bool, error) {
	switch v := raw.(type) {
	case string:
		name := strings.TrimSpace(v)
		if name == "" {
			return core.ColumnInfo{}, false, nil
		}
		return core.ColumnInfo{Name: name, DataType: "string", Nullable: true}, true, nil
	case map[string]any:
		rawName, hasName := v["name"]
		name := strings.TrimSpace(fmt.Sprint(rawName))
		if !hasName || name == "" || name == "<nil>" {
			return core.ColumnInfo{}, false, fmt.Errorf("schema column is missing name")
		}
		typ := strings.TrimSpace(fmt.Sprint(firstNonEmpty(v["data_type"], v["type"], v["db_type"])))
		if typ == "" {
			typ = "string"
		}
		nullable := true
		if rawNullable, ok := v["nullable"].(bool); ok {
			nullable = rawNullable
		}
		return core.ColumnInfo{Name: name, DataType: typ, Nullable: nullable}, true, nil
	default:
		return core.ColumnInfo{}, false, fmt.Errorf("unsupported schema column item type %T", raw)
	}
}

func samplesFromSchemaHint(raw any) ([]map[string]any, error) {
	raw, err := decodeJSONStringHint(raw)
	if err != nil {
		return nil, err
	}
	switch v := raw.(type) {
	case []any:
		samples := make([]map[string]any, 0, len(v))
		for _, item := range v {
			rec, ok, err := sampleRecordFromHint(item)
			if err != nil {
				return nil, err
			}
			if ok {
				samples = append(samples, rec)
			}
		}
		return samples, nil
	case map[string]any:
		rec, ok, err := sampleRecordFromHint(v)
		if err != nil || !ok {
			return nil, err
		}
		return []map[string]any{rec}, nil
	default:
		return nil, fmt.Errorf("unsupported sample hint type %T", raw)
	}
}

func sampleRecordFromHint(raw any) (map[string]any, bool, error) {
	m, ok := raw.(map[string]any)
	if !ok {
		return nil, false, fmt.Errorf("sample record must be an object, got %T", raw)
	}
	if data, ok := m["data"].(map[string]any); ok {
		return map[string]any{"operation": "INSERT", "data": data}, true, nil
	}
	data := make(map[string]any, len(m))
	for k, v := range m {
		switch k {
		case "operation", "metadata", "before":
			continue
		default:
			data[k] = v
		}
	}
	if len(data) == 0 {
		return nil, false, nil
	}
	return map[string]any{"operation": "INSERT", "data": data}, true, nil
}

func decodeJSONStringHint(raw any) (any, error) {
	s, ok := raw.(string)
	if !ok {
		return raw, nil
	}
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return nil, fmt.Errorf("empty JSON schema hint")
	}
	if !strings.HasPrefix(trimmed, "{") && !strings.HasPrefix(trimmed, "[") {
		return raw, nil
	}
	var decoded any
	if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
		return nil, err
	}
	return decoded, nil
}

func columnMetadataToCore(cols []columnMetadata) []core.ColumnInfo {
	out := make([]core.ColumnInfo, 0, len(cols))
	for _, col := range cols {
		if strings.TrimSpace(col.Name) == "" {
			continue
		}
		typ := strings.TrimSpace(col.DataType)
		if typ == "" {
			typ = "string"
		}
		out = append(out, core.ColumnInfo{Name: col.Name, DataType: typ, Nullable: col.Nullable})
	}
	return out
}

func firstNonEmpty(values ...any) any {
	for _, value := range values {
		if strings.TrimSpace(fmt.Sprint(value)) != "" && fmt.Sprint(value) != "<nil>" {
			return value
		}
	}
	return ""
}

func addOperationalGuidance(spec *pipeline.Spec, result *PreflightResult) {
	if spec == nil || result == nil {
		return
	}
	addPreflightGuidance(result, PreflightGuidance{
		Level:    "info",
		Category: "delivery",
		Code:     "delivery-at-least-once",
		Message:  "pipelines use at-least-once delivery; records may replay after restart, checkpoint reset, or transient sink failure",
		Action:   "use business keys with upsert/replace, ReplacingMergeTree-style sinks, or an explicit deduplicate transform when duplicates are not acceptable",
	})

	if isStreamingOrReplaySource(spec.Source.Type) {
		addPreflightGuidance(result, PreflightGuidance{
			Level:    "info",
			Category: "checkpoint",
			Code:     "checkpoint-bounds-replay",
			Message:  fmt.Sprintf("source %q can replay from the last committed checkpoint", spec.Source.Type),
			Action:   "keep checkpoint_interval_sec and batch_size small enough for the duplicate/replay window your sink can absorb",
		})
	}

	if guidance := replayAbsorptionGuidance(spec); guidance.Code != "" {
		addPreflightGuidance(result, guidance)
	}

	// pre_write streaming safety warning (MySQL/PostgreSQL sink)
	if spec.Sink.Type == "mysql" || spec.Sink.Type == "postgres" || spec.Sink.Type == "postgresql" {
		if rawPW, ok := spec.Sink.Config["pre_write"]; ok && rawPW != nil {
			if pwMap, ok := rawPW.(map[string]any); ok {
				action, _ := pwMap["action"].(string)
				if action == "truncate" || action == "truncate_partition" {
					level := "warning"
					if isStreamingOrReplaySource(spec.Source.Type) {
						level = "error"
					}
					tableName, _ := spec.Sink.Config["table"].(string)
					if tableName == "" {
						tableName = "<dynamic>"
					}
					msg := fmt.Sprintf("pre_write action %q on %s wipes target data before each batch", action, tableName)
					if isStreamingOrReplaySource(spec.Source.Type) {
						msg = fmt.Sprintf("pre_write action %q is unsafe for CDC/streaming source %s: it wipes the target on every batch", action, spec.Source.Type)
					}
					addPreflightGuidance(result, PreflightGuidance{
						Level:    level,
						Category: "pre_write",
						Code:     "pre-write-idempotency",
						Message:  msg,
						Action:   "only use pre_write with once/cron/periodic batch pipelines; rely on upsert+pk_columns for CDC replay absorption",
					})
				}
			}
		}
	}

	addPreflightGuidance(result, PreflightGuidance{
		Level:    "info",
		Category: "dlq",
		Code:     "dlq-linear-replay",
		Message:  "failed records are persisted to the configured SQL storage backend and can be replayed for linear pipelines",
		Action:   "review DLQ before checkpoint reset or bulk replay; DAG DLQ entries currently require manual recovery because node-level replay is not implemented",
	})
}

func addConnectorReadiness(spec *pipeline.Spec, result *PreflightResult) {
	if spec == nil || result == nil {
		return
	}
	for _, ref := range []struct {
		kind string
		typ  string
	}{
		{kind: "source", typ: spec.Source.Type},
		{kind: "sink", typ: spec.Sink.Type},
	} {
		desc, ok := findConnectorDescriptor(ref.kind, ref.typ)
		if !ok {
			continue
		}
		result.Readiness = append(result.Readiness, PreflightConnectorReadiness{
			Kind:     desc.Kind,
			Type:     desc.Type,
			Maturity: desc.Maturity,
			Status:   desc.Readiness.Status,
			Summary:  desc.Readiness.Summary,
			Gates:    desc.Readiness.Gates,
		})
		for _, gate := range desc.Readiness.Gates {
			if gate.Status != "missing" && gate.Status != "partial" {
				continue
			}
			addPreflightGuidance(result, PreflightGuidance{
				Level:    "warning",
				Category: "readiness",
				Code:     fmt.Sprintf("readiness-%s-%s-%s", desc.Kind, desc.Type, gate.Code),
				Message:  fmt.Sprintf("%s %q readiness gate %q is %s: %s", desc.Kind, desc.Type, gate.Label, gate.Status, gate.Evidence),
				Action:   gate.Remediation,
			})
		}
	}
}

func findConnectorDescriptor(kind, typ string) (ConnectorDescriptor, bool) {
	for _, desc := range connectorDescriptors() {
		if desc.Kind == kind && desc.Type == typ {
			return desc, true
		}
	}
	return ConnectorDescriptor{}, false
}

func addFirstRunRecommendations(spec *pipeline.Spec, result *PreflightResult) {
	if spec == nil || result == nil {
		return
	}
	sourceType := strings.ToLower(spec.Source.Type)
	sinkType := strings.ToLower(spec.Sink.Type)

	if spec.DLQ == nil || !spec.DLQ.Enable {
		addPreflightRecommendation(result, PreflightRecommendation{
			Path:   "dlq.enable",
			Value:  true,
			Reason: "Enable DLQ so failed records stay visible and replayable instead of becoming an operational blind spot.",
			Safety: "safe",
		})
	}

	if isStreamingOrReplaySource(sourceType) {
		if spec.BatchSize == 0 || spec.BatchSize > 500 {
			addPreflightRecommendation(result, PreflightRecommendation{
				Path:   "batch_size",
				Value:  500,
				Reason: "Bound streaming batches to reduce the duplicate window after sink failure or restart.",
				Safety: "safe",
			})
		}
		if spec.CheckpointIntervalSec == 0 || spec.CheckpointIntervalSec > 10 {
			addPreflightRecommendation(result, PreflightRecommendation{
				Path:   "checkpoint_interval_sec",
				Value:  10,
				Reason: "Commit checkpoints frequently enough to keep at-least-once replay bounded.",
				Safety: "safe",
			})
		}
	}

	switch sinkType {
	case "mysql", "postgres", "postgresql", "doris":
		mode := strings.ToLower(stringField(spec.Sink.Config, "batch_mode", ""))
		if mode != "upsert" && mode != "replace" {
			addPreflightRecommendation(result, PreflightRecommendation{
				Path:   "sink.config.batch_mode",
				Value:  "upsert",
				Reason: "Use upsert mode so replayed records update the same business rows instead of inserting duplicates.",
				Safety: "review",
			})
		}
		// CDC pipelines with pk_columns_from_metadata derive keys per-table at
		// runtime, so a static pk_columns recommendation is not actionable.
		if !sinkDerivesPKFromMetadata(spec) && len(stringSliceField(spec.Sink.Config, "pk_columns")) == 0 {
			addPreflightRecommendation(result, PreflightRecommendation{
				Path:   "sink.config.pk_columns",
				Value:  preferredReplayKeyColumns(spec),
				Reason: "Set stable business keys before relying on upsert/replay absorption. Preflight prefers source pk/cursor/key hints when available.",
				Safety: "review",
			})
		}
	case "clickhouse":
		if !sinkDerivesPKFromMetadata(spec) && len(stringSliceField(spec.Sink.Config, "pk_columns")) == 0 {
			addPreflightRecommendation(result, PreflightRecommendation{
				Path:   "sink.config.pk_columns",
				Value:  preferredReplayKeyColumns(spec),
				Reason: "ClickHouse replay absorption depends on a stable ORDER BY/business key. Preflight prefers source pk/cursor/key hints when available.",
				Safety: "review",
			})
		}
		if strings.TrimSpace(stringField(spec.Sink.Config, "version_column", "")) == "" {
			addPreflightRecommendation(result, PreflightRecommendation{
				Path:   "sink.config.version_column",
				Value:  "_version",
				Reason: "A version column keeps replay/update ordering explicit for ReplacingMergeTree-style writes.",
				Safety: "review",
			})
		}
	case "elasticsearch", "es":
		if strings.TrimSpace(stringField(spec.Sink.Config, "id_column", "")) == "" {
			addPreflightRecommendation(result, PreflightRecommendation{
				Path:   "sink.config.id_column",
				Value:  "id",
				Reason: "A deterministic document id makes replay overwrite the same document instead of creating duplicates.",
				Safety: "review",
			})
		}
	case "file_sink":
		if strings.TrimSpace(stringField(spec.Sink.Config, "prefix", "")) == "" {
			addPreflightRecommendation(result, PreflightRecommendation{
				Path:   "sink.config.prefix",
				Value:  safeOutputPrefix(spec.Name, "_"),
				Reason: "Use a pipeline-specific file prefix so replayed batches and DLQ replays are easy to isolate and audit.",
				Safety: "safe",
			})
		}
	case "s3":
		if strings.TrimSpace(stringField(spec.Sink.Config, "prefix", "")) == "" {
			addPreflightRecommendation(result, PreflightRecommendation{
				Path:   "sink.config.prefix",
				Value:  safeOutputPrefix(spec.Name, "/"),
				Reason: "Use a pipeline-specific object prefix so content-addressed replay output stays isolated from other jobs.",
				Safety: "safe",
			})
		}
	case "kafka":
		if strings.TrimSpace(stringField(spec.Sink.Config, "key_column", "")) == "" {
			addPreflightRecommendation(result, PreflightRecommendation{
				Path:   "sink.config.key_column",
				Value:  preferredReplayKeyColumn(spec),
				Reason: "Use a stable Kafka message key so downstream compaction or consumers can absorb at-least-once replay. Preflight prefers source pk/cursor/key hints when available.",
				Safety: "review",
			})
		}
		if !boolField(spec.Sink.Config, "auto_create_topic", false) {
			addPreflightRecommendation(result, PreflightRecommendation{
				Path:   "sink.config.auto_create_topic",
				Value:  true,
				Reason: "Enable topic creation for first-run smoke tests, or create the topic explicitly before production use.",
				Safety: "review",
			})
		}
	}

	addSchemaFieldRecommendations(spec, result)
}

func preferredReplayKeyColumns(spec *pipeline.Spec) []string {
	return []string{preferredReplayKeyColumn(spec)}
}

func preferredReplayKeyColumn(spec *pipeline.Spec) string {
	if spec == nil {
		return "id"
	}
	for _, field := range []string{"pk_column", "cursor_column", "key_column"} {
		if value := strings.TrimSpace(stringField(spec.Source.Config, field, "")); value != "" {
			return value
		}
	}
	return "id"
}

func safeOutputPrefix(name, suffix string) string {
	name = strings.ToLower(strings.TrimSpace(name))
	var b strings.Builder
	lastDash := false
	for _, r := range name {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	out := strings.Trim(b.String(), "-")
	if out == "" {
		out = "pipeline"
	}
	return out + suffix
}

func addPreflightRecommendation(result *PreflightResult, recommendation PreflightRecommendation) {
	if result == nil || recommendation.Path == "" {
		return
	}
	for _, existing := range result.Recommendations {
		if existing.Path == recommendation.Path {
			return
		}
	}
	result.Recommendations = append(result.Recommendations, recommendation)
}

func addSchemaFieldRecommendations(spec *pipeline.Spec, result *PreflightResult) {
	if spec == nil || result == nil || len(result.FieldIssues) == 0 {
		return
	}
	missingTargetColumn := false
	conversions := map[string]string{}
	for _, issue := range result.FieldIssues {
		switch issue.Check {
		case "schema-field-missing":
			missingTargetColumn = true
		case "schema-field-type":
			if target := typeConvertTarget(issue.TargetType); target != "" {
				conversions[issue.Field] = target
			}
		}
	}
	if missingTargetColumn && sinkSupportsSchemaDriftAddColumns(spec.Sink.Type) && strings.ToLower(stringField(spec.Sink.Config, "schema_drift", "")) != "add_columns" {
		addPreflightRecommendation(result, PreflightRecommendation{
			Path:   "sink.config.schema_drift",
			Value:  "add_columns",
			Reason: "Allow the sink to add missing target columns reported by schema preflight.",
			Safety: "review",
		})
	}
	if len(conversions) > 0 {
		nextTransforms, changed := transformsWithTypeConversions(spec.Transforms, conversions)
		if changed {
			addPreflightRecommendation(result, PreflightRecommendation{
				Path:   "transforms",
				Value:  nextTransforms,
				Reason: "Add or update a type_convert transform so source fields match target column types before the sink.",
				Safety: "review",
			})
		}
	}
}

func sinkSupportsSchemaDriftAddColumns(sinkType string) bool {
	switch strings.ToLower(sinkType) {
	case "mysql", "postgres", "postgresql", "clickhouse", "doris", "jdbc":
		return true
	default:
		return false
	}
}

func typeConvertTarget(targetType string) string {
	switch preflightTypeFamily(targetType) {
	case "int", "uint":
		return "int"
	case "float", "decimal":
		return "float"
	case "bool":
		return "bool"
	case "date", "time":
		return "datetime"
	case "string":
		return "string"
	default:
		return ""
	}
}

func transformsWithTypeConversions(transforms []pipeline.TransformSpec, conversions map[string]string) ([]pipeline.TransformSpec, bool) {
	if len(conversions) == 0 {
		return transforms, false
	}
	next := make([]pipeline.TransformSpec, len(transforms))
	changed := false
	found := false
	for i, transform := range transforms {
		next[i] = pipeline.TransformSpec{
			Type:          transform.Type,
			Connection:    transform.Connection,
			ConnectionRef: transform.ConnectionRef,
			Config:        copyMap(transform.Config),
		}
		if transform.Type != "type_convert" {
			continue
		}
		found = true
		existing := stringMapField(next[i].Config, "conversions")
		for field, target := range conversions {
			if existing[field] == target {
				continue
			}
			existing[field] = target
			changed = true
		}
		next[i].Config["conversions"] = existing
	}
	if !found {
		next = append(next, pipeline.TransformSpec{
			Type:   "type_convert",
			Config: map[string]any{"conversions": conversions},
		})
		changed = true
	}
	return next, changed
}

func copyMap(in map[string]any) map[string]any {
	if in == nil {
		return map[string]any{}
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func addPreflightGuidance(result *PreflightResult, guidance PreflightGuidance) {
	if result == nil || guidance.Code == "" {
		return
	}
	for _, existing := range result.Guidance {
		if existing.Code == guidance.Code {
			return
		}
	}
	result.Guidance = append(result.Guidance, guidance)
}

func replayAbsorptionGuidance(spec *pipeline.Spec) PreflightGuidance {
	sourceType := strings.ToLower(spec.Source.Type)
	sinkType := strings.ToLower(spec.Sink.Type)
	if isAppendOrientedSink(sinkType) {
		level := "info"
		if isStreamingOrReplaySource(sourceType) {
			level = "warning"
		}
		return PreflightGuidance{
			Level:    level,
			Category: "delivery",
			Code:     "append-only-sink-replay",
			Message:  fmt.Sprintf("sink %q is append-oriented; replayed records are written again instead of being absorbed as updates", spec.Sink.Type),
			Action:   "use this only for append-only audit/event streams, or switch to an upsert-capable sink for mutable business data",
		}
	}

	switch sinkType {
	case "mysql", "postgres", "postgresql", "doris":
		mode := strings.ToLower(stringField(spec.Sink.Config, "batch_mode", ""))
		if mode != "upsert" && mode != "replace" {
			return PreflightGuidance{
				Level:    "warning",
				Category: "delivery",
				Code:     "relational-sink-insert-mode",
				Message:  fmt.Sprintf("sink %q is not configured for upsert/replace, so reruns or replay can create duplicates or constraint errors", spec.Sink.Type),
				Action:   "set sink.config.batch_mode to upsert/replace and configure pk_columns or a stable business key",
			}
		}
	case "clickhouse":
		if len(stringSliceField(spec.Sink.Config, "pk_columns")) == 0 {
			return PreflightGuidance{
				Level:    "warning",
				Category: "delivery",
				Code:     "clickhouse-replacing-key-missing",
				Message:  "ClickHouse replay absorption depends on a stable ORDER BY/business key",
				Action:   "set sink.config.pk_columns and version_column for mutable CDC-style data",
			}
		}
	case "maxcompute", "odps":
		mode := strings.ToLower(stringField(spec.Sink.Config, "write_mode", "append"))
		if mode != "partition_overwrite" {
			return PreflightGuidance{
				Level:    "warning",
				Category: "delivery",
				Code:     "maxcompute-append-replay",
				Message:  "MaxCompute append writes can duplicate records when data is replayed",
				Action:   "use partition_overwrite only with an explicit partition replay plan, or design downstream deduplication on business keys",
			}
		}
	}
	return PreflightGuidance{}
}

func isStreamingOrReplaySource(sourceType string) bool {
	switch strings.ToLower(sourceType) {
	case "mysql_cdc", "mysql_snapshot_cdc", "postgres_cdc", "kafka", "redis":
		return true
	default:
		return false
	}
}

func isAppendOrientedSink(sinkType string) bool {
	switch strings.ToLower(sinkType) {
	case "file_sink", "s3", "kafka", "elasticsearch", "es":
		return true
	default:
		return false
	}
}

func schemaPreflightMatters(sinkType string) bool {
	switch strings.ToLower(sinkType) {
	case "mysql", "postgres", "postgresql", "clickhouse", "doris", "maxcompute", "odps", "elasticsearch", "es":
		return true
	default:
		return false
	}
}

func buildPreflightDDLPreview(spec *pipeline.Spec, schema core.SchemaInfo) *PreflightDDLPreview {
	dialect := ddlDialectForSink(spec.Sink.Type)
	if dialect == "" || len(schema.Columns) == 0 {
		return nil
	}
	table := targetTableName(spec, dialect)
	if table == "" {
		return nil
	}

	columns := append([]core.ColumnInfo(nil), schema.Columns...)
	sort.SliceStable(columns, func(i, j int) bool {
		return strings.ToLower(columns[i].Name) < strings.ToLower(columns[j].Name)
	})

	warnings := []string{}
	if !boolField(spec.Sink.Config, "auto_create", false) {
		warnings = append(warnings, "auto_create is disabled; this preview is informational and will not be applied automatically")
	}
	statement, stmtWarnings := createTablePreviewStatement(dialect, table, columns, spec.Sink.Config)
	warnings = append(warnings, stmtWarnings...)
	if statement == "" {
		return nil
	}
	return &PreflightDDLPreview{
		Dialect:    dialect,
		Table:      table,
		Statements: []string{statement},
		Warnings:   warnings,
	}
}

func ddlDialectForSink(sinkType string) string {
	switch strings.ToLower(sinkType) {
	case "mysql":
		return "mysql"
	case "postgres", "postgresql":
		return "postgresql"
	case "clickhouse":
		return "clickhouse"
	case "doris":
		return "doris"
	case "maxcompute", "odps":
		return "maxcompute"
	default:
		return ""
	}
}

func targetTableName(spec *pipeline.Spec, dialect string) string {
	cfg := spec.Sink.Config
	table := stringField(cfg, "table", "")
	if table == "" {
		return ""
	}
	switch dialect {
	case "mysql", "clickhouse", "doris":
		if database := stringField(cfg, "database", ""); database != "" {
			return database + "." + table
		}
	case "postgresql":
		schema := stringField(cfg, "schema", "public")
		return schema + "." + table
	case "maxcompute":
		if project := stringField(cfg, "project", ""); project != "" {
			return project + "." + table
		}
	}
	return table
}

func createTablePreviewStatement(dialect, table string, columns []core.ColumnInfo, cfg map[string]any) (string, []string) {
	switch dialect {
	case "mysql":
		return createRelationalDDLPreview(dialect, table, columns, "`", "`", " ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"), nil
	case "postgresql":
		return createRelationalDDLPreview(dialect, table, columns, `"`, `"`, ""), nil
	case "doris":
		stmt := createRelationalDDLPreview(dialect, table, columns, "`", "`", " ENGINE=OLAP DUPLICATE KEY() DISTRIBUTED BY HASH() BUCKETS 1")
		return stmt, []string{"Doris distribution/key model is workload-specific; review DUPLICATE KEY and distribution clauses before applying"}
	case "clickhouse":
		return createClickHouseDDLPreview(table, columns, cfg), nil
	case "maxcompute":
		return createMaxComputeDDLPreview(table, columns, cfg), nil
	default:
		return "", nil
	}
}

func createRelationalDDLPreview(dialect, table string, columns []core.ColumnInfo, quoteLeft, quoteRight, suffix string) string {
	defs := make([]string, 0, len(columns))
	for _, col := range columns {
		colType := preflightDDLType(dialect, col.DataType)
		def := fmt.Sprintf("%s %s", quoteQualifiedIdentifier(col.Name, quoteLeft, quoteRight), colType)
		if !col.Nullable {
			def += " NOT NULL"
		}
		defs = append(defs, def)
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)%s;",
		quoteQualifiedIdentifier(table, quoteLeft, quoteRight), strings.Join(defs, ",\n  "), suffix)
}

func createClickHouseDDLPreview(table string, columns []core.ColumnInfo, cfg map[string]any) string {
	versionCol := stringField(cfg, "version_column", "_version")
	defs := make([]string, 0, len(columns)+1)
	fieldSet := map[string]bool{}
	for _, col := range columns {
		fieldSet[strings.ToLower(col.Name)] = true
		defs = append(defs, fmt.Sprintf("%s %s", quoteQualifiedIdentifier(col.Name, "`", "`"), preflightDDLType("clickhouse", col.DataType)))
	}
	if !fieldSet[strings.ToLower(versionCol)] {
		defs = append(defs, fmt.Sprintf("%s Int64", quoteQualifiedIdentifier(versionCol, "`", "`")))
	}
	orderBy := "tuple()"
	pkCols := stringSliceField(cfg, "pk_columns")
	if len(pkCols) == 0 && fieldSet["id"] {
		pkCols = []string{"id"}
	}
	if len(pkCols) > 0 {
		quoted := make([]string, 0, len(pkCols))
		for _, col := range pkCols {
			quoted = append(quoted, quoteQualifiedIdentifier(col, "`", "`"))
		}
		orderBy = strings.Join(quoted, ", ")
	}
	return fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n) ENGINE = ReplacingMergeTree(%s) ORDER BY (%s);",
		quoteQualifiedIdentifier(table, "`", "`"), strings.Join(defs, ",\n  "), quoteQualifiedIdentifier(versionCol, "`", "`"), orderBy)
}

func createMaxComputeDDLPreview(table string, columns []core.ColumnInfo, cfg map[string]any) string {
	partitions := maxComputePartitionColumns(cfg)
	partitionSet := map[string]bool{}
	for _, p := range partitions {
		partitionSet[strings.ToLower(p)] = true
	}
	targetTypes := stringMapField(cfg, "columns")
	defs := make([]string, 0, len(columns))
	for _, col := range columns {
		if partitionSet[strings.ToLower(col.Name)] {
			continue
		}
		colType := targetTypes[col.Name]
		if colType == "" {
			colType = targetTypes[strings.ToLower(col.Name)]
		}
		if colType == "" {
			colType = preflightDDLType("maxcompute", col.DataType)
		}
		defs = append(defs, fmt.Sprintf("%s %s", quoteQualifiedIdentifier(col.Name, "`", "`"), colType))
	}
	if len(defs) == 0 {
		return ""
	}
	stmt := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (\n  %s\n)",
		quoteQualifiedIdentifier(table, "`", "`"), strings.Join(defs, ",\n  "))
	if len(partitions) > 0 {
		partDefs := make([]string, 0, len(partitions))
		for _, p := range partitions {
			partDefs = append(partDefs, fmt.Sprintf("%s STRING", quoteQualifiedIdentifier(p, "`", "`")))
		}
		stmt += fmt.Sprintf("\nPARTITIONED BY (\n  %s\n)", strings.Join(partDefs, ",\n  "))
	}
	return stmt + ";"
}

func maxComputePartitionColumns(cfg map[string]any) []string {
	seen := map[string]bool{}
	var out []string
	add := func(name string) {
		name = strings.TrimSpace(name)
		if name == "" || seen[strings.ToLower(name)] {
			return
		}
		seen[strings.ToLower(name)] = true
		out = append(out, name)
	}
	for k := range stringMapField(cfg, "partition") {
		add(k)
	}
	for _, field := range stringSliceField(cfg, "partition_fields") {
		add(field)
	}
	sort.Strings(out)
	return out
}

func preflightDDLType(dialect, sourceType string) string {
	family := preflightTypeFamily(sourceType)
	switch family {
	case "int":
		switch dialect {
		case "clickhouse":
			return "Int64"
		case "postgresql":
			return "BIGINT"
		default:
			return "BIGINT"
		}
	case "uint":
		switch dialect {
		case "clickhouse":
			return "UInt64"
		case "postgresql":
			return "NUMERIC(20,0)"
		default:
			return "BIGINT"
		}
	case "float":
		if dialect == "clickhouse" {
			return "Float64"
		}
		if dialect == "postgresql" {
			return "DOUBLE PRECISION"
		}
		return "DOUBLE"
	case "decimal":
		if dialect == "clickhouse" {
			return "Decimal(38,10)"
		}
		return "DECIMAL(38,10)"
	case "bool":
		switch dialect {
		case "clickhouse":
			return "UInt8"
		case "mysql":
			return "TINYINT(1)"
		default:
			return "BOOLEAN"
		}
	case "date":
		return "DATE"
	case "time":
		switch dialect {
		case "clickhouse":
			return "DateTime64(3)"
		case "postgresql":
			return "TIMESTAMP(3)"
		case "maxcompute":
			return "TIMESTAMP"
		default:
			return "DATETIME(3)"
		}
	case "json":
		switch dialect {
		case "postgresql":
			return "JSONB"
		case "clickhouse":
			return "String"
		case "maxcompute":
			return "STRING"
		default:
			return "JSON"
		}
	case "bytes":
		switch dialect {
		case "postgresql":
			return "BYTEA"
		case "clickhouse":
			return "String"
		case "maxcompute":
			return "STRING"
		default:
			return "BLOB"
		}
	default:
		switch dialect {
		case "clickhouse":
			return "String"
		case "maxcompute":
			return "STRING"
		case "postgresql":
			return "TEXT"
		default:
			return "VARCHAR(255)"
		}
	}
}

func preflightTypeFamily(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	if t == "" {
		return "string"
	}
	t = unwrapPreflightType(t, "nullable")
	t = unwrapPreflightType(t, "lowcardinality")
	if strings.HasPrefix(t, "array(") || strings.HasPrefix(t, "map(") || strings.HasPrefix(t, "tuple(") {
		return "json"
	}
	base := t
	if idx := strings.IndexAny(base, "( "); idx >= 0 {
		base = base[:idx]
	}
	base = strings.Trim(base, "`\"")
	switch base {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint", "serial", "bigserial", "int1", "int2", "int4", "int8", "int16", "int32", "int64":
		return "int"
	case "uint8", "uint16", "uint32", "uint64":
		return "uint"
	case "float", "double", "real", "float4", "float8", "float32", "float64":
		return "float"
	case "decimal", "numeric", "number", "decimal32", "decimal64", "decimal128", "decimal256":
		return "decimal"
	case "bool", "boolean":
		return "bool"
	case "date", "date32":
		return "date"
	case "datetime", "timestamp", "timestamptz", "time", "timetz", "datetime64":
		return "time"
	case "binary", "varbinary", "blob", "tinyblob", "mediumblob", "longblob", "bytea":
		return "bytes"
	case "json", "jsonb", "object":
		return "json"
	default:
		if strings.Contains(t, "unsigned") && strings.Contains(t, "int") {
			return "uint"
		}
		return "string"
	}
}

func unwrapPreflightType(t, wrapper string) string {
	prefix := wrapper + "("
	for strings.HasPrefix(t, prefix) && strings.HasSuffix(t, ")") {
		t = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(t, prefix), ")"))
	}
	return t
}

func quoteQualifiedIdentifier(name, left, right string) string {
	parts := strings.Split(name, ".")
	for i, part := range parts {
		part = strings.TrimSpace(part)
		parts[i] = left + strings.ReplaceAll(part, right, right+right) + right
	}
	return strings.Join(parts, ".")
}

func parseSchemaFieldIssues(message string, schema core.SchemaInfo) []PreflightFieldIssue {
	sourceTypes := map[string]string{}
	for _, col := range schema.Columns {
		sourceTypes[strings.ToLower(col.Name)] = col.DataType
	}
	var issues []PreflightFieldIssue
	for _, field := range bracketedCSVAfter(message, "missing target columns [") {
		issues = append(issues, PreflightFieldIssue{
			Level:       "error",
			Field:       field,
			Check:       "schema-field-missing",
			SourceType:  sourceTypes[strings.ToLower(field)],
			Message:     fmt.Sprintf("field %q is missing from the target schema", field),
			Remediation: "add the target column, enable schema_drift=add_columns when supported, or project the field out before the sink",
		})
	}
	for _, field := range bracketedCSVAfter(message, "partition field(s) [") {
		issues = append(issues, PreflightFieldIssue{
			Level:       "error",
			Field:       field,
			Check:       "schema-partition-field-missing",
			SourceType:  sourceTypes[strings.ToLower(field)],
			Message:     fmt.Sprintf("partition field %q is missing from the source schema", field),
			Remediation: "add the field with project/add_field before the sink or configure it as a static partition",
		})
	}
	for _, item := range bracketedSemicolonAfter(message, "incompatible target column types [") {
		field, sourceType, targetType := parseIncompatibleField(item)
		if field == "" {
			continue
		}
		if sourceType == "" {
			sourceType = sourceTypes[strings.ToLower(field)]
		}
		issues = append(issues, PreflightFieldIssue{
			Level:       "error",
			Field:       field,
			Check:       "schema-field-type",
			SourceType:  sourceType,
			TargetType:  targetType,
			Message:     fmt.Sprintf("field %q has incompatible source and target types", field),
			Remediation: "change the target column type or add a transform/type_convert step before the sink",
		})
	}
	return issues
}

func bracketedCSVAfter(s, marker string) []string {
	body := bracketedBodyAfter(s, marker)
	if body == "" {
		return nil
	}
	parts := strings.Split(body, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func bracketedSemicolonAfter(s, marker string) []string {
	body := bracketedBodyAfter(s, marker)
	if body == "" {
		return nil
	}
	parts := strings.Split(body, ";")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func bracketedBodyAfter(s, marker string) string {
	idx := strings.Index(s, marker)
	if idx < 0 {
		return ""
	}
	start := idx + len(marker)
	end := strings.Index(s[start:], "]")
	if end < 0 {
		return ""
	}
	return s[start : start+end]
}

func parseIncompatibleField(item string) (field, sourceType, targetType string) {
	tokens := strings.Fields(item)
	if len(tokens) == 0 {
		return "", "", ""
	}
	field = tokens[0]
	for _, token := range tokens[1:] {
		switch {
		case strings.HasPrefix(token, "source="):
			sourceType = strings.TrimPrefix(token, "source=")
		case strings.HasPrefix(token, "target="):
			targetType = strings.TrimPrefix(token, "target=")
		}
	}
	return field, sourceType, targetType
}

// ── MySQL grant checker ──────────────────────────────────────────────

var requiredCDCGrantees = []string{"REPLICATION SLAVE", "REPLICATION CLIENT", "SELECT", "RELOAD", "SHOW DATABASES"}

// checkMySQLGrants verifies the CDC user has the required replication grants.
// It queries SHOW GRANTS and matches expected strings.
func checkMySQLGrants(ctx context.Context, db *sql.DB, user, host string) []string {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SHOW GRANTS FOR '%s'@'%%'", user))
	if err != nil {
		// Try without host specifier
		rows, err = db.QueryContext(ctx, "SHOW GRANTS")
		if err != nil {
			return nil // can't verify, skip
		}
	}
	defer rows.Close()

	grantTexts := []string{}
	for rows.Next() {
		var g string
		if err := rows.Scan(&g); err == nil {
			grantTexts = append(grantTexts, strings.ToUpper(g))
		}
	}

	var missing []string
	for _, required := range requiredCDCGrantees {
		found := false
		pattern := strings.ToUpper(required)
		for _, g := range grantTexts {
			if strings.Contains(g, pattern) || strings.Contains(g, "ALL PRIVILEGES") || strings.Contains(g, "GRANT ALL") {
				found = true
				break
			}
		}
		if !found {
			missing = append(missing, required)
		}
	}
	return missing
}

// ── Config helpers ────────────────────────────────────────────────────

func stringField(cfg map[string]any, key, def string) string {
	if v, ok := cfg[key].(string); ok {
		return v
	}
	return def
}

func intField(cfg map[string]any, key string, def int) int {
	switch v := cfg[key].(type) {
	case int:
		return v
	case float64:
		return int(v)
	}
	return def
}

func boolField(cfg map[string]any, key string, def bool) bool {
	if v, ok := cfg[key].(bool); ok {
		return v
	}
	return def
}

func stringSliceField(cfg map[string]any, key string) []string {
	var result []string
	switch arr := cfg[key].(type) {
	case []string:
		result = append(result, arr...)
	case []interface{}:
		for _, v := range arr {
			if s, ok := v.(string); ok {
				result = append(result, s)
			}
		}
	}
	return result
}

func stringMapField(cfg map[string]any, key string) map[string]string {
	result := map[string]string{}
	switch m := cfg[key].(type) {
	case map[string]string:
		for k, v := range m {
			result[k] = v
		}
	case map[string]any:
		for k, v := range m {
			if s, ok := v.(string); ok {
				result[k] = s
			}
		}
	}
	return result
}

func headerMapField(cfg map[string]any, key string) map[string]string {
	result := map[string]string{}
	switch m := cfg[key].(type) {
	case map[string]string:
		for k, v := range m {
			if strings.TrimSpace(k) != "" {
				result[k] = v
			}
		}
	case map[string]any:
		for k, v := range m {
			if strings.TrimSpace(k) != "" {
				result[k] = fmt.Sprint(v)
			}
		}
	}
	return result
}
