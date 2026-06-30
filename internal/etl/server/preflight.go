package server

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/registry"
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

	// Source checks
	s.checkMySQLCDC(ctx, spec, result)

	// Sink checks
	if sink, ok := s.checkSinkReachable(ctx, spec, result); ok {
		defer func() { _ = sink.Close() }()
		s.checkSchemaCompatibility(ctx, spec, sink, result)
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
		if len(stringSliceField(spec.Sink.Config, "pk_columns")) == 0 {
			addPreflightRecommendation(result, PreflightRecommendation{
				Path:   "sink.config.pk_columns",
				Value:  []string{"id"},
				Reason: "Set stable business keys before relying on upsert/replay absorption.",
				Safety: "review",
			})
		}
	case "clickhouse":
		if len(stringSliceField(spec.Sink.Config, "pk_columns")) == 0 {
			addPreflightRecommendation(result, PreflightRecommendation{
				Path:   "sink.config.pk_columns",
				Value:  []string{"id"},
				Reason: "ClickHouse replay absorption depends on a stable ORDER BY/business key.",
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
				Value:  "id",
				Reason: "Use a stable Kafka message key so downstream compaction or consumers can absorb at-least-once replay.",
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
