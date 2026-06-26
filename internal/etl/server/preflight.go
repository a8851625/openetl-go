package server

import (
	"context"
	"database/sql"
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

// PreflightResult is the outcome of running all preflight checks.
type PreflightResult struct {
	Passed      bool                  `json:"passed"`
	Issues      []PreflightIssue      `json:"issues,omitempty"`
	FieldIssues []PreflightFieldIssue `json:"field_issues,omitempty"`
	DDLPreview  *PreflightDDLPreview  `json:"ddl_preview,omitempty"`
	Summary     string                `json:"summary"`
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

	// Source checks
	s.checkMySQLCDC(ctx, spec, result)

	// Sink checks
	if sink, ok := s.checkSinkReachable(ctx, spec, result); ok {
		defer func() { _ = sink.Close() }()
		s.checkSchemaCompatibility(ctx, spec, sink, result)
	}

	if len(result.Issues) == 0 {
		result.Summary = "all checks passed"
	} else if !result.Passed {
		result.Summary = fmt.Sprintf("%d issue(s) found", len(result.Issues))
	} else {
		result.Summary = fmt.Sprintf("%d warning(s) found", len(result.Issues))
	}
	return result
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
			check = "maxcompute-writer"
			remediation = "MaxCompute/ODPS sink is currently an experimental descriptor/schema contract; enable a build with a real MaxCompute writer before starting this pipeline"
			result.Passed = false
		}
		result.Issues = append(result.Issues, PreflightIssue{
			Level:       level,
			Check:       check,
			Message:     fmt.Sprintf("sink %q is not reachable: %v", spec.Sink.Type, err),
			Remediation: remediation,
		})
		if isMaxComputeSinkType(spec.Sink.Type) {
			// The writer is disabled, but MaxCompute's local schema and
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

	if err := validator.ValidateSchema(probeCtx, schema); err != nil {
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
