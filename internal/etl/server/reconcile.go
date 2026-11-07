package server

// Data reconciliation (对账) for pipelines.
//
// The reconciler answers a question the runner cannot: "is what the sink holds
// actually equal to what the source holds right now?" Runner stats only prove
// delivery accounting, and checkpoint/DLQ evidence only proves the pipeline's
// own bookkeeping. Reconciliation therefore compares the two databases
// directly, per table:
//
//  1. row count (always);
//  2. a content checksum over the common columns (default; `deep=false` skips);
//  3. when the two sides disagree and the table has a single numeric key, a
//     bucketed digest comparison that localises which key range drifted.
//
// The first release deliberately supports only pairs that expose a SQL query
// surface on both ends: MySQL-family sources (mysql_batch / mysql_cdc /
// mysql_snapshot_cdc) and postgres_cdc against mysql / doris / clickhouse /
// postgres targets. Kafka, file, http and object-store ends have no query
// surface, so they are rejected with an explicit reason instead of a fake
// "match".
//
// Limits are returned with every report so an operator cannot mistake it for
// an exactly-once proof; see reconcileLimits.

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

const (
	reconcileDialectMySQL      = "mysql"
	reconcileDialectDoris      = "doris" // MySQL wire protocol
	reconcileDialectPostgres   = "postgres"
	reconcileDialectClickHouse = "clickhouse"

	reconcileVerdictMatch         = "match"
	reconcileVerdictCountMismatch = "count_mismatch"
	reconcileVerdictContentDiff   = "content_mismatch"
	reconcileVerdictSchemaDiff    = "schema_mismatch"
	reconcileVerdictMissingTarget = "missing_target_table"
	reconcileVerdictUnsupported   = "unsupported"
	reconcileVerdictError         = "error"

	// reconcileNullMarker is the canonical text used for NULL so a NULL does not
	// vanish inside the concatenation. A real value that literally equals the
	// marker is indistinguishable from NULL; this is documented in the report
	// limits rather than papered over.
	reconcileNullMarker = "<NULL>"

	// reconcileRowHashColumn is the synthetic key column written by
	// mysql_snapshot_cdc no_pk_snapshot=hash targets.
	reconcileRowHashColumn = "_row_hash"

	defaultReconcileBuckets   = 16
	maxReconcileBuckets       = 64
	maxReconcileMismatchRows  = 20
	defaultReconcileTimeoutMS = 60_000
	maxReconcileTables        = 200
)

type reconcileRequest struct {
	Tables     []string          `json:"tables"`
	Deep       *bool             `json:"deep"`
	Buckets    int               `json:"buckets"`
	KeyColumns map[string]string `json:"key_columns"`
	TimeoutSec int               `json:"timeout_sec"`
	// SampleRows opts into a row-level diff: when > 0 the reconciler fetches up
	// to that many rows per side (inside the first mismatching buckets) and
	// reports which keys are missing, extra, or hold different content.
	SampleRows int `json:"sample_rows"`
}

// reconcileRowDiff is one row-level difference found by key sampling: a key that
// exists only in the source, only in the target, or in both with different
// content. It is deliberately bounded and key-addressed — it identifies what to
// fix, it is not a full table diff.
type reconcileRowDiff struct {
	Key       string `json:"key"`
	Kind      string `json:"kind"`
	SourceRow string `json:"source_row,omitempty"`
	TargetRow string `json:"target_row,omitempty"`
}

const (
	reconcileRowDiffMissingInTarget = "missing_in_target"
	reconcileRowDiffExtraInTarget   = "extra_in_target"
	reconcileRowDiffValueMismatch   = "value_mismatch"

	maxReconcileSampleRows       = 100
	maxReconcileRowDiffBuckets   = 3
	maxReconcileRowDiffTotal     = 100
	reconcileRowDiffScopeBuckets = "bucket"
	reconcileRowDiffScopeHead    = "head"
)

type reconcileBucketMismatch struct {
	Bucket       int    `json:"bucket"`
	SourceRows   int64  `json:"source_rows"`
	TargetRows   int64  `json:"target_rows"`
	SourceDigest string `json:"source_digest,omitempty"`
	TargetDigest string `json:"target_digest,omitempty"`
}

type reconcileTableReport struct {
	SourceTable          string                    `json:"source_table"`
	TargetTable          string                    `json:"target_table"`
	Verdict              string                    `json:"verdict"`
	SourceRows           int64                     `json:"source_rows"`
	TargetRows           int64                     `json:"target_rows"`
	RowDelta             int64                     `json:"row_delta"`
	SourceDigest         string                    `json:"source_digest,omitempty"`
	TargetDigest         string                    `json:"target_digest,omitempty"`
	ComparedColumns      []string                  `json:"compared_columns,omitempty"`
	TargetColumnsUsed    []string                  `json:"target_columns_used,omitempty"`
	MissingTargetColumns []string                  `json:"missing_target_columns,omitempty"`
	ExtraTargetColumns   []string                  `json:"extra_target_columns,omitempty"`
	BucketKey            string                    `json:"bucket_key,omitempty"`
	Buckets              int                       `json:"buckets,omitempty"`
	BucketMismatches     []reconcileBucketMismatch `json:"bucket_mismatches,omitempty"`
	BucketScanTruncated  bool                      `json:"bucket_scan_truncated,omitempty"`
	RowDiffScope         string                    `json:"row_diff_scope,omitempty"`
	RowDiffs             []reconcileRowDiff        `json:"row_diffs,omitempty"`
	RowDiffTruncated     bool                      `json:"row_diff_truncated,omitempty"`
	Detail               string                    `json:"detail,omitempty"`
	DurationMS           int64                     `json:"duration_ms"`
}

type reconcileSummary struct {
	Tables          int `json:"tables"`
	Matched         int `json:"matched"`
	CountMismatch   int `json:"count_mismatch"`
	ContentMismatch int `json:"content_mismatch"`
	SchemaMismatch  int `json:"schema_mismatch"`
	Errors          int `json:"errors"`
	Unsupported     int `json:"unsupported"`
}

type reconcileSideInfo struct {
	Role     string `json:"role"`
	Type     string `json:"type"`
	Dialect  string `json:"dialect"`
	Database string `json:"database"`
	Endpoint string `json:"endpoint,omitempty"`
}

type reconcileReport struct {
	PipelineID   string                 `json:"pipeline_id"`
	PipelineName string                 `json:"pipeline_name,omitempty"`
	PipelineStat string                 `json:"pipeline_status,omitempty"`
	Source       reconcileSideInfo      `json:"source"`
	Target       reconcileSideInfo      `json:"target"`
	Deep         bool                   `json:"deep"`
	Buckets      int                    `json:"buckets"`
	Tables       []reconcileTableReport `json:"tables"`
	Summary      reconcileSummary       `json:"summary"`
	Verdict      string                 `json:"verdict"`
	Limits       []string               `json:"limits"`
	StartedAt    time.Time              `json:"started_at"`
	FinishedAt   time.Time              `json:"finished_at"`
	DurationMS   int64                  `json:"duration_ms"`
}

// reconcileSide is one end of the comparison: a SQL handle plus the dialect
// rules needed to build portable count/digest statements.
type reconcileSide struct {
	info    reconcileSideInfo
	db      *sql.DB
	chCfg   map[string]any
	closers []func()
	// queryFn overrides the SQL execution path. It exists so the comparison
	// engine can be tested without live databases; production paths always
	// leave it nil.
	queryFn func(ctx context.Context, sqlText string, keys []string) ([][]string, error)
}

func (s *reconcileSide) close() {
	for _, fn := range s.closers {
		fn()
	}
	s.closers = nil
}

// query runs a statement and returns rows as ordered string cells. keys gives
// the expected column count (and, for the ClickHouse HTTP path, the JSON keys).
func (s *reconcileSide) query(ctx context.Context, sqlText string, keys []string) ([][]string, error) {
	if s.queryFn != nil {
		return s.queryFn(ctx, sqlText, keys)
	}
	if s.db != nil {
		rows, err := s.db.QueryContext(ctx, sqlText)
		if err != nil {
			return nil, err
		}
		defer rows.Close()
		var out [][]string
		for rows.Next() {
			cells := make([]any, len(keys))
			ptrs := make([]any, len(keys))
			for i := range cells {
				ptrs[i] = &cells[i]
			}
			if err := rows.Scan(ptrs...); err != nil {
				return nil, err
			}
			line := make([]string, len(cells))
			for i := range cells {
				line[i] = reconcileValueString(cells[i])
			}
			out = append(out, line)
		}
		return out, rows.Err()
	}
	if s.chCfg != nil {
		data, err := clickHouseHTTPQuery(ctx, s.chCfg, s.info.Database, sqlText+" FORMAT JSON")
		if err != nil {
			return nil, err
		}
		out := make([][]string, 0, len(data))
		for _, row := range data {
			line := make([]string, len(keys))
			for i, key := range keys {
				line[i] = reconcileValueString(row[key])
			}
			out = append(out, line)
		}
		return out, nil
	}
	return nil, fmt.Errorf("reconcile side %s is not open", s.info.Role)
}

// reconcileValueString canonicalises a scanned cell for comparison. Both sides
// run the same SQL-side text projection, so this only has to render numbers and
// bytes consistently.
func reconcileValueString(v any) string {
	switch vv := v.(type) {
	case nil:
		return ""
	case string:
		return vv
	case []byte:
		return string(vv)
	case time.Time:
		return vv.UTC().Format("2006-01-02 15:04:05.999999")
	case float64:
		if vv == float64(int64(vv)) {
			return strconv.FormatInt(int64(vv), 10)
		}
		return strconv.FormatFloat(vv, 'f', -1, 64)
	case float32:
		return reconcileValueString(float64(vv))
	case bool:
		return strconv.FormatBool(vv)
	default:
		return fmt.Sprint(vv)
	}
}

// reconcileDialectForSource maps a source type to the SQL dialect used to read
// it, or "" when the source has no SQL query surface.
func reconcileDialectForSource(sourceType string) string {
	switch strings.ToLower(strings.TrimSpace(sourceType)) {
	case "mysql_batch", "mysql_cdc", "mysql_snapshot_cdc":
		return reconcileDialectMySQL
	case "postgres_cdc":
		return reconcileDialectPostgres
	default:
		return ""
	}
}

// reconcileDialectForSink maps a sink type to the SQL dialect used to read it.
func reconcileDialectForSink(sinkType string) string {
	switch strings.ToLower(strings.TrimSpace(sinkType)) {
	case "mysql":
		return reconcileDialectMySQL
	case "doris":
		return reconcileDialectDoris
	case "postgres", "postgresql":
		return reconcileDialectPostgres
	case "clickhouse":
		return reconcileDialectClickHouse
	default:
		return ""
	}
}

// reconcileSourceTables returns the tables a source will sync, or an
// unsupported reason when the source config has no table contract.
func reconcileSourceTables(spec *pipeline.Spec) ([]string, string) {
	cfg := spec.Source.Config
	sourceType := strings.ToLower(strings.TrimSpace(spec.Source.Type))
	var tables []string
	for _, t := range stringSlice(cfg["tables"]) {
		if t = strings.TrimSpace(t); t != "" {
			tables = append(tables, t)
		}
	}
	if len(tables) == 0 {
		if t := strings.TrimSpace(str(cfg, "table")); t != "" {
			tables = append(tables, t)
		}
	}
	if len(tables) == 0 {
		switch sourceType {
		case "mysql_batch":
			if str(cfg, "query") != "" {
				return nil, "mysql_batch defines its source with a query and has no stable table contract, so reconciliation is unsupported"
			}
			return nil, "source.config declares neither table nor tables, so the tables to reconcile cannot be located"
		default:
			return nil, "source.config 未声明 table 或 tables，无法定位要对账的表"
		}
	}
	if len(tables) > maxReconcileTables {
		tables = tables[:maxReconcileTables]
	}
	return tables, ""
}

// reconcileTargetTable maps a source table to the sink table name, honouring a
// static table, a table_template, or the sink's own table_mapping rules.
func reconcileTargetTable(spec *pipeline.Spec, sourceTable string) string {
	cfg := spec.Sink.Config
	sourceDB := str(spec.Source.Config, "database")
	if spec.TableMapping != nil {
		for pattern, target := range spec.TableMapping.Rules {
			if reconcileTablePatternMatch(pattern, sourceTable) {
				return target
			}
		}
	}
	if tmpl := strings.TrimSpace(str(cfg, "table_template")); tmpl != "" {
		out := strings.ReplaceAll(tmpl, "{table}", sourceTable)
		out = strings.ReplaceAll(out, "{db}", sourceDB)
		return out
	}
	if table := strings.TrimSpace(str(cfg, "table")); table != "" {
		return table
	}
	return sourceTable
}

// reconcileTablePatternMatch implements the same simple glob the table mapping
// processor uses ("ods_*", exact match, and "*").
func reconcileTablePatternMatch(pattern, table string) bool {
	pattern = strings.TrimSpace(pattern)
	if pattern == "" {
		return false
	}
	if pattern == table || pattern == "*" {
		return true
	}
	if strings.HasSuffix(pattern, "*") {
		return strings.HasPrefix(table, strings.TrimSuffix(pattern, "*"))
	}
	if strings.HasPrefix(pattern, "*") {
		return strings.HasSuffix(table, strings.TrimPrefix(pattern, "*"))
	}
	return false
}

// ── SQL generation ───────────────────────────────────────────────────────────

func reconcileQuoteIdent(dialect, ident string) string {
	switch dialect {
	case reconcileDialectPostgres:
		return `"` + strings.ReplaceAll(ident, `"`, `""`) + `"`
	default:
		return "`" + strings.ReplaceAll(ident, "`", "``") + "`"
	}
}

// reconcileTableRef renders a (possibly schema-qualified) table reference.
// defaultSchema only applies to PostgreSQL, where an unqualified table lives in
// a schema rather than the connection's default database.
func reconcileTableRef(dialect, defaultSchema, table string) string {
	table = strings.TrimSpace(table)
	if i := strings.LastIndex(table, "."); i > 0 {
		return reconcileQuoteIdent(dialect, table[:i]) + "." + reconcileQuoteIdent(dialect, table[i+1:])
	}
	if dialect == reconcileDialectPostgres {
		schema := strings.TrimSpace(defaultSchema)
		if schema == "" {
			schema = "public"
		}
		return reconcileQuoteIdent(dialect, schema) + "." + reconcileQuoteIdent(dialect, table)
	}
	return reconcileQuoteIdent(dialect, table)
}

func reconcileSeparator(dialect string) string {
	switch dialect {
	case reconcileDialectPostgres:
		return "chr(1)"
	case reconcileDialectClickHouse:
		return "char(1)"
	default:
		return "CHAR(1)"
	}
}

// reconcileNullLiteral renders the canonical NULL marker. The marker contains
// no quotes or backslashes, so one literal form is valid in every supported
// dialect.
func reconcileNullLiteral() string {
	return "'" + reconcileNullMarker + "'"
}

func reconcileTextExpr(dialect, column string) string {
	q := reconcileQuoteIdent(dialect, column)
	null := reconcileNullLiteral()
	switch dialect {
	case reconcileDialectPostgres:
		return fmt.Sprintf("COALESCE(%s::text, %s)", q, null)
	case reconcileDialectClickHouse:
		return fmt.Sprintf("coalesce(toString(%s), %s)", q, null)
	default:
		return fmt.Sprintf("COALESCE(CAST(%s AS CHAR), %s)", q, null)
	}
}

func reconcileCountSQL(dialect, tableRef string) string {
	return fmt.Sprintf("SELECT COUNT(*) FROM %s", tableRef)
}

// reconcileDigestSQL builds the whole-table digest: row count plus the sum of
// per-row hashes over the canonical text projection of the compared columns.
func reconcileDigestSQL(dialect, tableRef string, columns []string) string {
	parts := make([]string, 0, len(columns))
	for _, c := range columns {
		parts = append(parts, reconcileTextExpr(dialect, c))
	}
	sep := reconcileSeparator(dialect)
	var sum string
	switch dialect {
	case reconcileDialectPostgres:
		sum = fmt.Sprintf("COALESCE(SUM(hashtextextended(concat_ws(%s, %s), 0)), 0)", sep, strings.Join(parts, ", "))
	case reconcileDialectClickHouse:
		sum = fmt.Sprintf("COALESCE(sum(sipHash64(arrayStringConcat([%s], %s))), 0)", strings.Join(parts, ", "), sep)
	default:
		sum = fmt.Sprintf("COALESCE(SUM(CRC32(CONCAT_WS(%s, %s))), 0)", sep, strings.Join(parts, ", "))
	}
	return fmt.Sprintf("SELECT COUNT(*), %s FROM %s", sum, tableRef)
}

// reconcileBucketExpr buckets rows by the modulo of a numeric key so both sides
// land in the same bucket without needing identical hash functions.
func reconcileBucketExpr(dialect, keyColumn string, buckets int) string {
	if buckets <= 1 {
		buckets = 1
	}
	q := reconcileQuoteIdent(dialect, keyColumn)
	switch dialect {
	case reconcileDialectClickHouse:
		return fmt.Sprintf("modulo(toInt64(%s), %d)", q, buckets)
	default:
		return fmt.Sprintf("MOD(%s, %d)", q, buckets)
	}
}

// reconcileBucketSQL builds the per-bucket digest statement. The bucket column
// is always the first projection so both sides sort identically.
func reconcileBucketSQL(dialect, tableRef string, columns []string, keyColumn string, buckets int) string {
	digest := reconcileDigestSQL(dialect, tableRef, columns)
	bucket := reconcileBucketExpr(dialect, keyColumn, buckets)
	// INSERT the bucket projection: SELECT <bucket>, COUNT(*), <sum> ...
	rest := strings.TrimPrefix(digest, "SELECT ")
	// MySQL requires the projected expression to be repeated in GROUP BY.
	groupBy := bucket
	return fmt.Sprintf("SELECT %s, %s GROUP BY %s ORDER BY 1", bucket, rest, groupBy)
}

// ── side opening ─────────────────────────────────────────────────────────────

func (s *Server) openReconcileSource(ctx context.Context, spec *pipeline.Spec) (*reconcileSide, error) {
	dialect := reconcileDialectForSource(spec.Source.Type)
	if dialect == "" {
		return nil, fmt.Errorf("source type %q has no SQL query surface, so reconciliation is unsupported (supported sources: mysql_batch, mysql_cdc, mysql_snapshot_cdc, postgres_cdc)", spec.Source.Type)
	}
	cfg := spec.Source.Config
	side := &reconcileSide{info: reconcileSideInfo{
		Role:     "source",
		Type:     spec.Source.Type,
		Dialect:  dialect,
		Database: str(cfg, "database"),
	}}
	switch dialect {
	case reconcileDialectMySQL:
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&timeout=5s",
			str(cfg, "user"), str(cfg, "password"), strDefault(cfg, "host", "localhost"),
			intDefault(cfg, "port", 3306), side.info.Database)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return nil, fmt.Errorf("打开 MySQL 源连接失败: %w", err)
		}
		side.db = db
		side.closers = append(side.closers, func() { _ = db.Close() })
		side.info.Endpoint = fmt.Sprintf("%s:%d", strDefault(cfg, "host", "localhost"), intDefault(cfg, "port", 3306))
	case reconcileDialectPostgres:
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			str(cfg, "user"), str(cfg, "password"), strDefault(cfg, "host", "localhost"),
			intDefault(cfg, "port", 5432), side.info.Database, strDefault(cfg, "sslmode", "prefer"))
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return nil, fmt.Errorf("打开 PostgreSQL 源连接失败: %w", err)
		}
		side.db = db
		side.closers = append(side.closers, func() { _ = db.Close() })
		side.info.Endpoint = fmt.Sprintf("%s:%d", strDefault(cfg, "host", "localhost"), intDefault(cfg, "port", 5432))
	}
	if side.db != nil {
		pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := side.db.PingContext(pingCtx); err != nil {
			side.close()
			return nil, fmt.Errorf("连接源库失败（%s）: %w", side.info.Endpoint, err)
		}
	}
	return side, nil
}

func (s *Server) openReconcileTarget(ctx context.Context, spec *pipeline.Spec) (*reconcileSide, error) {
	dialect := reconcileDialectForSink(spec.Sink.Type)
	if dialect == "" {
		return nil, fmt.Errorf("sink type %q has no SQL query surface, so reconciliation is unsupported (supported sinks: mysql, doris, postgres, postgresql, clickhouse)", spec.Sink.Type)
	}
	cfg := spec.Sink.Config
	side := &reconcileSide{info: reconcileSideInfo{
		Role:     "target",
		Type:     spec.Sink.Type,
		Dialect:  dialect,
		Database: str(cfg, "database"),
	}}
	switch dialect {
	case reconcileDialectMySQL, reconcileDialectDoris:
		port := 3306
		if dialect == reconcileDialectDoris {
			port = 9030
		}
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&timeout=5s",
			strDefault(cfg, "user", "root"), str(cfg, "password"), strDefault(cfg, "host", "localhost"),
			intDefault(cfg, "port", port), side.info.Database)
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return nil, fmt.Errorf("打开目标连接失败: %w", err)
		}
		side.db = db
		side.closers = append(side.closers, func() { _ = db.Close() })
		side.info.Endpoint = fmt.Sprintf("%s:%d", strDefault(cfg, "host", "localhost"), intDefault(cfg, "port", port))
	case reconcileDialectPostgres:
		dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
			str(cfg, "user"), str(cfg, "password"), strDefault(cfg, "host", "localhost"),
			intDefault(cfg, "port", 5432), side.info.Database, strDefault(cfg, "sslmode", "prefer"))
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return nil, fmt.Errorf("打开目标连接失败: %w", err)
		}
		side.db = db
		side.closers = append(side.closers, func() { _ = db.Close() })
		side.info.Endpoint = fmt.Sprintf("%s:%d", strDefault(cfg, "host", "localhost"), intDefault(cfg, "port", 5432))
	case reconcileDialectClickHouse:
		if strings.EqualFold(str(cfg, "protocol"), "http") || strings.HasPrefix(str(cfg, "host"), "http://") || strings.HasPrefix(str(cfg, "host"), "https://") {
			side.chCfg = cfg
			side.info.Endpoint = clickHouseHTTPBaseURL(cfg)
			probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			defer cancel()
			if _, err := side.query(probeCtx, "SELECT 1 AS ok", []string{"ok"}); err != nil {
				return nil, fmt.Errorf("连接 ClickHouse（HTTP）失败: %w", err)
			}
		} else {
			dbName := strDefault(cfg, "database", "default")
			if side.info.Database == "" {
				side.info.Database = dbName
			}
			dsn := clickHouseSQLDSN(cfg, dbName)
			db, err := sql.Open("clickhouse", dsn)
			if err != nil {
				return nil, fmt.Errorf("打开 ClickHouse（native）连接失败: %w", err)
			}
			side.db = db
			side.closers = append(side.closers, func() { _ = db.Close() })
			side.info.Endpoint = fmt.Sprintf("%s:%d", strDefault(cfg, "host", "localhost"), intDefault(cfg, "port", 9000))
		}
	}
	if side.db != nil {
		pingCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := side.db.PingContext(pingCtx); err != nil {
			side.close()
			return nil, fmt.Errorf("连接目标库失败（%s）: %w", side.info.Endpoint, err)
		}
	}
	return side, nil
}

// reconcileColumns reads the column list of one table on one side.
func reconcileColumns(ctx context.Context, side *reconcileSide, table string) ([]columnMetadata, []string, error) {
	if side.db != nil {
		switch side.info.Dialect {
		case reconcileDialectPostgres:
			return postgresColumns(ctx, side.db, side.info.Database, table)
		case reconcileDialectClickHouse:
			return clickHouseColumns(ctx, side.db, side.info.Database, table)
		default:
			return mysqlColumns(ctx, side.db, side.info.Database, table)
		}
	}
	if side.chCfg != nil {
		return clickHouseHTTPColumns(ctx, side.chCfg, side.info.Database, stripSchema(table))
	}
	return nil, nil, fmt.Errorf("reconcile side %s is not open", side.info.Role)
}

// ── per-table reconciliation ─────────────────────────────────────────────────

func (s *Server) reconcileOneTable(ctx context.Context, src, tgt *reconcileSide, spec *pipeline.Spec, sourceTable string, req reconcileRequest, buckets int) reconcileTableReport {
	started := time.Now()
	report := reconcileTableReport{
		SourceTable: sourceTable,
		TargetTable: reconcileTargetTable(spec, sourceTable),
	}
	defer func() { report.DurationMS = time.Since(started).Milliseconds() }()

	lookupColumns := reconcileColumns
	if s.reconcileColumnsFn != nil {
		lookupColumns = s.reconcileColumnsFn
	}
	srcCols, srcPK, err := lookupColumns(ctx, src, sourceTable)
	if err != nil {
		report.Verdict = reconcileVerdictError
		report.Detail = fmt.Sprintf("读取源表列失败: %v", err)
		return report
	}
	if len(srcCols) == 0 {
		report.Verdict = reconcileVerdictError
		report.Detail = fmt.Sprintf("源表 %s 不存在或没有可见列", sourceTable)
		return report
	}
	tgtCols, _, err := lookupColumns(ctx, tgt, report.TargetTable)
	if err != nil || len(tgtCols) == 0 {
		report.Verdict = reconcileVerdictMissingTarget
		report.Detail = fmt.Sprintf("目标表 %s 不存在或没有可见列", report.TargetTable)
		if err != nil {
			report.Detail = fmt.Sprintf("%s: %v", report.Detail, err)
		}
		return report
	}

	tgtNames := make(map[string]string, len(tgtCols))
	for _, c := range tgtCols {
		tgtNames[strings.ToLower(c.Name)] = c.Name
	}
	srcNames := make(map[string]struct{}, len(srcCols))
	for _, c := range srcCols {
		srcNames[strings.ToLower(c.Name)] = struct{}{}
		if targetName, ok := tgtNames[strings.ToLower(c.Name)]; ok {
			report.ComparedColumns = append(report.ComparedColumns, c.Name)
			// Column names are matched case-insensitively but referenced with
			// each side's own spelling, so a MySQL `ID` and a ClickHouse `id`
			// still compare correctly.
			report.TargetColumnsUsed = append(report.TargetColumnsUsed, targetName)
		} else {
			report.MissingTargetColumns = append(report.MissingTargetColumns, c.Name)
		}
	}
	rowHashTarget := false
	for _, c := range tgtCols {
		if strings.EqualFold(c.Name, reconcileRowHashColumn) {
			rowHashTarget = true
		}
		if _, ok := srcNames[strings.ToLower(c.Name)]; !ok {
			report.ExtraTargetColumns = append(report.ExtraTargetColumns, c.Name)
		}
	}

	srcRef := reconcileTableRef(src.info.Dialect, str(spec.Source.Config, "schema"), sourceTable)
	tgtRef := reconcileTableRef(tgt.info.Dialect, str(spec.Sink.Config, "schema"), report.TargetTable)

	if len(report.ComparedColumns) == 0 {
		report.Verdict = reconcileVerdictSchemaDiff
		report.Detail = "源表与目标表没有共同列，无法比较内容"
		return report
	}

	if rows, err := src.query(ctx, reconcileCountSQL(src.info.Dialect, srcRef), []string{"rows"}); err == nil && len(rows) == 1 {
		report.SourceRows = reconcileInt64(rows[0][0])
	} else if err != nil {
		report.Verdict = reconcileVerdictError
		report.Detail = fmt.Sprintf("统计源表行数失败: %v", err)
		return report
	}
	if rows, err := tgt.query(ctx, reconcileCountSQL(tgt.info.Dialect, tgtRef), []string{"rows"}); err == nil && len(rows) == 1 {
		report.TargetRows = reconcileInt64(rows[0][0])
	} else if err != nil {
		report.Verdict = reconcileVerdictError
		report.Detail = fmt.Sprintf("统计目标表行数失败: %v", err)
		return report
	}
	report.RowDelta = report.SourceRows - report.TargetRows

	if req.DeepOrDefault() {
		if rows, err := src.query(ctx, reconcileDigestSQL(src.info.Dialect, srcRef, report.ComparedColumns), []string{"rows", "digest"}); err == nil && len(rows) == 1 {
			report.SourceDigest = rows[0][1]
		} else if err != nil {
			report.Verdict = reconcileVerdictError
			report.Detail = fmt.Sprintf("计算源表摘要失败: %v", err)
			return report
		}
		if rows, err := tgt.query(ctx, reconcileDigestSQL(tgt.info.Dialect, tgtRef, report.TargetColumnsUsed), []string{"rows", "digest"}); err == nil && len(rows) == 1 {
			report.TargetDigest = rows[0][1]
		} else if err != nil {
			report.Verdict = reconcileVerdictError
			report.Detail = fmt.Sprintf("计算目标表摘要失败: %v", err)
			return report
		}
	}

	if rowHashTarget && report.SourceRows != report.TargetRows {
		// A no_pk_snapshot=hash table stores one row per distinct content hash, so
		// duplicate source rows legitimately collapse. Say so instead of
		// reporting an unexplained count mismatch.
		report.Detail = fmt.Sprintf("目标表由 %s 内容哈希快照写入：源端完全相同的多行会合并为一行，行数差（源 %d / 目标 %d）可能只是重复行折叠，而非丢失数据",
			reconcileRowHashColumn, report.SourceRows, report.TargetRows)
	}

	switch {
	case len(report.MissingTargetColumns) > 0:
		report.Verdict = reconcileVerdictSchemaDiff
		report.Detail = fmt.Sprintf("目标表缺少源列: %s", strings.Join(report.MissingTargetColumns, ", "))
	case report.SourceRows != report.TargetRows:
		report.Verdict = reconcileVerdictCountMismatch
		if !rowHashTarget {
			report.Detail = fmt.Sprintf("行数不一致：源 %d / 目标 %d", report.SourceRows, report.TargetRows)
		}
	case req.DeepOrDefault() && report.SourceDigest != report.TargetDigest:
		report.Verdict = reconcileVerdictContentDiff
		report.Detail = "行数一致但内容摘要不一致"
	default:
		report.Verdict = reconcileVerdictMatch
	}

	if report.Verdict != reconcileVerdictMatch && report.Verdict != reconcileVerdictSchemaDiff {
		key := s.reconcileBucketKey(spec, sourceTable, srcPK, req)
		if key == "" {
			return report
		}
		report.BucketKey = key
		report.Buckets = buckets
		report.RowDiffScope = reconcileRowDiffScopeBuckets
		mismatches, truncated, err := reconcileCompareBuckets(ctx, src, tgt, srcRef, tgtRef, report.ComparedColumns, report.TargetColumnsUsed, key, buckets)
		if err != nil {
			report.Detail = fmt.Sprintf("%s（桶定位失败: %v）", report.Detail, err)
			return report
		}
		report.BucketMismatches = mismatches
		report.BucketScanTruncated = truncated

		// Row-level diff: sample inside the mismatching buckets (or the head of
		// the table when no bucketing is possible) so an operator gets concrete
		// keys instead of only a bucket index.
		if req.SampleRows > 0 {
			limit := req.SampleRows
			if limit > maxReconcileSampleRows {
				limit = maxReconcileSampleRows
			}
			rowDiffs, rowTruncated, rowErr := reconcileSampleRowDiffs(ctx, src, tgt, srcRef, tgtRef,
				report.ComparedColumns, report.TargetColumnsUsed, key, buckets, mismatches, limit)
			if rowErr != nil {
				report.Detail = fmt.Sprintf("%s（行级差异采样失败: %v）", report.Detail, rowErr)
			} else {
				report.RowDiffs = rowDiffs
				report.RowDiffTruncated = rowTruncated
				if len(mismatches) == 0 {
					// No bucket localisation was possible: the sample covers the
					// head of the table only, which the scope makes explicit.
					report.RowDiffScope = reconcileRowDiffScopeHead
				}
			}
		}
	}
	return report
}

// reconcileSampleRowDiffs samples the mismatching buckets first (bounded), then
// falls back to the head of the table when bucketing is unavailable or every
// bucket matched. Scope is reported so the caller knows how far the sample
// reaches.
func reconcileSampleRowDiffs(ctx context.Context, src, tgt *reconcileSide, srcRef, tgtRef string,
	srcColumns, tgtColumns []string, key string, buckets int, mismatches []reconcileBucketMismatch, limit int) ([]reconcileRowDiff, bool, error) {
	if len(mismatches) == 0 {
		diffs, truncated, err := reconcileCompareRows(ctx, src, tgt, srcRef, tgtRef, srcColumns, tgtColumns, key, 1, -1, limit)
		return diffs, truncated, err
	}
	scanned := 0
	all := make([]reconcileRowDiff, 0, limit)
	truncated := false
	for _, mismatch := range mismatches {
		if scanned >= maxReconcileRowDiffBuckets || len(all) >= limit {
			truncated = true
			break
		}
		scanned++
		diffs, bucketTruncated, err := reconcileCompareRows(ctx, src, tgt, srcRef, tgtRef, srcColumns, tgtColumns, key, buckets, mismatch.Bucket, limit)
		if err != nil {
			return all, truncated, err
		}
		if bucketTruncated {
			truncated = true
		}
		for _, diff := range diffs {
			if len(all) >= limit {
				truncated = true
				break
			}
			all = append(all, diff)
		}
	}
	return all, truncated, nil
}

// reconcileBucketKey returns a numeric key column usable for bucket
// localisation: an explicit override wins, otherwise the source primary key is
// used when it is a single numeric column.
func (s *Server) reconcileBucketKey(spec *pipeline.Spec, table string, srcPK []string, req reconcileRequest) string {
	if req.KeyColumns != nil {
		if override, ok := req.KeyColumns[table]; ok {
			override = strings.TrimSpace(override)
			if safeDBIdent(override) {
				return override
			}
			return ""
		}
	}
	if len(srcPK) != 1 {
		return ""
	}
	return srcPK[0]
}

func reconcileCompareBuckets(ctx context.Context, src, tgt *reconcileSide, srcRef, tgtRef string, srcColumns, tgtColumns []string, key string, buckets int) ([]reconcileBucketMismatch, bool, error) {
	srcRows, err := src.query(ctx, reconcileBucketSQL(src.info.Dialect, srcRef, srcColumns, key, buckets), []string{"bucket", "rows", "digest"})
	if err != nil {
		return nil, false, fmt.Errorf("源表分桶失败: %w", err)
	}
	tgtRows, err := tgt.query(ctx, reconcileBucketSQL(tgt.info.Dialect, tgtRef, tgtColumns, key, buckets), []string{"bucket", "rows", "digest"})
	if err != nil {
		return nil, false, fmt.Errorf("目标表分桶失败: %w", err)
	}
	type bucketState struct {
		sourceRows, targetRows int64
		sourceDigest           string
		targetDigest           string
	}
	merged := map[int]*bucketState{}
	get := func(b int) *bucketState {
		if st, ok := merged[b]; ok {
			return st
		}
		st := &bucketState{}
		merged[b] = st
		return st
	}
	for _, row := range srcRows {
		st := get(int(reconcileInt64(row[0])))
		st.sourceRows = reconcileInt64(row[1])
		st.sourceDigest = row[2]
	}
	for _, row := range tgtRows {
		st := get(int(reconcileInt64(row[0])))
		st.targetRows = reconcileInt64(row[1])
		st.targetDigest = row[2]
	}
	keys := make([]int, 0, len(merged))
	for b := range merged {
		keys = append(keys, b)
	}
	sort.Ints(keys)
	var mismatches []reconcileBucketMismatch
	truncated := false
	for _, b := range keys {
		st := merged[b]
		if st.sourceRows == st.targetRows && st.sourceDigest == st.targetDigest {
			continue
		}
		if len(mismatches) >= maxReconcileMismatchRows {
			truncated = true
			break
		}
		mismatches = append(mismatches, reconcileBucketMismatch{
			Bucket:       b,
			SourceRows:   st.sourceRows,
			TargetRows:   st.targetRows,
			SourceDigest: st.sourceDigest,
			TargetDigest: st.targetDigest,
		})
	}
	return mismatches, truncated, nil
}

func reconcileInt64(s string) int64 {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0
	}
	if v, err := strconv.ParseInt(s, 10, 64); err == nil {
		return v
	}
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return int64(v)
	}
	return 0
}

func (r reconcileRequest) DeepOrDefault() bool {
	if r.Deep == nil {
		return true
	}
	return *r.Deep
}

// ── entry point + HTTP surface ───────────────────────────────────────────────

func reconcileLimits() []string {
	return []string{
		"摘要基于全列规范化文本的求和哈希，NULL 与字面量 " + reconcileNullMarker + " 不可区分；BLOB/JSON 按文本形式比较。",
		"仅比较两端共有的列；目标缺列直接判定 schema_mismatch。",
		"桶定位需要单一数值主键（可用 key_columns 覆盖），否则只给全表结论。",
		"对账是采样时刻的静态比较：pipeline 运行中执行可能看到在途写入差异，建议在 checkpoint 静默后复核。",
		"不检查行级历史（谁改的、改了几次），只比较当前状态是否一致。",
		"Kafka / file / http / 对象存储 / ES 端没有 SQL 查询面，不支持对账。",
		"目标表带 " + reconcileRowHashColumn + " 列（no_pk_snapshot=hash 的产物）时，源端完全相同的行会折叠为一行，行数差不一定代表数据丢失。",
		"行级差异（sample_rows>0）只采样不重放：按 key 取每行摘要，最多 " + strconv.Itoa(maxReconcileSampleRows) + " 行/侧、最多 " + strconv.Itoa(maxReconcileRowDiffBuckets) + " 个不一致桶，超出部分以 row_diff_truncated 标记；未定位到桶时只覆盖表头（row_diff_scope=head）。",
	}
}

// RunPipelineReconcile performs the comparison for one pipeline spec.
func (s *Server) RunPipelineReconcile(ctx context.Context, id string, spec *pipeline.Spec, req reconcileRequest) (*reconcileReport, error) {
	if spec == nil {
		return nil, fmt.Errorf("pipeline %q not found", id)
	}
	if spec.Source.Type == "" {
		return nil, fmt.Errorf("pipeline %q has no source", id)
	}
	if reconcileDialectForSource(spec.Source.Type) == "" {
		return nil, fmt.Errorf("source type %q has no SQL query surface, so reconciliation is unsupported (supported sources: mysql_batch, mysql_cdc, mysql_snapshot_cdc, postgres_cdc)", spec.Source.Type)
	}
	if reconcileDialectForSink(spec.Sink.Type) == "" {
		return nil, fmt.Errorf("sink type %q has no SQL query surface, so reconciliation is unsupported (supported sinks: mysql, doris, postgres, postgresql, clickhouse)", spec.Sink.Type)
	}
	if len(spec.Transforms) > 0 {
		return nil, fmt.Errorf("this pipeline declares %d transform(s): reconciliation compares raw source and target rows column by column, so transformed fields cannot be aligned; refusing to report a misleading verdict", len(spec.Transforms))
	}

	tables, unsupported := reconcileSourceTables(spec)
	if unsupported != "" {
		return nil, fmt.Errorf("%s", unsupported)
	}
	if len(req.Tables) > 0 {
		wanted := make(map[string]struct{}, len(req.Tables))
		for _, t := range req.Tables {
			if t = strings.TrimSpace(t); t != "" {
				wanted[t] = struct{}{}
			}
		}
		var filtered []string
		for _, t := range tables {
			if _, ok := wanted[t]; ok {
				filtered = append(filtered, t)
				delete(wanted, t)
			}
		}
		if len(wanted) > 0 {
			var unknown []string
			for t := range wanted {
				unknown = append(unknown, t)
			}
			sort.Strings(unknown)
			return nil, fmt.Errorf("requested table(s) are outside this pipeline's configured tables: %s", strings.Join(unknown, ", "))
		}
		tables = filtered
	}
	if len(tables) == 0 {
		return nil, fmt.Errorf("no table to reconcile")
	}

	buckets := req.Buckets
	if buckets <= 0 {
		buckets = defaultReconcileBuckets
	}
	if buckets > maxReconcileBuckets {
		buckets = maxReconcileBuckets
	}
	timeout := time.Duration(req.TimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = time.Duration(defaultReconcileTimeoutMS) * time.Millisecond
	}

	report := &reconcileReport{
		PipelineID:   id,
		PipelineName: spec.Name,
		Deep:         req.DeepOrDefault(),
		Buckets:      buckets,
		Limits:       reconcileLimits(),
		StartedAt:    time.Now(),
	}
	openSource := s.reconcileSourceOpener
	if openSource == nil {
		openSource = s.openReconcileSource
	}
	openTarget := s.reconcileTargetOpener
	if openTarget == nil {
		openTarget = s.openReconcileTarget
	}
	src, err := openSource(ctx, spec)
	if err != nil {
		return nil, err
	}
	defer src.close()
	tgt, err := openTarget(ctx, spec)
	if err != nil {
		return nil, err
	}
	defer tgt.close()
	report.Source = src.info
	report.Target = tgt.info

	// Resolved source tables may carry a schema qualifier (postgres); the
	// per-table lookups use the bare name and let the dialect add the schema.
	for _, table := range tables {
		tableCtx, cancel := context.WithTimeout(ctx, timeout)
		tableReport := s.reconcileOneTable(tableCtx, src, tgt, spec, table, req, buckets)
		cancel()
		report.Tables = append(report.Tables, tableReport)
	}

	report.Summary.Tables = len(report.Tables)
	for _, t := range report.Tables {
		switch t.Verdict {
		case reconcileVerdictMatch:
			report.Summary.Matched++
		case reconcileVerdictCountMismatch:
			report.Summary.CountMismatch++
		case reconcileVerdictContentDiff:
			report.Summary.ContentMismatch++
		case reconcileVerdictSchemaDiff:
			report.Summary.SchemaMismatch++
		case reconcileVerdictUnsupported:
			report.Summary.Unsupported++
		default:
			report.Summary.Errors++
		}
	}
	switch {
	case report.Summary.Tables == 0:
		report.Verdict = reconcileVerdictUnsupported
	case report.Summary.Matched == report.Summary.Tables:
		report.Verdict = reconcileVerdictMatch
	case report.Summary.Errors > 0:
		report.Verdict = reconcileVerdictError
	case report.Summary.CountMismatch > 0:
		report.Verdict = reconcileVerdictCountMismatch
	case report.Summary.ContentMismatch > 0:
		report.Verdict = reconcileVerdictContentDiff
	case report.Summary.SchemaMismatch > 0:
		report.Verdict = reconcileVerdictSchemaDiff
	default:
		report.Verdict = reconcileVerdictError
	}
	report.FinishedAt = time.Now()
	report.DurationMS = report.FinishedAt.Sub(report.StartedAt).Milliseconds()
	return report, nil
}

// reconcileCapabilities documents the supported matrix for GET requests so the
// contract is discoverable without reading code.
func reconcileCapabilities() map[string]any {
	return map[string]any{
		"sources":    []string{"mysql_batch", "mysql_cdc", "mysql_snapshot_cdc", "postgres_cdc"},
		"sinks":      []string{"mysql", "doris", "postgres", "postgresql", "clickhouse"},
		"verdicts":   []string{reconcileVerdictMatch, reconcileVerdictCountMismatch, reconcileVerdictContentDiff, reconcileVerdictSchemaDiff, reconcileVerdictMissingTarget, reconcileVerdictUnsupported, reconcileVerdictError},
		"request":    "POST the same path with an optional body {tables:[], deep:bool, buckets:int, sample_rows:int, key_columns:{table:col}, timeout_sec:int}",
		"limits":     reconcileLimits(),
		"transforms": "Pipelines that declare transforms are refused: reconciliation compares raw source/target columns, and transformed fields cannot be aligned column by column",
	}
}

// handlePipelineReconcile serves POST (run) and GET (capabilities) on
// /api/v2/pipelines/{id}/reconcile.
func (s *Server) handlePipelineReconcile(w http.ResponseWriter, r *http.Request, id string, spec *pipeline.Spec, runner pipeline.RunnerInterface) {
	if spec == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "pipeline not found"})
		return
	}
	switch r.Method {
	case http.MethodGet:
		json.NewEncoder(w).Encode(reconcileCapabilities())
		return
	case http.MethodPost:
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "reconcile supports GET (capabilities) and POST (run)"})
		return
	}
	var req reconcileRequest
	if r.Body != nil {
		if err := decodeOptionalJSONBody(r, &req); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			json.NewEncoder(w).Encode(map[string]any{"error": "invalid body: " + err.Error()})
			return
		}
	}
	report, err := s.RunPipelineReconcile(r.Context(), id, spec, req)
	if err != nil {
		w.WriteHeader(http.StatusBadRequest)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	if runner != nil {
		report.PipelineStat = string(runner.Status())
	}
	s.audit(r, "pipeline.reconcile", id)
	json.NewEncoder(w).Encode(report)
}

// decodeOptionalJSONBody decodes a JSON body, treating an empty body as "no
// overrides" instead of an error so a bare POST runs with defaults.
func decodeOptionalJSONBody(r *http.Request, dst any) error {
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	if len(strings.TrimSpace(string(body))) == 0 {
		return nil
	}
	return json.Unmarshal(body, dst)
}

// ── row-level diff (key sampling) ────────────────────────────────────────────

// reconcileRowDigestExpr hashes one row over the compared columns. The whole
// table digest sums this expression, so a row-level digest is the same value a
// single row contributes to that sum.
func reconcileRowDigestExpr(dialect string, columns []string) string {
	parts := make([]string, 0, len(columns))
	for _, c := range columns {
		parts = append(parts, reconcileTextExpr(dialect, c))
	}
	sep := reconcileSeparator(dialect)
	switch dialect {
	case reconcileDialectPostgres:
		return fmt.Sprintf("hashtextextended(concat_ws(%s, %s), 0)", sep, strings.Join(parts, ", "))
	case reconcileDialectClickHouse:
		return fmt.Sprintf("sipHash64(arrayStringConcat([%s], %s))", strings.Join(parts, ", "), sep)
	default:
		return fmt.Sprintf("CRC32(CONCAT_WS(%s, %s))", sep, strings.Join(parts, ", "))
	}
}

// reconcileRowSQL projects (key, row digest) for up to limit rows, optionally
// restricted to one bucket so the sample stays inside the range that already
// proved to be drifting.
func reconcileRowSQL(dialect, tableRef string, columns []string, key string, buckets, bucket, limit int) string {
	keyExpr := reconcileTextExpr(dialect, key)
	digest := reconcileRowDigestExpr(dialect, columns)
	query := fmt.Sprintf("SELECT %s, %s FROM %s", keyExpr, digest, tableRef)
	if bucket >= 0 && buckets > 1 {
		query += fmt.Sprintf(" WHERE %s = %d", reconcileBucketExpr(dialect, key, buckets), bucket)
	}
	return fmt.Sprintf("%s ORDER BY 1 LIMIT %d", query, limit)
}

// reconcileCompareRows samples rows from both sides and classifies the
// differences by key. Rows are compared on their canonical text projection so
// the same value compares equal across dialects.
func reconcileCompareRows(ctx context.Context, src, tgt *reconcileSide, srcRef, tgtRef string, srcColumns, tgtColumns []string, key string, buckets, bucket, limit int) ([]reconcileRowDiff, bool, error) {
	srcRows, err := src.query(ctx, reconcileRowSQL(src.info.Dialect, srcRef, srcColumns, key, buckets, bucket, limit), []string{"key", "digest"})
	if err != nil {
		return nil, false, fmt.Errorf("源表行采样失败: %w", err)
	}
	tgtRows, err := tgt.query(ctx, reconcileRowSQL(tgt.info.Dialect, tgtRef, tgtColumns, key, buckets, bucket, limit), []string{"key", "digest"})
	if err != nil {
		return nil, false, fmt.Errorf("目标表行采样失败: %w", err)
	}

	type rowState struct {
		sourceDigest, targetDigest string
		hasSource, hasTarget       bool
	}
	merged := map[string]*rowState{}
	order := make([]string, 0, len(srcRows)+len(tgtRows))
	for _, row := range srcRows {
		state, ok := merged[row[0]]
		if !ok {
			state = &rowState{}
			merged[row[0]] = state
			order = append(order, row[0])
		}
		state.sourceDigest = row[1]
		state.hasSource = true
	}
	for _, row := range tgtRows {
		state, ok := merged[row[0]]
		if !ok {
			state = &rowState{}
			merged[row[0]] = state
			order = append(order, row[0])
		}
		state.targetDigest = row[1]
		state.hasTarget = true
	}

	diffs := make([]reconcileRowDiff, 0, len(order))
	truncated := false
	for _, keyValue := range order {
		state := merged[keyValue]
		var diff reconcileRowDiff
		switch {
		case state.hasSource && !state.hasTarget:
			diff = reconcileRowDiff{Key: keyValue, Kind: reconcileRowDiffMissingInTarget}
		case !state.hasSource && state.hasTarget:
			diff = reconcileRowDiff{Key: keyValue, Kind: reconcileRowDiffExtraInTarget}
		case state.sourceDigest != state.targetDigest:
			diff = reconcileRowDiff{Key: keyValue, Kind: reconcileRowDiffValueMismatch}
		default:
			continue
		}
		if len(diffs) >= maxReconcileRowDiffTotal {
			truncated = true
			break
		}
		diffs = append(diffs, diff)
	}
	if len(srcRows) >= limit || len(tgtRows) >= limit {
		truncated = true
	}
	return diffs, truncated, nil
}
