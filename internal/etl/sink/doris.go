package sink

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/sink/typing"
)

func init() {
	registry.RegisterSink("doris", func(config map[string]any) (core.Sink, error) {
		return NewDorisSink(config)
	})
}

// DorisSink writes records to Apache Doris via Stream Load (HTTP) or MySQL
// protocol (INSERT). It supports auto-create, schema drift, batch upsert
// (via Doris Unique Key model), and DELETE operations.
//
// Doris speaks MySQL wire protocol on port 9030 (FE), and exposes a high-
// performance Stream Load HTTP API on port 8030 (FE). This sink uses:
//
//   - MySQL protocol for DDL (CREATE TABLE, ALTER TABLE) and DELETE
//   - Stream Load for high-throughput batch INSERT/UPSERT (default)
//   - MySQL protocol INSERT as a fallback write mode
//
// Config:
//
//	host:          Doris FE host
//	port:          MySQL protocol port (default 9030)
//	http_port:     Stream Load HTTP port (default 8030)
//	user:          Doris user (default "root")
//	password:      Doris password
//	database:      Target database name
//	table:         Target table name (can be overridden per-record via metadata.table)
//	write_mode:    "stream_load" (default) or "insert"
//	batch_mode:    "insert" (default) or "upsert" — both use INSERT; Doris
//	               Unique Key model handles dedup automatically
//	pk_columns:    Primary key columns (used for DELETE and auto-create key model)
//	stream_load_timeout_sec: Stream Load HTTP timeout (default 30)
//	stream_load_format: "json" (default) or "csv"
//	auto_create:   Auto-create target table if missing
//	schema_drift:  "ignore" | "fail" | "add_columns"
//	insert_chunk_size: Rows per INSERT statement for insert mode (default 500)
type DorisSink struct {
	name     string
	host     string
	port     int
	httpPort int
	user     string
	password string
	database string
	table    string

	writeMode string // "stream_load" | "insert"
	batchMode string // "insert" | "upsert" (informational; Doris UK model dedupes)
	pkColumns []string

	streamLoadTimeout time.Duration
	streamLoadFormat  string
	streamLoadScheme  string
	tlsSkipVerify     bool

	insertChunkSize int

	autoCreate             bool
	schemaDrift            string
	ddLPolicy              DDLPolicy
	allowMixedCDCNonAtomic bool

	db           *sql.DB
	httpClient   *http.Client
	schemaCache  *core.SchemaCache
	sinkCounters // P4-20: per-sink write metrics (SK-4)
}

func NewDorisSink(config map[string]any) (*DorisSink, error) {
	s := &DorisSink{
		name:              "doris",
		port:              9030,
		httpPort:          8030,
		user:              "root",
		writeMode:         "stream_load",
		batchMode:         "insert",
		streamLoadTimeout: 30 * time.Second,
		streamLoadFormat:  "json",
		streamLoadScheme:  "http",
		insertChunkSize:   500,
	}
	if v, ok := config["name"]; ok {
		s.name = v.(string)
	}
	if v, ok := config["host"]; ok {
		s.host = v.(string)
	}
	if v, ok := config["port"]; ok {
		switch p := v.(type) {
		case int:
			s.port = p
		case float64:
			s.port = int(p)
		}
	}
	if v, ok := config["http_port"]; ok {
		switch p := v.(type) {
		case int:
			s.httpPort = p
		case float64:
			s.httpPort = int(p)
		}
	}
	if v, ok := config["user"]; ok {
		s.user = v.(string)
	}
	if v, ok := config["password"]; ok {
		s.password = v.(string)
	}
	if v, ok := config["database"]; ok {
		s.database = v.(string)
	}
	if v, ok := config["table"]; ok {
		s.table = v.(string)
	}
	if v, ok := config["write_mode"]; ok {
		s.writeMode = v.(string)
	}
	if v, ok := config["batch_mode"]; ok {
		s.batchMode = v.(string)
	}
	s.pkColumns = append(s.pkColumns, stringSliceConfig(config, "pk_columns")...)
	if v, ok := config["stream_load_timeout_sec"]; ok {
		switch t := v.(type) {
		case int:
			s.streamLoadTimeout = time.Duration(t) * time.Second
		case float64:
			s.streamLoadTimeout = time.Duration(t) * time.Second
		}
	}
	if v, ok := config["stream_load_format"]; ok {
		if vs, ok := v.(string); ok && (vs == "json" || vs == "csv") {
			s.streamLoadFormat = vs
		}
	}
	if v, ok := config["stream_load_scheme"]; ok {
		if vs, ok := v.(string); ok && (vs == "http" || vs == "https") {
			s.streamLoadScheme = vs
		}
	}
	if v, ok := config["https"]; ok {
		if b, ok := v.(bool); ok && b {
			s.streamLoadScheme = "https"
		}
	}
	if v, ok := config["tls_skip_verify"]; ok {
		if b, ok := v.(bool); ok {
			s.tlsSkipVerify = b
		}
	}
	if v, ok := config["insert_chunk_size"]; ok {
		switch cs := v.(type) {
		case int:
			s.insertChunkSize = cs
		case float64:
			s.insertChunkSize = int(cs)
		}
	}
	if s.insertChunkSize <= 0 {
		s.insertChunkSize = 500
	}
	if v, ok := config["auto_create"]; ok {
		if b, ok := v.(bool); ok {
			s.autoCreate = b
		}
	}
	if v, ok := config["schema_drift"]; ok {
		if vs, ok := v.(string); ok {
			s.schemaDrift = vs
		}
	}
	if s.schemaDrift == "" {
		s.schemaDrift = "ignore"
	}
	if v, ok := config["ddl_policy"]; ok {
		if vs, ok := v.(string); ok {
			s.ddLPolicy = DDLPolicy(vs)
		}
	}
	if s.ddLPolicy == "" {
		s.ddLPolicy = DDLPolicyReject
	}
	if v, ok := config["allow_mixed_cdc_non_atomic"]; ok {
		if b, ok := v.(bool); ok {
			s.allowMixedCDCNonAtomic = b
		}
	}
	s.schemaCache = core.NewSchemaCache()
	return s, nil
}

func (s *DorisSink) Name() string { return s.name }

// SinkMetrics implements core.SinkMetricsProvider (P4-20, SK-4).
func (s *DorisSink) SinkMetrics() core.SinkMetrics { return s.metricsFor(s.name) }

func (s *DorisSink) ValidateSchema(ctx context.Context, schema core.SchemaInfo) error {
	if len(schema.Columns) == 0 || s.table == "" {
		return nil
	}
	exists, err := s.tableExists(ctx, s.table)
	if err != nil {
		return fmt.Errorf("validate doris schema: check table %s.%s: %w", s.database, s.table, err)
	}
	if !exists {
		if s.autoCreate {
			if s.batchMode == "upsert" && len(s.pkColumns) == 0 && !schemaHasColumn(schema, "id") {
				return fmt.Errorf("schema validation failed for doris %s.%s: batch_mode=upsert with auto_create requires pk_columns or an id column for a stable Doris UNIQUE KEY model", s.database, s.table)
			}
			return nil
		}
		return fmt.Errorf("schema validation failed for doris %s.%s: target table does not exist; enable auto_create or create a Doris UNIQUE KEY table first", s.database, s.table)
	}
	target, err := s.getExistingColumnInfo(ctx, s.table)
	if err != nil {
		return fmt.Errorf("validate doris schema: read columns for %s.%s: %w", s.database, s.table, err)
	}
	if err := validateSchemaCompatibility(schema, target, schemaValidationOptions{
		targetName:     fmt.Sprintf("doris %s.%s", s.database, s.table),
		allowMissing:   s.schemaDrift == string(core.DriftAddCols),
		missingRemedy:  "enable schema_drift=add_columns or add the columns manually",
		allowTypeSync:  false,
		typeSyncRemedy: "change the Doris target column type or add a transform/type_convert before the sink",
	}); err != nil {
		return err
	}
	if s.batchMode == "upsert" || len(s.pkColumns) > 0 {
		if len(s.pkColumns) == 0 {
			return fmt.Errorf("schema validation failed for doris %s.%s: batch_mode=upsert requires pk_columns so checkpoint/DLQ replay targets a stable Doris UNIQUE KEY", s.database, s.table)
		}
		if err := s.validateUniqueKeyModel(ctx, s.table); err != nil {
			return err
		}
	}
	return nil
}

func (s *DorisSink) Open(ctx context.Context) error {
	// MySQL protocol connection (for DDL + DELETE + fallback INSERT)
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&loc=Local",
		s.user, s.password, s.host, s.port, s.database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("connect doris (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping doris (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}
	s.db = db

	// HTTP client for Stream Load
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if s.tlsSkipVerify {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true}
	}
	s.httpClient = &http.Client{
		Timeout:   s.streamLoadTimeout,
		Transport: transport,
	}
	return nil
}

func (s *DorisSink) Write(ctx context.Context, records []core.Record) (err error) {
	defer func() {
		if err != nil {
			s.recordError()
		}
	}() // P5-12: count write failures
	if len(records) == 0 {
		return nil
	}
	start := time.Now()

	// Separate DDL records from data records.
	var ddlRecords, dataRecords []core.Record
	for _, rec := range records {
		if rec.Operation == core.OpDDL {
			ddlRecords = append(ddlRecords, rec)
		} else {
			dataRecords = append(dataRecords, rec)
		}
	}

	// Apply DDL first according to ddl_policy (schema changes precede data).
	if err := ApplyDDLRecords(ctx, ddlRecords, s.ddLPolicy, func(ctx context.Context, ddl, table string) error {
		if err := validateDorisApplyDDL(ddl); err != nil {
			return err
		}
		if _, err := s.db.ExecContext(ctx, ddl); err != nil {
			return fmt.Errorf("execute DDL %q: %w", ddl, err)
		}
		s.schemaCache.InvalidateCache(table)
		return nil
	}); err != nil {
		return err
	}

	if len(dataRecords) == 0 {
		return nil
	}

	// Compact by (table, PK) in source order so mixed CDC batches on the same
	// key do not get reordered by the write/delete phase split.
	dataRecords = CompactRecordsByPK(dataRecords, func(table string) []string {
		if len(s.pkColumns) > 0 {
			return s.pkColumns
		}
		return []string{"id"}
	})

	// Auto-create missing tables and handle schema drift before writing.
	if err := s.ensureTablesAndColumns(ctx, dataRecords); err != nil {
		return err
	}

	// Separate deletes from inserts/upserts.
	var deletes, writes []core.Record
	for _, rec := range dataRecords {
		if rec.Operation == core.OpDelete {
			deletes = append(deletes, rec)
		} else {
			writes = append(writes, rec)
		}
	}
	if len(writes) > 0 && len(deletes) > 0 && !s.allowMixedCDCNonAtomic {
		return fmt.Errorf("doris sink refuses mixed write/delete CDC batch because Stream Load and MySQL DELETE are not atomic together; set allow_mixed_cdc_non_atomic=true to accept at-least-once non-atomic semantics")
	}

	// Apply writes before deletes so mixed CDC batches preserve source order for
	// delete-after-update cases. Deletes are still isolated into one bulk phase.
	if len(writes) > 0 {
		switch s.writeMode {
		case "insert":
			if err := s.writeViaInsert(ctx, writes); err != nil {
				return err
			}
		default:
			if err := s.writeViaStreamLoad(ctx, writes); err != nil {
				return err
			}
		}
	}
	if len(deletes) > 0 {
		if err := s.batchDeleteRecords(ctx, deletes); err != nil {
			return err
		}
	}
	s.recordMetrics(len(records), time.Since(start))
	return nil
}

// applyDDL executes a DDL statement on the Doris target via MySQL protocol
// and invalidates the schema cache.
func (s *DorisSink) applyDDL(ctx context.Context, ddlRec core.Record) error {
	ddl := ddlRec.Metadata.DDL
	if ddl == "" {
		return nil
	}
	if _, err := s.db.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("execute DDL %q: %w", ddl, err)
	}
	// Invalidate schema cache.
	s.schemaCache.InvalidateCache(ddlRec.Metadata.Table)
	return nil
}

// ── Stream Load (HTTP) ─────────────────────────────────────────────────

// writeViaStreamLoad groups records by table and sends each group via Doris
// Stream Load HTTP API. Doris's Unique Key model handles upsert automatically.
func (s *DorisSink) writeViaStreamLoad(ctx context.Context, records []core.Record) error {
	// Group by table to send one Stream Load request per table.
	tableGroups := make(map[string][]core.Record)
	for _, rec := range records {
		tableName := s.resolveTable(rec)
		tableGroups[tableName] = append(tableGroups[tableName], rec)
	}

	for tableName, recs := range tableGroups {
		if err := s.streamLoad(ctx, tableName, recs); err != nil {
			return fmt.Errorf("stream load to %s: %w", tableName, err)
		}
	}
	return nil
}

// streamLoad sends a batch of records to Doris via the Stream Load HTTP API.
// Uses a deterministic label per batch so Doris can deduplicate on retry.
// Retries transient failures (5xx, network errors) with exponential backoff.
func (s *DorisSink) streamLoad(ctx context.Context, tableName string, records []core.Record) error {
	backoff := time.Second
	const maxBackoff = 10 * time.Second
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		err := s.streamLoadOnce(ctx, tableName, records)
		if err == nil {
			return nil
		}
		lastErr = err
		// Only retry on transient errors.
		if !isRetryableStreamLoadErr(err) {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		if backoff < maxBackoff {
			backoff *= 2
		}
	}
	return lastErr
}

func isRetryableStreamLoadErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	// Network/timeout errors are retryable.
	if strings.Contains(msg, "connection") || strings.Contains(msg, "timeout") || strings.Contains(msg, "EOF") || strings.Contains(msg, "refused") {
		return true
	}
	return core.IsRetryableError(err) || strings.Contains(msg, "HTTP 5") || strings.Contains(msg, "HTTP 429")
}

// streamLoadOnce performs a single Stream Load HTTP request without retry.
func (s *DorisSink) streamLoadOnce(ctx context.Context, tableName string, records []core.Record) error {
	var body []byte
	var contentType string

	if s.streamLoadFormat == "csv" {
		body = s.buildCSVBody(records, tableName)
		contentType = "text/csv"
	} else {
		body = s.buildJSONBody(records)
		contentType = "application/json"
	}

	streamURL := fmt.Sprintf("%s://%s:%d/api/%s/%s/_stream_load",
		s.streamLoadScheme, s.host, s.httpPort, url.PathEscape(s.database), url.PathEscape(tableName))

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, streamURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build stream load request: %w", err)
	}

	// Doris Stream Load headers
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("Expect", "100-continue")
	req.Header.Set("format", s.streamLoadFormat)
	if s.streamLoadFormat == "json" {
		req.Header.Set("read_json_by_line", "true")
	}
	// Deterministic label: hash of (database, table, payload) so retries
	// produce the same label and Doris can deduplicate. Different batches
	// get different labels because the payload differs.
	labelHash := sha256.Sum256(append([]byte(fmt.Sprintf("%s:%d/%s.%s|", s.host, s.httpPort, s.database, tableName)), body...))
	label := fmt.Sprintf("etl_%x", labelHash[:16])
	req.Header.Set("label", label)

	// For CSV, specify column order so Doris maps correctly.
	if s.streamLoadFormat == "csv" {
		cols := s.csvColumnOrder(records)
		if len(cols) > 0 {
			req.Header.Set("columns", strings.Join(cols, ","))
		}
		req.Header.Set("column_separator", ",")
	}

	// Auth
	if s.user != "" {
		req.SetBasicAuth(s.user, s.password)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("stream load HTTP request: %w", err)
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))

	if resp.StatusCode != http.StatusOK {
		return classifyDorisStreamLoadError(resp.StatusCode, fmt.Sprintf("stream load failed (HTTP %d): %s", resp.StatusCode, string(respBody)))
	}

	// Parse Doris Stream Load response JSON to check success.
	var slResp struct {
		Status           string `json:"status"`
		StatusCode       int    `json:"StatusCode"`
		Message          string `json:"Message"`
		NumberTotalRows  int    `json:"NumberTotalRows"`
		NumberLoadedRows int    `json:"NumberLoadedRows"`
	}
	if err := json.Unmarshal(respBody, &slResp); err != nil {
		// If the response is not valid JSON, it's likely an error page.
		return core.ClassifiedError{Class: core.ErrorClassTransient, Err: fmt.Errorf("stream load: unparseable response (HTTP %d): %s", resp.StatusCode, string(respBody)[:min(len(respBody), 200)])}
	}
	if slResp.Status == "Fail" || slResp.StatusCode >= 400 {
		return classifyDorisStreamLoadError(slResp.StatusCode, fmt.Sprintf("stream load rejected: %s (loaded %d/%d)",
			slResp.Message, slResp.NumberLoadedRows, slResp.NumberTotalRows))
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func classifyDorisStreamLoadError(statusCode int, message string) error {
	class := core.ErrorClassData
	lower := strings.ToLower(message)
	switch {
	case statusCode == http.StatusUnauthorized || statusCode == http.StatusForbidden:
		class = core.ErrorClassAuth
	case statusCode == http.StatusTooManyRequests || statusCode >= 500:
		class = core.ErrorClassTransient
	case strings.Contains(lower, "unknown column") || strings.Contains(lower, "schema") || (strings.Contains(lower, "table") && strings.Contains(lower, "not exist")):
		class = core.ErrorClassSchema
	case strings.Contains(lower, "data quality") || strings.Contains(lower, "too many filtered rows") || strings.Contains(lower, "invalid") || strings.Contains(lower, "out of range") || strings.Contains(lower, "data too long"):
		class = core.ErrorClassData
	default:
		if statusCode == 0 {
			class = core.ClassifyError(fmt.Errorf("%s", message))
		}
	}
	return core.ClassifiedError{Class: class, Err: fmt.Errorf("%s", message)}
}

// buildJSONBody converts records to Doris Stream Load JSON format.
// Each record becomes a JSON object on its own line (JSON line format).
func (s *DorisSink) buildJSONBody(records []core.Record) []byte {
	var buf bytes.Buffer
	for _, rec := range records {
		data, err := json.Marshal(rec.Data)
		if err != nil {
			data = []byte(`{}`)
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

// csvColumnOrder returns the sorted column names from the first record.
func (s *DorisSink) csvColumnOrder(records []core.Record) []string {
	if len(records) == 0 {
		return nil
	}
	cols := make([]string, 0, len(records[0].Data))
	for k := range records[0].Data {
		cols = append(cols, k)
	}
	sort.Strings(cols)
	return cols
}

// buildCSVBody converts records to CSV format for Stream Load.
// Column order is derived from the first record and sorted alphabetically.
func (s *DorisSink) buildCSVBody(records []core.Record, tableName string) []byte {
	if len(records) == 0 {
		return nil
	}
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	cols := s.csvColumnOrder(records)

	for _, rec := range records {
		row := make([]string, len(cols))
		for i, c := range cols {
			val := rec.Data[c]
			if val != nil {
				row[i] = fmt.Sprint(val)
			}
		}
		_ = w.Write(row)
	}
	w.Flush()
	return buf.Bytes()
}

// ── MySQL protocol INSERT (fallback) ────────────────────────────────────

func (s *DorisSink) writeViaInsert(ctx context.Context, records []core.Record) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback()
		}
	}()

	type groupKey struct {
		table string
		cols  string
	}
	type groupBuf struct {
		cols []string
		rows [][]any
	}
	groups := make(map[groupKey]*groupBuf)
	var order []groupKey

	for _, rec := range records {
		tableName := s.resolveTable(rec)
		cols := make([]string, 0, len(rec.Data))
		for k := range rec.Data {
			cols = append(cols, k)
		}
		sort.Strings(cols)
		key := groupKey{table: tableName, cols: strings.Join(cols, ",")}

		g, ok := groups[key]
		if !ok {
			g = &groupBuf{cols: cols}
			groups[key] = g
			order = append(order, key)
		}
		row := make([]any, len(cols))
		for i, c := range cols {
			row[i] = rec.Data[c]
		}
		g.rows = append(g.rows, row)
	}

	for _, key := range order {
		g := groups[key]
		if err := s.batchInsert(ctx, tx, key.table, g.cols, g.rows); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	return nil
}

func (s *DorisSink) batchInsert(ctx context.Context, tx *sql.Tx, table string, cols []string, rows [][]any) error {
	for offset := 0; offset < len(rows); offset += s.insertChunkSize {
		end := offset + s.insertChunkSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[offset:end]
		if len(chunk) == 0 {
			continue
		}
		query := s.buildInsertStatement(table, cols, len(chunk))
		args := make([]any, 0, len(chunk)*len(cols))
		for _, row := range chunk {
			args = append(args, row...)
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("batch insert %s (rows=%d): %w", table, len(chunk), err)
		}
	}
	return nil
}

func (s *DorisSink) buildInsertStatement(table string, cols []string, rowCount int) string {
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = quoteIdentMySQL(c)
	}
	colList := strings.Join(quotedCols, ",")
	rowPlaceholder := "(" + strings.Repeat("?,", len(cols)-1) + "?)"

	var b strings.Builder
	b.WriteString("INSERT INTO ")
	b.WriteString(quoteIdentMySQL(table))
	b.WriteString(" (")
	b.WriteString(colList)
	b.WriteString(") VALUES ")
	b.WriteString(strings.Repeat(rowPlaceholder+",", rowCount-1))
	b.WriteString(rowPlaceholder)
	return b.String()
}

// ── DELETE via MySQL protocol ───────────────────────────────────────────

func (s *DorisSink) batchDeleteRecords(ctx context.Context, records []core.Record) error {
	// Group by table
	tableGroups := make(map[string][]core.Record)
	for _, rec := range records {
		tableName := s.resolveTable(rec)
		tableGroups[tableName] = append(tableGroups[tableName], rec)
	}

	pkCols := s.pkColumns
	if len(pkCols) == 0 {
		pkCols = []string{"id"}
	}

	for tableName, recs := range tableGroups {
		for offset := 0; offset < len(recs); offset += s.insertChunkSize {
			end := offset + s.insertChunkSize
			if end > len(recs) {
				end = len(recs)
			}
			chunk := recs[offset:end]
			if len(chunk) == 0 {
				continue
			}
			if len(pkCols) == 1 {
				query := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
					quoteIdentMySQL(tableName), quoteIdentMySQL(pkCols[0]), strings.Repeat("?,", len(chunk)-1)+"?")
				args := make([]any, 0, len(chunk))
				for _, rec := range chunk {
					keys, err := ResolveDeleteKeys(rec, pkCols)
					if err != nil {
						return fmt.Errorf("batch delete %s: %w", tableName, err)
					}
					args = append(args, keys[pkCols[0]])
				}
				if _, err := s.db.ExecContext(ctx, query, args...); err != nil {
					return fmt.Errorf("batch delete %s (rows=%d): %w", tableName, len(chunk), err)
				}
				continue
			}

			quotedPk := make([]string, len(pkCols))
			for i, c := range pkCols {
				quotedPk[i] = quoteIdentMySQL(c) + "=?"
			}
			rowCond := "(" + strings.Join(quotedPk, " AND ") + ")"

			var b strings.Builder
			b.WriteString("DELETE FROM ")
			b.WriteString(quoteIdentMySQL(tableName))
			b.WriteString(" WHERE ")
			b.WriteString(strings.Repeat(rowCond+" OR ", len(chunk)-1))
			b.WriteString(rowCond)

			args := make([]any, 0, len(chunk)*len(pkCols))
			for _, rec := range chunk {
				keys, err := ResolveDeleteKeys(rec, pkCols)
				if err != nil {
					return fmt.Errorf("batch delete %s: %w", tableName, err)
				}
				for _, pk := range pkCols {
					args = append(args, keys[pk])
				}
			}
			if _, err := s.db.ExecContext(ctx, b.String(), args...); err != nil {
				return fmt.Errorf("batch delete %s (rows=%d): %w", tableName, len(chunk), err)
			}
		}
	}
	return nil
}

// ── Schema Management (SchemaManager) ───────────────────────────────────

// EnsureSchema implements core.SchemaManager.
func (s *DorisSink) EnsureSchema(ctx context.Context, tableName string, fields []string, fieldValues map[string]any) error {
	return core.EnsureSchemaGeneric(ctx, s.schemaCache, tableName, fields, fieldValues,
		s.autoCreate, core.SchemaDriftMode(s.schemaDrift),
		s.tableExists, s.createTableFromFields, s.getExistingColumns, s.addColumn,
	)
}

func (s *DorisSink) ensureTablesAndColumns(ctx context.Context, records []core.Record) error {
	if !s.autoCreate && s.schemaDrift != "add_columns" && s.schemaDrift != "fail" {
		return nil
	}
	tableCols, tableValues := s.collectSchemaInputs(records)
	for tableName, cols := range tableCols {
		if err := s.EnsureSchema(ctx, tableName, cols, tableValues[tableName]); err != nil {
			return err
		}
	}
	return nil
}

func (s *DorisSink) collectSchemaInputs(records []core.Record) (map[string][]string, map[string]map[string]any) {
	tableCols := make(map[string][]string)
	tableValues := make(map[string]map[string]any)
	for _, rec := range records {
		tableName := s.resolveTable(rec)
		if tableName == "" {
			continue
		}
		if tableValues[tableName] == nil {
			tableValues[tableName] = make(map[string]any)
		}
		for k := range rec.Data {
			found := false
			for _, existing := range tableCols[tableName] {
				if existing == k {
					found = true
					break
				}
			}
			if !found {
				tableCols[tableName] = append(tableCols[tableName], k)
			}
			if _, ok := tableValues[tableName][k]; !ok && rec.Data[k] != nil {
				tableValues[tableName][k] = rec.Data[k]
			}
		}
	}
	return tableCols, tableValues
}

func (s *DorisSink) tableExists(ctx context.Context, table string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?`,
		s.database, table,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *DorisSink) getExistingColumns(ctx context.Context, table string) (map[string]bool, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?`,
		s.database, table,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols := make(map[string]bool)
	for rows.Next() {
		var col string
		if err := rows.Scan(&col); err != nil {
			return nil, err
		}
		cols[col] = true
	}
	return cols, nil
}

func (s *DorisSink) getExistingColumnInfo(ctx context.Context, table string) ([]core.ColumnInfo, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT column_name, column_type, is_nullable FROM information_schema.columns WHERE table_schema = ? AND table_name = ? ORDER BY ordinal_position`,
		s.database, table,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var cols []core.ColumnInfo
	for rows.Next() {
		var name, dataType, nullable string
		if err := rows.Scan(&name, &dataType, &nullable); err != nil {
			return nil, err
		}
		cols = append(cols, core.ColumnInfo{Name: name, DataType: dataType, Nullable: strings.EqualFold(nullable, "YES")})
	}
	return cols, rows.Err()
}

func (s *DorisSink) validateUniqueKeyModel(ctx context.Context, table string) error {
	createStmt, err := s.showCreateTable(ctx, table)
	if err != nil {
		return fmt.Errorf("validate doris unique key model for %s.%s: %w", s.database, table, err)
	}
	uniqueKeys := parseDorisUniqueKeyColumns(createStmt)
	if len(uniqueKeys) == 0 {
		return fmt.Errorf("schema validation failed for doris %s.%s: batch_mode=upsert requires a Doris UNIQUE KEY table; existing table is not Unique Key or SHOW CREATE TABLE did not expose UNIQUE KEY", s.database, table)
	}
	if !sameIdentifierSet(uniqueKeys, s.pkColumns) {
		return fmt.Errorf("schema validation failed for doris %s.%s: pk_columns %v do not match Doris UNIQUE KEY %v; replay-safe upsert requires the configured business key to be the table unique key", s.database, table, s.pkColumns, uniqueKeys)
	}
	return nil
}

func (s *DorisSink) showCreateTable(ctx context.Context, table string) (string, error) {
	rows, err := s.db.QueryContext(ctx, "SHOW CREATE TABLE "+quoteIdentMySQL(table))
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if !rows.Next() {
		return "", sql.ErrNoRows
	}
	cols, err := rows.Columns()
	if err != nil {
		return "", err
	}
	values := make([]sql.NullString, len(cols))
	scan := make([]any, len(cols))
	for i := range values {
		scan[i] = &values[i]
	}
	if err := rows.Scan(scan...); err != nil {
		return "", err
	}
	for i, col := range cols {
		if strings.EqualFold(col, "Create Table") && values[i].Valid {
			return values[i].String, nil
		}
	}
	if len(values) > 1 && values[1].Valid {
		return values[1].String, nil
	}
	return "", fmt.Errorf("SHOW CREATE TABLE returned no create statement")
}

func parseDorisUniqueKeyColumns(createStmt string) []string {
	upper := strings.ToUpper(createStmt)
	idx := strings.Index(upper, "UNIQUE KEY")
	if idx < 0 {
		return nil
	}
	rest := createStmt[idx+len("UNIQUE KEY"):]
	open := strings.Index(rest, "(")
	if open < 0 {
		return nil
	}
	rest = rest[open+1:]
	close := strings.Index(rest, ")")
	if close < 0 {
		return nil
	}
	rawCols := strings.Split(rest[:close], ",")
	cols := make([]string, 0, len(rawCols))
	for _, raw := range rawCols {
		col := normalizeIdentifier(raw)
		if col != "" {
			cols = append(cols, col)
		}
	}
	return cols
}

func normalizeIdentifier(v string) string {
	v = strings.TrimSpace(v)
	v = strings.Trim(v, "`\"[]")
	if dot := strings.LastIndex(v, "."); dot >= 0 {
		v = v[dot+1:]
		v = strings.Trim(v, "`\"[]")
	}
	return strings.ToLower(v)
}

func sameIdentifierSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	seen := make(map[string]int, len(a))
	for _, item := range a {
		seen[normalizeIdentifier(item)]++
	}
	for _, item := range b {
		key := normalizeIdentifier(item)
		if seen[key] == 0 {
			return false
		}
		seen[key]--
	}
	return true
}

func schemaHasColumn(schema core.SchemaInfo, name string) bool {
	for _, col := range schema.Columns {
		if strings.EqualFold(col.Name, name) {
			return true
		}
	}
	return false
}

func orderDorisColumns(columns, keyCols []string) []string {
	keySet := make(map[string]bool, len(keyCols))
	out := make([]string, 0, len(columns))
	for _, key := range keyCols {
		keyNorm := normalizeIdentifier(key)
		if keySet[keyNorm] {
			continue
		}
		keySet[keyNorm] = true
		out = append(out, key)
	}

	rest := make([]string, 0, len(columns)-len(out))
	for _, col := range columns {
		if !keySet[normalizeIdentifier(col)] {
			rest = append(rest, col)
		}
	}
	sort.Strings(rest)
	return append(out, rest...)
}

func validateDorisApplyDDL(ddl string) error {
	normalized := strings.ToUpper(strings.Join(strings.Fields(ddl), " "))
	if strings.HasPrefix(normalized, "ALTER TABLE ") && strings.Contains(normalized, " ADD COLUMN ") {
		for _, blocked := range []string{" DROP ", " DROP COLUMN ", " MODIFY ", " CHANGE ", " RENAME ", " TRUNCATE "} {
			if strings.Contains(normalized, blocked) {
				return fmt.Errorf("doris ddl_policy=apply only permits safe ALTER TABLE ADD COLUMN statements, got %q", ddl)
			}
		}
		return nil
	}
	return fmt.Errorf("doris ddl_policy=apply only permits safe ALTER TABLE ADD COLUMN statements, got %q", ddl)
}

// createTableFromFields creates a Doris table with UNIQUE KEY model.
// Doris requires ENGINE=OLAP and a key definition. We use pk_columns, or id
// only when present, so replay-safe upsert never relies on an arbitrary column.
func (s *DorisSink) createTableFromFields(ctx context.Context, table string, columns []string, fieldValues map[string]any) error {
	ddl, err := s.buildCreateTableDDL(table, columns, fieldValues)
	if err != nil || ddl == "" {
		return err
	}
	_, err = s.db.ExecContext(ctx, ddl)
	return err
}

func (s *DorisSink) buildCreateTableDDL(table string, columns []string, fieldValues map[string]any) (string, error) {
	if len(columns) == 0 {
		return "", nil
	}
	sort.Strings(columns)

	// Determine key columns
	columnByNorm := make(map[string]string, len(columns))
	for _, c := range columns {
		columnByNorm[normalizeIdentifier(c)] = c
	}
	keyCols := make([]string, 0, len(s.pkColumns))
	if len(s.pkColumns) > 0 {
		seen := make(map[string]bool, len(s.pkColumns))
		for _, configured := range s.pkColumns {
			norm := normalizeIdentifier(configured)
			if seen[norm] {
				continue
			}
			actual, ok := columnByNorm[norm]
			if !ok {
				return "", fmt.Errorf("create doris table %s: pk_column %q is not present in source fields %v", table, configured, columns)
			}
			seen[norm] = true
			keyCols = append(keyCols, actual)
		}
	} else {
		// Default: use "id" if present. Do not silently pick the first
		// arbitrary column: replay-safe Doris upsert requires a stable key.
		if actual, ok := columnByNorm["id"]; ok {
			keyCols = []string{actual}
		}
		if len(keyCols) == 0 {
			return "", fmt.Errorf("create doris table %s: pk_columns is required when no id column is present; Doris production upsert requires an explicit UNIQUE KEY", table)
		}
	}
	columns = orderDorisColumns(columns, keyCols)
	keySet := make(map[string]bool)
	for _, c := range keyCols {
		keySet[normalizeIdentifier(c)] = true
	}

	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(quoteIdentMySQL(table))
	b.WriteString(" (\n")

	for i, c := range columns {
		if i > 0 {
			b.WriteString(",\n")
		}
		b.WriteString("  ")
		b.WriteString(quoteIdentMySQL(c))
		b.WriteString(" ")

		if keySet[normalizeIdentifier(c)] {
			b.WriteString(inferDorisKeyType(c, fieldValues[c]))
			b.WriteString(" NOT NULL")
		} else {
			b.WriteString(inferDorisType(c, fieldValues[c]))
		}
	}

	// UNIQUE KEY clause
	quotedKeys := make([]string, len(keyCols))
	for i, c := range keyCols {
		quotedKeys[i] = quoteIdentMySQL(c)
	}
	b.WriteString("\n) ENGINE=OLAP\n")
	b.WriteString("UNIQUE KEY(")
	b.WriteString(strings.Join(quotedKeys, ", "))
	b.WriteString(")\n")
	b.WriteString("DISTRIBUTED BY HASH(")
	b.WriteString(strings.Join(quotedKeys, ", "))
	b.WriteString(") BUCKETS 10\n")
	b.WriteString("PROPERTIES (\n")
	b.WriteString("  \"replication_allocation\" = \"tag.location.default: 1\",\n")
	b.WriteString("  \"light_schema_change\" = \"true\"\n")
	b.WriteString(")")

	return b.String(), nil
}

// addColumn adds a column to an existing Doris table.
// Doris supports lightweight schema changes for ADD COLUMN.
func (s *DorisSink) addColumn(ctx context.Context, table, column string, fieldValues map[string]any) error {
	ddl := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s",
		quoteIdentMySQL(table), quoteIdentMySQL(column), inferDorisType(column, fieldValues[column]))
	_, err := s.db.ExecContext(ctx, ddl)
	return err
}

// inferDorisType delegates to the unified typing engine so auto-created
// Doris tables get name-hinted + value-driven types (id→BIGINT, amount→
// DECIMAL, _at→DATETIME, …) consistent with the other relational sinks,
// instead of the old name-only local inference (P4-22, SK-1).
func inferDorisType(colName string, v any) string {
	return typing.InferFromValue(typing.DialectDoris, colName, v)
}

func inferDorisKeyType(colName string, v any) string {
	t := inferDorisType(colName, v)
	// Doris UNIQUE KEY columns should avoid non-comparable or oversized types.
	switch t {
	case "JSON", "STRING":
		return "VARCHAR(255)"
	}
	return t
}

// ── Helpers ─────────────────────────────────────────────────────────────

func (s *DorisSink) resolveTable(rec core.Record) string {
	if s.table != "" {
		return s.table
	}
	return rec.Metadata.Table
}

func (s *DorisSink) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

var _ core.SchemaValidator = (*DorisSink)(nil)
