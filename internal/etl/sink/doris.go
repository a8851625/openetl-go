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

	db          *sql.DB
	httpClient  *http.Client
	schemaCache *core.SchemaCache
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
	if v, ok := config["pk_columns"]; ok {
		if cols, ok := v.([]interface{}); ok {
			for _, c := range cols {
				if cs, ok := c.(string); ok {
					s.pkColumns = append(s.pkColumns, cs)
				}
			}
		}
	}
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
		s.ddLPolicy = DDLPolicyApply
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
	defer func() { if err != nil { s.recordError() } }() // P5-12: count write failures
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
	// HTTP 5xx responses are retryable.
	return strings.Contains(msg, "HTTP 5")
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
	// Deterministic label: hash of (database, table, payload) so retries
	// produce the same label and Doris can deduplicate. Different batches
	// get different labels because the payload differs.
	labelHash := sha256.Sum256(append([]byte(s.database+"."+tableName+"|"), body...))
	label := fmt.Sprintf("etl_%x", labelHash[:16])
	req.Header.Set("label", label)
	// Tell Doris to merge based on Unique Key (default behavior for UK model tables)
	req.Header.Set("merge_type", "MERGE")

	// For CSV, specify column order so Doris maps correctly.
	if s.streamLoadFormat == "csv" {
		cols := s.csvColumnOrder(records)
		if len(cols) > 0 {
			req.Header.Set("columns", strings.Join(cols, ","))
		}
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
		return fmt.Errorf("stream load failed (HTTP %d): %s", resp.StatusCode, string(respBody))
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
		return fmt.Errorf("stream load: unparseable response (HTTP %d): %s", resp.StatusCode, string(respBody)[:min(len(respBody), 200)])
	}
	if slResp.Status == "Fail" || slResp.StatusCode >= 400 {
		return fmt.Errorf("stream load rejected: %s (loaded %d/%d)",
			slResp.Message, slResp.NumberLoadedRows, slResp.NumberTotalRows)
	}

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
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
	tableCols := make(map[string][]string)
	for _, rec := range records {
		tableName := s.resolveTable(rec)
		if tableName == "" {
			continue
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
		}
	}
	for tableName, cols := range tableCols {
		if err := s.EnsureSchema(ctx, tableName, cols, nil); err != nil {
			return err
		}
	}
	return nil
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

// createTableFromFields creates a Doris table with UNIQUE KEY model.
// Doris requires ENGINE=OLAP and a key definition. We use UNIQUE KEY on
// pk_columns (or the first few columns if pk_columns is not set).
func (s *DorisSink) createTableFromFields(ctx context.Context, table string, columns []string, fieldValues map[string]any) error {
	if len(columns) == 0 {
		return nil
	}
	sort.Strings(columns)

	// Determine key columns
	keyCols := s.pkColumns
	if len(keyCols) == 0 {
		// Default: use "id" if present, otherwise first column
		for _, c := range columns {
			if c == "id" || c == "ID" {
				keyCols = []string{c}
				break
			}
		}
		if len(keyCols) == 0 {
			keyCols = []string{columns[0]}
		}
	}
	keySet := make(map[string]bool)
	for _, c := range keyCols {
		keySet[c] = true
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

		if keySet[c] {
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
	b.WriteString(",\n  INDEX idx_")
	b.WriteString(sanitizeIndexName(table))
	b.WriteString(" (")
	b.WriteString(strings.Join(quotedKeys, ", "))
	b.WriteString(") USING BTREE")
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

	_, err := s.db.ExecContext(ctx, b.String())
	return err
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
	if rec.Metadata.Table != "" {
		return rec.Metadata.Table
	}
	return s.table
}

func (s *DorisSink) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
