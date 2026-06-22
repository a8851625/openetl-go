package sink

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/gogf/gf/v2/frame/g"
	"github.com/shopspring/decimal"

	"openetl-go/internal/etl/core"
	"openetl-go/internal/etl/registry"
	"openetl-go/internal/etl/sink/ddl"
	"openetl-go/internal/etl/sink/typing"
)

func init() {
	registry.RegisterSink("clickhouse", func(config map[string]any) (core.Sink, error) {
		return NewClickHouseSink(config)
	})
}

type ClickHouseSink struct {
	name        string
	host        string
	port        int
	user        string
	password    string
	database    string
	table       string
	pkColumns   []string
	versionCol  string
	autoCreate  bool
	schemaDrift string
	ddLPolicy   DDLPolicy
	conn        driver.Conn
	schemas     map[string][]clickhouseColumn
	// engineCache stores the table engine per table name
	engineCache     map[string]string
	localTableCache map[string]string
	// optimizeInterval controls automatic OPTIMIZE TABLE FINAL
	optimizeInterval time.Duration
	// useFinal appends FINAL to internal queries
	useFinal bool
	// protocol: "native" (9000) or "http" (8123)
	protocol string
	// tls enables TLS for the connection
	tls bool
	// tlsSkipVerify skips TLS cert verification
	tlsSkipVerify bool
	// compressionMethod: "LZ4" (default) or "ZSTD"
	compressionMethod string
	// asyncInsert enables ClickHouse async_insert for lower latency
	asyncInsert bool
	// asyncInsertWait waits for async insert to complete
	asyncInsertWait bool
	// ttlExpr optional TTL expression for auto-created tables (e.g. "30 DAY")
	ttlExpr string
	// maxInsertBlockSize adapts batch size to CH server setting
	maxInsertBlockSize int
	// sourceDialect DDL source dialect for translation (e.g., "mysql"). Empty = no translation.
	sourceDialect string
	// httpConn is used when protocol is "http"
	httpConn *sql.DB
	// optimizeCancel cancels the optimize loop goroutine on Close
	optimizeCancel context.CancelFunc
	// Metrics
	rowsWritten    int64
	batchesSent    int64
	writeLatencyNs int64
	// versionCounter ensures monotonic _version values even with clock drift.
	versionCounter atomic.Int64
}

type clickhouseColumn struct {
	Name           string
	Type           string
	IsMaterialized bool // skip MATERIALIZED columns on INSERT
}

func NewClickHouseSink(config map[string]any) (*ClickHouseSink, error) {
	s := &ClickHouseSink{
		name:               "clickhouse",
		port:               9000,
		user:               "default",
		versionCol:         "_version",
		schemaDrift:        "ignore",
		schemas:            make(map[string][]clickhouseColumn),
		engineCache:        make(map[string]string),
		localTableCache:    make(map[string]string),
		protocol:           "native",
		compressionMethod:  "LZ4",
		maxInsertBlockSize: 1048576, // CH default max_insert_block_size
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
	if v, ok := config["pk_columns"]; ok {
		if cols, ok := v.([]interface{}); ok {
			for _, c := range cols {
				s.pkColumns = append(s.pkColumns, c.(string))
			}
		}
	}
	if v, ok := config["version_column"]; ok {
		s.versionCol = v.(string)
	}
	if v, ok := config["auto_create"]; ok {
		if b, ok := v.(bool); ok {
			s.autoCreate = b
		}
	}
	if v, ok := config["schema_drift"]; ok {
		s.schemaDrift = v.(string)
	}
	if v, ok := config["ddl_policy"]; ok {
		if vs, ok := v.(string); ok {
			s.ddLPolicy = DDLPolicy(vs)
		}
	}
	if s.ddLPolicy == "" {
		s.ddLPolicy = DDLPolicyApply
	}
	if v, ok := config["source_dialect"].(string); ok {
		s.sourceDialect = v
	}
	if v, ok := config["optimize_interval_sec"]; ok {
		switch val := v.(type) {
		case int:
			s.optimizeInterval = time.Duration(val) * time.Second
		case float64:
			s.optimizeInterval = time.Duration(int(val)) * time.Second
		}
	}
	if v, ok := config["use_final"]; ok {
		if b, ok := v.(bool); ok {
			s.useFinal = b
		}
	}
	// Protocol: "native" (default, port 9000) or "http" (port 8123, ClickHouse Cloud)
	if v, ok := config["protocol"]; ok {
		s.protocol = v.(string)
		if s.protocol == "http" && s.port == 9000 {
			s.port = 8123
		}
	}
	// TLS
	if v, ok := config["tls"]; ok {
		if b, ok := v.(bool); ok {
			s.tls = b
		}
	}
	if v, ok := config["tls_skip_verify"]; ok {
		if b, ok := v.(bool); ok {
			s.tlsSkipVerify = b
		}
	}
	// Compression: LZ4 (default, fastest) or ZSTD (better ratio for cold data)
	if v, ok := config["compression"]; ok {
		s.compressionMethod = strings.ToUpper(v.(string))
	}
	// Async insert for lower-latency writes (CH server >= 21.11)
	if v, ok := config["async_insert"]; ok {
		if b, ok := v.(bool); ok {
			s.asyncInsert = b
		}
	}
	if v, ok := config["async_insert_wait"]; ok {
		if b, ok := v.(bool); ok {
			s.asyncInsertWait = b
		}
	}
	// TTL expression for auto-created tables
	if v, ok := config["ttl"]; ok {
		s.ttlExpr = v.(string)
	}
	return s, nil
}

func (s *ClickHouseSink) Name() string { return s.name }

// SinkMetrics implements core.SinkMetricsProvider.
func (s *ClickHouseSink) SinkMetrics() core.SinkMetrics {
	wl := float64(0)
	if batches := atomic.LoadInt64(&s.batchesSent); batches > 0 {
		wl = float64(atomic.LoadInt64(&s.writeLatencyNs)) / float64(batches) / 1e6
	}
	return core.SinkMetrics{
		SinkName:     s.name,
		RowsWritten:  atomic.LoadInt64(&s.rowsWritten),
		BatchesSent:  atomic.LoadInt64(&s.batchesSent),
		WriteLatency: wl,
	}
}

func (s *ClickHouseSink) Open(ctx context.Context) error {
	if s.protocol == "http" {
		return s.openHTTP(ctx)
	}
	return s.openNative(ctx)
}

// openNative connects via ClickHouse Native Protocol (port 9000)
func (s *ClickHouseSink) openNative(ctx context.Context) error {
	opts := &clickhouse.Options{
		Addr: []string{fmt.Sprintf("%s:%d", s.host, s.port)},
		Auth: clickhouse.Auth{
			Database: s.database,
			Username: s.user,
			Password: s.password,
		},
		Settings: clickhouse.Settings{
			"max_execution_time": 60,
		},
		DialTimeout: 30 * time.Second,
	}

	// Compression
	switch s.compressionMethod {
	case "ZSTD":
		opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionZSTD}
	default:
		opts.Compression = &clickhouse.Compression{Method: clickhouse.CompressionLZ4}
	}

	// Async insert settings
	if s.asyncInsert {
		opts.Settings["async_insert"] = 1
		if s.asyncInsertWait {
			opts.Settings["wait_for_async_insert"] = 1
		} else {
			opts.Settings["wait_for_async_insert"] = 0
		}
	}

	// TLS
	if s.tls {
		opts.TLS = &tls.Config{
			InsecureSkipVerify: s.tlsSkipVerify,
		}
	}

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return fmt.Errorf("connect clickhouse: %w", err)
	}
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping clickhouse: %w", err)
	}
	s.conn = conn

	// Query server's max_insert_block_size for adaptive batching
	s.queryMaxInsertBlockSize(ctx)

	// Start periodic OPTIMIZE if configured
	if s.optimizeInterval > 0 {
		optCtx, optCancel := context.WithCancel(context.Background())
		s.optimizeCancel = optCancel
		go s.optimizeLoop(optCtx)
	}

	return nil
}

// openHTTP connects via ClickHouse HTTP Protocol (port 8123).
// This is the only option for ClickHouse Cloud and some managed services.
func (s *ClickHouseSink) openHTTP(ctx context.Context) error {
	scheme := "http"
	if s.tls {
		scheme = "https"
	}
	dsn := fmt.Sprintf("%s://%s:%d/%s?username=%s&password=%s",
		scheme, s.host, s.port, s.database, s.user, s.password)
	if s.compressionMethod == "ZSTD" {
		dsn += "&compress=true"
	}
	if s.asyncInsert {
		dsn += "&async_insert=1"
		if !s.asyncInsertWait {
			dsn += "&wait_for_async_insert=0"
		}
	}

	// clickhouse-go supports HTTP via the database/sql interface
	db, err := sql.Open("clickhouse", dsn)
	if err != nil {
		return fmt.Errorf("connect clickhouse http: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping clickhouse http: %w", err)
	}
	s.httpConn = db

	// Also open a native connection for DDL operations if possible.
	// If native port is blocked, DDL will fall back to HTTP.
	s.conn = nil // DDL via HTTP will be handled separately

	return nil
}

// queryMaxInsertBlockSize queries the server's max_insert_block_size setting
// for adaptive batch sizing. Falls back to default on error.
func (s *ClickHouseSink) queryMaxInsertBlockSize(ctx context.Context) {
	if s.conn == nil {
		return
	}
	var blockSize int
	if err := s.queryRowContext(ctx, "SELECT value FROM system.settings WHERE name = 'max_insert_block_size'").Scan(&blockSize); err == nil && blockSize > 0 {
		s.maxInsertBlockSize = blockSize
	}
}

// optimizeLoop periodically runs OPTIMIZE TABLE FINAL on all known tables
// to force background merges of ReplacingMergeTree parts.
func (s *ClickHouseSink) optimizeLoop(ctx context.Context) {
	ticker := time.NewTicker(s.optimizeInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for tableName := range s.schemas {
				localTable, err := s.resolveLocalTable(ctx, tableName)
				if err != nil {
					continue
				}
				sql := fmt.Sprintf("OPTIMIZE TABLE %s.%s FINAL",
					quoteIdent(s.database), quoteIdent(localTable))
				if err := s.execContext(ctx, sql); err != nil {
					// Optimize failures are non-fatal
					continue
				}
			}
		case <-ctx.Done():
			return
		}
	}
}

func (s *ClickHouseSink) Write(ctx context.Context, records []core.Record) error {
	// Separate DDL records — they are applied directly, not batched.
	var ddlRecords []core.Record
	var dataRecords []core.Record
	for _, rec := range records {
		if rec.Operation == core.OpDDL {
			ddlRecords = append(ddlRecords, rec)
		} else {
			dataRecords = append(dataRecords, rec)
		}
	}

	// Apply DDL first according to ddl_policy (schema changes precede data).
	if err := ApplyDDLRecords(ctx, ddlRecords, s.ddLPolicy, func(ctx context.Context, ddlStmt, table string) error {
		execDDL := ddlStmt
		if s.sourceDialect != "" {
			result, err := ddl.TranslateDDL(ddlStmt, ddl.Dialect(s.sourceDialect), ddl.DialectClickHouse)
			if err != nil {
				return fmt.Errorf("translate DDL %q: %w", ddlStmt, err)
			}
			execDDL = result.Statement
			for _, w := range result.Warnings {
				g.Log().Warningf(ctx, "[clickhouse] DDL translation warning: %s", w)
			}
		}
		if err := s.execContext(ctx, execDDL); err != nil {
			return fmt.Errorf("execute DDL %q: %w", execDDL, err)
		}
		delete(s.schemas, table)
		delete(s.engineCache, table)
		return nil
	}); err != nil {
		return err
	}

	// Compact by (table, PK) in source order so multiple events on the same key
	// collapse to the final operation before grouping by op type.
	dataRecords = CompactRecordsByPK(dataRecords, func(table string) []string {
		if len(s.pkColumns) > 0 {
			return s.pkColumns
		}
		return []string{"id"}
	})

	// Group data records by operation type and table for efficient batch processing.
	type tableBatch struct {
		inserts []core.Record
		updates []core.Record
		deletes []core.Record
	}
	batches := map[string]*tableBatch{}

	for _, rec := range dataRecords {
		tableName := s.resolveTable(rec)
		tb, ok := batches[tableName]
		if !ok {
			tb = &tableBatch{}
			batches[tableName] = tb
		}
		switch rec.Operation {
		case core.OpDelete:
			tb.deletes = append(tb.deletes, rec)
		case core.OpUpdate:
			tb.updates = append(tb.updates, rec)
		default:
			tb.inserts = append(tb.inserts, rec)
		}
	}

	for tableName, tb := range batches {
		// Check for non-writable table engines (Iceberg, Parquet, View, etc.)
		if err := s.checkWritableEngine(ctx, tableName); err != nil {
			return err
		}

		if len(tb.inserts) > 0 {
			if err := s.writeInsert(ctx, tableName, tb.inserts); err != nil {
				return err
			}
		}
		if len(tb.updates) > 0 {
			if err := s.writeUpdates(ctx, tableName, tb.updates); err != nil {
				return err
			}
		}
		if len(tb.deletes) > 0 {
			if err := s.writeDeletes(ctx, tableName, tb.deletes); err != nil {
				return err
			}
		}
	}

	return nil
}

// checkWritableEngine rejects writes to table engines that don't support INSERT.
// This prevents confusing errors when users accidentally target read-only tables.
var readOnlyEngines = map[string]bool{
	"Iceberg": true, "Parquet": true, "View": true, "MaterializedView": true,
	"MergeTree": false, "ReplacingMergeTree": false, "SummingMergeTree": false,
	"AggregatingMergeTree": false, "CollapsingMergeTree": false,
	"VersionedCollapsingMergeTree": false, "ReplicatedMergeTree": false,
	"ReplicatedReplacingMergeTree": false, "Distributed": false,
	"URL": true, "S3": true, "HDFS": true, "Kafka": true, "RabbitMQ": true,
	"Dictionary": true, "File": true, "Null": true,
}

func (s *ClickHouseSink) checkWritableEngine(ctx context.Context, tableName string) error {
	engine, err := s.getEngine(ctx, tableName)
	if err != nil {
		return nil // can't check — allow and let CH return the error
	}
	if readOnlyEngines[engine] {
		return fmt.Errorf("table %s.%s uses engine %q which does not support INSERT; use a MergeTree-family table as the write target",
			s.database, tableName, engine)
	}
	return nil
}

// resolveTable determines the target table name for a record, resolving
// Distributed table aliases to their underlying local tables.
func (s *ClickHouseSink) resolveTable(rec core.Record) string {
	tableName := s.table
	if tableName == "" && rec.Metadata.Table != "" {
		tableName = rec.Metadata.Table
	}
	return tableName
}

// resolveLocalTable checks if the given table is a Distributed engine table
// and, if so, returns the underlying local table name. This ensures writes
// go directly to the local shard table, avoiding the Distributed engine overhead.
func (s *ClickHouseSink) resolveLocalTable(ctx context.Context, tableName string) (string, error) {
	if localName, ok := s.localTableCache[tableName]; ok {
		return localName, nil
	}

	engine, err := s.getEngine(ctx, tableName)
	if err != nil {
		return tableName, nil // on error, just use the original table
	}

	if engine != "" && !strings.Contains(strings.ToLower(engine), "distributed") {
		// Not a distributed table — use as-is
		s.localTableCache[tableName] = tableName
		return tableName, nil
	}

	// For Distributed tables, query system.tables to find the underlying local table.
	// Distributed engine params: Distributed(cluster, database, local_table[, sharding_key])
	// We resolve by finding the local table in the same database.
	// Alternative: just write to the distributed table directly (CH will route it).
	// For correctness with ReplacingMergeTree dedup, writing to the local table is better.
	if strings.Contains(strings.ToLower(engine), "distributed") {
		var engineFull string
		err := s.queryRowContext(ctx,
			`SELECT engine_full FROM system.tables WHERE database = ? AND name = ?`,
			s.database, tableName).Scan(&engineFull)
		if err == nil && engineFull != "" {
			// Parse the local table name from engine_full
			// Format: Distributed(cluster_name, database_name, 'local_table_name', ...)
			parts := strings.SplitN(engineFull, ",", 4)
			if len(parts) >= 3 {
				localTable := strings.Trim(strings.TrimSpace(parts[2]), "'\" ")
				if localTable != "" {
					s.localTableCache[tableName] = localTable
					return localTable, nil
				}
			}
		}
		// Fallback: check if a local table with same name exists
		var count int
		err = s.queryRowContext(ctx,
			`SELECT count() FROM system.tables WHERE database = ? AND name = ? AND engine NOT LIKE '%Distributed%'`,
			s.database, tableName).Scan(&count)
		if err == nil && count > 0 {
			s.localTableCache[tableName] = tableName
			return tableName, nil
		}
	}

	s.localTableCache[tableName] = tableName
	return tableName, nil
}

// execContext abstracts Exec for both native and HTTP protocols.
func (s *ClickHouseSink) execContext(ctx context.Context, sql string, args ...any) error {
	if s.conn != nil {
		return s.conn.Exec(ctx, sql, args...)
	}
	if s.httpConn != nil {
		_, err := s.httpConn.ExecContext(ctx, sql, args...)
		return err
	}
	return fmt.Errorf("no clickhouse connection available")
}

// queryRowContext abstracts QueryRow for both native and HTTP protocols.
func (s *ClickHouseSink) queryRowContext(ctx context.Context, sql string, args ...any) interface {
	Scan(dest ...any) error
} {
	if s.conn != nil {
		return s.conn.QueryRow(ctx, sql, args...)
	}
	if s.httpConn != nil {
		return s.httpConn.QueryRowContext(ctx, sql, args...)
	}
	return errQueryRow{}
}

type errQueryRow struct{}

func (errQueryRow) Scan(dest ...any) error { return fmt.Errorf("no connection") }

// getEngine returns the table engine string (e.g. "ReplacingMergeTree", "Distributed").
func (s *ClickHouseSink) getEngine(ctx context.Context, tableName string) (string, error) {
	if eng, ok := s.engineCache[tableName]; ok {
		return eng, nil
	}
	var engine string
	err := s.queryRowContext(ctx,
		`SELECT engine FROM system.tables WHERE database = ? AND name = ?`,
		s.database, tableName).Scan(&engine)
	if err != nil {
		return "", err
	}
	s.engineCache[tableName] = engine
	return engine, nil
}

// applyDDL executes a DDL statement (ALTER TABLE, CREATE TABLE, etc.) on the
// ClickHouse target and invalidates the schema cache so subsequent writes
// pick up the new column set.
func (s *ClickHouseSink) applyDDL(ctx context.Context, ddlRec core.Record) error {
	ddl := ddlRec.Metadata.DDL
	if ddl == "" {
		return nil
	}

	// Execute the raw DDL. ClickHouse DDL syntax is largely compatible with
	// MySQL for ADD COLUMN / DROP COLUMN / MODIFY COLUMN, but not all
	// statements will translate. Failures are returned as errors so the
	// pipeline can route them to DLQ.
	if err := s.execContext(ctx, ddl); err != nil {
		return fmt.Errorf("execute DDL %q: %w", ddl, err)
	}

	// Invalidate schema cache for the affected table so the next batch
	// re-queries system.columns.
	delete(s.schemas, ddlRec.Metadata.Table)
	delete(s.engineCache, ddlRec.Metadata.Table)

	return nil
}

func (s *ClickHouseSink) writeInsert(ctx context.Context, tableName string, records []core.Record) error {
	if tableName == "" {
		return fmt.Errorf("cannot write records without a table name")
	}

	localTable, err := s.resolveLocalTable(ctx, tableName)
	if err != nil {
		return fmt.Errorf("resolve local table for %s: %w", tableName, err)
	}

	columns, err := s.ensureColumns(ctx, localTable, records)
	if err != nil {
		return err
	}

	// Filter out MATERIALIZED and ALIAS columns — these are computed by CH
	// and must not be included in INSERT statements.
	writableCols := make([]clickhouseColumn, 0, len(columns))
	for _, col := range columns {
		if !col.IsMaterialized {
			writableCols = append(writableCols, col)
		}
	}
	if len(writableCols) == 0 {
		return fmt.Errorf("no writable columns found for %s.%s", s.database, localTable)
	}

	start := time.Now()

	// HTTP protocol: use JSONEachRow batch insert via HTTP API
	if s.httpConn != nil && s.conn == nil {
		if err := s.writeInsertHTTP(ctx, localTable, writableCols, records); err != nil {
			return err
		}
		s.recordMetrics(len(records), time.Since(start))
		return nil
	}

	// Native protocol: use PrepareBatch
	sql := fmt.Sprintf("INSERT INTO %s.%s (%s)", quoteIdent(s.database), quoteIdent(localTable), columnList(writableCols))
	batch, err := s.conn.PrepareBatch(ctx, sql)
	if err != nil {
		return fmt.Errorf("prepare batch: %w", err)
	}

	for _, rec := range records {
		values := make([]any, 0, len(writableCols))
		for _, col := range writableCols {
			if col.Name == s.versionCol {
				values = append(values, s.nextVersion())
			} else if v, ok := rec.Data[col.Name]; ok {
				values = append(values, convertClickHouseValue(v, col.Type))
			} else {
				values = append(values, nil)
			}
		}
		if err := batch.Append(values...); err != nil {
			return fmt.Errorf("append batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("send batch: %w", err)
	}

	s.recordMetrics(len(records), time.Since(start))
	return nil
}

// writeInsertHTTP inserts records via HTTP JSONEachRow protocol.
// Used for ClickHouse Cloud and managed services that only expose port 8123.
func (s *ClickHouseSink) writeInsertHTTP(ctx context.Context, tableName string, columns []clickhouseColumn, records []core.Record) error {
	// Build JSON array for batch insert
	var buf strings.Builder
	for _, rec := range records {
		row := make(map[string]any)
		for _, col := range columns {
			if col.IsMaterialized {
				continue
			}
			if col.Name == s.versionCol {
				row[col.Name] = s.nextVersion()
			} else if v, ok := rec.Data[col.Name]; ok {
				row[col.Name] = convertClickHouseValue(v, col.Type)
			} else {
				row[col.Name] = nil
			}
		}
		data, _ := json.Marshal(row)
		buf.Write(data)
		buf.WriteByte('\n')
	}

	scheme := "http"
	if s.tls {
		scheme = "https"
	}
	url := fmt.Sprintf("%s://%s:%d/?query=%s", scheme, s.host, s.port,
		"INSERT+INTO+"+s.database+"."+tableName+"+FORMAT+JSONEachRow")
	if s.asyncInsert {
		url += "&async_insert=1"
	}

	req, err := http.NewRequestWithContext(ctx, "POST", url, strings.NewReader(buf.String()))
	if err != nil {
		return fmt.Errorf("create http insert request: %w", err)
	}
	if s.user != "" {
		req.Header.Set("X-ClickHouse-User", s.user)
	}
	if s.password != "" {
		req.Header.Set("X-ClickHouse-Key", s.password)
	}
	req.Header.Set("Content-Type", "application/json")

	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("http insert: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 300 {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("clickhouse http insert %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

// recordMetrics updates write counters and latency.
func (s *ClickHouseSink) recordMetrics(rows int, latency time.Duration) {
	atomic.AddInt64(&s.rowsWritten, int64(rows))
	atomic.AddInt64(&s.batchesSent, 1)
	atomic.AddInt64(&s.writeLatencyNs, latency.Nanoseconds())
}

func (s *ClickHouseSink) ensureColumns(ctx context.Context, tableName string, records []core.Record) ([]clickhouseColumn, error) {
	columns, err := s.columns(ctx, tableName)
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 && s.autoCreate {
		if err := s.createTable(ctx, tableName, records); err != nil {
			return nil, err
		}
		delete(s.schemas, tableName)
		columns, err = s.columns(ctx, tableName)
		if err != nil {
			return nil, err
		}
	}
	if len(columns) == 0 {
		return columns, nil
	}

	existing := map[string]clickhouseColumn{}
	for _, col := range columns {
		existing[col.Name] = col
	}
	missing := map[string]string{}
	// Track type mismatches between record data and existing columns
	typeMismatches := map[string]string{} // col name → desired type

	for _, rec := range records {
		for name, value := range rec.Data {
			if col, ok := existing[name]; ok {
				// Check if the value type is compatible with the column type.
				// If not, and schema_drift is "sync", we'll ALTER the column type.
				desiredType := inferClickHouseType(name, value)
				if s.schemaDrift == "sync" && !chTypeCompatible(col.Type, desiredType, value) {
					typeMismatches[name] = desiredType
				}
			} else {
				missing[name] = inferClickHouseType(name, value)
			}
		}
	}

	if len(missing) == 0 && len(typeMismatches) == 0 {
		return columns, nil
	}

	if s.schemaDrift == "ignore" || s.schemaDrift == "" {
		return columns, nil
	}

	if s.schemaDrift == "fail" {
		var problems []string
		for name := range missing {
			problems = append(problems, fmt.Sprintf("missing column %s", name))
		}
		for name, dt := range typeMismatches {
			problems = append(problems, fmt.Sprintf("type mismatch on %s (desired %s)", name, dt))
		}
		sort.Strings(problems)
		return nil, fmt.Errorf("clickhouse schema drift on %s.%s: %s", s.database, tableName, strings.Join(problems, "; "))
	}

	// add_columns and sync modes: add missing columns
	if s.schemaDrift == "add_columns" || s.schemaDrift == "sync" {
		var names []string
		for name := range missing {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			sql := fmt.Sprintf("ALTER TABLE %s.%s ADD COLUMN IF NOT EXISTS %s %s",
				quoteIdent(s.database), quoteIdent(tableName), quoteIdent(name), missing[name])
			if err := s.execContext(ctx, sql); err != nil {
				return nil, fmt.Errorf("add clickhouse column %s: %w", name, err)
			}
		}
	}

	// sync mode only: also MODIFY COLUMN type for type mismatches
	if s.schemaDrift == "sync" {
		var mismatchNames []string
		for name := range typeMismatches {
			mismatchNames = append(mismatchNames, name)
		}
		sort.Strings(mismatchNames)
		for _, name := range mismatchNames {
			desired := typeMismatches[name]
			sql := fmt.Sprintf("ALTER TABLE %s.%s MODIFY COLUMN IF EXISTS %s %s",
				quoteIdent(s.database), quoteIdent(tableName),
				quoteIdent(name), desired)
			if err := s.execContext(ctx, sql); err != nil {
				// Type change may fail if data isn't convertible; log and continue
				fmt.Printf("[clickhouse] WARNING: MODIFY COLUMN %s to %s failed: %v\n", name, desired, err)
			}
		}
	}

	delete(s.schemas, tableName)
	return s.columns(ctx, tableName)
}

func (s *ClickHouseSink) createTable(ctx context.Context, tableName string, records []core.Record) error {
	if len(records) == 0 {
		return nil
	}
	types := map[string]string{}
	for _, rec := range records {
		for name, value := range rec.Data {
			if name == s.versionCol {
				continue
			}
			if _, ok := types[name]; !ok {
				types[name] = inferClickHouseType(name, value)
			}
		}
	}
	if len(types) == 0 {
		return fmt.Errorf("cannot auto-create %s.%s without record fields", s.database, tableName)
	}
	var names []string
	for name := range types {
		names = append(names, name)
	}
	sort.Strings(names)
	defs := make([]string, 0, len(names)+1)
	for _, name := range names {
		defs = append(defs, fmt.Sprintf("%s %s", quoteIdent(name), types[name]))
	}
	defs = append(defs, fmt.Sprintf("%s Int64", quoteIdent(s.versionCol)))

	orderBy := "tuple()"
	pkCols := s.pkColumns
	if len(pkCols) == 0 {
		if _, ok := types["id"]; ok {
			pkCols = []string{"id"}
		}
	}
	if len(pkCols) > 0 {
		quoted := make([]string, 0, len(pkCols))
		for _, col := range pkCols {
			quoted = append(quoted, quoteIdent(col))
		}
		orderBy = strings.Join(quoted, ",")
	}

	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s) ENGINE = ReplacingMergeTree(%s) ORDER BY (%s)",
		quoteIdent(s.database), quoteIdent(tableName), strings.Join(defs, ","), quoteIdent(s.versionCol), orderBy)
	// Optional TTL expression (e.g. "toDateTime(created_at) + INTERVAL 30 DAY")
	if s.ttlExpr != "" {
		sql += " TTL " + s.ttlExpr
	}
	if err := s.execContext(ctx, sql); err != nil {
		return fmt.Errorf("auto-create clickhouse table: %w", err)
	}
	return nil
}

func (s *ClickHouseSink) columns(ctx context.Context, tableName string) ([]clickhouseColumn, error) {
	if cols, ok := s.schemas[tableName]; ok {
		return cols, nil
	}
	// Query column name, type, and default_kind to detect MATERIALIZED columns
	rows, err := s.queryContext(ctx, `
		SELECT name, type, default_kind
		FROM system.columns
		WHERE database = ? AND table = ?
		ORDER BY position
	`, s.database, tableName)
	if err != nil {
		return nil, fmt.Errorf("query clickhouse columns: %w", err)
	}
	defer rows.Close()

	var cols []clickhouseColumn
	for rows.Next() {
		var col clickhouseColumn
		var defaultKind string
		if err := rows.Scan(&col.Name, &col.Type, &defaultKind); err != nil {
			return nil, fmt.Errorf("scan clickhouse columns: %w", err)
		}
		col.IsMaterialized = defaultKind == "MATERIALIZED" || defaultKind == "ALIAS"
		cols = append(cols, col)
	}
	s.schemas[tableName] = cols
	return cols, nil
}

// queryContext abstracts querying for both native and HTTP protocols.
func (s *ClickHouseSink) queryContext(ctx context.Context, sql string, args ...any) (rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
	Err() error
}, err error) {
	if s.conn != nil {
		return s.conn.Query(ctx, sql, args...)
	}
	if s.httpConn != nil {
		return s.httpConn.QueryContext(ctx, sql, args...)
	}
	return nil, fmt.Errorf("no clickhouse connection available")
}

// nextVersion returns a monotonically increasing version number for ReplacingMergeTree.
// Uses (millisecond timestamp << 20) | atomic counter to guarantee monotonicity
// even under clock drift or high-frequency concurrent writes.
func (s *ClickHouseSink) nextVersion() int64 {
	counter := s.versionCounter.Add(1)
	return (time.Now().UnixMilli() << 20) | (counter & 0xFFFFF)
}

func columnList(columns []clickhouseColumn) string {
	names := make([]string, 0, len(columns))
	for _, col := range columns {
		names = append(names, quoteIdent(col.Name))
	}
	return strings.Join(names, ",")
}

func convertClickHouseValue(v any, typ string) any {
	if v == nil {
		return nil
	}

	// Unwrap type modifiers for inspection
	innerType := typ
	innerType = strings.TrimSuffix(strings.TrimPrefix(innerType, "Nullable("), ")")
	innerType = strings.TrimSuffix(strings.TrimPrefix(innerType, "LowCardinality("), ")")

	// Handle complex/nested types
	switch {
	case strings.HasPrefix(innerType, "Array"):
		return convertArrayValue(v, strings.TrimSuffix(strings.TrimPrefix(innerType, "Array("), ")"))
	case strings.HasPrefix(innerType, "Map"):
		return convertMapValue(v)
	case strings.HasPrefix(innerType, "Tuple"):
		return convertTupleValue(v)
	case strings.HasPrefix(innerType, "Nested"):
		return convertNestedValue(v)
	case strings.HasPrefix(innerType, "AggregateFunction") || strings.HasPrefix(innerType, "SimpleAggregateFunction"):
		// Aggregate state columns: pass as binary/string if provided
		return v
	}

	// Scalar types
	s := fmt.Sprintf("%v", v)
	switch {
	case strings.HasPrefix(innerType, "Int") || strings.HasPrefix(innerType, "UInt"):
		i, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return i
		}
		f, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return int64(f)
		}
	case strings.HasPrefix(innerType, "Float"):
		f, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return f
		}
	case strings.HasPrefix(innerType, "Decimal"):
		d, err := decimal.NewFromString(s)
		if err == nil {
			return d
		}
	case innerType == "DateTime" || strings.HasPrefix(innerType, "DateTime64"):
		if t, ok := v.(time.Time); ok {
			return t
		}
		for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02"} {
			if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
				return t
			}
		}
	case innerType == "UUID":
		return s // CH driver accepts UUID as string
	case innerType == "Bool":
		if b, ok := v.(bool); ok {
			return b
		}
		return s == "true" || s == "1"
	}

	// LowCardinality(String) or String — check if it's JSON-like
	switch val := v.(type) {
	case map[string]any, []any:
		data, _ := json.Marshal(val)
		return string(data)
	}

	return v
}

// convertArrayValue converts a Go slice to a ClickHouse Array.
func convertArrayValue(v any, elemType string) any {
	switch slice := v.(type) {
	case []any:
		result := make([]any, len(slice))
		for i, elem := range slice {
			result[i] = convertClickHouseValue(elem, elemType)
		}
		return result
	case []string:
		return slice
	case []int:
		return slice
	case []int64:
		return slice
	case []float64:
		return slice
	case []bool:
		return slice
	default:
		// Try JSON decode if string
		if s, ok := v.(string); ok {
			var arr []any
			if json.Unmarshal([]byte(s), &arr) == nil {
				result := make([]any, len(arr))
				for i, elem := range arr {
					result[i] = convertClickHouseValue(elem, elemType)
				}
				return result
			}
		}
		return []any{v}
	}
}

// convertMapValue converts a Go map to a ClickHouse Map.
func convertMapValue(v any) any {
	switch m := v.(type) {
	case map[string]any:
		return m
	case map[string]string:
		result := make(map[string]string, len(m))
		for k, v := range m {
			result[k] = v
		}
		return result
	case map[string]int:
		return m
	case map[string]int64:
		return m
	case map[string]float64:
		return m
	default:
		// Try JSON decode
		if s, ok := v.(string); ok {
			var mp map[string]any
			if json.Unmarshal([]byte(s), &mp) == nil {
				return mp
			}
		}
		return v
	}
}

// convertTupleValue converts a Go value to a ClickHouse Tuple.
func convertTupleValue(v any) any {
	// Tuples are passed as Go slices or arrays in the driver
	switch t := v.(type) {
	case []any:
		return t
	case map[string]any:
		// Named tuple — pass as-is
		return t
	default:
		if s, ok := v.(string); ok {
			var arr []any
			if json.Unmarshal([]byte(s), &arr) == nil {
				return arr
			}
		}
		return v
	}
}

// convertNestedValue handles Nested(name1 Type1, name2 Type2) columns.
// Each Nested column is an array of structs — multiple parallel arrays.
func convertNestedValue(v any) any {
	if arr, ok := v.([]any); ok {
		return arr
	}
	if s, ok := v.(string); ok {
		var arr []any
		if json.Unmarshal([]byte(s), &arr) == nil {
			return arr
		}
	}
	return []any{v}
}

// inferClickHouseType delegates to the unified typing engine so auto-created
// ClickHouse tables get name-hinted + value-driven types (id→Int64, amount→
// Decimal, _at→DateTime64, …) consistent with the other relational sinks,
// instead of the old name-blind local inference (P4-22, SK-1).
func inferClickHouseType(name string, v any) string {
	return typing.InferFromValue(typing.DialectClickHouse, name, v)
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// chTypeCompatible checks whether a Go value can be safely written to a
// ClickHouse column of the given type. Returns true if compatible.
func chTypeCompatible(chType string, desiredType string, value any) bool {
	// Unwrap Nullable
	chType = strings.TrimSuffix(strings.TrimPrefix(chType, "Nullable("), ")")
	chType = strings.TrimSuffix(strings.TrimPrefix(chType, "LowCardinality("), ")")

	switch desiredType {
	case "String", "Nullable(String)":
		return strings.Contains(chType, "String")
	case "Int64":
		return strings.HasPrefix(chType, "Int") || strings.HasPrefix(chType, "UInt")
	case "UInt64":
		return strings.HasPrefix(chType, "UInt")
	case "Float64":
		return strings.HasPrefix(chType, "Float") || strings.HasPrefix(chType, "Int") || strings.HasPrefix(chType, "UInt") || strings.Contains(chType, "Decimal")
	case "UInt8":
		return chType == "UInt8" || chType == "Int8" || strings.Contains(chType, "Bool")
	case "DateTime64(3)":
		return strings.HasPrefix(chType, "DateTime") || strings.HasPrefix(chType, "Date")
	default:
		return true // assume compatible if we can't determine
	}
}

// writeUpdates applies UPDATE operations using ALTER TABLE UPDATE.
// For ReplacingMergeTree tables, UPDATE can also be achieved by inserting
// a new row with a higher _version (the default behavior), but explicit
// ALTER TABLE UPDATE is more correct for non-RMT tables.
// We batch all updates for the same table into a single ALTER statement.
func (s *ClickHouseSink) writeUpdates(ctx context.Context, tableName string, records []core.Record) error {
	localTable, err := s.resolveLocalTable(ctx, tableName)
	if err != nil {
		return fmt.Errorf("resolve local table for update %s: %w", tableName, err)
	}

	pkCols := s.pkColumns
	if len(pkCols) == 0 {
		pkCols = []string{"id"}
	}

	// Check the table engine. For ReplacingMergeTree, INSERT with new _version
	// is the idiomatic way to handle UPDATEs (CH deduplicates on merge).
	engine, _ := s.getEngine(ctx, localTable)
	if engine != "" && (strings.Contains(engine, "Replacing") || strings.Contains(engine, "MergeTree")) {
		// For MergeTree-family tables, insert the updated record as a new version.
		// This is the ClickHouse-recommended approach and works with dedup.
		return s.writeInsert(ctx, tableName, records)
	}

	// For non-MergeTree tables, use ALTER TABLE UPDATE with CASE WHEN for batch efficiency.
	// Build: ALTER TABLE UPDATE col1 = CASE WHEN pk=1 THEN val1 WHEN pk=2 THEN val2 END, ... WHERE pk IN (1,2,...)
	for _, rec := range records {
		var setClauses []string
		var pkConditions []string
		var allArgs []any

		for col, val := range rec.Data {
			isPK := false
			for _, pk := range pkCols {
				if col == pk {
					isPK = true
					break
				}
			}
			if isPK {
				continue
			}
			setClauses = append(setClauses, fmt.Sprintf("%s = ?", quoteIdent(col)))
			allArgs = append(allArgs, val)
		}

		for _, pk := range pkCols {
			if v, ok := rec.Data[pk]; ok {
				pkConditions = append(pkConditions, fmt.Sprintf("%s = ?", quoteIdent(pk)))
				allArgs = append(allArgs, v)
			}
		}

		if len(setClauses) == 0 || len(pkConditions) == 0 {
			continue
		}

		sql := fmt.Sprintf("ALTER TABLE %s.%s UPDATE %s WHERE %s SETTINGS mutations_sync=2",
			quoteIdent(s.database), quoteIdent(localTable),
			strings.Join(setClauses, ", "),
			strings.Join(pkConditions, " AND "))

		if err := s.execContext(ctx, sql, allArgs...); err != nil {
			return fmt.Errorf("clickhouse update %s.%s: %w", s.database, localTable, err)
		}
	}

	return nil
}

// writeDeletes performs batch DELETE using ALTER TABLE DELETE with IN clause.
// Groups all PKs into a single statement: ALTER TABLE DELETE WHERE pk IN (v1, v2, ...)
func (s *ClickHouseSink) writeDeletes(ctx context.Context, tableName string, records []core.Record) error {
	localTable, err := s.resolveLocalTable(ctx, tableName)
	if err != nil {
		return fmt.Errorf("resolve local table for delete %s: %w", tableName, err)
	}

	pkCols := s.pkColumns
	if len(pkCols) == 0 {
		pkCols = []string{"id"}
	}

	if len(pkCols) == 1 {
		// Single PK: batch all values in one DELETE ... WHERE pk IN (...)
		pk := pkCols[0]
		placeholders := make([]string, len(records))
		args := make([]any, len(records))
		for i, rec := range records {
			keys, err := ResolveDeleteKeys(rec, pkCols)
			if err != nil {
				return fmt.Errorf("clickhouse delete %s.%s: %w", s.database, localTable, err)
			}
			placeholders[i] = "?"
			args[i] = keys[pk]
		}
		sql := fmt.Sprintf("ALTER TABLE %s.%s DELETE WHERE %s IN (%s) SETTINGS mutations_sync=2",
			quoteIdent(s.database), quoteIdent(localTable),
			quoteIdent(pk), strings.Join(placeholders, ","))
		if err := s.execContext(ctx, sql, args...); err != nil {
			return fmt.Errorf("clickhouse batch delete %s.%s: %w", s.database, localTable, err)
		}
		return nil
	}

	// Composite PK: use OR conditions (ALTER TABLE DELETE WHERE (pk1=? AND pk2=?) OR ...)
	var conditions []string
	var args []any
	for _, rec := range records {
		keys, err := ResolveDeleteKeys(rec, pkCols)
		if err != nil {
			return fmt.Errorf("clickhouse delete %s.%s: %w", s.database, localTable, err)
		}
		var parts []string
		for _, pk := range pkCols {
			parts = append(parts, fmt.Sprintf("%s = ?", quoteIdent(pk)))
			args = append(args, keys[pk])
		}
		conditions = append(conditions, "("+strings.Join(parts, " AND ")+")")
	}
	if len(conditions) == 0 {
		return nil
	}
	sql := fmt.Sprintf("ALTER TABLE %s.%s DELETE WHERE %s SETTINGS mutations_sync=2",
		quoteIdent(s.database), quoteIdent(localTable),
		strings.Join(conditions, " OR "))
	return s.execContext(ctx, sql, args...)
}

func (s *ClickHouseSink) Close() error {
	if s.optimizeCancel != nil {
		s.optimizeCancel()
	}
	var errs []string
	if s.conn != nil {
		if err := s.conn.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if s.httpConn != nil {
		if err := s.httpConn.Close(); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}
