package sink

import (
	"context"
	"crypto/tls"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"net"
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

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/sink/ddl"
	"github.com/a8851625/openetl-go/internal/etl/sink/typing"
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
	deleteCol   string
	versionMode string
	autoCreate  bool
	schemaDrift string
	ddLPolicy   DDLPolicy
	conn        driver.Conn
	schemas     map[string][]clickhouseColumn
	// engineCache stores the table engine per table name
	engineCache     map[string]string
	localTableCache map[string]string
	// versionSchemaCache records tables whose source-order RMT contract was
	// verified against system.columns + system.tables.engine_full. The cache is
	// invalidated together with the schema/engine caches after DDL.
	versionSchemaCache map[string]bool
	// tableTemplate, when set (e.g. "ods_{table}"), fans out records to
	// per-record target tables derived from metadata ({table}/{db}); when
	// empty the static configured table (or metadata table) is used.
	tableTemplate string
	// pkColumnsFromMetadata derives per-table primary keys from JSON-object
	// Metadata.Key values (multi-table CDC: heterogeneous keys per table).
	pkColumnsFromMetadata bool
	// pkByTable holds the per-table key columns for the batch being written.
	pkByTable map[string][]string
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
	// httpHosts is the failover list for protocol=http: host[:port] entries
	// tried round-robin after the first failure. Empty means single-host via
	// s.host/s.port (pre-existing behavior).
	httpHosts []string
	// httpHostIdx is the round-robin cursor for httpHosts failover.
	httpHostIdx atomic.Int32
	// optimizeCancel cancels the optimize loop goroutine on Close
	optimizeCancel context.CancelFunc
	// Metrics
	rowsWritten    int64
	batchesSent    int64
	writeLatencyNs int64
	writeErrors    int64
	// CH-C1 dedup state (clickhouse_dedup.go): writeBatches counts logical
	// Write calls for token derivation; dedupState holds the last fully
	// acknowledged batch until SinkCommitMetadata collects it.
	writeBatches           atomic.Uint64
	dedupBatchSeq          uint64
	dedupFirst, dedupLast  string
	dedupPositioned        bool
	dedupBatchRows         atomic.Int64
	dedupState             atomic.Pointer[sinkDedupState]
	insertDedupSupported   atomic.Bool
	insertDedupProbeDone   atomic.Bool
	dedupPipelineKey       atomic.Value // string
	// tableMetricsImpl gives per-table write metrics (GAP-6: which target
	// table drags a multi-table batch).
	tableMetricsImpl *tableMetricsSet
}

const (
	clickHouseVersionModeSourceOrder = "source_order"
	clickHouseVersionModeAppend      = "append"
	defaultClickHouseVersionColumn   = "_version"
	defaultClickHouseDeleteColumn    = "_is_deleted"
)

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
		versionCol:         defaultClickHouseVersionColumn,
		deleteCol:          defaultClickHouseDeleteColumn,
		versionMode:        clickHouseVersionModeSourceOrder,
		schemaDrift:        "ignore",
		schemas:            make(map[string][]clickhouseColumn),
		engineCache:        make(map[string]string),
		localTableCache:    make(map[string]string),
		versionSchemaCache: make(map[string]bool),
		tableMetricsImpl:   newTableMetricsSet(),
		protocol:           "native",
		compressionMethod:  "LZ4",
		asyncInsertWait:    true,
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
	if v, ok := config["table_template"]; ok {
		s.tableTemplate = v.(string)
	}
	if v, ok := config["pk_columns_from_metadata"]; ok {
		if b, ok := v.(bool); ok {
			s.pkColumnsFromMetadata = b
		}
	}
	s.pkColumns = append(s.pkColumns, stringSliceConfig(config, "pk_columns")...)
	if v, ok := config["version_column"]; ok {
		if value, ok := v.(string); ok {
			s.versionCol = strings.TrimSpace(value)
		}
	}
	if v, ok := config["delete_column"]; ok {
		if value, ok := v.(string); ok {
			s.deleteCol = strings.TrimSpace(value)
		}
	}
	if v, ok := config["version_mode"]; ok {
		if value, ok := v.(string); ok {
			s.versionMode = strings.ToLower(strings.TrimSpace(value))
		}
	}
	if v, ok := config["auto_create"]; ok {
		if b, ok := v.(bool); ok {
			s.autoCreate = b
		}
	}
	if v, ok := config["schema_drift"]; ok {
		switch val := v.(type) {
		case string:
			s.schemaDrift = val
		case bool:
			if val {
				s.schemaDrift = "add_columns"
			} else {
				s.schemaDrift = "ignore"
			}
		}
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
	// HTTP failover hosts: host or host:port entries tried round-robin after
	// the first connection failure. Only meaningful with protocol=http; the
	// native protocol ignores it (clickhouse-go manages its own addressing).
	if raw, ok := config["hosts"].([]any); ok {
		for _, h := range raw {
			if hs, ok := h.(string); ok && hs != "" {
				s.httpHosts = append(s.httpHosts, hs)
			}
		}
	} else if hs, ok := config["hosts"].(string); ok && hs != "" {
		for _, h := range strings.Split(hs, ",") {
			if h = strings.TrimSpace(h); h != "" {
				s.httpHosts = append(s.httpHosts, h)
			}
		}
	}
	if s.versionMode == "" {
		s.versionMode = clickHouseVersionModeSourceOrder
	}
	if s.versionMode != clickHouseVersionModeSourceOrder && s.versionMode != clickHouseVersionModeAppend {
		return nil, fmt.Errorf("clickhouse version_mode must be source_order or append, got %q", s.versionMode)
	}
	if s.versionMode == clickHouseVersionModeSourceOrder {
		if s.versionCol == "" {
			return nil, fmt.Errorf("clickhouse version_column cannot be empty in version_mode=source_order")
		}
		if s.deleteCol == "" {
			return nil, fmt.Errorf("clickhouse delete_column cannot be empty in version_mode=source_order")
		}
		if strings.EqualFold(s.versionCol, s.deleteCol) {
			return nil, fmt.Errorf("clickhouse version_column and delete_column must be different in version_mode=source_order")
		}
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
		Errors:       atomic.LoadInt64(&s.writeErrors),
	}
}

// tableMetrics records per-target-table write counters (GAP-6, shared
// tableMetricsSet).
func (s *ClickHouseSink) tableMetrics() *tableMetricsSet { return s.tableMetricsImpl }

// TableWriteStats returns per-table write counters (multi-table fan-out
// observability; exposed for API/logging consumers).
func (s *ClickHouseSink) TableWriteStats() []TableWriteStats { return s.tableMetricsImpl.snapshot() }

func (s *ClickHouseSink) ValidateSchema(ctx context.Context, schema core.SchemaInfo) error {
	if len(schema.Columns) == 0 || s.table == "" {
		return nil
	}
	columns, err := s.columns(ctx, s.table)
	if err != nil {
		return fmt.Errorf("validate clickhouse schema: read columns for %s.%s: %w", s.database, s.table, err)
	}
	if len(columns) == 0 {
		if s.autoCreate {
			return nil
		}
		return fmt.Errorf("schema validation failed for clickhouse %s.%s: target table does not exist; enable auto_create or create the table first", s.database, s.table)
	}
	target := make([]core.ColumnInfo, 0, len(columns))
	for _, col := range columns {
		if col.IsMaterialized {
			continue
		}
		target = append(target, core.ColumnInfo{Name: col.Name, DataType: col.Type})
	}
	return validateSchemaCompatibility(schema, target, schemaValidationOptions{
		targetName:     fmt.Sprintf("clickhouse %s.%s", s.database, s.table),
		allowMissing:   s.schemaDrift == "add_columns" || s.schemaDrift == "sync",
		missingRemedy:  "enable schema_drift=add_columns or add the columns manually",
		allowTypeSync:  s.schemaDrift == "sync",
		typeSyncRemedy: "enable schema_drift=sync, change the target column type, or add a transform/type_convert before the sink",
	})
}

// ValidateTargetContract checks a configured static target before a source is
// opened. Dynamic table/template targets are checked on their first write.
func (s *ClickHouseSink) ValidateTargetContract(ctx context.Context) error {
	if strings.TrimSpace(s.table) == "" {
		return nil
	}
	columns, err := s.columns(ctx, s.table)
	if err != nil {
		return fmt.Errorf("read clickhouse target contract for %s.%s: %w", s.database, s.table, err)
	}
	if len(columns) == 0 {
		if s.autoCreate {
			return nil
		}
		return fmt.Errorf("clickhouse target %s.%s does not exist; enable auto_create or create it before startup", s.database, s.table)
	}
	if s.effectiveVersionMode() == clickHouseVersionModeSourceOrder {
		return s.validateSourceOrderTable(ctx, s.table, columns)
	}
	engine, err := s.getEngine(ctx, s.table)
	if err != nil {
		return fmt.Errorf("read clickhouse append target engine for %s.%s: %w", s.database, s.table, err)
	}
	if strings.Contains(strings.ToLower(engine), "replacingmergetree") {
		return fmt.Errorf("clickhouse version_mode=append requires a non-replacing MergeTree target, but %s.%s uses %s", s.database, s.table, engine)
	}
	return nil
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
		return fmt.Errorf("connect clickhouse (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}
	if err := conn.Ping(ctx); err != nil {
		return fmt.Errorf("ping clickhouse (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
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
		return fmt.Errorf("connect clickhouse http (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping clickhouse http (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
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

func (s *ClickHouseSink) Write(ctx context.Context, records []core.Record) (err error) {
	defer func() {
		if err != nil {
			s.recordError()
		}
	}() // P5-12: count write failures
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

	// Validate and normalize the complete data batch before executing DDL or
	// writing any row. This keeps an unavailable source position or an UPDATE /
	// DELETE in append mode from producing a partial target-side side effect.
	dataRecords, err = s.prepareDataRecords(dataRecords)
	if err != nil {
		return err
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
			// Checkpoint reset / at-least-once replay can re-deliver ADD COLUMN
			// after the column already exists — treat as success (PR-2).
			msg := strings.ToLower(err.Error())
			if strings.Contains(msg, "already exists") || strings.Contains(msg, "column with this name already exists") {
				delete(s.schemas, table)
				delete(s.engineCache, table)
				delete(s.versionSchemaCache, table)
				return nil
			}
			return fmt.Errorf("execute DDL %q: %w", execDDL, err)
		}
		delete(s.schemas, table)
		delete(s.engineCache, table)
		delete(s.versionSchemaCache, table)
		return nil
	}); err != nil {
		return err
	}

	// Do not compact by arrival order. In source_order mode every source
	// version must reach ReplacingMergeTree so a replayed old event cannot win
	// merely because it arrived last. In append mode every INSERT is likewise
	// an intentional append and must not be collapsed.

	// Group data records by operation type and table for efficient batch processing.
	type tableBatch struct {
		inserts []core.Record
		updates []core.Record
		deletes []core.Record
	}
	batches := map[string]*tableBatch{}

	for _, rec := range dataRecords {
		tableName, err := s.resolveTable(rec)
		if err != nil {
			return err
		}
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

	// CH-C1: stamp this logical batch before any table sub-batch goes out.
	s.beginDedupBatch(records)
	defer func() {
		if err != nil {
			// Any failure path invalidates the pending boundary; only a fully
			// acknowledged Write may surface commit metadata.
			s.invalidateDedupBatch()
		}
	}()

	for tableName, tb := range batches {
		start := time.Now()
		failed := false
		rows := len(tb.inserts) + len(tb.updates) + len(tb.deletes)
		defer func() { s.tableMetricsImpl.record(tableName, rows, time.Since(start), failed) }()
		// Check for non-writable table engines (Iceberg, Parquet, View, etc.)
		if err := s.checkWritableEngine(ctx, tableName); err != nil {
			failed = true
			return err
		}

		if len(tb.inserts) > 0 {
			if err := s.writeInsert(ctx, tableName, tb.inserts); err != nil {
				failed = true
				return err
			}
		}
		if len(tb.updates) > 0 {
			if err := s.writeUpdates(ctx, tableName, tb.updates); err != nil {
				failed = true
				return err
			}
		}
		if len(tb.deletes) > 0 {
			if err := s.writeDeletes(ctx, tableName, tb.deletes); err != nil {
				failed = true
				return err
			}
		}
	}

	// CH-C1: every table sub-batch acknowledged — publish the batch boundary.
	tableRows := make(map[string]int, len(batches))
	for tableName, tb := range batches {
		tableRows[tableName] = len(tb.inserts) + len(tb.updates) + len(tb.deletes)
	}
	s.finalizeDedupBatch(s.dedupPipelineKey.Load().(string), dataRecords, tableRows)

	return nil
}

// prepareDataRecords establishes the ClickHouse delivery contract before any
// target-side effect. source_order injects source-owned UInt64 versions and a
// delete flag; append accepts INSERT-only records and never fabricates order.
func (s *ClickHouseSink) prepareDataRecords(records []core.Record) ([]core.Record, error) {
	pkByTable, err := s.pkColumnsByTable(records)
	if err != nil {
		return nil, err
	}
	s.pkByTable = pkByTable
	if len(records) == 0 {
		return records, nil
	}

	mode := s.effectiveVersionMode()
	out := make([]core.Record, 0, len(records)+1)
	for _, rec := range records {
		if mode == clickHouseVersionModeAppend {
			if rec.Operation != "" && rec.Operation != core.OpInsert {
				return nil, fmt.Errorf("clickhouse version_mode=append accepts INSERT-only records; got operation %s for %s.%s; use version_mode=source_order with source position metadata for mutable CDC data", rec.Operation, rec.Metadata.Database, rec.Metadata.Table)
			}
			out = append(out, cloneClickHouseRecord(rec))
			continue
		}

		order := core.SourceOrder(rec)
		if !order.Available || !order.VersionAvailable {
			return nil, clickHouseSourceOrderError(rec, order)
		}
		prepared := cloneClickHouseRecord(rec)
		prepared.Data[s.effectiveVersionColumn()] = order.Version
		prepared.Data[s.effectiveDeleteColumn()] = uint8(0)

		switch prepared.Operation {
		case core.OpDelete:
			prepared.Data[s.effectiveDeleteColumn()] = uint8(1)
			out = append(out, prepared)
		case core.OpUpdate:
			changed, tombstone, err := s.keyChangeTombstone(prepared, order.Version)
			if err != nil {
				return nil, err
			}
			out = append(out, prepared)
			if changed {
				out = append(out, tombstone)
			}
		default:
			out = append(out, prepared)
		}
	}
	return out, nil
}

func (s *ClickHouseSink) effectiveVersionMode() string {
	if s.versionMode == "" {
		return clickHouseVersionModeSourceOrder
	}
	return s.versionMode
}

func (s *ClickHouseSink) effectiveVersionColumn() string {
	if s.versionCol == "" {
		return defaultClickHouseVersionColumn
	}
	return s.versionCol
}

func (s *ClickHouseSink) effectiveDeleteColumn() string {
	if s.deleteCol == "" {
		return defaultClickHouseDeleteColumn
	}
	return s.deleteCol
}

func cloneClickHouseRecord(rec core.Record) core.Record {
	clone := rec
	clone.Data = make(map[string]any, len(rec.Data)+2)
	for key, value := range rec.Data {
		clone.Data[key] = value
	}
	if rec.Before != nil {
		clone.Before = make(map[string]any, len(rec.Before))
		for key, value := range rec.Before {
			clone.Before[key] = value
		}
	}
	return clone
}

func clickHouseSourceOrderError(rec core.Record, order core.SourceOrderResult) error {
	fields := "metadata.source_type and a connector-owned durable position"
	sourceType := strings.ToLower(strings.TrimSpace(rec.Metadata.SourceType))
	if sourceType == "" {
		sourceType = strings.ToLower(strings.TrimSpace(rec.Metadata.Source))
	}
	switch sourceType {
	case core.SourceTypeMySQLCDC:
		fields = "metadata.binlog_file and metadata.binlog_pos"
	case core.SourceTypeMySQLSnapshotCDC:
		if strings.EqualFold(rec.Metadata.SourcePhase, core.SourcePhaseSnapshot) || rec.Metadata.BinlogFile == "" {
			fields = "metadata.snapshot_handoff_file and metadata.snapshot_handoff_pos for snapshot rows"
		} else {
			fields = "metadata.binlog_file and metadata.binlog_pos for CDC rows"
		}
	case core.SourceTypePostgresCDC:
		fields = "metadata.lsn (PostgreSQL initial snapshot rows currently have no numeric source order)"
	case core.SourceTypeKafka:
		fields = "metadata.partition and metadata.offset"
	case core.SourceTypeMySQLBatch:
		fields = "metadata.cursor and metadata.cursor_kind=numeric"
	}
	return fmt.Errorf("clickhouse version_mode=source_order cannot derive a UInt64 version for source_type=%q table=%q: reason=%s; required fields: %s; do not fall back to wall clock, and use version_mode=append only for INSERT-only data", sourceType, rec.Metadata.Table, order.Reason, fields)
}

// keyChangeTombstone expands UPDATE(old PK -> new PK) into a tombstone for the
// old sorting key plus the live row for the new key, both at the same source
// version. Canal-style partial before images are merged over the full after
// image so unchanged components of a composite key remain available.
func (s *ClickHouseSink) keyChangeTombstone(rec core.Record, version uint64) (bool, core.Record, error) {
	table, err := s.resolveTable(rec)
	if err != nil {
		return false, core.Record{}, err
	}
	pkCols := s.primaryKeyColumns(table)
	if len(pkCols) == 0 {
		return false, core.Record{}, fmt.Errorf("clickhouse source_order UPDATE for table %q has no primary key columns; configure pk_columns or pk_columns_from_metadata", table)
	}

	oldData := make(map[string]any, len(rec.Data)+len(rec.Before)+2)
	for key, value := range rec.Data {
		oldData[key] = value
	}
	for key, value := range rec.Before {
		oldData[key] = value
	}
	changed := false
	for _, column := range pkCols {
		after, afterOK := rec.Data[column]
		before, beforeOK := oldData[column]
		if !afterOK || after == nil || !beforeOK || before == nil {
			return false, core.Record{}, fmt.Errorf("clickhouse source_order UPDATE for table %q cannot compare primary-key column %q across before/after images", table, column)
		}
		if original, explicitlyBefore := rec.Before[column]; explicitlyBefore && !clickHouseKeyValueEqual(original, after) {
			changed = true
		}
	}
	if !changed {
		return false, core.Record{}, nil
	}

	tombstone := cloneClickHouseRecord(rec)
	tombstone.Operation = core.OpDelete
	tombstone.Data = oldData
	tombstone.Data[s.effectiveVersionColumn()] = version
	tombstone.Data[s.effectiveDeleteColumn()] = uint8(1)
	return true, tombstone, nil
}

func clickHouseKeyValueEqual(left, right any) bool {
	return fmt.Sprint(left) == fmt.Sprint(right)
}

func (s *ClickHouseSink) primaryKeyColumns(table string) []string {
	if byTable, ok := s.pkByTable[table]; ok && len(byTable) > 0 {
		return byTable
	}
	if len(s.pkColumns) > 0 {
		return s.pkColumns
	}
	return []string{"id"}
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

// resolveTable determines the target table name for a record. With a
// table_template (e.g. "ods_{table}"), {table}/{db} are substituted from
// record metadata; otherwise the static configured table (or record metadata
// table when static is empty) is used. A template that references {table}/
// {db} but the record carries no such metadata is a configuration error
// rather than silently emitting a malformed name (e.g. "ods_") which would
// mix unrelated tables into one destination.
func (s *ClickHouseSink) resolveTable(rec core.Record) (string, error) {
	if s.tableTemplate == "" {
		if s.table != "" {
			return s.table, nil
		}
		return rec.Metadata.Table, nil
	}
	if strings.Contains(s.tableTemplate, "{db}") && rec.Metadata.Database == "" {
		return "", fmt.Errorf("clickhouse sink: table_template %q references {db} but record has no database metadata", s.tableTemplate)
	}
	if strings.Contains(s.tableTemplate, "{table}") && rec.Metadata.Table == "" {
		return "", fmt.Errorf("clickhouse sink: table_template %q references {table} but record has no table metadata", s.tableTemplate)
	}
	table := strings.ReplaceAll(s.tableTemplate, "{db}", rec.Metadata.Database)
	table = strings.ReplaceAll(table, "{table}", rec.Metadata.Table)
	if table == "" {
		return "", fmt.Errorf("clickhouse sink: table_template %q resolved to empty", s.tableTemplate)
	}
	return table, nil
}

// pkColumnsByTable validates and derives the per-table primary key columns
// used by a metadata-driven batch. The shared contract verifies the declared
// complete identity; this sink never falls back to static pk_columns or id for
// an ordinary record whose metadata identity is missing or incomplete.
func (s *ClickHouseSink) pkColumnsByTable(records []core.Record) (map[string][]string, error) {
	if !s.pkColumnsFromMetadata {
		return map[string][]string{}, nil
	}
	started := time.Now()
	validation, err := validateMetadataPKBatch(s.name, records, s.pkColumns, func(record core.Record) (metadataPKTarget, error) {
		table, resolveErr := s.resolveTable(record)
		return metadataPKTarget{Database: s.database, Table: table}, resolveErr
	})
	if err != nil {
		recordMetadataPKFailure(s.tableMetricsImpl, err, time.Since(started))
		return nil, err
	}
	return validation.ColumnsByTable, nil
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
	//
	// Checkpoint reset / at-least-once replay can re-deliver ADD COLUMN after
	// the column already exists. Treat that as success so schema-drift paths
	// remain idempotent under PR-2 fault matrices.
	if err := s.execContext(ctx, ddl); err != nil {
		msg := strings.ToLower(err.Error())
		if strings.Contains(msg, "already exists") || strings.Contains(msg, "column with this name already exists") {
			delete(s.schemas, ddlRec.Metadata.Table)
			delete(s.engineCache, ddlRec.Metadata.Table)
			delete(s.versionSchemaCache, ddlRec.Metadata.Table)
			return nil
		}
		return fmt.Errorf("execute DDL %q: %w", ddl, err)
	}

	// Invalidate schema cache for the affected table so the next batch
	// re-queries system.columns.
	delete(s.schemas, ddlRec.Metadata.Table)
	delete(s.engineCache, ddlRec.Metadata.Table)
	delete(s.versionSchemaCache, ddlRec.Metadata.Table)

	return nil
}

func (s *ClickHouseSink) writeInsert(ctx context.Context, tableName string, records []core.Record) error {
	return s.writeInsertSelected(ctx, tableName, records, nil)
}

// writeInsertSelected writes all table columns when selected is nil. A
// tombstone supplies selected={PKs,version,delete}; omitted non-key columns
// then receive ClickHouse defaults and cannot make a delete fail merely
// because the source only retained its business key.
func (s *ClickHouseSink) writeInsertSelected(ctx context.Context, tableName string, records []core.Record, selected map[string]bool) error {
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
		if !col.IsMaterialized && (selected == nil || selected[col.Name]) {
			writableCols = append(writableCols, col)
		}
	}
	if len(writableCols) == 0 {
		return fmt.Errorf("no writable columns found for %s.%s", s.database, localTable)
	}

	start := time.Now()

	// CH-C1: probe server support once; the deterministic dedup token is
	// attached to the INSERT by both protocols (native: query setting via
	// ctx below; HTTP: URL param inside writeInsertHTTP).
	s.detectInsertDedupSupport(ctx)

	// HTTP protocol: use JSONEachRow batch insert via HTTP API
	if s.httpConn != nil && s.conn == nil {
		if err := s.writeInsertHTTP(ctx, localTable, writableCols, records); err != nil {
			return classifyClickHouseWriteError(err)
		}
		s.recordMetrics(len(records), time.Since(start))
		return nil
	}

	// Native protocol: use PrepareBatch
	sql := fmt.Sprintf("INSERT INTO %s.%s (%s)", quoteIdent(s.database), quoteIdent(localTable), columnList(writableCols))
	nativeCtx := withInsertDedupToken(ctx, s.pendingDedupToken(), s.insertDedupSupported.Load())
	batch, err := s.conn.PrepareBatch(nativeCtx, sql)
	if err != nil {
		return classifyClickHouseWriteError(fmt.Errorf("prepare batch: %w", err))
	}

	for _, rec := range records {
		values := make([]any, 0, len(writableCols))
		for _, col := range writableCols {
			value, valueErr := s.clickHouseColumnValue(rec, col, false)
			if valueErr != nil {
				return valueErr
			}
			values = append(values, value)
		}
		if err := batch.Append(values...); err != nil {
			return fmt.Errorf("append batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return classifyClickHouseWriteError(fmt.Errorf("send batch: %w", err))
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
			value, err := s.clickHouseColumnValue(rec, col, true)
			if err != nil {
				return err
			}
			row[col.Name] = value
		}
		data, err := json.Marshal(row)
		if err != nil {
			return fmt.Errorf("marshal clickhouse JSONEachRow for %s.%s: %w", s.database, tableName, err)
		}
		buf.Write(data)
		buf.WriteByte('\n')
	}

	scheme := "http"
	if s.tls {
		scheme = "https"
	}
	buildURL := func(addr string) string {
		host, port := addr, fmt.Sprintf("%d", s.port)
		if h, p, err := net.SplitHostPort(addr); err == nil {
			host, port = h, p
		}
		url := fmt.Sprintf("%s://%s:%s/?query=%s", scheme, host, port,
			"INSERT+INTO+"+s.database+"."+tableName+"+FORMAT+JSONEachRow")
		if s.asyncInsert {
			url += "&async_insert=1"
		}
		// CH-C1: HTTP path carries the same deterministic dedup token as the
		// native path so both protocols expose identical server-side dedup.
		if s.insertDedupSupported.Load() {
			url += "&insert_dedup_token=" + s.pendingDedupToken()
		}
		return url
	}

	// Failover list: the primary host plus configured httpHosts entries.
	// Each candidate is tried once; connection errors move to the next host
	// (round-robin start so traffic spreads after recovery), while HTTP error
	// responses are server-side failures and are returned immediately.
	addresses := []string{fmt.Sprintf("%s:%d", s.host, s.port)}
	addresses = append(addresses, s.httpHosts...)
	first := int(s.httpHostIdx.Add(1)-1) % len(addresses)
	if first < 0 {
		first += len(addresses)
	}
	client := &http.Client{Timeout: 30 * time.Second}
	var lastErr error
	for i := 0; i < len(addresses); i++ {
		addr := addresses[(first+i)%len(addresses)]
		req, err := http.NewRequestWithContext(ctx, "POST", buildURL(addr), strings.NewReader(buf.String()))
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

		resp, err := client.Do(req)
		if err != nil {
			lastErr = fmt.Errorf("http insert via %s: %w", addr, err)
			continue // connection-level failure: try next host
		}
		if resp.StatusCode >= 300 {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			return fmt.Errorf("clickhouse http insert %d: %s", resp.StatusCode, string(body))
		}
		resp.Body.Close()
		return nil
	}
	return lastErr
}

func (s *ClickHouseSink) clickHouseColumnValue(rec core.Record, col clickhouseColumn, httpValue bool) (any, error) {
	value, ok := rec.Data[col.Name]
	if !ok {
		return nil, nil
	}
	if s.effectiveVersionMode() == clickHouseVersionModeSourceOrder {
		switch col.Name {
		case s.effectiveVersionColumn():
			version, ok := value.(uint64)
			if !ok {
				return nil, fmt.Errorf("clickhouse reserved version column %q requires uint64, got %T", col.Name, value)
			}
			return version, nil
		case s.effectiveDeleteColumn():
			switch deleted := value.(type) {
			case uint8:
				return deleted, nil
			case bool:
				if deleted {
					return uint8(1), nil
				}
				return uint8(0), nil
			default:
				return nil, fmt.Errorf("clickhouse reserved tombstone column %q requires uint8/bool, got %T", col.Name, value)
			}
		}
	}
	if httpValue {
		return convertClickHouseHTTPValue(value, col.Type), nil
	}
	return convertClickHouseValue(value, col.Type), nil
}

// recordMetrics updates write counters and latency.
func (s *ClickHouseSink) recordMetrics(rows int, latency time.Duration) {
	atomic.AddInt64(&s.rowsWritten, int64(rows))
	atomic.AddInt64(&s.batchesSent, 1)
	atomic.AddInt64(&s.writeLatencyNs, latency.Nanoseconds())
}

// recordError increments the write-error counter (call on write failure). P5-12.
func (s *ClickHouseSink) recordError() { atomic.AddInt64(&s.writeErrors, 1) }

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
	if s.effectiveVersionMode() == clickHouseVersionModeSourceOrder {
		if err := s.validateSourceOrderTable(ctx, tableName, columns); err != nil {
			return nil, err
		}
	} else if engine, engineErr := s.getEngine(ctx, tableName); engineErr == nil && strings.Contains(strings.ToLower(engine), "replacingmergetree") {
		return nil, fmt.Errorf("clickhouse version_mode=append requires a non-replacing MergeTree target, but %s.%s uses %s; use a plain MergeTree table or version_mode=source_order with the UInt64 version/tombstone contract", s.database, tableName, engine)
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
			declared := rec.Metadata.ColumnTypes[name]
			if col, ok := existing[name]; ok {
				// Check if the value type is compatible with the column type.
				// If not, and schema_drift is "sync", we'll ALTER the column type.
				desiredType := inferClickHouseType(name, value, declared)
				if s.schemaDrift == "sync" && !chTypeCompatible(col.Type, desiredType, value) {
					typeMismatches[name] = desiredType
				}
			} else {
				missing[name] = inferClickHouseType(name, value, declared)
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
	delete(s.versionSchemaCache, tableName)
	return s.columns(ctx, tableName)
}

func (s *ClickHouseSink) createTable(ctx context.Context, tableName string, records []core.Record) error {
	if len(records) == 0 {
		return nil
	}
	types := map[string]string{}
	declared := map[string]string{}
	for _, rec := range records {
		for name, ct := range rec.Metadata.ColumnTypes {
			if _, ok := declared[name]; !ok {
				declared[name] = ct
			}
		}
	}
	for _, rec := range records {
		for name, value := range rec.Data {
			if s.effectiveVersionMode() == clickHouseVersionModeSourceOrder && (name == s.effectiveVersionColumn() || name == s.effectiveDeleteColumn()) {
				continue
			}
			if _, ok := types[name]; !ok {
				types[name] = inferClickHouseType(name, value, declared[name])
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
	reservedColumns := 0
	if s.effectiveVersionMode() == clickHouseVersionModeSourceOrder {
		reservedColumns = 2
	}
	defs := make([]string, 0, len(names)+reservedColumns)
	for _, name := range names {
		defs = append(defs, fmt.Sprintf("%s %s", quoteIdent(name), types[name]))
	}
	if s.effectiveVersionMode() == clickHouseVersionModeSourceOrder {
		defs = append(defs,
			fmt.Sprintf("%s UInt64", quoteIdent(s.effectiveVersionColumn())),
			fmt.Sprintf("%s UInt8", quoteIdent(s.effectiveDeleteColumn())),
		)
	}

	orderBy := "tuple()"
	pkCols := s.pkColumns
	if pkByTable, ok := s.pkByTable[tableName]; ok {
		pkCols = pkByTable
	}
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

	engine := "MergeTree"
	if s.effectiveVersionMode() == clickHouseVersionModeSourceOrder {
		engine = fmt.Sprintf("ReplacingMergeTree(%s, %s)", quoteIdent(s.effectiveVersionColumn()), quoteIdent(s.effectiveDeleteColumn()))
	}
	sql := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s.%s (%s) ENGINE = %s ORDER BY (%s)",
		quoteIdent(s.database), quoteIdent(tableName), strings.Join(defs, ","), engine, orderBy)
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

func (s *ClickHouseSink) validateSourceOrderTable(ctx context.Context, tableName string, columns []clickhouseColumn) error {
	if s.versionSchemaCache != nil && s.versionSchemaCache[tableName] {
		return nil
	}
	engine, err := s.getEngine(ctx, tableName)
	if err != nil {
		return fmt.Errorf("validate clickhouse source-order table %s.%s engine: %w", s.database, tableName, err)
	}
	var engineFull string
	if err := s.queryRowContext(ctx,
		`SELECT engine_full FROM system.tables WHERE database = ? AND name = ?`,
		s.database, tableName).Scan(&engineFull); err != nil {
		return fmt.Errorf("validate clickhouse source-order table %s.%s engine definition: %w", s.database, tableName, err)
	}
	if err := validateClickHouseSourceOrderSchema(engine, engineFull, columns, s.effectiveVersionColumn(), s.effectiveDeleteColumn()); err != nil {
		return fmt.Errorf("clickhouse source-order table %s.%s is incompatible: %w; migrate by creating a new table with %s UInt64, %s UInt8 and ReplacingMergeTree(%s, %s), backfill current rows with a documented source version, then atomically switch the pipeline; legacy wall-clock Int64 versions cannot be compared safely with source positions",
			s.database, tableName, err,
			quoteIdent(s.effectiveVersionColumn()), quoteIdent(s.effectiveDeleteColumn()),
			quoteIdent(s.effectiveVersionColumn()), quoteIdent(s.effectiveDeleteColumn()))
	}
	if s.versionSchemaCache == nil {
		s.versionSchemaCache = make(map[string]bool)
	}
	s.versionSchemaCache[tableName] = true
	return nil
}

func validateClickHouseSourceOrderSchema(engine, engineFull string, columns []clickhouseColumn, versionCol, deleteCol string) error {
	if !strings.Contains(strings.ToLower(engine), "replacingmergetree") {
		return fmt.Errorf("engine %q is not ReplacingMergeTree-compatible", engine)
	}
	columnTypes := make(map[string]clickhouseColumn, len(columns))
	for _, column := range columns {
		columnTypes[column.Name] = column
	}
	version, ok := columnTypes[versionCol]
	if !ok {
		return fmt.Errorf("missing version column %q", versionCol)
	}
	if version.IsMaterialized || !strings.EqualFold(strings.TrimSpace(version.Type), "UInt64") {
		return fmt.Errorf("version column %q has type %q, want writable UInt64", versionCol, version.Type)
	}
	deleted, ok := columnTypes[deleteCol]
	if !ok {
		return fmt.Errorf("missing tombstone column %q", deleteCol)
	}
	if deleted.IsMaterialized || !strings.EqualFold(strings.TrimSpace(deleted.Type), "UInt8") {
		return fmt.Errorf("tombstone column %q has type %q, want writable UInt8", deleteCol, deleted.Type)
	}

	args, err := clickHouseEngineArguments(engineFull)
	if err != nil {
		return err
	}
	if len(args) < 2 || !strings.EqualFold(normalizeClickHouseEngineIdentifier(args[len(args)-2]), versionCol) || !strings.EqualFold(normalizeClickHouseEngineIdentifier(args[len(args)-1]), deleteCol) {
		return fmt.Errorf("engine definition %q must use version/tombstone as its final arguments (%s, %s)", engineFull, versionCol, deleteCol)
	}
	return nil
}

func clickHouseEngineArguments(engineFull string) ([]string, error) {
	open := strings.IndexByte(engineFull, '(')
	if open < 0 {
		return nil, fmt.Errorf("engine definition %q has no argument list", engineFull)
	}
	// system.tables.engine_full contains the complete engine clause, for
	// example:
	//
	//   ReplacingMergeTree(_version, _is_deleted) ORDER BY (id, tenant_id)
	//
	// The final ')' therefore belongs to ORDER BY, not to the engine argument
	// list. Parse until the ')' matching the first '(' instead of slicing at
	// LastIndexByte, which incorrectly folds the trailing clause into the
	// engine arguments and reports a valid auto-created table as unbalanced.
	input := engineFull[open+1:]
	var args []string
	start := 0
	depth := 0
	var quote rune
	escaped := false
	for index, r := range input {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' && quote != 0 {
			escaped = true
			continue
		}
		if quote != 0 {
			if r == quote {
				quote = 0
			}
			continue
		}
		switch r {
		case '\'', '"', '`':
			quote = r
		case '(':
			depth++
		case ')':
			if depth == 0 {
				args = append(args, strings.TrimSpace(input[start:index]))
				return args, nil
			}
			depth--
		case ',':
			if depth == 0 {
				args = append(args, strings.TrimSpace(input[start:index]))
				start = index + 1
			}
		}
	}
	return nil, fmt.Errorf("engine definition %q has unbalanced arguments", engineFull)
}

func normalizeClickHouseEngineIdentifier(value string) string {
	value = strings.TrimSpace(value)
	for len(value) >= 2 {
		first, last := value[0], value[len(value)-1]
		if (first == '`' && last == '`') || (first == '"' && last == '"') || (first == '\'' && last == '\'') {
			value = strings.TrimSpace(value[1 : len(value)-1])
			continue
		}
		break
	}
	return value
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
		// Empty/blank strings (e.g. MySQL '' synced into an existing numeric
		// column) must not abort the whole batch: keep MySQL-ish semantics
		// and convert them to 0. Non-numeric garbage is left untouched so it
		// still fails loudly at AppendRow and lands in the DLQ instead of
		// being silently coerced.
		if strings.TrimSpace(s) == "" {
			return int64(0)
		}
		i, err := strconv.ParseInt(s, 10, 64)
		if err == nil {
			return i
		}
		f, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return int64(f)
		}
	case strings.HasPrefix(innerType, "Float"):
		if strings.TrimSpace(s) == "" {
			return float64(0)
		}
		f, err := strconv.ParseFloat(s, 64)
		if err == nil {
			return f
		}
	case strings.HasPrefix(innerType, "Decimal"):
		if strings.TrimSpace(s) == "" {
			return decimal.NewFromInt(0)
		}
		d, err := decimal.NewFromString(s)
		if err == nil {
			return d
		}
	case innerType == "Date":
		// Date columns only carry YYYY-MM-DD. Source pipelines that round-trip
		// through kafka envelopes serialize MySQL DATE as RFC3339 ("2020-01-01T00:00:00+08:00");
		// the clickhouse-go driver rejects that with 'extra text' when targeting a
		// Date column, so truncate to the calendar date and let the driver parse it.
		// Empty/blank strings map to NULL for Nullable(Date) and epoch day for Date,
		// mirroring the DateTime empty-string rule. Non-date unparseable strings
		// fall through unchanged so the driver fails loudly into the DLQ.
		if t, ok := v.(time.Time); ok {
			return t
		}
		if strings.TrimSpace(s) == "" {
			if strings.HasPrefix(typ, "Nullable(") {
				return nil
			}
			return time.Unix(0, 0).UTC()
		}
		for _, layout := range []string{time.RFC3339, "2006-01-02 15:04:05", "2006-01-02T15:04:05", "2006-01-02"} {
			if t, err := time.ParseInLocation(layout, s, time.Local); err == nil {
				return t
			}
		}
	case innerType == "DateTime" || strings.HasPrefix(innerType, "DateTime64"):
		if t, ok := v.(time.Time); ok {
			return t
		}
		// Empty/blank strings map to NULL for nullable columns and to epoch
		// (1970-01-01 00:00:00) for non-nullable DateTime columns, mirroring
		// the numeric empty-string->0 rule. This avoids a parse failure that
		// would otherwise abort the whole batch when a source DATETIME column
		// carries '' on some rows. Non-empty unparseable strings (e.g.
		// work_time="[1,2,3]") fall through unchanged so the driver fails
		// loudly and the row lands in the DLQ.
		if strings.TrimSpace(s) == "" {
			if strings.HasPrefix(typ, "Nullable(") {
				return nil
			}
			return time.Unix(0, 0).UTC()
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

func convertClickHouseHTTPValue(v any, typ string) any {
	converted := convertClickHouseValue(v, typ)
	if converted == nil {
		return nil
	}

	innerType := typ
	innerType = strings.TrimSuffix(strings.TrimPrefix(innerType, "Nullable("), ")")
	innerType = strings.TrimSuffix(strings.TrimPrefix(innerType, "LowCardinality("), ")")

	if t, ok := converted.(time.Time); ok {
		switch {
		case innerType == "Date":
			return t.Format("2006-01-02")
		case innerType == "DateTime":
			return t.Format("2006-01-02 15:04:05")
		case strings.HasPrefix(innerType, "DateTime64"):
			return t.Format("2006-01-02 15:04:05.000")
		}
	}

	return converted
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

// inferClickHouseType resolves the DDL type for an auto-created/evolved
// ClickHouse column with explicit priority:
//  1. declared — the source-side column type (Metadata.ColumnTypes, e.g. MySQL
//     information_schema COLUMN_TYPE / Debezium field type), mapped onto
//     ClickHouse DDL (so a source varchar request_id never becomes Int64);
//  2. unified typing engine — sample value + column-name heuristics.
func inferClickHouseType(name string, v any, declared string) string {
	if mapped := typing.MapSourceType(typing.DialectClickHouse, declared); mapped != "" {
		return mapped
	}
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

// writeUpdates writes source-ordered replacement rows. append mode rejects
// UPDATE during prepareDataRecords, so reaching this method outside
// source_order is a defensive configuration failure.
func (s *ClickHouseSink) writeUpdates(ctx context.Context, tableName string, records []core.Record) error {
	if s.effectiveVersionMode() != clickHouseVersionModeSourceOrder {
		return fmt.Errorf("clickhouse version_mode=%s cannot write UPDATE records", s.effectiveVersionMode())
	}
	return s.writeInsert(ctx, tableName, records)
}

// writeDeletes persists source-ordered tombstones. ReplacingMergeTree's
// two-argument deleted-row contract keeps a high-version delete present in
// merge state, so a late replay of an older INSERT cannot resurrect the row.
func (s *ClickHouseSink) writeDeletes(ctx context.Context, tableName string, records []core.Record) error {
	if s.effectiveVersionMode() != clickHouseVersionModeSourceOrder {
		return fmt.Errorf("clickhouse version_mode=%s cannot write DELETE records", s.effectiveVersionMode())
	}
	selected := map[string]bool{
		s.effectiveVersionColumn(): true,
		s.effectiveDeleteColumn():  true,
	}
	for _, column := range s.primaryKeyColumns(tableName) {
		selected[column] = true
	}
	for _, rec := range records {
		for column := range selected {
			if _, ok := rec.Data[column]; !ok {
				return fmt.Errorf("clickhouse tombstone for %s.%s is missing required column %q", s.database, tableName, column)
			}
		}
	}
	return s.writeInsertSelected(ctx, tableName, records, selected)
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
