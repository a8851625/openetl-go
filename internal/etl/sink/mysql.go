package sink

import (
	"context"
	"crypto/tls"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/sink/typing"
)

func init() {
	registry.RegisterSink("mysql", func(config map[string]any) (core.Sink, error) {
		return NewMySQLSink(config)
	})
}

type MySQLSink struct {
	name      string
	host      string
	port      int
	user      string
	password  string
	database  string
	table     string
	pkColumns []string
	batchMode string
	db        *sql.DB
	// insertChunkSize controls how many rows are placed into a single
	// INSERT statement. MySQL has a max_allowed_packet limit (default 4MB)
	// and placeholder limits; chunking keeps us well below them.
	insertChunkSize int
	// autoCreate: if true, create target table automatically when it doesn't exist.
	autoCreate bool
	// schemaDrift: "ignore" (default) | "fail" | "add_columns"
	schemaDrift string
	// ddLPolicy: "reject" (default) | "ignore" | "apply"
	ddLPolicy     DDLPolicy
	tlsEnabled    bool
	tlsSkipVerify bool
	// schemaCache avoids repeated information_schema queries.
	schemaCache  *core.SchemaCache
	sinkCounters // P4-20: per-sink write metrics (SK-4)
}

func NewMySQLSink(config map[string]any) (*MySQLSink, error) {
	s := &MySQLSink{
		name:            "mysql",
		port:            3306,
		batchMode:       "insert",
		insertChunkSize: 500,
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
	s.pkColumns = append(s.pkColumns, stringSliceConfig(config, "pk_columns")...)
	if v, ok := config["batch_mode"]; ok {
		s.batchMode = v.(string)
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
	if v, ok := config["tls"]; ok {
		if b, ok := v.(bool); ok {
			s.tlsEnabled = b
		}
	}
	if v, ok := config["tls_skip_verify"]; ok {
		if b, ok := v.(bool); ok {
			s.tlsSkipVerify = b
		}
	}
	s.schemaCache = core.NewSchemaCache()
	return s, nil
}

func (s *MySQLSink) Name() string { return s.name }

// SinkMetrics implements core.SinkMetricsProvider (P4-20, SK-4).
func (s *MySQLSink) SinkMetrics() core.SinkMetrics { return s.metricsFor(s.name) }

func (s *MySQLSink) ValidateSchema(ctx context.Context, schema core.SchemaInfo) error {
	if len(schema.Columns) == 0 || s.table == "" {
		return nil
	}
	exists, err := s.tableExists(ctx, s.table)
	if err != nil {
		return fmt.Errorf("validate mysql schema: check table %s.%s: %w", s.database, s.table, err)
	}
	if !exists {
		if s.autoCreate {
			return nil
		}
		return fmt.Errorf("schema validation failed for mysql %s.%s: target table does not exist; enable auto_create or create the table first", s.database, s.table)
	}
	target, err := s.getExistingColumnInfo(ctx, s.table)
	if err != nil {
		return fmt.Errorf("validate mysql schema: read columns for %s.%s: %w", s.database, s.table, err)
	}
	return validateSchemaCompatibility(schema, target, schemaValidationOptions{
		targetName:     fmt.Sprintf("mysql %s.%s", s.database, s.table),
		allowMissing:   s.schemaDrift == string(core.DriftAddCols),
		missingRemedy:  "enable schema_drift=add_columns or add the columns manually",
		allowTypeSync:  false,
		typeSyncRemedy: "change the target column type or add a transform/type_convert before the sink",
	})
}

func (s *MySQLSink) Open(ctx context.Context) error {
	tlsParam := ""
	if s.tlsEnabled {
		tlsName := fmt.Sprintf("openetl-%p", s)
		if err := mysqldriver.RegisterTLSConfig(tlsName, &tls.Config{InsecureSkipVerify: s.tlsSkipVerify}); err != nil {
			return fmt.Errorf("register mysql tls config: %w", err)
		}
		tlsParam = "&tls=" + tlsName
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&loc=Local&timeout=10s&readTimeout=60s&writeTimeout=60s%s",
		s.user, s.password, s.host, s.port, s.database, tlsParam)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return fmt.Errorf("connect mysql (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping mysql (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}
	s.db = db
	return nil
}

func (s *MySQLSink) Write(ctx context.Context, records []core.Record) (err error) {
	defer func() {
		if err != nil {
			s.recordError()
		}
	}() // P5-12: count write failures
	if len(records) == 0 {
		return nil
	}
	start := time.Now()

	// Separate DDL records and handle them according to ddl_policy.
	var ddlRecords, dataRecords []core.Record
	for _, rec := range records {
		if rec.Operation == core.OpDDL {
			ddlRecords = append(ddlRecords, rec)
		} else {
			dataRecords = append(dataRecords, rec)
		}
	}
	if err := ApplyDDLRecords(ctx, ddlRecords, s.ddLPolicy, func(ctx context.Context, ddl, table string) error {
		_, err := s.db.ExecContext(ctx, ddl)
		return err
	}); err != nil {
		return err
	}
	records = dataRecords
	if len(records) == 0 {
		return nil
	}

	// Auto-create missing tables and handle schema drift before writing.
	if err := s.ensureTablesAndColumns(ctx, records); err != nil {
		return err
	}

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

	// Compact the batch so multiple events on the same (table, PK) collapse to
	// the final operation in source order. Without this, grouping by op kind
	// would reorder DELETE(pk=1)→INSERT(pk=1) into INSERT→DELETE.
	records = CompactRecordsByPK(records, func(table string) []string {
		if len(s.pkColumns) > 0 {
			return s.pkColumns
		}
		return []string{"id"}
	})

	// Group records by (table, sorted-column-signature) so we can emit one
	// multi-row INSERT per group. Heterogeneous batches still produce a few
	// statements rather than N.
	type groupKey struct {
		table string
		cols  string
	}
	type groupBuf struct {
		cols       []string
		rows       [][]any
		ops        []core.OpType
		deleteRows [][]any // separately batched
	}
	groups := make(map[groupKey]*groupBuf)
	var groupOrder []groupKey

	for _, rec := range records {
		tableName := s.table
		if rec.Metadata.Table != "" {
			tableName = rec.Metadata.Table
		}
		// Sort column names for a deterministic signature.
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
			groupOrder = append(groupOrder, key)
		}

		// Route delete vs insert/upsert into separate statement kinds.
		if rec.Operation == core.OpDelete {
			vals, err := s.deleteValues(cols, rec)
			if err != nil {
				return err
			}
			g.deleteRows = append(g.deleteRows, vals)
		} else {
			row := make([]any, len(cols))
			for i, c := range cols {
				row[i] = normalizeMySQLValue(rec.Data[c])
			}
			g.rows = append(g.rows, row)
			g.ops = append(g.ops, rec.Operation)
		}
	}

	for _, key := range groupOrder {
		g := groups[key]
		if len(g.rows) > 0 {
			mode := s.batchMode
			// If any op is Update we must use upsert.
			for _, op := range g.ops {
				if op == core.OpUpdate {
					mode = "upsert"
					break
				}
			}
			if err := s.batchInsert(ctx, tx, key.table, g.cols, g.rows, mode); err != nil {
				return err
			}
		}
		if len(g.deleteRows) > 0 {
			if err := s.batchDelete(ctx, tx, key.table, g.deleteRows); err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	s.recordMetrics(len(records), time.Since(start))
	return nil
}

func (s *MySQLSink) batchInsert(ctx context.Context, tx *sql.Tx, table string, cols []string, rows [][]any, mode string) error {
	if len(cols) == 0 {
		return fmt.Errorf("batch insert %s: record has no writable columns", table)
	}
	for offset := 0; offset < len(rows); offset += s.insertChunkSize {
		end := offset + s.insertChunkSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[offset:end]
		if len(chunk) == 0 {
			continue
		}
		query := s.buildBatchInsertStatement(table, cols, len(chunk), mode)
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

func normalizeMySQLValue(v any) any {
	s, ok := v.(string)
	if !ok {
		return v
	}
	if ts, ok := parseMySQLTimeString(s); ok {
		return ts
	}
	return v
}

func parseMySQLTimeString(s string) (time.Time, bool) {
	if len(s) < len("2006-01-02") || s[4] != '-' || s[7] != '-' {
		return time.Time{}, false
	}
	layouts := []string{
		time.RFC3339Nano,
		"2006-01-02 15:04:05.999999999",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		ts, err := time.Parse(layout, s)
		if err == nil {
			return ts, true
		}
	}
	return time.Time{}, false
}

// buildBatchInsertStatement builds a single multi-row INSERT (or INSERT
// IGNORE / ON DUPLICATE KEY UPDATE) statement for `rowCount` rows. Pure
// function for testability.
func (s *MySQLSink) buildBatchInsertStatement(table string, cols []string, rowCount int, mode string) string {
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = quoteIdentMySQL(c)
	}
	colList := strings.Join(quotedCols, ",")
	rowPlaceholder := "(" + strings.Repeat("?,", len(cols)-1) + "?)"
	qTable := quoteIdentMySQL(table)

	var b strings.Builder
	if mode == "upsert" {
		b.WriteString("INSERT INTO ")
		b.WriteString(qTable)
		b.WriteString(" (")
		b.WriteString(colList)
		b.WriteString(") VALUES ")
	} else {
		b.WriteString("INSERT IGNORE INTO ")
		b.WriteString(qTable)
		b.WriteString(" (")
		b.WriteString(colList)
		b.WriteString(") VALUES ")
	}
	b.WriteString(strings.Repeat(rowPlaceholder+",", rowCount-1))
	b.WriteString(rowPlaceholder)

	if mode == "upsert" {
		b.WriteString(" ON DUPLICATE KEY UPDATE ")
		updates := make([]string, len(cols))
		for i, c := range quotedCols {
			updates[i] = c + "=VALUES(" + c + ")"
		}
		b.WriteString(strings.Join(updates, ","))
	}
	return b.String()
}

func (s *MySQLSink) batchDelete(ctx context.Context, tx *sql.Tx, table string, rows [][]any) error {
	for offset := 0; offset < len(rows); offset += s.insertChunkSize {
		end := offset + s.insertChunkSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[offset:end]
		if len(chunk) == 0 {
			continue
		}
		query := s.buildBatchDeleteStatement(table, len(chunk))
		args := make([]any, 0, len(chunk)*len(s.pkColumns))
		for _, row := range chunk {
			args = append(args, row...)
		}
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("batch delete %s (rows=%d): %w", table, len(chunk), err)
		}
	}
	return nil
}

// buildBatchDeleteStatement builds an OR-joined DELETE statement for rowCount
// rows. Pure function for testability.
func (s *MySQLSink) buildBatchDeleteStatement(table string, rowCount int) string {
	pkCols := s.pkColumns
	if len(pkCols) == 0 {
		pkCols = []string{"id"}
	}
	if len(pkCols) == 1 {
		return fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
			quoteIdentMySQL(table), quoteIdentMySQL(pkCols[0]), strings.Repeat("?,", rowCount-1)+"?")
	}
	quotedCols := make([]string, len(pkCols))
	for i, c := range pkCols {
		quotedCols[i] = quoteIdentMySQL(c)
	}
	condList := make([]string, len(pkCols))
	for i, c := range quotedCols {
		condList[i] = c + "=?"
	}
	rowCond := "(" + strings.Join(condList, " AND ") + ")"

	var b strings.Builder
	b.WriteString("DELETE FROM ")
	b.WriteString(quoteIdentMySQL(table))
	b.WriteString(" WHERE ")
	b.WriteString(strings.Repeat(rowCond+" OR ", rowCount-1))
	b.WriteString(rowCond)
	return b.String()
}

func (s *MySQLSink) deleteValues(cols []string, rec core.Record) ([]any, error) {
	pkCols := s.pkColumns
	if len(pkCols) == 0 {
		pkCols = []string{"id"}
	}
	keys, err := ResolveDeleteKeys(rec, pkCols)
	if err != nil {
		return nil, err
	}
	row := make([]any, 0, len(pkCols))
	for _, pk := range pkCols {
		row = append(row, keys[pk])
	}
	return row, nil
}

// EnsureSchema implements core.SchemaManager.
func (s *MySQLSink) EnsureSchema(ctx context.Context, tableName string, fields []string, fieldValues map[string]any) error {
	return core.EnsureSchemaGeneric(ctx, s.schemaCache, tableName, fields, fieldValues,
		s.autoCreate, core.SchemaDriftMode(s.schemaDrift),
		s.tableExists, s.createTableFromFields, s.getExistingColumns, s.addColumn,
	)
}

// ensureTablesAndColumns auto-creates missing tables and adds missing columns
// based on the record data. This is called before each Write batch.
func (s *MySQLSink) ensureTablesAndColumns(ctx context.Context, records []core.Record) error {
	if !s.autoCreate && s.schemaDrift != "add_columns" && s.schemaDrift != "fail" {
		return nil
	}

	// Collect unique tables and their columns + sample values from the batch.
	type colInfo struct {
		cols    []string
		samples map[string]any // column name → first non-nil value seen
	}
	tableMeta := make(map[string]*colInfo)
	for _, rec := range records {
		tableName := s.table
		if rec.Metadata.Table != "" {
			tableName = rec.Metadata.Table
		}
		if tableName == "" {
			continue
		}
		ti, ok := tableMeta[tableName]
		if !ok {
			ti = &colInfo{samples: make(map[string]any)}
			tableMeta[tableName] = ti
		}
		for k, v := range rec.Data {
			// Track unique column names.
			found := false
			for _, existing := range ti.cols {
				if existing == k {
					found = true
					break
				}
			}
			if !found {
				ti.cols = append(ti.cols, k)
			}
			// Store first non-nil value as a sample for type inference.
			if v != nil {
				if _, has := ti.samples[k]; !has {
					ti.samples[k] = v
				}
			}
		}
	}

	for tableName, ti := range tableMeta {
		if err := s.EnsureSchema(ctx, tableName, ti.cols, ti.samples); err != nil {
			return err
		}
	}
	return nil
}

// tableExists checks information_schema for the target table.
func (s *MySQLSink) tableExists(ctx context.Context, table string) (bool, error) {
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

// getExistingColumns returns the set of column names for a table.
func (s *MySQLSink) getExistingColumns(ctx context.Context, table string) (map[string]bool, error) {
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

func (s *MySQLSink) getExistingColumnInfo(ctx context.Context, table string) ([]core.ColumnInfo, error) {
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

// createTableFromFields creates a target table with columns inferred from
// field names and sample values using the unified type mapper.
func (s *MySQLSink) createTableFromFields(ctx context.Context, table string, columns []string, fieldValues map[string]any) error {
	if len(columns) == 0 {
		return nil
	}

	sort.Strings(columns)

	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(quoteIdentMySQL(table))
	b.WriteString(" (")

	// Determine PK column (default "id" if present).
	pkCol := ""
	for _, c := range columns {
		if c == "id" || c == "ID" || c == "Id" {
			pkCol = c
			break
		}
	}

	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(quoteIdentMySQL(c))
		b.WriteString(" ")

		if c == pkCol {
			b.WriteString("BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY")
		} else {
			// Use the typing package to infer the column type from the sample value.
			colType := typing.InferFromValue(typing.DialectMySQL, c, fieldValues[c])
			b.WriteString(colType)
		}
	}
	b.WriteString(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")

	_, err := s.db.ExecContext(ctx, b.String())
	return err
}

// addColumn adds a single column to an existing table, inferring its type
// from the sample value.
func (s *MySQLSink) addColumn(ctx context.Context, table, column string, fieldValues map[string]any) error {
	colType := typing.InferFromValue(typing.DialectMySQL, column, fieldValues[column])
	ddl := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", quoteIdentMySQL(table), quoteIdentMySQL(column), colType)
	_, err := s.db.ExecContext(ctx, ddl)
	return err
}

func (s *MySQLSink) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}
