package sink

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/sink/typing"
)

func init() {
	registry.RegisterSink("postgres", func(config map[string]any) (core.Sink, error) {
		return NewPostgresSink(config)
	})
	registry.RegisterSink("postgresql", func(config map[string]any) (core.Sink, error) {
		return NewPostgresSink(config)
	})
}

type PostgresSink struct {
	name             string
	host             string
	port             int
	user             string
	password         string
	database         string
	table            string
	schema           string
	sslmode          string
	pkColumns        []string
	batchMode        string
	incrementColumns map[string]string
	pool             *pgxpool.Pool
	insertChunkSize  int
	autoCreate          bool
	schemaDrift         string
	ddLPolicy           DDLPolicy
	columnTypes         map[string]string
	sourceSchemaColumns map[string]string
	schemaCache         *core.SchemaCache
	preWrite            *PreWriteConfig
	sinkCounters        // P4-20: per-sink write metrics (SK-4)
}

func NewPostgresSink(config map[string]any) (*PostgresSink, error) {
	s := &PostgresSink{
		name:            "postgres",
		port:            5432,
		batchMode:       "insert",
		schema:          "public",
		sslmode:         "prefer",
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
	if v, ok := config["schema"]; ok {
		s.schema = v.(string)
	}
	if v, ok := config["sslmode"]; ok {
		if vs, ok := v.(string); ok {
			s.sslmode = vs
		}
	}
	s.pkColumns = append(s.pkColumns, stringSliceConfig(config, "pk_columns")...)
	if v, ok := config["batch_mode"]; ok {
		s.batchMode = v.(string)
	}
	if v, ok := config["increment_columns"]; ok {
		s.incrementColumns = stringMapConfig(config, "increment_columns")
		_ = v
	}
	if s.batchMode == "increment" && len(s.incrementColumns) == 0 {
		return nil, fmt.Errorf("postgres sink batch_mode=increment requires non-empty increment_columns (target_col -> source_field)")
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
	s.columnTypes = stringMapConfig(config, "column_types")
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
	pw, err := ParsePreWriteConfig(config)
	if err != nil {
		return nil, err
	}
	s.preWrite = pw
	s.schemaCache = core.NewSchemaCache()
	return s, nil
}

func (s *PostgresSink) Name() string { return s.name }

// SetSourceSchema implements core.SourceSchemaConsumer.
func (s *PostgresSink) SetSourceSchema(schema core.SchemaInfo) {
	if len(schema.Columns) == 0 {
		return
	}
	m := make(map[string]string, len(schema.Columns))
	for _, c := range schema.Columns {
		name := strings.TrimSpace(c.Name)
		if name == "" || strings.TrimSpace(c.DataType) == "" {
			continue
		}
		m[strings.ToLower(name)] = c.DataType
	}
	s.sourceSchemaColumns = m
}

func (s *PostgresSink) resolveColumnDDL(column string, sample any, recordDeclared map[string]string) string {
	override := ""
	if s.columnTypes != nil {
		override = s.columnTypes[column]
		if override == "" {
			override = s.columnTypes[strings.ToLower(column)]
		}
	}
	declared := ""
	if recordDeclared != nil {
		declared = recordDeclared[column]
		if declared == "" {
			declared = recordDeclared[strings.ToLower(column)]
		}
	}
	if declared == "" && s.sourceSchemaColumns != nil {
		declared = s.sourceSchemaColumns[strings.ToLower(column)]
	}
	return typing.ResolveColumnDDL(typing.DialectPostgreSQL, column, sample, declared, override)
}

// quotedFQTNPg returns the fully-qualified, PostgreSQL-quoted table name for
// pre_write statements.
func (s *PostgresSink) quotedFQTNPg(table string) string {
	parts := strings.SplitN(table, ".", 2)
	if len(parts) == 2 {
		return quoteIdentPg(parts[0]) + "." + quoteIdentPg(parts[1])
	}
	if s.schema != "" {
		return quoteIdentPg(s.schema) + "." + quoteIdentPg(table)
	}
	return quoteIdentPg(table)
}

// SinkMetrics implements core.SinkMetricsProvider (P4-20, SK-4).
func (s *PostgresSink) SinkMetrics() core.SinkMetrics { return s.metricsFor(s.name) }

func (s *PostgresSink) ValidateSchema(ctx context.Context, schema core.SchemaInfo) error {
	if len(schema.Columns) == 0 || s.table == "" {
		return nil
	}
	exists, err := s.pgTableExists(ctx, s.table)
	if err != nil {
		return fmt.Errorf("validate postgres schema: check table %s.%s: %w", s.schema, s.table, err)
	}
	if !exists {
		if s.autoCreate {
			return nil
		}
		return fmt.Errorf("schema validation failed for postgres %s.%s: target table does not exist; enable auto_create or create the table first", s.schema, s.table)
	}
	target, err := s.pgGetColumnInfo(ctx, s.table)
	if err != nil {
		return fmt.Errorf("validate postgres schema: read columns for %s.%s: %w", s.schema, s.table, err)
	}
	return validateSchemaCompatibility(schema, target, schemaValidationOptions{
		targetName:     fmt.Sprintf("postgres %s.%s", s.schema, s.table),
		allowMissing:   s.schemaDrift == string(core.DriftAddCols),
		missingRemedy:  "enable schema_drift=add_columns or add the columns manually",
		allowTypeSync:  false,
		typeSyncRemedy: "change the target column type or add a transform/type_convert before the sink",
	})
}

func (s *PostgresSink) Open(ctx context.Context) error {
	s.preWrite.ResetExecution()
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		s.user, s.password, s.host, s.port, s.database, s.sslmode)
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return fmt.Errorf("parse pg config: %w", err)
	}
	cfg.MaxConns = 10
	cfg.MinConns = 2
	cfg.MaxConnLifetime = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect postgres (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}
	if err := pool.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}
	s.pool = pool
	return nil
}

func (s *PostgresSink) Write(ctx context.Context, records []core.Record) (err error) {
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
		_, err := s.pool.Exec(ctx, ddl)
		return err
	}); err != nil {
		return err
	}
	records = dataRecords
	if len(records) == 0 {
		return nil
	}

	// Auto-create missing tables and handle schema drift.
	if err := s.ensureSchemaForBatch(ctx, records); err != nil {
		return err
	}

	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			tx.Rollback(ctx)
		}
	}()

	// Execute pre-write action (DELETE/TRUNCATE) before inserting the batch.
	if s.preWrite.Enabled() {
		targetTable := s.table
		if targetTable == "" {
			for _, rec := range records {
				if rec.Metadata.Table != "" {
					targetTable = rec.Metadata.Table
					break
				}
			}
		}
		if targetTable != "" {
			if err := s.preWrite.ExecPgx(ctx, func(ctx context.Context, stmt string, _ ...any) error {
				_, e := tx.Exec(ctx, stmt)
				return e
			}, s.quotedFQTNPg(targetTable)); err != nil {
				return err
			}
		}
	}

	// Compact by (table, PK) in source order to preserve CDC semantics.
	records = CompactRecordsByPK(records, func(table string) []string {
		if len(s.pkColumns) > 0 {
			return s.pkColumns
		}
		return []string{"id"}
	})

	// Group records by sorted-column signature to enable multi-row VALUES.
	type groupKey struct {
		table string
		cols  string
	}
	type groupBuf struct {
		cols       []string
		rows       [][]any
		ops        []core.OpType
		deleteRows [][]any
	}
	groups := make(map[groupKey]*groupBuf)
	var groupOrder []groupKey

	for _, rec := range records {
		tableName := s.table
		if rec.Metadata.Table != "" {
			tableName = rec.Metadata.Table
		}
		// Skip GENERATED columns — they cannot be written.
		genSet := s.generatedColumnsFor(ctx, tableName)
		var cols []string
		for _, k := range sortedKeys(rec.Data) {
			if genSet[k] {
				continue
			}
			cols = append(cols, k)
		}
		key := groupKey{table: tableName, cols: strings.Join(cols, ",")}

		g, ok := groups[key]
		if !ok {
			g = &groupBuf{cols: cols}
			groups[key] = g
			groupOrder = append(groupOrder, key)
		}
		if rec.Operation == core.OpDelete {
			vals, err := s.deleteValues(cols, rec)
			if err != nil {
				return err
			}
			g.deleteRows = append(g.deleteRows, vals)
		} else {
			row := make([]any, len(cols))
			for i, c := range cols {
				row[i] = rec.Data[c]
			}
			g.rows = append(g.rows, row)
			g.ops = append(g.ops, rec.Operation)
		}
	}

	for _, key := range groupOrder {
		g := groups[key]
		if len(g.rows) > 0 {
			mode := s.batchMode
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

	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	s.recordMetrics(len(records), time.Since(start))
	return nil
}

func (s *PostgresSink) batchInsert(ctx context.Context, tx pgx.Tx, table string, cols []string, rows [][]any, mode string) error {
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = pgQuote(c)
	}
	colList := strings.Join(quotedCols, ", ")

	pkCols := s.pkColumns
	if len(pkCols) == 0 {
		pkCols = []string{"id"}
	}
	conflictCols := make([]string, len(pkCols))
	for i, c := range pkCols {
		conflictCols[i] = pgQuote(c)
	}
	pkSet := make(map[string]bool, len(pkCols))
	for _, p := range pkCols {
		pkSet[p] = true
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

		var b strings.Builder
		placeholderIdx := 1
		if mode == "upsert" {
			b.WriteString("INSERT INTO ")
		} else {
			b.WriteString("INSERT INTO ")
		}
		b.WriteString(s.qualifiedTableFor(table))
		b.WriteString(" (")
		b.WriteString(colList)
		b.WriteString(") VALUES ")

		args := make([]any, 0, len(chunk)*len(cols))
		for i, row := range chunk {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString("(")
			for j := range cols {
				if j > 0 {
					b.WriteString(",")
				}
				b.WriteString("$")
				b.WriteString(fmt.Sprintf("%d", placeholderIdx))
				placeholderIdx++
			}
			b.WriteString(")")
			args = append(args, row...)
		}

		if mode == "upsert" {
			var updates []string
			for _, k := range cols {
				if !pkSet[k] {
					updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", pgQuote(k), pgQuote(k)))
				}
			}
			if len(updates) == 0 {
				b.WriteString(" ON CONFLICT (")
				b.WriteString(strings.Join(conflictCols, ", "))
				b.WriteString(") DO NOTHING")
			} else {
				b.WriteString(" ON CONFLICT (")
				b.WriteString(strings.Join(conflictCols, ", "))
				b.WriteString(") DO UPDATE SET ")
				b.WriteString(strings.Join(updates, ", "))
			}
		} else if mode == "increment" {
			// Increment mode: target_col = COALESCE(target_col, 0) + EXCLUDED.source_field
			var updates []string
			for targetCol, sourceField := range s.incrementColumns {
				src := sourceField
				if src == "" {
					src = targetCol
				}
				updates = append(updates, fmt.Sprintf("%s = COALESCE(%s, 0) + EXCLUDED.%s", pgQuote(targetCol), pgQuote(targetCol), pgQuote(src)))
			}
			// Non-PK, non-increment columns still update to EXCLUDED values.
			for _, k := range cols {
				if pkSet[k] {
					continue
				}
				if _, isInc := s.incrementColumns[k]; isInc {
					continue
				}
				updates = append(updates, fmt.Sprintf("%s = EXCLUDED.%s", pgQuote(k), pgQuote(k)))
			}
			if len(updates) == 0 {
				b.WriteString(" ON CONFLICT (")
				b.WriteString(strings.Join(conflictCols, ", "))
				b.WriteString(") DO NOTHING")
			} else {
				b.WriteString(" ON CONFLICT (")
				b.WriteString(strings.Join(conflictCols, ", "))
				b.WriteString(") DO UPDATE SET ")
				b.WriteString(strings.Join(updates, ", "))
			}
		}

		if _, err := tx.Exec(ctx, b.String(), args...); err != nil {
			return fmt.Errorf("batch insert %s (rows=%d): %w", table, len(chunk), err)
		}
	}
	return nil
}

func (s *PostgresSink) batchDelete(ctx context.Context, tx pgx.Tx, table string, rows [][]any) error {
	pkCols := s.pkColumns
	if len(pkCols) == 0 {
		pkCols = []string{"id"}
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
		if len(pkCols) == 1 {
			var b strings.Builder
			b.WriteString("DELETE FROM ")
			b.WriteString(s.qualifiedTableFor(table))
			b.WriteString(" WHERE ")
			b.WriteString(pgQuote(pkCols[0]))
			b.WriteString(" IN (")
			args := make([]any, 0, len(chunk))
			for i, row := range chunk {
				if i > 0 {
					b.WriteString(",")
				}
				b.WriteString("$")
				b.WriteString(fmt.Sprintf("%d", i+1))
				args = append(args, row[0])
			}
			b.WriteString(")")
			if _, err := tx.Exec(ctx, b.String(), args...); err != nil {
				return fmt.Errorf("batch delete %s (rows=%d): %w", table, len(chunk), err)
			}
			continue
		}
		var b strings.Builder
		b.WriteString("DELETE FROM ")
		b.WriteString(s.qualifiedTableFor(table))
		b.WriteString(" WHERE ")
		placeholderIdx := 1
		args := make([]any, 0, len(chunk)*len(pkCols))
		for i, row := range chunk {
			if i > 0 {
				b.WriteString(" OR ")
			}
			b.WriteString("(")
			for j := range pkCols {
				if j > 0 {
					b.WriteString(" AND ")
				}
				b.WriteString(pgQuote(pkCols[j]))
				b.WriteString("=$")
				b.WriteString(fmt.Sprintf("%d", placeholderIdx))
				placeholderIdx++
			}
			b.WriteString(")")
			args = append(args, row...)
		}
		if _, err := tx.Exec(ctx, b.String(), args...); err != nil {
			return fmt.Errorf("batch delete %s (rows=%d): %w", table, len(chunk), err)
		}
	}
	return nil
}

func (s *PostgresSink) deleteValues(cols []string, rec core.Record) ([]any, error) {
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

func (s *PostgresSink) qualifiedTable() string {
	if s.schema != "" {
		return fmt.Sprintf("%s.%s", pgQuote(s.schema), pgQuote(s.table))
	}
	return pgQuote(s.table)
}

func (s *PostgresSink) qualifiedTableFor(table string) string {
	if s.schema != "" {
		return fmt.Sprintf("%s.%s", pgQuote(s.schema), pgQuote(table))
	}
	return pgQuote(table)
}

// ── Schema Management (SchemaManager interface) ──────────────────────

// EnsureSchema implements core.SchemaManager.
func (s *PostgresSink) EnsureSchema(ctx context.Context, tableName string, fields []string, fieldValues map[string]any) error {
	return core.EnsureSchemaGeneric(ctx, s.schemaCache, tableName, fields, fieldValues,
		s.autoCreate, core.SchemaDriftMode(s.schemaDrift),
		s.pgTableExists, s.pgCreateTable, s.pgGetColumns, s.pgAddColumn,
	)
}

func (s *PostgresSink) ensureSchemaForBatch(ctx context.Context, records []core.Record) error {
	if !s.autoCreate && s.schemaDrift != "add_columns" && s.schemaDrift != "fail" {
		return nil
	}
	type colInfo struct {
		cols     []string
		samples  map[string]any
		declared map[string]string
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
			ti = &colInfo{samples: make(map[string]any), declared: make(map[string]string)}
			tableMeta[tableName] = ti
		}
		for k, v := range rec.Data {
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
			if v != nil {
				if _, has := ti.samples[k]; !has {
					ti.samples[k] = v
				}
			}
		}
		for col, typ := range rec.Metadata.ColumnTypes {
			if col == "" || typ == "" {
				continue
			}
			if _, has := ti.declared[col]; !has {
				ti.declared[col] = typ
			}
		}
	}
	for tableName, ti := range tableMeta {
		samples := ti.samples
		if len(ti.declared) > 0 {
			samples = make(map[string]any, len(ti.samples)+1)
			for k, v := range ti.samples {
				samples[k] = v
			}
			samples["__column_types__"] = ti.declared
		}
		if err := s.EnsureSchema(ctx, tableName, ti.cols, samples); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresSink) pgTableExists(ctx context.Context, table string) (bool, error) {
	var count int
	err := s.pool.QueryRow(ctx,
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = $1 AND table_name = $2`,
		s.schema, table,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *PostgresSink) pgGetColumns(ctx context.Context, table string) (map[string]bool, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2`,
		s.schema, table,
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

func (s *PostgresSink) pgGetColumnInfo(ctx context.Context, table string) ([]core.ColumnInfo, error) {
	rows, err := s.pool.Query(ctx,
		`SELECT c.column_name, c.data_type, c.is_nullable,
		        COALESCE(a.attgenerated, '') AS attgenerated
		 FROM information_schema.columns c
		 JOIN pg_attribute a ON a.attrelid = (quote_ident($1)||'.'||quote_ident($2))::regclass AND a.attname = c.column_name
		 WHERE c.table_schema = $1 AND c.table_name = $2
		 ORDER BY c.ordinal_position`,
		s.schema, table,
	)
	if err != nil {
		// Fallback for older PostgreSQL / permission issues: skip generated detection.
		rows, ferr := s.pool.Query(ctx,
			`SELECT column_name, data_type, is_nullable, '' AS attgenerated FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`,
			s.schema, table,
		)
		if ferr != nil {
			return nil, ferr
		}
		defer rows.Close()
		var cols []core.ColumnInfo
		for rows.Next() {
			var name, dataType, nullable, gen string
			if err := rows.Scan(&name, &dataType, &nullable, &gen); err != nil {
				return nil, err
			}
			cols = append(cols, core.ColumnInfo{Name: name, DataType: dataType, Nullable: strings.EqualFold(nullable, "YES"), Generated: gen != ""})
		}
		return cols, rows.Err()
	}
	defer rows.Close()
	var cols []core.ColumnInfo
	for rows.Next() {
		var name, dataType, nullable, gen string
		if err := rows.Scan(&name, &dataType, &nullable, &gen); err != nil {
			return nil, err
		}
		cols = append(cols, core.ColumnInfo{Name: name, DataType: dataType, Nullable: strings.EqualFold(nullable, "YES"), Generated: gen != ""})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return cols, nil
}

// generatedColumnsFor returns the set of GENERATED column names for the target
// table, cached per table for the sink lifetime. Generated columns cannot be
// written and must be excluded from INSERT/UPDATE column sets. Best-effort: on
// introspection error an empty set is returned so the write surfaces the DB error.
func (s *PostgresSink) generatedColumnsFor(ctx context.Context, table string) map[string]bool {
	tableKey := s.schema + "." + table
	if cached, ok := s.schemaCache.GeneratedColumns(tableKey); ok {
		return cached
	}
	cols, err := s.pgGetColumnInfo(ctx, table)
	if err != nil {
		return map[string]bool{}
	}
	genSet := map[string]bool{}
	for _, c := range cols {
		if c.Generated {
			genSet[c.Name] = true
		}
	}
	s.schemaCache.SetGeneratedColumns(tableKey, genSet)
	return genSet
}

func (s *PostgresSink) pgCreateTable(ctx context.Context, table string, columns []string, fieldValues map[string]any) error {
	if len(columns) == 0 {
		return nil
	}
	sort.Strings(columns)

	pkCol := ""
	for _, c := range columns {
		if c == "id" || c == "ID" {
			pkCol = c
			break
		}
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (`, pgQuote(s.schema), pgQuote(table)))
	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(pgQuote(c))
		if c == pkCol {
			b.WriteString(" BIGSERIAL PRIMARY KEY")
		} else {
			var recordDeclared map[string]string
			if fieldValues != nil {
				if m, ok := fieldValues["__column_types__"].(map[string]string); ok {
					recordDeclared = m
				}
			}
			colType := s.resolveColumnDDL(c, fieldValues[c], recordDeclared)
			b.WriteString(" ")
			b.WriteString(colType)
		}
	}
	b.WriteString(")")

	_, err := s.pool.Exec(ctx, b.String())
	return err
}

func (s *PostgresSink) pgAddColumn(ctx context.Context, table, column string, fieldValues map[string]any) error {
	var recordDeclared map[string]string
	if fieldValues != nil {
		if m, ok := fieldValues["__column_types__"].(map[string]string); ok {
			recordDeclared = m
		}
	}
	colType := s.resolveColumnDDL(column, fieldValues[column], recordDeclared)
	ddl := fmt.Sprintf(`ALTER TABLE %s.%s ADD COLUMN %s %s`, pgQuote(s.schema), pgQuote(table), pgQuote(column), colType)
	_, err := s.pool.Exec(ctx, ddl)
	return err
}

func (s *PostgresSink) Close() error {
	if s.pool != nil {
		s.pool.Close()
	}
	return nil
}

func pgQuote(name string) string {
	return fmt.Sprintf(`"%s"`, strings.ReplaceAll(name, `"`, `""`))
}

func sortedKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
