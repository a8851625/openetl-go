package sink

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	mysqldriver "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/sink/typing"
)

func init() {
	registry.RegisterSink("jdbc", func(config map[string]any) (core.Sink, error) {
		return NewJDBCSink(config)
	})
}

type JDBCSink struct {
	name                   string
	dsnRaw                 string
	driverOverride         string
	driver                 string
	table                  string
	database               string
	schema                 string
	pkColumns              []string
	batchMode              string
	db                     *sql.DB
	insertChunkSize        int
	autoCreate             bool
	schemaDrift            string
	ddLPolicy              DDLPolicy
	schemaCache            *core.SchemaCache
	isMySQL                bool
	tlsEnabled             bool
	tlsSkipVerify          bool
	tlsCAFile              string
	allowUnsupportedDriver bool
	sinkCounters           // P4-20: per-sink write metrics (SK-4)
}

func NewJDBCSink(config map[string]any) (*JDBCSink, error) {
	s := &JDBCSink{
		name:            "jdbc",
		batchMode:       "insert",
		insertChunkSize: 500,
		schema:          "public",
	}
	if v, ok := config["name"]; ok {
		if vs, ok := v.(string); ok {
			s.name = vs
		}
	}
	if v, ok := config["dsn"]; ok {
		if vs, ok := v.(string); ok {
			s.dsnRaw = vs
		}
	}
	if v, ok := config["driver"]; ok {
		if vs, ok := v.(string); ok {
			s.driverOverride = vs
		}
	}
	if v, ok := config["table"]; ok {
		if vs, ok := v.(string); ok {
			s.table = vs
		}
	}
	if v, ok := config["schema"]; ok {
		if vs, ok := v.(string); ok {
			s.schema = vs
		}
	}
	s.pkColumns = append(s.pkColumns, stringSliceConfig(config, "pk_columns")...)
	if v, ok := config["batch_mode"]; ok {
		if vs, ok := v.(string); ok {
			s.batchMode = vs
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
	if v, ok := config["tls_ca_cert"]; ok {
		if vs, ok := v.(string); ok {
			s.tlsCAFile = vs
		}
	}
	if v, ok := config["allow_unsupported_driver"]; ok {
		if b, ok := v.(bool); ok {
			s.allowUnsupportedDriver = b
		}
	}
	s.schemaCache = core.NewSchemaCache()
	return s, nil
}

func (s *JDBCSink) Name() string { return s.name }

// SinkMetrics implements core.SinkMetricsProvider (P4-20, SK-4).
func (s *JDBCSink) SinkMetrics() core.SinkMetrics { return s.metricsFor(s.name) }

func (s *JDBCSink) Open(ctx context.Context) error {
	driver, nativeDSN, database, err := parseDSN(s.dsnRaw, s)
	if err != nil {
		return fmt.Errorf("parse dsn: %w", err)
	}
	if s.driverOverride != "" {
		driver = s.driverOverride
	}
	s.driver = driver
	s.database = database
	s.isMySQL = (driver == "mysql")
	if !s.allowUnsupportedDriver && driver != "mysql" && driver != "pgx" {
		return fmt.Errorf("jdbc sink stable mode supports only mysql and postgres/postgresql DSNs; got driver %q (set allow_unsupported_driver=true for best-effort beta semantics)", driver)
	}

	db, err := sql.Open(driver, nativeDSN)
	if err != nil {
		return fmt.Errorf("connect %s: %w", driver, err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping %s: %w", driver, err)
	}
	s.db = db
	return nil
}

func (s *JDBCSink) Write(ctx context.Context, records []core.Record) (err error) {
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

	// Compact by (table, PK) in source order to preserve CDC semantics.
	records = CompactRecordsByPK(records, func(table string) []string {
		if len(s.pkColumns) > 0 {
			return s.pkColumns
		}
		return []string{"id"}
	})

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

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	s.recordMetrics(len(records), time.Since(start))
	return nil
}

func (s *JDBCSink) batchInsert(ctx context.Context, tx *sql.Tx, table string, cols []string, rows [][]any, mode string) error {
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

func (s *JDBCSink) buildBatchInsertStatement(table string, cols []string, rowCount int, mode string) string {
	quotedCols := make([]string, len(cols))
	for i, c := range cols {
		quotedCols[i] = s.quote(c)
	}
	colList := strings.Join(quotedCols, ",")
	rowPlaceholder := "(" + strings.Repeat("?,", len(cols)-1) + "?)"

	var b strings.Builder
	if s.isMySQL {
		if mode == "upsert" {
			b.WriteString("INSERT INTO ")
		} else {
			b.WriteString("INSERT IGNORE INTO ")
		}
	} else {
		b.WriteString("INSERT INTO ")
	}
	b.WriteString(s.quote(table))
	b.WriteString(" (")
	b.WriteString(colList)
	b.WriteString(") VALUES ")
	if s.isMySQL {
		b.WriteString(strings.Repeat(rowPlaceholder+",", rowCount-1))
		b.WriteString(rowPlaceholder)
	} else {
		placeholderIdx := 1
		for i := 0; i < rowCount; i++ {
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
		}
	}

	if mode == "upsert" {
		if s.isMySQL {
			b.WriteString(" ON DUPLICATE KEY UPDATE ")
			updates := make([]string, len(cols))
			for i, c := range quotedCols {
				updates[i] = c + "=VALUES(" + c + ")"
			}
			b.WriteString(strings.Join(updates, ","))
		} else {
			pkCols := s.pkColumns
			if len(pkCols) == 0 {
				pkCols = []string{"id"}
			}
			conflictCols := make([]string, len(pkCols))
			for i, c := range pkCols {
				conflictCols[i] = s.quote(c)
			}
			pkSet := make(map[string]bool)
			for _, c := range pkCols {
				pkSet[c] = true
			}
			var updates []string
			for _, c := range cols {
				if !pkSet[c] {
					q := s.quote(c)
					updates = append(updates, q+" = EXCLUDED."+q)
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
		}
	}
	return b.String()
}

func (s *JDBCSink) batchDelete(ctx context.Context, tx *sql.Tx, table string, rows [][]any) error {
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

func (s *JDBCSink) buildBatchDeleteStatement(table string, rowCount int) string {
	pkCols := s.pkColumns
	if len(pkCols) == 0 {
		pkCols = []string{"id"}
	}
	if len(pkCols) == 1 {
		var placeholders string
		if s.isMySQL {
			placeholders = strings.Repeat("?,", rowCount-1) + "?"
		} else {
			parts := make([]string, rowCount)
			for i := range parts {
				parts[i] = fmt.Sprintf("$%d", i+1)
			}
			placeholders = strings.Join(parts, ",")
		}
		return fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)", s.quote(table), s.quote(pkCols[0]), placeholders)
	}
	quotedCols := make([]string, len(pkCols))
	for i, c := range pkCols {
		quotedCols[i] = s.quote(c)
	}
	condList := make([]string, len(pkCols))
	placeholderIdx := 1
	for i, c := range quotedCols {
		if s.isMySQL {
			condList[i] = c + "=?"
		} else {
			condList[i] = c + "=$" + fmt.Sprintf("%d", placeholderIdx)
			placeholderIdx++
		}
	}
	rowCond := "(" + strings.Join(condList, " AND ") + ")"

	var b strings.Builder
	b.WriteString("DELETE FROM ")
	b.WriteString(s.quote(table))
	b.WriteString(" WHERE ")
	if s.isMySQL {
		b.WriteString(strings.Repeat(rowCond+" OR ", rowCount-1))
		b.WriteString(rowCond)
	} else {
		for i := 0; i < rowCount; i++ {
			if i > 0 {
				b.WriteString(" OR ")
			}
			b.WriteString("(")
			for j, c := range quotedCols {
				if j > 0 {
					b.WriteString(" AND ")
				}
				b.WriteString(c)
				b.WriteString("=$")
				b.WriteString(fmt.Sprintf("%d", i*len(pkCols)+j+1))
			}
			b.WriteString(")")
		}
	}
	return b.String()
}

func (s *JDBCSink) deleteValues(cols []string, rec core.Record) ([]any, error) {
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

func (s *JDBCSink) EnsureSchema(ctx context.Context, tableName string, fields []string, fieldValues map[string]any) error {
	return core.EnsureSchemaGeneric(ctx, s.schemaCache, tableName, fields, fieldValues,
		s.autoCreate, core.SchemaDriftMode(s.schemaDrift),
		s.tableExists, s.createTableFromFields, s.getExistingColumns, s.addColumn,
	)
}

func (s *JDBCSink) ensureTablesAndColumns(ctx context.Context, records []core.Record) error {
	if !s.autoCreate && s.schemaDrift != "add_columns" && s.schemaDrift != "fail" {
		return nil
	}

	type colInfo struct {
		cols    []string
		samples map[string]any
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
	}
	for tableName, ti := range tableMeta {
		if err := s.EnsureSchema(ctx, tableName, ti.cols, ti.samples); err != nil {
			return err
		}
	}
	return nil
}

func (s *JDBCSink) tableExists(ctx context.Context, table string) (bool, error) {
	schemaName := s.schema
	if s.isMySQL {
		schemaName = s.database
	}
	var count int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?`,
		schemaName, table,
	).Scan(&count)
	if err != nil {
		return false, err
	}
	return count > 0, nil
}

func (s *JDBCSink) getExistingColumns(ctx context.Context, table string) (map[string]bool, error) {
	schemaName := s.schema
	if s.isMySQL {
		schemaName = s.database
	}
	rows, err := s.db.QueryContext(ctx,
		`SELECT column_name FROM information_schema.columns WHERE table_schema = ? AND table_name = ?`,
		schemaName, table,
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

func inferColumnType(name string, isMySQL bool) string {
	lower := strings.ToLower(name)
	switch {
	case strings.Contains(lower, "id"):
		return "BIGINT"
	case strings.Contains(lower, "amount") || strings.Contains(lower, "price") || strings.Contains(lower, "total"):
		return "DECIMAL(19,4)"
	case strings.Contains(lower, "time") || strings.Contains(lower, "date") || strings.Contains(lower, "created") || strings.Contains(lower, "updated"):
		return "TIMESTAMP"
	case strings.Contains(lower, "email"):
		return "VARCHAR(255)"
	default:
		if isMySQL {
			return "TEXT"
		}
		return "VARCHAR(1024)"
	}
}

func (s *JDBCSink) createTableFromFields(ctx context.Context, table string, columns []string, fieldValues map[string]any) error {
	if len(columns) == 0 {
		return nil
	}

	dialect := typing.DialectMySQL
	if s.driver == "pgx" {
		dialect = typing.DialectPostgreSQL
	}

	sort.Strings(columns)

	pkCol := ""
	for _, c := range columns {
		if c == "id" || c == "ID" || c == "Id" {
			pkCol = c
			break
		}
	}

	var b strings.Builder
	b.WriteString("CREATE TABLE IF NOT EXISTS ")
	b.WriteString(s.quote(table))
	b.WriteString(" (")

	for i, c := range columns {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(s.quote(c))
		b.WriteString(" ")
		if c == pkCol {
			if s.isMySQL {
				b.WriteString("BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY")
			} else {
				b.WriteString("BIGINT NOT NULL PRIMARY KEY")
			}
		} else {
			b.WriteString(typing.InferFromValue(dialect, c, fieldValues[c]))
		}
	}

	if s.isMySQL {
		b.WriteString(") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4")
	} else {
		b.WriteString(")")
	}

	_, err := s.db.ExecContext(ctx, b.String())
	return err
}

func (s *JDBCSink) addColumn(ctx context.Context, table, column string, fieldValues map[string]any) error {
	dialect := typing.DialectMySQL
	if s.driver == "pgx" {
		dialect = typing.DialectPostgreSQL
	}
	colType := typing.InferFromValue(dialect, column, fieldValues[column])
	var ddl string
	if s.isMySQL {
		ddl = fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", s.quote(table), s.quote(column), colType)
	} else {
		ddl = fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", s.quote(table), s.quote(column), colType)
	}
	_, err := s.db.ExecContext(ctx, ddl)
	return err
}

func (s *JDBCSink) Close() error {
	if s.db != nil {
		return s.db.Close()
	}
	return nil
}

func (s *JDBCSink) quote(name string) string {
	if s.isMySQL {
		return quoteIdentMySQL(name)
	}
	return quoteIdentPg(name)
}

// ── DSN Parsing ──────────────────────────────────────────────────────

func parseDSN(raw string, s *JDBCSink) (driver, native string, database string, err error) {
	idx := strings.Index(raw, "://")
	if idx == -1 {
		return "", raw, "", nil
	}
	prefix := raw[:idx]

	switch prefix {
	case "mysql":
		return parseMySQLDSN(raw, s)
	case "postgres", "postgresql":
		return parsePostgresDSN(raw, s)
	default:
		rest := raw[idx+3:]
		u, parseErr := url.Parse(raw)
		if parseErr == nil {
			database = strings.TrimPrefix(u.Path, "/")
		}
		return prefix, rest, database, nil
	}
}

func parseMySQLDSN(raw string, s *JDBCSink) (driver, native, database string, err error) {
	u, parseErr := url.Parse(raw)
	if parseErr != nil {
		rest := raw[strings.Index(raw, "://")+3:]
		return "mysql", rest, "", nil
	}
	user := u.User.String()
	host := u.Host
	db := strings.TrimPrefix(u.Path, "/")
	q := u.RawQuery

	var b strings.Builder
	b.WriteString(user)
	b.WriteString("@tcp(")
	b.WriteString(host)
	b.WriteString(")/")
	b.WriteString(db)

	params := "parseTime=true&charset=utf8mb4&loc=Local"
	if q != "" {
		params = q + "&" + params
	}
	if s != nil && s.tlsEnabled {
		tlsName := fmt.Sprintf("openetl-%p", s)
		tlsConfig := &tls.Config{InsecureSkipVerify: s.tlsSkipVerify}
		if s.tlsCAFile != "" {
			caCert, readErr := os.ReadFile(s.tlsCAFile)
			if readErr != nil {
				return "mysql", "", db, fmt.Errorf("read tls ca cert: %w", readErr)
			}
			pool := x509.NewCertPool()
			if !pool.AppendCertsFromPEM(caCert) {
				return "mysql", "", db, fmt.Errorf("failed to parse ca cert from %s", s.tlsCAFile)
			}
			tlsConfig.RootCAs = pool
		}
		if regErr := mysqldriver.RegisterTLSConfig(tlsName, tlsConfig); regErr != nil {
			return "mysql", "", db, fmt.Errorf("register mysql tls config: %w", regErr)
		}
		params += "&tls=" + tlsName
	}
	b.WriteString("?")
	b.WriteString(params)

	return "mysql", b.String(), db, nil
}

func parsePostgresDSN(raw string, s *JDBCSink) (driver, native, database string, err error) {
	u, parseErr := url.Parse(raw)
	if parseErr == nil {
		database = strings.TrimPrefix(u.Path, "/")
	}
	dsn := raw
	if s != nil {
		sep := "?"
		if strings.Contains(dsn, "?") {
			sep = "&"
		}
		if s.tlsEnabled {
			dsn = dsn + sep + "sslmode=require"
		} else {
			dsn = dsn + sep + "sslmode=disable"
		}
	}
	return "pgx", dsn, database, nil
}
