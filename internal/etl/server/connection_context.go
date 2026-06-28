package server

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
	_ "github.com/go-sql-driver/mysql"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

type connectionContextResponse struct {
	Connection      *storage.ConnectionEntry   `json:"connection"`
	Descriptor      *ConnectorDescriptor       `json:"descriptor,omitempty"`
	Recommendations []connectionRecommendation `json:"recommendations"`
	Introspection   connectionIntrospection    `json:"introspection"`
}

type connectionRecommendation struct {
	Field  string `json:"field"`
	Value  any    `json:"value"`
	Reason string `json:"reason"`
}

type connectionIntrospection struct {
	OK        bool             `json:"ok"`
	Kind      string           `json:"kind"`
	Type      string           `json:"type"`
	Status    string           `json:"status"`
	Error     string           `json:"error,omitempty"`
	Databases []string         `json:"databases,omitempty"`
	Tables    []tableMetadata  `json:"tables,omitempty"`
	Topics    []topicMetadata  `json:"topics,omitempty"`
	Schema    []columnMetadata `json:"schema,omitempty"`
	Sample    []map[string]any `json:"sample,omitempty"`
	Warnings  []string         `json:"warnings,omitempty"`
	CheckedAt time.Time        `json:"checked_at"`
}

type tableMetadata struct {
	Database   string           `json:"database,omitempty"`
	Schema     string           `json:"schema,omitempty"`
	Name       string           `json:"name"`
	Columns    []columnMetadata `json:"columns,omitempty"`
	PrimaryKey []string         `json:"primary_key,omitempty"`
}

type columnMetadata struct {
	Name     string `json:"name"`
	DataType string `json:"data_type,omitempty"`
	Nullable bool   `json:"nullable,omitempty"`
}

type topicMetadata struct {
	Name       string              `json:"name"`
	Partitions []partitionMetadata `json:"partitions,omitempty"`
}

type partitionMetadata struct {
	ID           int32 `json:"id"`
	OldestOffset int64 `json:"oldest_offset,omitempty"`
	NewestOffset int64 `json:"newest_offset,omitempty"`
	Leader       int32 `json:"leader,omitempty"`
}

func (s *Server) connectionContext(w http.ResponseWriter, r *http.Request, name string) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	conn, err := s.store.GetConnection(r.Context(), name)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
		return
	}
	if conn == nil {
		w.WriteHeader(http.StatusNotFound)
		json.NewEncoder(w).Encode(map[string]any{"error": "connection not found"})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()

	introspection := introspectConnection(ctx, conn)
	resp := connectionContextResponse{
		Connection:      maskConnection(conn),
		Descriptor:      descriptorForConnection(conn.Kind, conn.Type),
		Recommendations: recommendationsForConnection(conn.Kind, conn.Type, conn.Config),
		Introspection:   introspection,
	}
	json.NewEncoder(w).Encode(resp)
}

func descriptorForConnection(kind, typ string) *ConnectorDescriptor {
	for _, d := range connectorDescriptors() {
		if d.Kind == kind && d.Type == typ {
			dd := d
			return &dd
		}
	}
	return nil
}

func introspectConnection(ctx context.Context, conn *storage.ConnectionEntry) connectionIntrospection {
	result := connectionIntrospection{
		OK:        true,
		Kind:      conn.Kind,
		Type:      conn.Type,
		Status:    "ok",
		CheckedAt: time.Now().UTC(),
	}
	if conn.Kind != "source" {
		result.Warnings = append(result.Warnings, "introspection currently focuses on source connections")
		return result
	}
	var err error
	switch conn.Type {
	case "demo":
		result.Schema, result.Sample = introspectDemoSource(conn.Config)
	case "file":
		result.Schema, result.Sample, err = introspectFileSource(ctx, conn.Config)
	case "http":
		result.Sample, err = introspectHTTPSource(ctx, conn.Config)
		result.Schema = schemaFromSamples(result.Sample)
	case "mysql_batch", "mysql_cdc", "mysql_snapshot_cdc":
		err = introspectMySQLSource(ctx, conn.Config, &result)
	case "postgres_cdc":
		err = introspectPostgresSource(ctx, conn.Config, &result)
	case "kafka":
		err = introspectKafkaSource(conn.Config, &result)
	default:
		result.Warnings = append(result.Warnings, "no source-specific introspection adapter is available")
	}
	if err != nil {
		result.OK = false
		result.Status = "error"
		result.Error = err.Error()
	}
	return result
}

func recommendationsForConnection(kind, typ string, cfg map[string]any) []connectionRecommendation {
	var out []connectionRecommendation
	add := func(field string, value any, reason string) {
		out = append(out, connectionRecommendation{Field: field, Value: value, Reason: reason})
	}
	if kind == "source" {
		switch typ {
		case "kafka":
			add("schedule.type", "streaming", "Kafka is a streaming source; replay is controlled by committed offsets.")
			add("batch_size", 500, "Smaller Kafka batches reduce replay size when a sink fails.")
			add("checkpoint_interval_sec", 10, "Commit offsets frequently enough to bound at-least-once replay.")
		case "mysql_cdc", "postgres_cdc", "mysql_snapshot_cdc":
			add("schedule.type", "streaming", "CDC sources should run continuously.")
			add("batch_size", 500, "CDC batches should keep sink latency and replay windows small.")
			add("checkpoint_interval_sec", 10, "Frequent checkpoints reduce binlog/WAL replay after restart.")
		case "mysql_batch":
			add("schedule.type", "once", "Batch database reads are safe as explicit one-shot or scheduled jobs.")
			add("batch_size", 1000, "A moderate batch size balances transaction cost and replay size.")
			add("checkpoint_interval_sec", 30, "Batch reads can checkpoint less aggressively than CDC.")
			if str(cfg, "query") != "" {
				add("cursor_column", str(cfg, "cursor_column"), "Custom queries need a stable cursor or explicit pk_column for replay-safe pagination.")
			}
		case "file", "http":
			add("schedule.type", "once", "File and HTTP reads are usually bounded extraction jobs.")
			add("batch_size", 1000, "Use a bounded batch to keep generated files and DLQ replay manageable.")
			add("checkpoint_interval_sec", 30, "Bounded reads do not need sub-second checkpointing.")
		case "demo":
			add("schedule.type", "once", "Demo data is mainly for first-run validation and smoke tests.")
			add("batch_size", 100, "Small demo batches keep UI-created jobs easy to inspect.")
			add("checkpoint_interval_sec", 1, "Fast checkpoints make first-run behavior visible immediately.")
		}
	}
	return out
}

func introspectDemoSource(cfg map[string]any) ([]columnMetadata, []map[string]any) {
	fields, _ := cfg["fields"].([]any)
	if len(fields) == 0 {
		fields = []any{map[string]any{"name": "id", "type": "counter"}, map[string]any{"name": "value", "type": "string"}}
	}
	row := map[string]any{}
	var cols []columnMetadata
	for _, item := range fields {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		name := str(m, "name")
		if name == "" {
			continue
		}
		typ := str(m, "type")
		cols = append(cols, columnMetadata{Name: name, DataType: typ})
		switch typ {
		case "counter", "int", "integer":
			row[name] = 1
		case "float":
			row[name] = 1.0
		case "bool", "boolean":
			row[name] = true
		default:
			row[name] = fmt.Sprintf("sample_%s", name)
		}
	}
	return cols, []map[string]any{{"operation": "INSERT", "data": row, "metadata": map[string]any{"source": "demo", "table": "demo"}}}
}

func introspectFileSource(ctx context.Context, cfg map[string]any) ([]columnMetadata, []map[string]any, error) {
	path := str(cfg, "path")
	if path == "" {
		return nil, nil, fmt.Errorf("file source requires path for introspection")
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()
	format := str(cfg, "format")
	if format == "" {
		format = "csv"
	}
	if format == "json" || strings.HasSuffix(path, ".jsonl") {
		return sampleJSONLines(ctx, f, 5)
	}
	return sampleCSV(ctx, f, boolDefault(cfg, "has_header", true), strDefault(cfg, "delimiter", ","))
}

func sampleJSONLines(ctx context.Context, r io.Reader, limit int) ([]columnMetadata, []map[string]any, error) {
	scanner := bufio.NewScanner(r)
	var sample []map[string]any
	for scanner.Scan() && len(sample) < limit {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var data map[string]any
		if err := json.Unmarshal([]byte(line), &data); err != nil {
			return nil, nil, err
		}
		sample = append(sample, map[string]any{"operation": "INSERT", "data": data})
	}
	if err := scanner.Err(); err != nil {
		return nil, nil, err
	}
	return schemaFromSamples(sample), sample, nil
}

func sampleCSV(ctx context.Context, r io.Reader, hasHeader bool, delimiter string) ([]columnMetadata, []map[string]any, error) {
	cr := csv.NewReader(r)
	if delimiter != "" {
		cr.Comma = []rune(delimiter)[0]
	}
	first, err := cr.Read()
	if err != nil {
		return nil, nil, err
	}
	headers := first
	if !hasHeader {
		headers = make([]string, len(first))
		for i := range headers {
			headers[i] = fmt.Sprintf("col%d", i+1)
		}
	}
	var rows [][]string
	if !hasHeader {
		rows = append(rows, first)
	}
	for len(rows) < 5 {
		select {
		case <-ctx.Done():
			return nil, nil, ctx.Err()
		default:
		}
		row, err := cr.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}
		rows = append(rows, row)
	}
	var sample []map[string]any
	for _, row := range rows {
		data := map[string]any{}
		for i, name := range headers {
			if i < len(row) {
				data[name] = row[i]
			}
		}
		sample = append(sample, map[string]any{"operation": "INSERT", "data": data})
	}
	cols := make([]columnMetadata, 0, len(headers))
	for _, name := range headers {
		cols = append(cols, columnMetadata{Name: name, DataType: "string", Nullable: true})
	}
	return cols, sample, nil
}

func introspectHTTPSource(ctx context.Context, cfg map[string]any) ([]map[string]any, error) {
	req, err := http.NewRequestWithContext(ctx, strDefault(cfg, "method", "GET"), str(cfg, "url"), strings.NewReader(str(cfg, "body")))
	if err != nil {
		return nil, err
	}
	if headers, ok := cfg["headers"].(map[string]any); ok {
		for k, v := range headers {
			req.Header.Set(k, fmt.Sprint(v))
		}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode >= 400 {
		return nil, fmt.Errorf("http sample returned %d: %s", resp.StatusCode, string(body))
	}
	var parsed any
	if err := json.Unmarshal(body, &parsed); err != nil {
		return []map[string]any{{"operation": "INSERT", "data": map[string]any{"body": string(body)}}}, nil
	}
	items := firstJSONArray(parsed, str(cfg, "result_key"))
	var sample []map[string]any
	for _, item := range items {
		if len(sample) >= 5 {
			break
		}
		if m, ok := item.(map[string]any); ok {
			sample = append(sample, map[string]any{"operation": "INSERT", "data": m})
		}
	}
	if len(sample) == 0 {
		if m, ok := parsed.(map[string]any); ok {
			sample = append(sample, map[string]any{"operation": "INSERT", "data": m})
		}
	}
	return sample, nil
}

func firstJSONArray(v any, key string) []any {
	if key != "" {
		if m, ok := v.(map[string]any); ok {
			if arr, ok := m[key].([]any); ok {
				return arr
			}
		}
	}
	if arr, ok := v.([]any); ok {
		return arr
	}
	if m, ok := v.(map[string]any); ok {
		for _, child := range m {
			if arr, ok := child.([]any); ok {
				return arr
			}
		}
	}
	return nil
}

func introspectMySQLSource(ctx context.Context, cfg map[string]any, result *connectionIntrospection) error {
	dbName := str(cfg, "database")
	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&timeout=5s&readTimeout=10s",
		str(cfg, "user"), str(cfg, "password"), str(cfg, "host"), intDefault(cfg, "port", 3306), dbName))
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	result.Databases, _ = querySingleColumn(ctx, db, `SELECT schema_name FROM information_schema.schemata WHERE schema_name NOT IN ('information_schema','performance_schema','mysql','sys') ORDER BY schema_name LIMIT 100`)
	tableNames, err := querySingleColumn(ctx, db, `SELECT table_name FROM information_schema.tables WHERE table_schema = ? AND table_type='BASE TABLE' ORDER BY table_name LIMIT 100`, dbName)
	if err != nil {
		return err
	}
	for _, table := range tableNames {
		result.Tables = append(result.Tables, tableMetadata{Database: dbName, Name: table})
	}
	target := str(cfg, "table")
	if target == "" {
		tables := stringSlice(cfg["tables"])
		if len(tables) == 1 {
			target = tables[0]
		}
	}
	if target != "" && safeDBIdent(target) {
		cols, pk, err := mysqlColumns(ctx, db, dbName, target)
		if err != nil {
			return err
		}
		result.Schema = cols
		result.Tables = upsertTableMetadata(result.Tables, tableMetadata{Database: dbName, Name: target, Columns: cols, PrimaryKey: pk})
		if str(cfg, "query") == "" {
			result.Sample, _ = sampleSQLTable(ctx, db, "mysql", target)
		}
	}
	return nil
}

func introspectPostgresSource(ctx context.Context, cfg map[string]any, result *connectionIntrospection) error {
	schemaName := "public"
	if tables := stringSlice(cfg["tables"]); len(tables) == 1 && strings.Contains(tables[0], ".") {
		parts := strings.SplitN(tables[0], ".", 2)
		schemaName = parts[0]
	}
	dsn := fmt.Sprintf("postgres://%s:%s@%s:%d/%s?sslmode=%s",
		str(cfg, "user"), str(cfg, "password"), str(cfg, "host"), intDefault(cfg, "port", 5432), str(cfg, "database"), strDefault(cfg, "sslmode", "prefer"))
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := db.PingContext(ctx); err != nil {
		return err
	}
	result.Databases, _ = querySingleColumn(ctx, db, `SELECT datname FROM pg_database WHERE datistemplate = false ORDER BY datname LIMIT 100`)
	tableNames, err := querySingleColumn(ctx, db, `SELECT table_name FROM information_schema.tables WHERE table_schema = $1 AND table_type='BASE TABLE' ORDER BY table_name LIMIT 100`, schemaName)
	if err != nil {
		return err
	}
	for _, table := range tableNames {
		result.Tables = append(result.Tables, tableMetadata{Schema: schemaName, Name: table})
	}
	target := ""
	if tables := stringSlice(cfg["tables"]); len(tables) == 1 {
		target = tables[0]
	}
	if target != "" && safeDBIdent(target) {
		cols, pk, err := postgresColumns(ctx, db, schemaName, target)
		if err != nil {
			return err
		}
		result.Schema = cols
		result.Tables = upsertTableMetadata(result.Tables, tableMetadata{Schema: schemaName, Name: target, Columns: cols, PrimaryKey: pk})
		result.Sample, _ = sampleSQLTable(ctx, db, "postgres", target)
	}
	return nil
}

func introspectKafkaSource(cfg map[string]any, result *connectionIntrospection) error {
	brokers := stringSlice(cfg["brokers"])
	if len(brokers) == 0 {
		brokers = []string{"localhost:9092"}
	}
	admin, err := sarama.NewClusterAdmin(brokers, sarama.NewConfig())
	if err != nil {
		return err
	}
	defer admin.Close()
	topics, err := admin.ListTopics()
	if err != nil {
		return err
	}
	names := make([]string, 0, len(topics))
	for name := range topics {
		names = append(names, name)
	}
	sort.Strings(names)
	limitedNames := names
	if len(limitedNames) > 100 {
		limitedNames = limitedNames[:100]
	}
	described, _ := admin.DescribeTopics(limitedNames)
	partitionByTopic := map[string][]partitionMetadata{}
	for _, topic := range described {
		for _, p := range topic.Partitions {
			partitionByTopic[topic.Name] = append(partitionByTopic[topic.Name], partitionMetadata{ID: p.ID, Leader: p.Leader})
		}
	}
	for _, name := range limitedNames {
		if len(result.Topics) >= 100 {
			break
		}
		meta := topicMetadata{Name: name, Partitions: partitionByTopic[name]}
		result.Topics = append(result.Topics, meta)
	}
	return nil
}

func mysqlColumns(ctx context.Context, db *sql.DB, database, table string) ([]columnMetadata, []string, error) {
	rows, err := db.QueryContext(ctx, `
		SELECT column_name, column_type, is_nullable, column_key
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position`, database, stripSchema(table))
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var cols []columnMetadata
	var pk []string
	for rows.Next() {
		var name, typ, nullable, key string
		if err := rows.Scan(&name, &typ, &nullable, &key); err != nil {
			return nil, nil, err
		}
		cols = append(cols, columnMetadata{Name: name, DataType: typ, Nullable: strings.EqualFold(nullable, "YES")})
		if key == "PRI" {
			pk = append(pk, name)
		}
	}
	return cols, pk, rows.Err()
}

func postgresColumns(ctx context.Context, db *sql.DB, fallbackSchema, table string) ([]columnMetadata, []string, error) {
	schemaName, tableName := splitSchema(fallbackSchema, table)
	rows, err := db.QueryContext(ctx, `
		SELECT c.column_name, c.data_type, c.is_nullable,
		       CASE WHEN kcu.column_name IS NULL THEN false ELSE true END AS is_pk
		FROM information_schema.columns c
		LEFT JOIN information_schema.table_constraints tc
		  ON tc.table_schema = c.table_schema AND tc.table_name = c.table_name AND tc.constraint_type = 'PRIMARY KEY'
		LEFT JOIN information_schema.key_column_usage kcu
		  ON kcu.constraint_name = tc.constraint_name AND kcu.table_schema = tc.table_schema AND kcu.table_name = tc.table_name AND kcu.column_name = c.column_name
		WHERE c.table_schema = $1 AND c.table_name = $2
		ORDER BY c.ordinal_position`, schemaName, tableName)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()
	var cols []columnMetadata
	var pk []string
	for rows.Next() {
		var name, typ, nullable string
		var isPK bool
		if err := rows.Scan(&name, &typ, &nullable, &isPK); err != nil {
			return nil, nil, err
		}
		cols = append(cols, columnMetadata{Name: name, DataType: typ, Nullable: strings.EqualFold(nullable, "YES")})
		if isPK {
			pk = append(pk, name)
		}
	}
	return cols, pk, rows.Err()
}

func sampleSQLTable(ctx context.Context, db *sql.DB, dialect, table string) ([]map[string]any, error) {
	if !safeDBIdent(table) {
		return nil, nil
	}
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 5", quoteTableName(dialect, table))
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	var sample []map[string]any
	for rows.Next() {
		values := make([]any, len(cols))
		ptrs := make([]any, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}
		data := map[string]any{}
		for i, col := range cols {
			data[col] = normalizeSQLValue(values[i])
		}
		sample = append(sample, map[string]any{"operation": "INSERT", "data": data})
	}
	return sample, rows.Err()
}

func querySingleColumn(ctx context.Context, db *sql.DB, query string, args ...any) ([]string, error) {
	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var s string
		if err := rows.Scan(&s); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func schemaFromSamples(sample []map[string]any) []columnMetadata {
	seen := map[string]columnMetadata{}
	for _, rec := range sample {
		data, _ := rec["data"].(map[string]any)
		for k, v := range data {
			if _, ok := seen[k]; !ok {
				seen[k] = columnMetadata{Name: k, DataType: inferredType(v), Nullable: v == nil}
			}
		}
	}
	names := make([]string, 0, len(seen))
	for name := range seen {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]columnMetadata, 0, len(names))
	for _, name := range names {
		out = append(out, seen[name])
	}
	return out
}

var safeDBIdentPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

func safeDBIdent(s string) bool { return safeDBIdentPattern.MatchString(s) }

func stripSchema(table string) string {
	if _, t := splitSchema("", table); t != "" {
		return t
	}
	return table
}

func splitSchema(fallback, table string) (string, string) {
	if idx := strings.LastIndex(table, "."); idx > 0 && idx < len(table)-1 {
		return table[:idx], table[idx+1:]
	}
	return fallback, table
}

func quoteTableName(dialect, table string) string {
	parts := strings.Split(table, ".")
	quote := "`"
	if dialect == "postgres" {
		quote = `"`
	}
	for i, part := range parts {
		parts[i] = quote + part + quote
	}
	return strings.Join(parts, ".")
}

func normalizeSQLValue(v any) any {
	switch vv := v.(type) {
	case []byte:
		return string(vv)
	case time.Time:
		return vv.Format(time.RFC3339Nano)
	default:
		return vv
	}
}

func upsertTableMetadata(list []tableMetadata, item tableMetadata) []tableMetadata {
	for i := range list {
		if list[i].Name == item.Name && (item.Schema == "" || list[i].Schema == item.Schema) {
			list[i] = item
			return list
		}
	}
	return append(list, item)
}

func inferredType(v any) string {
	switch v.(type) {
	case bool:
		return "bool"
	case float32, float64:
		return "float"
	case int, int32, int64, uint, uint32, uint64:
		return "int"
	case map[string]any:
		return "object"
	case []any:
		return "array"
	default:
		return "string"
	}
}

func str(cfg map[string]any, key string) string {
	if cfg == nil {
		return ""
	}
	if v, ok := cfg[key].(string); ok {
		return v
	}
	return ""
}

func strDefault(cfg map[string]any, key, fallback string) string {
	if v := str(cfg, key); v != "" {
		return v
	}
	return fallback
}

func intDefault(cfg map[string]any, key string, fallback int) int {
	if cfg == nil {
		return fallback
	}
	switch v := cfg[key].(type) {
	case int:
		return v
	case int64:
		return int(v)
	case float64:
		return int(v)
	case string:
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return fallback
}

func boolDefault(cfg map[string]any, key string, fallback bool) bool {
	if cfg == nil {
		return fallback
	}
	if v, ok := cfg[key].(bool); ok {
		return v
	}
	return fallback
}

func stringSlice(v any) []string {
	switch vv := v.(type) {
	case []string:
		return vv
	case []any:
		var out []string
		for _, item := range vv {
			if s, ok := item.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	default:
		return nil
	}
}
