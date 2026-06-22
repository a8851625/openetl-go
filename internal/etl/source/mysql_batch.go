package source

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"time"

	_ "github.com/go-sql-driver/mysql"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterSource("mysql_batch", func(config map[string]any) (core.Source, error) {
		return NewMySQLBatchSource(config)
	})
}

type mysqlBatchReader struct {
	db           *sql.DB
	table        string
	columns      []string
	pkCol        string
	lastID       int64
	limit        int
	done         bool
	customQuery  string
	cursorCol    string
	shardIndex   int
	shardTotal   int
	shardEnabled bool
}

type MySQLBatchSource struct {
	name         string
	host         string
	port         int
	user         string
	password     string
	database     string
	table        string
	columns      []string
	pkCol        string
	limit        int
	customQuery  string
	cursorCol    string
	shardIndex   int
	shardTotal   int
	shardEnabled bool
}

func NewMySQLBatchSource(config map[string]any) (*MySQLBatchSource, error) {
	s := &MySQLBatchSource{name: "mysql_batch", limit: 5000}
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
	_, hasPKConfig := config["pk_column"]
	if v, ok := config["pk_column"]; ok {
		s.pkCol = v.(string)
	}
	if v, ok := config["cursor_column"]; ok {
		s.cursorCol = v.(string)
	}
	if v, ok := config["limit"]; ok {
		switch l := v.(type) {
		case int:
			s.limit = l
		case float64:
			s.limit = int(l)
		}
	}
	if v, ok := config["columns"]; ok {
		if cols, ok := v.([]interface{}); ok {
			for _, c := range cols {
				s.columns = append(s.columns, c.(string))
			}
		}
	}
	if s.pkCol == "" {
		s.pkCol = "id"
	}
	if v, ok := config["query"]; ok {
		s.customQuery = v.(string)
	}
	if s.customQuery != "" {
		if s.cursorCol == "" {
			if !hasPKConfig {
				return nil, fmt.Errorf("mysql_batch custom query requires cursor_column or explicit pk_column for stable keyset pagination")
			}
			s.cursorCol = s.pkCol
		}
	}
	// Sharding support
	if v, ok := config["shard_index"]; ok {
		switch idx := v.(type) {
		case int:
			s.shardIndex = idx
		case float64:
			s.shardIndex = int(idx)
		}
	}
	if v, ok := config["shard_total"]; ok {
		switch t := v.(type) {
		case int:
			s.shardTotal = t
		case float64:
			s.shardTotal = int(t)
		}
	}
	s.shardEnabled = s.shardTotal > 1
	if err := validateIdentifier(s.table); err != nil {
		return nil, fmt.Errorf("invalid table name: %w", err)
	}
	if err := validateIdentifier(s.pkCol); err != nil {
		return nil, fmt.Errorf("invalid pk_column: %w", err)
	}
	if s.cursorCol != "" {
		if err := validateIdentifier(s.cursorCol); err != nil {
			return nil, fmt.Errorf("invalid cursor_column: %w", err)
		}
	}
	for _, c := range s.columns {
		if err := validateIdentifier(c); err != nil {
			return nil, fmt.Errorf("invalid column name %q: %w", c, err)
		}
	}
	return s, nil
}

func (s *MySQLBatchSource) Name() string { return s.name }

func (s *MySQLBatchSource) Open(ctx context.Context, cp *core.Checkpoint) (core.RecordReader, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&loc=Local&timeout=10s&readTimeout=300s",
		s.user, s.password, s.host, s.port, s.database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("connect mysql (host %s:%d, db %s): %w", s.host, s.port, s.database, err) // P5-15: WHERE context
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("ping mysql: %w", err)
	}

	var lastID int64
	if cp != nil {
		var pos mysqlBatchPosition
		if json.Unmarshal(cp.Position, &pos) == nil {
			lastID = pos.LastID
		}
	}

	return &mysqlBatchReader{
		db:           db,
		table:        s.table,
		columns:      s.columns,
		pkCol:        s.pkCol,
		lastID:       lastID,
		limit:        s.limit,
		customQuery:  s.customQuery,
		cursorCol:    s.cursorCol,
		shardIndex:   s.shardIndex,
		shardTotal:   s.shardTotal,
		shardEnabled: s.shardEnabled,
	}, nil
}

type mysqlBatchPosition struct {
	LastID int64 `json:"last_id"`
}

func (r *mysqlBatchReader) Read(ctx context.Context) (core.Record, error) {
	if r.done {
		return core.Record{}, io.EOF
	}
	recs, err := r.ReadBatch(ctx, 1)
	if err != nil {
		return core.Record{}, err
	}
	if len(recs) == 0 {
		return core.Record{}, io.EOF
	}
	return recs[0], nil
}

func (r *mysqlBatchReader) ReadBatch(ctx context.Context, n int) ([]core.Record, error) {
	if r.done {
		return nil, nil
	}

	limit := r.limit
	if n > 0 && n < limit {
		limit = n
	}

	var rows *sql.Rows
	var err error
	var columns []string

	if r.customQuery != "" {
		query := fmt.Sprintf("SELECT * FROM (%s) AS stable_query WHERE `%s` > ? ORDER BY `%s` LIMIT %d", r.customQuery, r.cursorCol, r.cursorCol, limit)
		if r.shardEnabled {
			query = fmt.Sprintf("SELECT * FROM (%s) AS stable_query WHERE `%s` > ? AND MOD(`%s`, %d) = %d ORDER BY `%s` LIMIT %d",
				r.customQuery, r.cursorCol, r.cursorCol, r.shardTotal, r.shardIndex, r.cursorCol, limit)
		}
		rows, err = r.db.QueryContext(ctx, query, r.lastID)
		if err != nil {
			return nil, fmt.Errorf("custom query: %w", err)
		}
	} else {
		cols := "*"
		if len(r.columns) > 0 {
			cols = ""
			for i, c := range r.columns {
				if i > 0 {
					cols += ", "
				}
				cols += "`" + c + "`"
			}
		}
		query := fmt.Sprintf("SELECT %s FROM %s WHERE %s > ?", cols, r.table, r.pkCol)
		if r.shardEnabled {
			query += fmt.Sprintf(" AND MOD(`%s`, %d) = %d", r.pkCol, r.shardTotal, r.shardIndex)
		}
		query += fmt.Sprintf(" ORDER BY %s LIMIT %d", r.pkCol, limit)
		rows, err = r.db.QueryContext(ctx, query, r.lastID)
		if err != nil {
			return nil, fmt.Errorf("query: %w", err)
		}
	}
	defer rows.Close()

	columns, _ = rows.Columns()
	var records []core.Record
	for rows.Next() {
		values := make([]any, len(columns))
		valuePtrs := make([]any, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}
		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, fmt.Errorf("mysql_batch scan row: %w", err)
		}

		data := make(map[string]any, len(columns))
		for i, name := range columns {
			data[name] = normalizeValue(values[i])
		}

		if r.customQuery != "" {
			if id, ok := data[r.cursorCol]; ok {
				r.updateLastID(id)
			} else {
				return nil, fmt.Errorf("custom query result missing cursor_column %q", r.cursorCol)
			}
		} else if id, ok := data[r.pkCol]; ok {
			r.updateLastID(id)
		}

		tableName := r.table
		if tableName == "" {
			tableName = "custom_query"
		}
		records = append(records, core.Record{
			Operation: core.OpInsert,
			Data:      data,
			Metadata: core.Metadata{
				Table:     tableName,
				Timestamp: time.Now(),
			},
		})
	}

	if r.customQuery != "" {
		if int64(len(records)) < int64(limit) {
			r.done = true
		}
	} else {
		if len(records) == 0 {
			r.done = true
		}
	}
	return records, nil
}

func (r *mysqlBatchReader) updateLastID(id any) {
	switch v := id.(type) {
	case int:
		if int64(v) > r.lastID {
			r.lastID = int64(v)
		}
	case int64:
		if v > r.lastID {
			r.lastID = v
		}
	case float64:
		if int64(v) > r.lastID {
			r.lastID = int64(v)
		}
	}
}

func (r *mysqlBatchReader) Snapshot(ctx context.Context) (core.Checkpoint, error) {
	pos := mysqlBatchPosition{LastID: r.lastID}
	data, _ := json.Marshal(pos)
	return core.Checkpoint{
		Source:    "mysql_batch",
		Position:  data,
		Timestamp: time.Now(),
	}, nil
}

func (r *mysqlBatchReader) CheckpointForRecord(ctx context.Context, rec core.Record) (core.Checkpoint, error) {
	lastID := r.lastID
	cursorCol := r.pkCol
	if r.customQuery != "" {
		cursorCol = r.cursorCol
	}
	if id, ok := rec.Data[cursorCol]; ok {
		switch v := id.(type) {
		case int64:
			lastID = v
		case int:
			lastID = int64(v)
		case float64:
			lastID = int64(v)
		}
	}
	pos := mysqlBatchPosition{LastID: lastID}
	data, _ := json.Marshal(pos)
	return core.Checkpoint{Source: "mysql_batch", Position: data, Timestamp: time.Now()}, nil
}

func (r *mysqlBatchReader) Close() error {
	return r.db.Close()
}

func normalizeValue(v any) any {
	switch t := v.(type) {
	case []byte:
		return string(t)
	case *sql.NullString:
		if t.Valid {
			return t.String
		}
		return nil
	case *sql.NullInt64:
		if t.Valid {
			return t.Int64
		}
		return nil
	case *sql.NullFloat64:
		if t.Valid {
			return t.Float64
		}
		return nil
	case *sql.NullBool:
		if t.Valid {
			return t.Bool
		}
		return nil
	case *sql.NullTime:
		if t.Valid {
			return t.Time
		}
		return nil
	}
	return v
}

var validIdentRe = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_.]*$`)

func validateIdentifier(name string) error {
	if name == "" {
		return nil
	}
	if !validIdentRe.MatchString(name) {
		return fmt.Errorf("identifier contains invalid characters: %q", name)
	}
	return nil
}
