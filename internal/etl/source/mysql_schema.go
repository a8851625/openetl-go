package source

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func openMySQLSchemaDB(ctx context.Context, user, password, host string, port int, database string) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&charset=utf8mb4&loc=Local&timeout=10s&readTimeout=60s",
		user, password, host, port, database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("connect mysql for schema (host %s:%d, db %s): %w", host, port, database, err)
	}
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping mysql for schema: %w", err)
	}
	return db, nil
}

func describeMySQLTableSchema(ctx context.Context, db *sql.DB, database, table string, selected []string) (core.SchemaInfo, error) {
	schemaName, tableName := mysqlInfoSchemaTarget(database, table)
	rows, err := db.QueryContext(ctx, `
		SELECT column_name, column_type, is_nullable
		FROM information_schema.columns
		WHERE table_schema = ? AND table_name = ?
		ORDER BY ordinal_position
	`, schemaName, tableName)
	if err != nil {
		return core.SchemaInfo{}, fmt.Errorf("query mysql information_schema columns for %s.%s: %w", schemaName, tableName, err)
	}
	defer rows.Close()

	selectedSet := map[string]bool{}
	selectedOrder := map[string]int{}
	for i, col := range selected {
		key := strings.ToLower(col)
		selectedSet[key] = true
		selectedOrder[key] = i
	}
	foundSelected := map[string]bool{}

	var cols []core.ColumnInfo
	for rows.Next() {
		var name, dataType, nullable string
		if err := rows.Scan(&name, &dataType, &nullable); err != nil {
			return core.SchemaInfo{}, fmt.Errorf("scan mysql schema column: %w", err)
		}
		if len(selectedSet) > 0 {
			key := strings.ToLower(name)
			if !selectedSet[key] {
				continue
			}
			foundSelected[key] = true
		}
		cols = append(cols, core.ColumnInfo{
			Name:     name,
			DataType: dataType,
			Nullable: strings.EqualFold(nullable, "YES"),
		})
	}
	if err := rows.Err(); err != nil {
		return core.SchemaInfo{}, fmt.Errorf("iterate mysql schema columns: %w", err)
	}
	if len(selectedSet) > 0 {
		for _, col := range selected {
			if !foundSelected[strings.ToLower(col)] {
				return core.SchemaInfo{}, fmt.Errorf("mysql table %s.%s missing configured column %q", schemaName, tableName, col)
			}
		}
		sortMySQLColumnsBySelection(cols, selectedOrder)
	}
	if len(cols) == 0 {
		return core.SchemaInfo{}, fmt.Errorf("mysql table %s.%s has no describable columns", schemaName, tableName)
	}
	return core.SchemaInfo{Columns: cols}, nil
}

func describeMySQLQuerySchema(ctx context.Context, db *sql.DB, query string) (core.SchemaInfo, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT * FROM (%s) AS openetl_schema_probe LIMIT 0", query))
	if err != nil {
		return core.SchemaInfo{}, fmt.Errorf("probe mysql custom query schema: %w", err)
	}
	defer rows.Close()
	types, err := rows.ColumnTypes()
	if err != nil {
		return core.SchemaInfo{}, fmt.Errorf("read mysql custom query column types: %w", err)
	}
	cols := make([]core.ColumnInfo, 0, len(types))
	for _, ct := range types {
		nullable, _ := ct.Nullable()
		cols = append(cols, core.ColumnInfo{
			Name:     ct.Name(),
			DataType: ct.DatabaseTypeName(),
			Nullable: nullable,
		})
	}
	if len(cols) == 0 {
		return core.SchemaInfo{}, fmt.Errorf("mysql custom query has no describable columns")
	}
	return core.SchemaInfo{Columns: cols}, nil
}

func singleDescribableMySQLTable(tables []string) (string, bool) {
	if len(tables) != 1 {
		return "", false
	}
	table := strings.TrimSpace(tables[0])
	if table == "" || table == "*" {
		return "", false
	}
	return table, true
}

func mysqlInfoSchemaTarget(database, table string) (string, string) {
	if idx := strings.LastIndex(table, "."); idx > 0 && idx < len(table)-1 {
		return table[:idx], table[idx+1:]
	}
	return database, table
}

func sortMySQLColumnsBySelection(cols []core.ColumnInfo, order map[string]int) {
	for i := 1; i < len(cols); i++ {
		for j := i; j > 0; j-- {
			left := order[strings.ToLower(cols[j-1].Name)]
			right := order[strings.ToLower(cols[j].Name)]
			if left <= right {
				break
			}
			cols[j-1], cols[j] = cols[j], cols[j-1]
		}
	}
}
