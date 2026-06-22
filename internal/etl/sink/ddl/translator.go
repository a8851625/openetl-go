// Package ddl provides DDL statement translation between SQL dialects.
// It parses common MySQL DDL statements (ALTER TABLE, CREATE TABLE) and
// translates them to the target dialect, including column type mapping.
package ddl

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/sink/typing"
)

// Dialect identifies the source or target SQL dialect.
type Dialect = typing.Dialect

const (
	DialectMySQL      = typing.DialectMySQL
	DialectPostgreSQL = typing.DialectPostgreSQL
	DialectClickHouse = typing.DialectClickHouse
)

// Result holds the translated DDL statement and any warnings.
type Result struct {
	Statement string   // translated DDL
	Warnings  []string // non-fatal warnings (e.g., limited feature support)
}

// TranslateDDL converts a source DDL statement to the target dialect.
// Returns an error if the DDL type is not supported for translation.
func TranslateDDL(stmt string, sourceDialect, targetDialect Dialect) (*Result, error) {
	if sourceDialect == targetDialect {
		return &Result{Statement: stmt}, nil
	}

	stmt = strings.TrimSpace(stmt)
	upper := strings.ToUpper(stmt)

	switch {
	case strings.HasPrefix(upper, "ALTER TABLE"):
		return translateAlterTable(stmt, sourceDialect, targetDialect)
	case strings.HasPrefix(upper, "CREATE TABLE"):
		return translateCreateTable(stmt, sourceDialect, targetDialect)
	case strings.HasPrefix(upper, "DROP TABLE"):
		// DROP TABLE is mostly portable
		return &Result{Statement: stmt}, nil
	case strings.HasPrefix(upper, "RENAME TABLE"):
		// RENAME TABLE is mostly portable
		return &Result{Statement: stmt}, nil
	default:
		return nil, fmt.Errorf("unsupported DDL type: %q", stmt[:min(len(stmt), 60)])
	}
}

// translateAlterTable handles ALTER TABLE statements.
func translateAlterTable(stmt string, sourceDialect, targetDialect Dialect) (*Result, error) {
	upper := strings.ToUpper(stmt)

	switch {
	case matchAlterPattern(upper, "ADD COLUMN") || matchAlterPattern(upper, "ADD ("):
		return translateAddColumn(stmt, sourceDialect, targetDialect)
	case matchAlterPattern(upper, "DROP COLUMN"):
		return translateDropColumn(stmt, targetDialect)
	case matchAlterPattern(upper, "MODIFY COLUMN"):
		return translateModifyColumn(stmt, sourceDialect, targetDialect)
	case matchAlterPattern(upper, "CHANGE COLUMN"):
		return translateChangeColumn(stmt, sourceDialect, targetDialect)
	case strings.Contains(upper, "RENAME COLUMN"):
		return &Result{Statement: stmt}, nil
	default:
		result := &Result{Statement: stmt}
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("ALTER TABLE variant may not be portable to %s", targetDialect))
		return result, nil
	}
}

// translateAddColumn handles ALTER TABLE ... ADD COLUMN.
// MySQL: ALTER TABLE t ADD COLUMN name type [options]
// CH:    ALTER TABLE t ADD COLUMN name type
// PG:    ALTER TABLE t ADD COLUMN name type
func translateAddColumn(stmt string, sourceDialect, targetDialect Dialect) (*Result, error) {
	table, colName, colType, err := parseAddColumn(stmt)
	if err != nil {
		return nil, err
	}

	// Translate the column type to the target dialect.
	if colType != "" {
		colType = translateColumnType(colType, sourceDialect, targetDialect)
	}

	result := &Result{}
	switch targetDialect {
	case DialectClickHouse:
		result.Statement = fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, colName, colType)
	case DialectPostgreSQL:
		result.Statement = fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, colName, colType)
	default:
		result.Statement = fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, colName, colType)
	}
	return result, nil
}

// translateDropColumn handles ALTER TABLE ... DROP COLUMN.
func translateDropColumn(stmt string, targetDialect Dialect) (*Result, error) {
	table, colName, err := parseDropColumn(stmt)
	if err != nil {
		return nil, err
	}

	switch targetDialect {
	case DialectClickHouse:
		return &Result{Statement: fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", table, colName)}, nil
	case DialectPostgreSQL:
		return &Result{Statement: fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", table, colName)}, nil
	default:
		return &Result{Statement: fmt.Sprintf("ALTER TABLE %s DROP COLUMN %s", table, colName)}, nil
	}
}

// translateModifyColumn handles ALTER TABLE ... MODIFY COLUMN.
// MySQL → ClickHouse: warn — CH has limited MODIFY support
// MySQL → PostgreSQL: ALTER COLUMN TYPE
func translateModifyColumn(stmt string, sourceDialect, targetDialect Dialect) (*Result, error) {
	table, colName, colType, err := parseModifyColumn(stmt)
	if err != nil {
		return nil, err
	}

	if colType != "" {
		colType = translateColumnType(colType, sourceDialect, targetDialect)
	}

	result := &Result{}
	switch targetDialect {
	case DialectClickHouse:
		result.Statement = fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", table, colName, colType)
		result.Warnings = append(result.Warnings,
			"ClickHouse MODIFY COLUMN only supports changing COMMENT, TTL, and similar metadata; type changes require careful handling")
	case DialectPostgreSQL:
		result.Statement = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", table, colName, colType)
	default:
		result.Statement = fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", table, colName, colType)
	}
	return result, nil
}

// translateChangeColumn handles ALTER TABLE ... CHANGE COLUMN (MySQL-specific).
func translateChangeColumn(stmt string, sourceDialect, targetDialect Dialect) (*Result, error) {
	table, oldName, newName, colType, err := parseChangeColumn(stmt)
	if err != nil {
		return nil, err
	}

	if colType != "" {
		colType = translateColumnType(colType, sourceDialect, targetDialect)
	}

	result := &Result{}
	if oldName == newName {
		// Same name = modify type
		switch targetDialect {
		case DialectClickHouse:
			result.Statement = fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", table, newName, colType)
			result.Warnings = append(result.Warnings, "ClickHouse MODIFY COLUMN has limited capabilities")
		case DialectPostgreSQL:
			result.Statement = fmt.Sprintf("ALTER TABLE %s ALTER COLUMN %s TYPE %s", table, newName, colType)
		default:
			result.Statement = fmt.Sprintf("ALTER TABLE %s MODIFY COLUMN %s %s", table, newName, colType)
		}
	} else {
		// Different names = rename + modify
		switch targetDialect {
		case DialectClickHouse:
			result.Statement = fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", table, oldName, newName)
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("column type change (to %s) not applied via RENAME; use separate MODIFY COLUMN", colType))
		case DialectPostgreSQL:
			result.Statement = fmt.Sprintf("ALTER TABLE %s RENAME COLUMN %s TO %s", table, oldName, newName)
			result.Warnings = append(result.Warnings,
				fmt.Sprintf("column type change (to %s) not applied; use ALTER COLUMN TYPE separately", colType))
		default:
			result.Statement = stmt
		}
	}
	return result, nil
}

// translateCreateTable handles CREATE TABLE statements.
// This is a best-effort pass-through with type mapping.
func translateCreateTable(stmt string, sourceDialect, targetDialect Dialect) (*Result, error) {
	// For now, pass through with a warning.
	// Full CREATE TABLE translation requires column definition parsing
	// which is handled by auto_create at the sink level.
	result := &Result{Statement: stmt}
	if sourceDialect != targetDialect {
		result.Warnings = append(result.Warnings,
			fmt.Sprintf("CREATE TABLE may not be portable: source=%s target=%s; auto_create handles this at sink level", sourceDialect, targetDialect))
	}
	return result, nil
}

// translateColumnType translates a column type string from source to target dialect.
func translateColumnType(colType string, sourceDialect, targetDialect Dialect) string {
	// Map common MySQL type names to the target dialect.
	// For the full mapping we use the typing package, but here we do
	// string-based translation of the type name from the DDL.
	colType = strings.TrimSpace(colType)
	upper := strings.ToUpper(colType)

	switch sourceDialect {
	case DialectMySQL:
		return translateMySQLTypeTo(upper, targetDialect)
	default:
		return colType
	}
}

// translateMySQLTypeTo maps a MySQL type name to the target dialect.
func translateMySQLTypeTo(mysqlType string, targetDialect Dialect) string {
	switch targetDialect {
	case DialectClickHouse:
		return mysqlTypeToClickHouse(mysqlType)
	case DialectPostgreSQL:
		return mysqlTypeToPostgreSQL(mysqlType)
	default:
		return mysqlType
	}
}

func mysqlTypeToClickHouse(t string) string {
	// Normalize: strip length/parentheses for matching
	base := strings.SplitN(t, "(", 2)[0]
	switch base {
	case "TINYINT":
		return "Int8"
	case "SMALLINT":
		return "Int16"
	case "INT", "INTEGER", "MEDIUMINT":
		return "Int32"
	case "BIGINT":
		return "Int64"
	case "FLOAT":
		return "Float32"
	case "DOUBLE", "REAL":
		return "Float64"
	case "DECIMAL", "NUMERIC":
		return keepParentheses(t, "Decimal")
	case "CHAR":
		return "String"
	case "VARCHAR":
		return "String"
	case "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		return "String"
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB":
		return "String"
	case "DATE":
		return "Date"
	case "DATETIME", "TIMESTAMP":
		return "DateTime64(3)"
	case "TIME":
		return "String"
	case "JSON":
		return "String"
	case "ENUM", "SET":
		return "String"
	default:
		return "String"
	}
}

func mysqlTypeToPostgreSQL(t string) string {
	base := strings.SplitN(t, "(", 2)[0]
	switch base {
	case "TINYINT":
		return "SMALLINT"
	case "SMALLINT":
		return "SMALLINT"
	case "INT", "INTEGER", "MEDIUMINT":
		return "INTEGER"
	case "BIGINT":
		return "BIGINT"
	case "FLOAT":
		return "REAL"
	case "DOUBLE", "REAL":
		return "DOUBLE PRECISION"
	case "DECIMAL", "NUMERIC":
		return keepParentheses(t, "DECIMAL")
	case "CHAR":
		return keepParentheses(t, "CHAR")
	case "VARCHAR":
		return keepParentheses(t, "VARCHAR")
	case "TEXT", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT":
		return "TEXT"
	case "BLOB", "TINYBLOB", "MEDIUMBLOB", "LONGBLOB":
		return "BYTEA"
	case "DATE":
		return "DATE"
	case "DATETIME":
		return "TIMESTAMP(3)"
	case "TIMESTAMP":
		return "TIMESTAMP(3)"
	case "TIME":
		return "TIME"
	case "JSON":
		return "JSONB"
	case "ENUM", "SET":
		return "TEXT"
	default:
		return "TEXT"
	}
}

// keepParentheses returns the base type with the original parentheses (e.g. "DECIMAL(10,2)").
func keepParentheses(original, newBase string) string {
	if idx := strings.Index(original, "("); idx >= 0 {
		return newBase + original[idx:]
	}
	return newBase
}

// ── DDL Parsers ─────────────────────────────────────────────────────

var (
	addColRe    = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(?:IF\s+EXISTS\s+)?(?:` + "`" + `?(\w+)` + "`" + `?\.)?` + "`" + `?(\w+)` + "`" + `?\s+ADD\s+(?:COLUMN\s+)?` + "`" + `?(\w+)` + "`" + `?\s+(.+?)(?:\s+(?:FIRST|AFTER|NOT\s+NULL|DEFAULT|COMMENT|COLLATE|CHARACTER\s+SET).*)?$`)
	dropColRe   = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(?:` + "`" + `?(\w+)` + "`" + `?\.)?` + "`" + `?(\w+)` + "`" + `?\s+DROP\s+(?:COLUMN\s+)?` + "`" + `?(\w+)` + "`" + `?`)
	modifyColRe = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(?:` + "`" + `?(\w+)` + "`" + `?\.)?` + "`" + `?(\w+)` + "`" + `?\s+MODIFY\s+(?:COLUMN\s+)?` + "`" + `?(\w+)` + "`" + `?\s+(.+?)(?:\s+(?:FIRST|AFTER|NOT\s+NULL|DEFAULT|COMMENT).*)?$`)
	changeColRe = regexp.MustCompile(`(?i)ALTER\s+TABLE\s+(?:` + "`" + `?(\w+)` + "`" + `?\.)?` + "`" + `?(\w+)` + "`" + `?\s+CHANGE\s+(?:COLUMN\s+)?` + "`" + `?(\w+)` + "`" + `?\s+` + "`" + `?(\w+)` + "`" + `?\s+(.+?)(?:\s+(?:FIRST|AFTER|NOT\s+NULL|DEFAULT|COMMENT).*)?$`)
)

func matchAlterPattern(upper, action string) bool {
	return strings.Contains(upper, action)
}

func parseAddColumn(stmt string) (table, colName, colType string, err error) {
	matches := addColRe.FindStringSubmatch(stmt)
	if len(matches) < 5 {
		return "", "", "", fmt.Errorf("cannot parse ADD COLUMN: %q", stmt)
	}
	// matches[1] = schema (optional), matches[2] = table, matches[3] = column, matches[4] = type
	table = matches[2]
	colName = matches[3]
	colType = strings.TrimSpace(matches[4])
	return table, colName, colType, nil
}

func parseDropColumn(stmt string) (table, colName string, err error) {
	matches := dropColRe.FindStringSubmatch(stmt)
	if len(matches) < 4 {
		return "", "", fmt.Errorf("cannot parse DROP COLUMN: %q", stmt)
	}
	return matches[2], matches[3], nil
}

func parseModifyColumn(stmt string) (table, colName, colType string, err error) {
	matches := modifyColRe.FindStringSubmatch(stmt)
	if len(matches) < 5 {
		return "", "", "", fmt.Errorf("cannot parse MODIFY COLUMN: %q", stmt)
	}
	table = matches[2]
	colName = matches[3]
	colType = strings.TrimSpace(matches[4])
	return table, colName, colType, nil
}

func parseChangeColumn(stmt string) (table, oldName, newName, colType string, err error) {
	matches := changeColRe.FindStringSubmatch(stmt)
	if len(matches) < 6 {
		return "", "", "", "", fmt.Errorf("cannot parse CHANGE COLUMN: %q", stmt)
	}
	table = matches[2]
	oldName = matches[3]
	newName = matches[4]
	colType = strings.TrimSpace(matches[5])
	return table, oldName, newName, colType, nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
