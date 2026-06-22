// Package typing provides a unified type mapping engine for converting between
// source column types and sink DDL types. It supports both value-driven inference
// (from Go types of record data) and schema-driven mapping (from source schema metadata),
// producing appropriate DDL types for each target database.
package typing

import (
	"strings"
	"time"
)

// Dialect identifies a target database dialect.
type Dialect string

const (
	DialectMySQL      Dialect = "mysql"
	DialectPostgreSQL Dialect = "postgresql"
	DialectClickHouse Dialect = "clickhouse"
	DialectDoris      Dialect = "doris"
)

// InferFromValue returns the DDL type string for a value with the given column name.
// Value type takes priority for unambiguous Go types ([]byte, time.Time, bool).
// Column name hints are used for string/numeric values (e.g. "amount" → DECIMAL, "email" → VARCHAR).
func InferFromValue(dialect Dialect, columnName string, value any) string {
	// Unambiguous Go types take priority over name hints.
	switch value.(type) {
	case []byte:
		return inferFromGoType(dialect, value)
	case time.Time:
		return inferFromGoType(dialect, value)
	case bool:
		return inferFromGoType(dialect, value)
	}
	// Column name hints for string and numeric values.
	if hint := nameHint(dialect, columnName); hint != "" {
		return hint
	}
	return inferFromGoType(dialect, value)
}

// InferFromValues examines multiple values for a column and returns the most
// appropriate type. If all values are nil, returns the default string type.
func InferFromValues(dialect Dialect, columnName string, values []any) string {
	// Try name hint first.
	if hint := nameHint(dialect, columnName); hint != "" {
		return hint
	}
	// Collect all non-nil values and infer from the most specific type.
	var nonNil []any
	for _, v := range values {
		if v != nil {
			nonNil = append(nonNil, v)
		}
	}
	if len(nonNil) == 0 {
		return defaultStringType(dialect)
	}
	// Use the first non-nil value.
	return inferFromGoType(dialect, nonNil[0])
}

// nameHint returns a DDL type based on column naming conventions, or empty
// string if no hint matches.
func nameHint(dialect Dialect, name string) string {
	lower := strings.ToLower(name)

	// ID columns → integer types
	if lower == "id" || strings.HasSuffix(lower, "_id") {
		switch dialect {
		case DialectMySQL:
			return "BIGINT"
		case DialectPostgreSQL:
			return "BIGINT"
		case DialectClickHouse:
			return "Int64"
		case DialectDoris:
			return "BIGINT"
		}
	}

	// Temporal columns → datetime types
	if strings.HasSuffix(lower, "_at") || strings.HasSuffix(lower, "_time") ||
		lower == "created" || lower == "updated" || lower == "deleted" ||
		strings.HasPrefix(lower, "time") || lower == "date" || lower == "timestamp" ||
		strings.HasSuffix(lower, "_date") || strings.HasSuffix(lower, "_ts") {
		switch dialect {
		case DialectMySQL:
			return "DATETIME(3)"
		case DialectPostgreSQL:
			return "TIMESTAMP(3)"
		case DialectClickHouse:
			return "DateTime64(3)"
		case DialectDoris:
			return "DATETIME"
		}
	}

	// Boolean columns
	if strings.HasPrefix(lower, "is_") || strings.HasPrefix(lower, "has_") ||
		lower == "active" || lower == "enabled" || lower == "deleted" ||
		strings.HasSuffix(lower, "_flag") {
		switch dialect {
		case DialectMySQL:
			return "TINYINT(1)"
		case DialectPostgreSQL:
			return "BOOLEAN"
		case DialectClickHouse:
			return "UInt8"
		case DialectDoris:
			return "BOOLEAN"
		}
	}

	// Amount/price → decimal
	if lower == "amount" || lower == "price" || lower == "total" ||
		lower == "cost" || lower == "fee" || lower == "balance" ||
		strings.HasSuffix(lower, "_amount") || strings.HasSuffix(lower, "_price") ||
		strings.HasSuffix(lower, "_total") || strings.HasSuffix(lower, "_fee") {
		switch dialect {
		case DialectMySQL:
			return "DECIMAL(18,2)"
		case DialectPostgreSQL:
			return "DECIMAL(18,2)"
		case DialectClickHouse:
			return "Decimal(18,2)"
		case DialectDoris:
			return "DECIMAL(18,2)"
		}
	}

	// Email columns
	if strings.HasSuffix(lower, "_email") || lower == "email" {
		switch dialect {
		case DialectMySQL:
			return "VARCHAR(255)"
		case DialectPostgreSQL:
			return "VARCHAR(255)"
		case DialectClickHouse:
			return "String"
		case DialectDoris:
			return "VARCHAR(255)"
		}
	}

	// JSON/data columns
	if lower == "json" || lower == "data" || lower == "metadata" ||
		lower == "payload" || lower == "extra" || lower == "attributes" ||
		strings.HasSuffix(lower, "_json") {
		switch dialect {
		case DialectMySQL:
			return "JSON"
		case DialectPostgreSQL:
			return "JSONB"
		case DialectClickHouse:
			return "String"
		case DialectDoris:
			return "JSON"
		}
	}

	return ""
}

// inferFromGoType returns the DDL type based on a Go value's type.
func inferFromGoType(dialect Dialect, value any) string {
	switch v := value.(type) {
	case bool:
		switch dialect {
		case DialectMySQL:
			return "TINYINT(1)"
		case DialectPostgreSQL:
			return "BOOLEAN"
		case DialectClickHouse:
			return "UInt8"
		case DialectDoris:
			return "BOOLEAN"
		}
	case int, int8, int16, int32:
		switch dialect {
		case DialectMySQL:
			return "INT"
		case DialectPostgreSQL:
			return "INTEGER"
		case DialectClickHouse:
			return "Int32"
		case DialectDoris:
			return "INT"
		}
	case int64:
		switch dialect {
		case DialectMySQL:
			return "BIGINT"
		case DialectPostgreSQL:
			return "BIGINT"
		case DialectClickHouse:
			return "Int64"
		case DialectDoris:
			return "BIGINT"
		}
	case uint, uint8, uint16, uint32:
		switch dialect {
		case DialectMySQL:
			return "INT UNSIGNED"
		case DialectPostgreSQL:
			return "INTEGER"
		case DialectClickHouse:
			return "UInt32"
		case DialectDoris:
			return "INT"
		}
	case uint64:
		switch dialect {
		case DialectMySQL:
			return "BIGINT UNSIGNED"
		case DialectPostgreSQL:
			return "BIGINT"
		case DialectClickHouse:
			return "UInt64"
		case DialectDoris:
			return "BIGINT"
		}
	case float32, float64:
		switch dialect {
		case DialectMySQL:
			return "DOUBLE"
		case DialectPostgreSQL:
			return "DOUBLE PRECISION"
		case DialectClickHouse:
			return "Float64"
		case DialectDoris:
			return "DOUBLE"
		}
	case time.Time:
		switch dialect {
		case DialectMySQL:
			return "DATETIME(3)"
		case DialectPostgreSQL:
			return "TIMESTAMP(3)"
		case DialectClickHouse:
			return "DateTime64(3)"
		case DialectDoris:
			return "DATETIME"
		}
	case string:
		// Check if the string looks like a timestamp.
		if isTimestampString(v) {
			switch dialect {
			case DialectMySQL:
				return "DATETIME(3)"
			case DialectPostgreSQL:
				return "TIMESTAMP(3)"
			case DialectClickHouse:
				return "DateTime64(3)"
			case DialectDoris:
				return "DATETIME"
			}
		}
		// String length heuristic.
		if len(v) <= 255 {
			switch dialect {
			case DialectMySQL:
				return "VARCHAR(255)"
			case DialectPostgreSQL:
				return "VARCHAR(255)"
			case DialectClickHouse:
				return "String"
			case DialectDoris:
				return "VARCHAR(255)"
			}
		}
		switch dialect {
		case DialectMySQL:
			return "TEXT"
		case DialectPostgreSQL:
			return "TEXT"
		case DialectClickHouse:
			return "String"
		case DialectDoris:
			return "STRING"
		}
	case []byte:
		switch dialect {
		case DialectMySQL:
			return "BLOB"
		case DialectPostgreSQL:
			return "BYTEA"
		case DialectClickHouse:
			return "String"
		case DialectDoris:
			return "STRING"
		}
	case nil:
		return nullableStringType(dialect)
	default:
		return defaultStringType(dialect)
	}
	return defaultStringType(dialect)
}

// defaultStringType returns the fallback string type for each dialect.
func defaultStringType(dialect Dialect) string {
	switch dialect {
	case DialectMySQL:
		return "TEXT"
	case DialectPostgreSQL:
		return "TEXT"
	case DialectClickHouse:
		return "String"
	case DialectDoris:
		return "STRING"
	}
	return "TEXT"
}

// nullableStringType returns the nullable string type for each dialect.
func nullableStringType(dialect Dialect) string {
	switch dialect {
	case DialectPostgreSQL:
		return "TEXT"
	default:
		return defaultStringType(dialect)
	}
}

// isTimestampString returns true if the string looks like an RFC 3339 timestamp.
func isTimestampString(s string) bool {
	if len(s) < 10 {
		return false
	}
	// Quick check: RFC 3339 contains 'T' between date and time, or space.
	if s[4] != '-' || s[7] != '-' {
		return false
	}
	_, err := time.Parse(time.RFC3339Nano, s)
	if err == nil {
		return true
	}
	// Also check common database formats.
	layouts := []string{
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if _, err := time.Parse(layout, s); err == nil {
			return true
		}
	}
	return false
}
