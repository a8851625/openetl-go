package typing

import (
	"fmt"
	"strconv"
	"strings"
)

// ResolveColumnDDL chooses a target DDL type with explicit priority:
//
//  1. override — sink.config.column_types (user contract, highest trust)
//  2. declaredSourceType — information_schema / Debezium field schema (mapped)
//  3. InferFromValue — sample value + column-name heuristics (weakest)
//
// Empty override/declared fall through. Unknown declared types also fall through
// to sample inference rather than producing an empty DDL fragment.
func ResolveColumnDDL(dialect Dialect, columnName string, sample any, declaredSourceType, override string) string {
	if o := strings.TrimSpace(override); o != "" {
		return o
	}
	if d := strings.TrimSpace(declaredSourceType); d != "" {
		if mapped := MapSourceType(dialect, d); mapped != "" {
			return mapped
		}
	}
	return InferFromValue(dialect, columnName, sample)
}

// MapSourceType maps a source/database/Debezium type name onto a sink dialect
// DDL fragment. Returns "" when the input is empty or unrecognized so callers
// can fall back to sample inference.
func MapSourceType(dialect Dialect, sourceType string) string {
	raw := strings.TrimSpace(sourceType)
	if raw == "" {
		return ""
	}
	lower := strings.ToLower(raw)
	// Strip logical type annotations: "io.debezium.time.Timestamp" etc.
	base := lower
	if i := strings.LastIndex(base, "."); i >= 0 {
		base = base[i+1:]
	}

	// Debezium / Kafka Connect primitive + common MySQL COLUMN_TYPE forms.
	switch {
	case base == "boolean" || base == "bool" || lower == "bit(1)":
		return boolDDL(dialect)
	case base == "int8" || base == "tinyint" || strings.HasPrefix(lower, "tinyint"):
		// MySQL often stores boolean as tinyint(1).
		if strings.Contains(lower, "(1)") {
			return boolDDL(dialect)
		}
		return intDDL(dialect, 8)
	case base == "int16" || base == "smallint" || strings.HasPrefix(lower, "smallint"):
		return intDDL(dialect, 16)
	case base == "int32" || base == "int" || base == "integer" || strings.HasPrefix(lower, "int(") || lower == "mediumint" || strings.HasPrefix(lower, "mediumint"):
		return intDDL(dialect, 32)
	case base == "int64" || base == "long" || base == "bigint" || strings.HasPrefix(lower, "bigint"):
		return intDDL(dialect, 64)
	case base == "float32" || base == "float" || strings.HasPrefix(lower, "float"):
		return floatDDL(dialect, false)
	case base == "float64" || base == "double" || strings.HasPrefix(lower, "double") || strings.HasPrefix(lower, "real"):
		return floatDDL(dialect, true)
	case base == "bytes" || base == "binary" || strings.HasPrefix(lower, "binary") || strings.HasPrefix(lower, "varbinary") || strings.Contains(lower, "blob"):
		return bytesDDL(dialect)
	case base == "string" || strings.HasPrefix(lower, "varchar") || strings.HasPrefix(lower, "char") ||
		strings.HasPrefix(lower, "text") || strings.HasPrefix(lower, "tinytext") ||
		strings.HasPrefix(lower, "mediumtext") || strings.HasPrefix(lower, "longtext") ||
		base == "json" || strings.HasPrefix(lower, "enum") || strings.HasPrefix(lower, "set"):
		// Preserve MySQL COLUMN_TYPE when targeting MySQL (varchar(64) etc.).
		if dialect == DialectMySQL && (strings.HasPrefix(lower, "varchar") || strings.HasPrefix(lower, "char") ||
			strings.HasPrefix(lower, "text") || strings.HasPrefix(lower, "enum") || strings.HasPrefix(lower, "set") || lower == "json") {
			return raw
		}
		return stringDDL(dialect, raw)
	case base == "decimal" || base == "numeric" || strings.HasPrefix(lower, "decimal") || strings.HasPrefix(lower, "numeric"):
		if dialect == DialectMySQL && (strings.HasPrefix(lower, "decimal") || strings.HasPrefix(lower, "numeric")) {
			return raw
		}
		return decimalDDL(dialect, raw)
	case base == "date":
		return dateDDL(dialect)
	case base == "time" || strings.HasPrefix(lower, "time(") || lower == "time":
		return timeDDL(dialect)
	case base == "year" || strings.HasPrefix(lower, "year"):
		// MySQL YEAR (1900-2155) is a 2/4-digit year integer, not a timestamp.
		// Mapping it to DateTime would force every value through timestamp
		// parsing and fail (e.g. value 2026 is not a parseable datetime).
		// UInt16 covers the full YEAR range and keeps numeric semantics.
		switch dialect {
		case DialectClickHouse:
			return "UInt16"
		case DialectMySQL, DialectDoris:
			return "SMALLINT"
		case DialectPostgreSQL:
			return "SMALLINT"
		default:
			return "SMALLINT"
		}
	case base == "timestamp" || base == "datetime" ||
		strings.HasPrefix(lower, "timestamp") || strings.HasPrefix(lower, "datetime") ||
		// Debezium logical temporal type short names
		base == "zonedtimestamp" || base == "microtimestamp" || base == "nanotimestamp" ||
		base == "microtime" || base == "nanotime":
		if dialect == DialectMySQL && (strings.HasPrefix(lower, "datetime") || strings.HasPrefix(lower, "timestamp")) {
			return raw
		}
		return timestampDDL(dialect)
	}
	// Pass through already-valid dialect DDL fragments (user override path).
	if looksLikeDDL(raw) {
		return raw
	}
	return ""
}

func looksLikeDDL(s string) bool {
	u := strings.ToUpper(strings.TrimSpace(s))
	for _, p := range []string{"INT", "BIGINT", "TINYINT", "SMALLINT", "VARCHAR", "TEXT", "DATETIME", "TIMESTAMP", "DATE", "DECIMAL", "NUMERIC", "DOUBLE", "FLOAT", "BOOL", "BYTEA", "BLOB", "JSON", "UUID"} {
		if strings.HasPrefix(u, p) {
			return true
		}
	}
	return false
}

func boolDDL(d Dialect) string {
	switch d {
	case DialectMySQL:
		return "TINYINT(1)"
	case DialectPostgreSQL:
		return "BOOLEAN"
	case DialectClickHouse:
		return "UInt8"
	case DialectDoris:
		return "BOOLEAN"
	default:
		return "BOOLEAN"
	}
}

func intDDL(d Dialect, bits int) string {
	switch d {
	case DialectMySQL:
		switch bits {
		case 8:
			return "TINYINT"
		case 16:
			return "SMALLINT"
		case 64:
			return "BIGINT"
		default:
			return "INT"
		}
	case DialectPostgreSQL:
		switch bits {
		case 16:
			return "SMALLINT"
		case 64:
			return "BIGINT"
		default:
			return "INTEGER"
		}
	case DialectClickHouse:
		switch bits {
		case 8:
			return "Int8"
		case 16:
			return "Int16"
		case 64:
			return "Int64"
		default:
			return "Int32"
		}
	case DialectDoris:
		switch bits {
		case 8:
			return "TINYINT"
		case 16:
			return "SMALLINT"
		case 64:
			return "BIGINT"
		default:
			return "INT"
		}
	default:
		return "INT"
	}
}

func floatDDL(d Dialect, double bool) string {
	switch d {
	case DialectMySQL:
		if double {
			return "DOUBLE"
		}
		return "FLOAT"
	case DialectPostgreSQL:
		if double {
			return "DOUBLE PRECISION"
		}
		return "REAL"
	case DialectClickHouse:
		if double {
			return "Float64"
		}
		return "Float32"
	case DialectDoris:
		if double {
			return "DOUBLE"
		}
		return "FLOAT"
	default:
		return "DOUBLE"
	}
}

func bytesDDL(d Dialect) string {
	switch d {
	case DialectMySQL, DialectDoris:
		return "BLOB"
	case DialectPostgreSQL:
		return "BYTEA"
	case DialectClickHouse:
		return "String"
	default:
		return "BLOB"
	}
}

func stringDDL(d Dialect, raw string) string {
	switch d {
	case DialectMySQL:
		if strings.EqualFold(raw, "json") {
			return "JSON"
		}
		return "TEXT"
	case DialectPostgreSQL:
		if strings.EqualFold(raw, "json") || strings.EqualFold(raw, "jsonb") {
			return "JSONB"
		}
		return "TEXT"
	case DialectClickHouse:
		return "String"
	case DialectDoris:
		return "STRING"
	default:
		return "TEXT"
	}
}

// decimalDDL preserves source precision/scale when the raw source type
// carries a (precision, scale) suffix — decimal(5,2) maps to Decimal(5,2)
// instead of the hard-coded Decimal(18,2) default (precision-loss residual
// fix). Unparseable or unsuffixed types keep the 18,2 default.
func decimalDDL(d Dialect, raw ...string) string {
	defaultDDL := func() string {
		switch d {
		case DialectMySQL, DialectDoris:
			return "DECIMAL(18,2)"
		case DialectPostgreSQL:
			return "NUMERIC(18,2)"
		case DialectClickHouse:
			return "Decimal(18, 2)"
		default:
			return "DECIMAL(18,2)"
		}
	}
	if len(raw) == 0 || raw[0] == "" {
		return defaultDDL()
	}
	p, s, ok := parsePrecisionScale(raw[0])
	if !ok {
		return defaultDDL()
	}
	// Sanity bounds: ClickHouse max Decimal precision is 76, MySQL/PG 65.
	// Scale cannot exceed precision. Clamp out-of-range values to the default
	// rather than emitting invalid DDL.
	if p < 1 || p > 65 || s > p {
		return defaultDDL()
	}
	switch d {
	case DialectMySQL, DialectDoris:
		return fmt.Sprintf("DECIMAL(%d,%d)", p, s)
	case DialectPostgreSQL:
		return fmt.Sprintf("NUMERIC(%d,%d)", p, s)
	case DialectClickHouse:
		if p > 76 {
			return defaultDDL()
		}
		return fmt.Sprintf("Decimal(%d, %d)", p, s)
	default:
		return fmt.Sprintf("DECIMAL(%d,%d)", p, s)
	}
}

// parsePrecisionScale extracts (precision, scale) from a raw source type
// like "decimal(10,2)" or "numeric(8, 0)" (optionally signed).
func parsePrecisionScale(raw string) (precision, scale int, ok bool) {
	open := strings.Index(raw, "(")
	closeIdx := strings.Index(raw, ")")
	if open < 0 || closeIdx <= open+1 {
		return 0, 0, false
	}
	inner := raw[open+1 : closeIdx]
	parts := strings.Split(inner, ",")
	trimmed := make([]int, 0, 2)
	for _, part := range parts {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil {
			return 0, 0, false
		}
		trimmed = append(trimmed, n)
	}
	if len(trimmed) == 1 {
		return trimmed[0], 0, true // decimal(10) == decimal(10,0)
	}
	if len(trimmed) != 2 {
		return 0, 0, false
	}
	return trimmed[0], trimmed[1], true
}

func dateDDL(d Dialect) string {
	switch d {
	case DialectClickHouse:
		return "Date"
	default:
		return "DATE"
	}
}

func timeDDL(d Dialect) string {
	switch d {
	case DialectMySQL, DialectDoris:
		return "TIME"
	case DialectPostgreSQL:
		return "TIME"
	case DialectClickHouse:
		return "String"
	default:
		return "TIME"
	}
}

func timestampDDL(d Dialect) string {
	switch d {
	case DialectMySQL:
		return "DATETIME(3)"
	case DialectPostgreSQL:
		return "TIMESTAMP(3)"
	case DialectClickHouse:
		return "DateTime64(3)"
	case DialectDoris:
		return "DATETIME"
	default:
		return "TIMESTAMP"
	}
}
