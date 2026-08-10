package typing

import (
	"testing"
	"time"
)

func TestInferFromValue(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name     string
		dialect  Dialect
		colName  string
		value    any
		expected string
	}{
		// Name hints
		{"id_mysql", DialectMySQL, "id", 1, "BIGINT"},
		{"id_pg", DialectPostgreSQL, "id", 1, "BIGINT"},
		{"id_ch", DialectClickHouse, "id", 1, "Int64"},
		{"user_id_mysql", DialectMySQL, "user_id", 42, "BIGINT"},
		{"created_at_mysql", DialectMySQL, "created_at", now, "DATETIME(3)"},
		{"created_at_pg", DialectPostgreSQL, "created_at", now, "TIMESTAMP(3)"},
		{"updated_time_pg", DialectPostgreSQL, "updated_time", now, "TIMESTAMP(3)"},
		{"is_active_mysql", DialectMySQL, "is_active", true, "TINYINT(1)"},
		{"has_subscription_pg", DialectPostgreSQL, "has_subscription", true, "BOOLEAN"},
		{"amount_mysql", DialectMySQL, "amount", 99.99, "DECIMAL(18,2)"},
		{"price_pg", DialectPostgreSQL, "price", 19.95, "DECIMAL(18,2)"},
		{"email_mysql", DialectMySQL, "email", "a@b.com", "VARCHAR(255)"},

		// Value-driven inference
		{"bool_mysql", DialectMySQL, "flag", true, "TINYINT(1)"},
		{"bool_pg", DialectPostgreSQL, "flag", true, "BOOLEAN"},
		{"int_mysql", DialectMySQL, "count", 42, "INT"},
		{"int_pg", DialectPostgreSQL, "count", 42, "INTEGER"},
		{"int64_mysql", DialectMySQL, "big_num", int64(9000000000), "BIGINT"},
		{"int64_pg", DialectPostgreSQL, "big_num", int64(9000000000), "BIGINT"},
		{"float_mysql", DialectMySQL, "score", 3.14, "DOUBLE"},
		{"float_pg", DialectPostgreSQL, "score", 3.14, "DOUBLE PRECISION"},
		{"time_mysql", DialectMySQL, "ts", now, "DATETIME(3)"},
		{"time_pg", DialectPostgreSQL, "ts", now, "TIMESTAMP(3)"},
		{"str_varchar_mysql", DialectMySQL, "name", "Alice", "VARCHAR(255)"},
		{"str_long_mysql", DialectMySQL, "description", "A very long string that exceeds 255 characters" + string(make([]byte, 256)), "TEXT"},
		{"str_pg", DialectPostgreSQL, "name", "Bob", "VARCHAR(255)"},
		{"bytes_mysql", DialectMySQL, "data", []byte{1, 2, 3}, "BLOB"},
		{"bytes_pg", DialectPostgreSQL, "data", []byte{1, 2, 3}, "BYTEA"},
		{"nil_mysql", DialectMySQL, "empty", nil, "TEXT"},
		{"nil_pg", DialectPostgreSQL, "empty", nil, "TEXT"},

		// Timestamp string
		{"ts_str_mysql", DialectMySQL, "event_time", "2024-01-15T10:30:00Z", "DATETIME(3)"},

		// Soft-delete flag must not become DATETIME (CDC deleted=0/1).
		{"deleted_flag_int_mysql", DialectMySQL, "deleted", 0, "TINYINT(1)"},
		{"deleted_flag_int64_mysql", DialectMySQL, "deleted", int64(0), "TINYINT(1)"},
		{"deleted_flag_float_json_mysql", DialectMySQL, "deleted", float64(0), "TINYINT(1)"},
		{"deleted_at_still_temporal", DialectMySQL, "deleted_at", "2024-01-15 10:30:00", "DATETIME(3)"},
		{"deleted_time_still_temporal", DialectMySQL, "deleted_time", now, "DATETIME(3)"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := InferFromValue(tt.dialect, tt.colName, tt.value)
			if got != tt.expected {
				t.Errorf("InferFromValue(%q, %q, %v) = %q, want %q",
					tt.dialect, tt.colName, tt.value, got, tt.expected)
			}
		})
	}
}

func TestInferFromValues(t *testing.T) {
	// Multiple values — should use name hint or first non-nil value.
	got := InferFromValues(DialectMySQL, "user_id", []any{nil, nil, int64(42)})
	if got != "BIGINT" {
		t.Errorf("expected BIGINT from name hint, got %q", got)
	}

	// All nil → default.
	got2 := InferFromValues(DialectMySQL, "something", []any{nil, nil})
	if got2 != "TEXT" {
		t.Errorf("expected TEXT for all-nil, got %q", got2)
	}
}

func TestIsTimestampString(t *testing.T) {
	tests := []struct {
		s        string
		expected bool
	}{
		{"2024-01-15T10:30:00Z", true},
		{"2024-01-15 10:30:00", true},
		{"2024-01-15", true},
		{"not a date", false},
		{"", false},
	}
	for _, tt := range tests {
		if got := isTimestampString(tt.s); got != tt.expected {
			t.Errorf("isTimestampString(%q) = %v, want %v", tt.s, got, tt.expected)
		}
	}
}

func TestInferFromValueTemporalHintRequiresParseableString(t *testing.T) {
	tests := []struct {
		name    string
		dialect Dialect
		col     string
		val     any
		want    string
	}{
		// Empty / junk strings in temporal name columns:
		// - non-empty junk (e.g. work_time="[1,2,3]", created_at="hello")
		//   must NOT build a DateTime column (would abort every AppendRow).
		// - empty strings keep the hint (all-empty column -> DATETIME,
		//   empty rows coerce to NULL/epoch at write time), mirroring the
		//   numeric empty-string rule.
		{name: "work_time_empty", dialect: DialectClickHouse, col: "work_time", val: "", want: "DateTime64(3)"},
		{name: "work_time_junk", dialect: DialectClickHouse, col: "work_time", val: "[1,2,3]", want: "String"},
		{name: "enabled_time_empty_mysql", dialect: DialectMySQL, col: "enabled_time", val: "", want: "DATETIME(3)"},
		{name: "created_at_not_date", dialect: DialectClickHouse, col: "created_at", val: "hello", want: "String"},
		// Parseable timestamp strings keep the temporal hint.
		{name: "created_at_rfc3339", dialect: DialectClickHouse, col: "created_at", val: "2026-05-31T21:23:39+08:00", want: "DateTime64(3)"},
		{name: "enabled_time_space", dialect: DialectClickHouse, col: "enabled_time", val: "2026-06-22 21:47:02", want: "DateTime64(3)"},
		{name: "date_added_date", dialect: DialectMySQL, col: "date_added", val: "2024-09-12", want: "DATETIME(3)"},
		// Empty string keeps the hint: an all-empty temporal column is
		// still treated as DATETIME (empty rows coerce to NULL/epoch).
		{name: "created_at_empty", dialect: DialectClickHouse, col: "created_at", val: "", want: "DateTime64(3)"},
		// Real time.Time values keep the hint regardless of name.
		{name: "created_at_timetime", dialect: DialectClickHouse, col: "created_at", val: time.Now(), want: "DateTime64(3)"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := InferFromValue(tt.dialect, tt.col, tt.val)
			if got != tt.want {
				t.Fatalf("InferFromValue(%s, %s, %v) = %q, want %q", tt.dialect, tt.col, tt.val, got, tt.want)
			}
		})
	}
}

func TestInferFromValueNumericHintRequiresParseableString(t *testing.T) {
	tests := []struct {
		name    string
		dialect Dialect
		col     string
		val     any
		want    string
	}{
		// hex/uuid text in *_id columns must not force an Int64 column.
		{name: "request_id_hex", dialect: DialectClickHouse, col: "request_id", val: "33b6338a2c78d0c7", want: "String"},
		{name: "request_id_hex_mysql", dialect: DialectMySQL, col: "request_id", val: "33b6338a2c78d0c7", want: "TEXT"},
		{name: "order_id_text", dialect: DialectClickHouse, col: "order_id", val: "ORD-10001", want: "String"},
		{name: "amount_text", dialect: DialectClickHouse, col: "amount", val: "12.5abc", want: "String"},
		// Parseable strings keep the numeric hint.
		{name: "request_id_numeric", dialect: DialectClickHouse, col: "request_id", val: "123456", want: "Int64"},
		{name: "user_id_float_str", dialect: DialectClickHouse, col: "user_id", val: "12.5", want: "Int64"},
		// Empty string keeps the hint (converted to 0 at write time).
		{name: "request_id_empty", dialect: DialectClickHouse, col: "request_id", val: "", want: "Int64"},
		// Non-string values unaffected.
		{name: "request_id_int", dialect: DialectClickHouse, col: "request_id", val: int64(7), want: "Int64"},
		{name: "deleted_flag_str", dialect: DialectClickHouse, col: "deleted", val: "0", want: "UInt8"},
		{name: "deleted_flag_text", dialect: DialectClickHouse, col: "deleted", val: "not-a-flag", want: "String"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := InferFromValue(tt.dialect, tt.col, tt.val)
			if got != tt.want {
				t.Fatalf("InferFromValue(%s, %s, %v) = %q, want %q", tt.dialect, tt.col, tt.val, got, tt.want)
			}
		})
	}
}
