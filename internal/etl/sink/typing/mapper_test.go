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
