package orchestrator

import (
	"testing"

	"openetl-go/internal/etl/core"
)

func TestEvalCondition(t *testing.T) {
	rec := core.Record{
		Data: map[string]any{
			"name":    "Alice",
			"age":     30,
			"score":   95.5,
			"balance": int64(1000),
			"status":  "active",
			"empty":   nil,
		},
	}

	tests := []struct {
		name     string
		cond     Condition
		expected bool
	}{
		// Eq
		{"eq_match", Condition{Field: "name", Operator: OpEq, Value: "Alice"}, true},
		{"eq_no_match", Condition{Field: "name", Operator: OpEq, Value: "Bob"}, false},
		{"eq_numeric", Condition{Field: "age", Operator: OpEq, Value: 30}, true},

		// Ne
		{"ne_match", Condition{Field: "name", Operator: OpNe, Value: "Bob"}, true},
		{"ne_no_match", Condition{Field: "name", Operator: OpNe, Value: "Alice"}, false},

		// Contains
		{"contains_match", Condition{Field: "name", Operator: OpContains, Value: "Ali"}, true},
		{"contains_no_match", Condition{Field: "name", Operator: OpContains, Value: "Bob"}, false},

		// Gt — numeric
		{"gt_match_float", Condition{Field: "score", Operator: OpGt, Value: 90.0}, true},
		{"gt_no_match_float", Condition{Field: "score", Operator: OpGt, Value: 100.0}, false},
		{"gt_match_int", Condition{Field: "age", Operator: OpGt, Value: 25}, true},
		{"gt_no_match_int", Condition{Field: "age", Operator: OpGt, Value: 35}, false},
		{"gt_match_int64", Condition{Field: "balance", Operator: OpGt, Value: int64(500)}, true},

		// Lt
		{"lt_match", Condition{Field: "age", Operator: OpLt, Value: 35}, true},
		{"lt_no_match", Condition{Field: "age", Operator: OpLt, Value: 25}, false},

		// Ge
		{"ge_equal", Condition{Field: "age", Operator: OpGe, Value: 30}, true},
		{"ge_greater", Condition{Field: "age", Operator: OpGe, Value: 25}, true},
		{"ge_no_match", Condition{Field: "age", Operator: OpGe, Value: 35}, false},

		// Le
		{"le_equal", Condition{Field: "age", Operator: OpLe, Value: 30}, true},
		{"le_less", Condition{Field: "age", Operator: OpLe, Value: 35}, true},
		{"le_no_match", Condition{Field: "age", Operator: OpLe, Value: 25}, false},

		// Regex
		{"regex_match", Condition{Field: "name", Operator: OpRegex, Value: "^A.*e$"}, true},
		{"regex_no_match", Condition{Field: "name", Operator: OpRegex, Value: "^B.*"}, false},
		{"regex_numeric_field", Condition{Field: "age", Operator: OpRegex, Value: "3[0-9]"}, true},

		// Missing field
		{"missing_field", Condition{Field: "nonexistent", Operator: OpEq, Value: "x"}, false},

		// Unknown operator — should return false (with warning log)
		{"unknown_op", Condition{Field: "name", Operator: ConditionOp("unknown"), Value: "x"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := evalCondition(tt.cond, rec)
			if got != tt.expected {
				t.Errorf("evalCondition(%s %s %v) = %v, want %v",
					tt.cond.Field, tt.cond.Operator, tt.cond.Value, got, tt.expected)
			}
		})
	}
}

func TestCompareNumeric(t *testing.T) {
	tests := []struct {
		name     string
		a, b     interface{}
		expected int
	}{
		{"float_lt", 1.5, 2.5, -1},
		{"float_gt", 3.0, 2.0, 1},
		{"float_eq", 2.0, 2.0, 0},
		{"int_vs_float", 3, 2.5, 1},
		{"int64_vs_int", int64(100), 50, 1},
		{"uint_vs_int", uint(50), 100, -1},
		{"string_fallback_lt", "apple", "banana", -1},
		{"string_fallback_gt", "zoo", "aardvark", 1},
		{"string_fallback_eq", "same", "same", 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := compareNumeric(tt.a, tt.b)
			if got != tt.expected {
				t.Errorf("compareNumeric(%v, %v) = %d, want %d", tt.a, tt.b, got, tt.expected)
			}
		})
	}
}

func TestToFloat64(t *testing.T) {
	tests := []struct {
		input  interface{}
		wantF  float64
		wantOk bool
	}{
		{float64(3.14), 3.14, true},
		{float32(2.5), 2.5, true},
		{int(42), 42, true},
		{int8(8), 8, true},
		{int16(16), 16, true},
		{int32(32), 32, true},
		{int64(64), 64, true},
		{uint(100), 100, true},
		{uint8(8), 8, true},
		{uint16(16), 16, true},
		{uint32(32), 32, true},
		{uint64(64), 64, true},
		{"not a number", 0, false},
		{nil, 0, false},
		{true, 0, false},
	}
	for _, tt := range tests {
		got, ok := toFloat64(tt.input)
		if ok != tt.wantOk {
			t.Errorf("toFloat64(%v) ok=%v, want %v", tt.input, ok, tt.wantOk)
		}
		if ok && got != tt.wantF {
			t.Errorf("toFloat64(%v) = %v, want %v", tt.input, got, tt.wantF)
		}
	}
}

func TestMatchRegex(t *testing.T) {
	tests := []struct {
		val, pattern interface{}
		expected     bool
	}{
		{"hello world", "^hello", true},
		{"hello world", "world$", true},
		{"hello", "^H.*o$", false}, // case-sensitive
		{"123", `\d+`, true},
		{"abc", `\d+`, false},
		{"Alice", `(?i)^alice$`, true}, // case-insensitive flag
	}
	for _, tt := range tests {
		got := matchRegex(tt.val, tt.pattern)
		if got != tt.expected {
			t.Errorf("matchRegex(%v, %v) = %v, want %v", tt.val, tt.pattern, got, tt.expected)
		}
	}

	// Invalid regex pattern should return false
	if matchRegex("test", `[invalid`) != false {
		t.Errorf("matchRegex with invalid pattern should return false")
	}
}
