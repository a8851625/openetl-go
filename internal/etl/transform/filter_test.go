package transform

import (
	"context"
	"errors"
	"testing"

	"openetl-go/internal/etl/core"
)

func applyFilter(t *testing.T, expr string, data map[string]any) (core.Record, bool) {
	t.Helper()
	tr := &FilterTransform{expression: expr}
	out, err := tr.Apply(context.Background(), core.Record{Operation: core.OpInsert, Data: data})
	if errors.Is(err, core.ErrRecordFiltered) {
		return out, false
	}
	if err != nil {
		t.Fatalf("filter apply: %v", err)
	}
	return out, true
}

func TestFilterFieldPresence(t *testing.T) {
	cases := []struct {
		name string
		expr string
		data map[string]any
		want bool
	}{
		{"present", "name", map[string]any{"name": "alice"}, true},
		{"missing", "name", map[string]any{"other": "x"}, false},
		{"nil_val", "name", map[string]any{"name": nil}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, pass := applyFilter(t, tc.expr, tc.data)
			if pass != tc.want {
				t.Errorf("got pass=%v, want %v", pass, tc.want)
			}
		})
	}
}

func TestFilterNotField(t *testing.T) {
	// !deleted_at means "keep records that DON'T have deleted_at"
	_, pass := applyFilter(t, "!deleted_at", map[string]any{"name": "x"})
	if !pass {
		t.Error("expected record without deleted_at to pass")
	}
	_, pass = applyFilter(t, "!deleted_at", map[string]any{"deleted_at": "2024-01-01"})
	if pass {
		t.Error("expected record with deleted_at to be filtered out")
	}
}

func TestFilterFieldIsNil(t *testing.T) {
	// "field nil" should be equivalent to !field (field is null).
	_, pass := applyFilter(t, "deleted_at nil", map[string]any{})
	if !pass {
		t.Error("expected record without deleted_at to pass 'deleted_at nil' filter")
	}
	_, pass = applyFilter(t, "deleted_at nil", map[string]any{"deleted_at": "x"})
	if pass {
		t.Error("expected record with deleted_at to fail 'deleted_at nil' filter")
	}
}

func TestFilterEqString(t *testing.T) {
	cases := []struct {
		name string
		expr string
		data map[string]any
		want bool
	}{
		{"match", "status == 'paid'", map[string]any{"status": "paid"}, true},
		{"no_match", "status == 'paid'", map[string]any{"status": "unpaid"}, false},
		{"missing", "status == 'paid'", map[string]any{}, false},
		{"ne_match", "status != 'paid'", map[string]any{"status": "unpaid"}, true},
		{"ne_nomatch", "status != 'paid'", map[string]any{"status": "paid"}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, pass := applyFilter(t, tc.expr, tc.data)
			if pass != tc.want {
				t.Errorf("got pass=%v, want %v", pass, tc.want)
			}
		})
	}
}

func TestFilterNumericComparison(t *testing.T) {
	cases := []struct {
		name string
		expr string
		data map[string]any
		want bool
	}{
		{"gt_pass", "amount > 100", map[string]any{"amount": 150.0}, true},
		{"gt_fail", "amount > 100", map[string]any{"amount": 50.0}, false},
		{"ge_eq", "amount >= 100", map[string]any{"amount": 100.0}, true},
		{"le_eq", "amount <= 100", map[string]any{"amount": 100.0}, true},
		{"lt_fail", "amount < 100", map[string]any{"amount": 100.0}, false},
		{"int_val", "amount > 100", map[string]any{"amount": 200}, true},
		{"string_num", "amount > 100", map[string]any{"amount": "200"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, pass := applyFilter(t, tc.expr, tc.data)
			if pass != tc.want {
				t.Errorf("got pass=%v, want %v", pass, tc.want)
			}
		})
	}
}

func TestFilterAnd(t *testing.T) {
	cases := []struct {
		name string
		expr string
		data map[string]any
		want bool
	}{
		{"both_true", "status == 'paid' && amount > 100", map[string]any{"status": "paid", "amount": 150.0}, true},
		{"one_false", "status == 'paid' && amount > 100", map[string]any{"status": "paid", "amount": 50.0}, false},
		{"both_false", "status == 'paid' && amount > 100", map[string]any{"status": "unpaid", "amount": 50.0}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, pass := applyFilter(t, tc.expr, tc.data)
			if pass != tc.want {
				t.Errorf("got pass=%v, want %v", pass, tc.want)
			}
		})
	}
}

func TestFilterOr(t *testing.T) {
	_, pass := applyFilter(t, "status == 'paid' || status == 'pending'", map[string]any{"status": "pending"})
	if !pass {
		t.Error("expected pass")
	}
	_, pass = applyFilter(t, "status == 'paid' || status == 'pending'", map[string]any{"status": "unpaid"})
	if pass {
		t.Error("expected filter")
	}
}

func TestFilterParens(t *testing.T) {
	// (a == 1 || b == 2) && c == 3
	rec := map[string]any{"a": 1.0, "c": 3.0}
	_, pass := applyFilter(t, "(a == 1 || b == 2) && c == 3", rec)
	if !pass {
		t.Error("expected pass")
	}
}

func TestFilterCompileError(t *testing.T) {
	cases := []string{
		"status =",   // missing value
		"> 5",        // missing field
		"status == ", // trailing operator
		"(unclosed",  // unbalanced parens
		"status ==",  // missing value
	}
	for _, expr := range cases {
		t.Run(expr, func(t *testing.T) {
			tr := &FilterTransform{expression: expr}
			_, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{}})
			if err == nil {
				t.Errorf("expected compile error for %q, got nil", expr)
			}
		})
	}
}

func TestFilterEmptyExpression(t *testing.T) {
	tr := &FilterTransform{expression: ""}
	_, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"x": 1}})
	if err != nil {
		t.Errorf("empty expression should pass through, got error: %v", err)
	}
}

// Backwards-compat: the original filter supported "deleted_at" + "nil" pattern.
// Verify the new implementation still handles it.
func TestFilterBackwardsCompatDeletedAt(t *testing.T) {
	_, pass := applyFilter(t, "!deleted_at", map[string]any{"name": "x"})
	if !pass {
		t.Error("backwards-compat: !deleted_at should pass for record without deleted_at")
	}
}
