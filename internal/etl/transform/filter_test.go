package transform

import (
	"context"
	"errors"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
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

// TestFilterStrictTypes guards TF-14: with strict_types=false (default) a
// numeric comparison against a non-numeric value silently drops the record
// (returns false); with strict_types=true it returns an error so the pipeline
// can route the record to DLQ instead. Also verifies the eval/compareValues
// refactor still passes normal numeric/string comparisons.
func TestFilterStrictTypes(t *testing.T) {
	ctx := context.Background()

	// Default (non-strict): numeric predicate vs string value → silent drop.
	tr := &FilterTransform{expression: "amount > 100"}
	out, err := tr.Apply(ctx, core.Record{Data: map[string]any{"amount": "not-a-number"}})
	if !errors.Is(err, core.ErrRecordFiltered) {
		t.Fatalf("non-strict numeric/non-numeric: expected ErrRecordFiltered, got out=%v err=%v", out.Data, err)
	}

	// Strict: same input → error (→ DLQ).
	tr = &FilterTransform{expression: "amount > 100", strictTypes: true}
	_, err = tr.Apply(ctx, core.Record{Data: map[string]any{"amount": "not-a-number"}})
	if err == nil || errors.Is(err, core.ErrRecordFiltered) {
		t.Fatalf("strict numeric/non-numeric: expected real error, got %v", err)
	}

	// Sanity: normal numeric match still works under strict.
	tr = &FilterTransform{expression: "amount > 100", strictTypes: true}
	out, err = tr.Apply(ctx, core.Record{Data: map[string]any{"amount": 250}})
	if err != nil || out.Data == nil {
		t.Fatalf("strict numeric match: expected pass, got out=%v err=%v", out.Data, err)
	}

	// Sanity: string equality still works (no numeric coercion).
	tr = &FilterTransform{expression: `status == "paid"`, strictTypes: true}
	out, err = tr.Apply(ctx, core.Record{Data: map[string]any{"status": "paid"}})
	if err != nil {
		t.Fatalf("strict string match: expected pass, got err=%v", err)
	}
}
