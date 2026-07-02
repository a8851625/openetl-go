package transform

import (
	"context"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func TestMapFieldsHitAndDefault(t *testing.T) {
	tr, err := NewMapFieldsTransform(map[string]any{
		"fields": []any{
			map[string]any{
				"field":   "status_code",
				"map":     map[string]any{"1": "ONLINE", "3": "NOT_CHARGING"},
				"default": "UNKNOWN",
			},
		},
	})
	if err != nil {
		t.Fatalf("NewMapFieldsTransform: %v", err)
	}
	cases := []struct {
		in   any
		want any
	}{
		{in: "1", want: "ONLINE"},
		{in: "3", want: "NOT_CHARGING"},
		{in: "9", want: "UNKNOWN"},
		{in: 1, want: "ONLINE"}, // numeric keys are stringified
	}
	for _, c := range cases {
		rec := core.Record{Data: map[string]any{"status_code": c.in}}
		out, err := tr.Apply(context.Background(), rec)
		if err != nil {
			t.Fatalf("Apply: %v", err)
		}
		if got := out.Data["status_code"]; got != c.want {
			t.Fatalf("in=%v got=%v want=%v", c.in, got, c.want)
		}
	}
}

func TestMapFieldsOnMissingKeep(t *testing.T) {
	tr, err := NewMapFieldsTransform(map[string]any{
		"fields": []any{
			map[string]any{
				"field":     "color",
				"map":       map[string]any{"R": "red"},
				"on_missing": "keep",
			},
		},
	})
	if err != nil {
		t.Fatalf("NewMapFieldsTransform: %v", err)
	}
	rec := core.Record{Data: map[string]any{"color": "B"}}
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := out.Data["color"]; got != "B" {
		t.Fatalf("on_missing=keep: got=%v want B", got)
	}
}

func TestMapFieldsOnMissingNull(t *testing.T) {
	tr, err := NewMapFieldsTransform(map[string]any{
		"fields": []any{
			map[string]any{
				"field":     "color",
				"map":       map[string]any{"R": "red"},
				"on_missing": "null",
			},
		},
	})
	if err != nil {
		t.Fatalf("NewMapFieldsTransform: %v", err)
	}
	rec := core.Record{Data: map[string]any{"color": "B"}}
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := out.Data["color"]; got != nil {
		t.Fatalf("on_missing=null: got=%v want nil", got)
	}
}

func TestMapFieldsMultipleFields(t *testing.T) {
	tr, err := NewMapFieldsTransform(map[string]any{
		"fields": []any{
			map[string]any{"field": "a", "map": map[string]any{"1": "x"}},
			map[string]any{"field": "b", "map": map[string]any{"2": "y"}},
		},
	})
	if err != nil {
		t.Fatalf("NewMapFieldsTransform: %v", err)
	}
	rec := core.Record{Data: map[string]any{"a": "1", "b": "2"}}
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Data["a"] != "x" || out.Data["b"] != "y" {
		t.Fatalf("got %#v", out.Data)
	}
}

func TestMapFieldsMissingFieldSkipped(t *testing.T) {
	tr, err := NewMapFieldsTransform(map[string]any{
		"fields": []any{
			map[string]any{"field": "absent", "map": map[string]any{"1": "x"}},
		},
	})
	if err != nil {
		t.Fatalf("NewMapFieldsTransform: %v", err)
	}
	rec := core.Record{Data: map[string]any{"other": "v"}}
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if _, ok := out.Data["absent"]; ok {
		t.Fatalf("absent should not be added")
	}
}

func TestMapFieldsValidateErrors(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]any
	}{
		{"no fields", map[string]any{}},
		{"empty fields", map[string]any{"fields": []any{}}},
		{"missing field name", map[string]any{"fields": []any{map[string]any{"map": map[string]any{"1": "x"}}}}},
		{"missing map", map[string]any{"fields": []any{map[string]any{"field": "x"}}}},
		{"empty map", map[string]any{"fields": []any{map[string]any{"field": "x", "map": map[string]any{}}}}},
		{"invalid on_missing", map[string]any{"fields": []any{map[string]any{"field": "x", "map": map[string]any{"1": "y"}, "on_missing": "delete"}}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := NewMapFieldsTransform(c.config); err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
		})
	}
}

func TestMapFieldsRegistryLookup(t *testing.T) {
	if !registry.HasTransform("map_fields") {
		t.Fatalf("map_fields not registered")
	}
	tr, err := registry.BuildTransform("map_fields", map[string]any{
		"fields": []any{
			map[string]any{"field": "s", "map": map[string]any{"1": "ok"}},
		},
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	rec := core.Record{Data: map[string]any{"s": "1"}}
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Data["s"] != "ok" {
		t.Fatalf("got %v", out.Data["s"])
	}
}
