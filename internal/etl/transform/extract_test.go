package transform

import (
	"context"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func TestExtractRegexHit(t *testing.T) {
	tr, err := NewExtractTransform(map[string]any{
		"rules": []any{
			map[string]any{
				"target":  "vendor",
				"pattern": "^(.+?)-",
				"group":   1,
			},
		},
	})
	if err != nil {
		t.Fatalf("NewExtractTransform: %v", err)
	}
	rec := core.Record{Data: map[string]any{"vendor": "acme-001"}}
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := out.Data["vendor"]; got != "acme" {
		t.Fatalf("got=%v want acme", got)
	}
}

func TestExtractRegexSourceField(t *testing.T) {
	tr, err := NewExtractTransform(map[string]any{
		"rules": []any{
			map[string]any{
				"target":       "vendor",
				"source_field": "material_name",
				"pattern":      "^(.+?)-",
				"group":        1,
			},
		},
	})
	if err != nil {
		t.Fatalf("NewExtractTransform: %v", err)
	}
	rec := core.Record{Data: map[string]any{"material_name": "acme-001"}}
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := out.Data["vendor"]; got != "acme" {
		t.Fatalf("got=%v want acme", got)
	}
}

func TestExtractRegexMissLeavesTargetUntouched(t *testing.T) {
	tr, err := NewExtractTransform(map[string]any{
		"rules": []any{
			map[string]any{
				"target":  "vendor",
				"pattern": "^zzz-",
				"group":   0,
			},
		},
	})
	if err != nil {
		t.Fatalf("NewExtractTransform: %v", err)
	}
	rec := core.Record{Data: map[string]any{"vendor": "acme-001"}}
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := out.Data["vendor"]; got != "acme-001" {
		t.Fatalf("regex miss should leave target untouched, got=%v", got)
	}
}

func TestExtractTemplateConcat(t *testing.T) {
	tr, err := NewExtractTransform(map[string]any{
		"rules": []any{
			map[string]any{
				"target":   "material_no",
				"template": "{{.material_name}}.{{.mes_optional_parts}}",
			},
		},
	})
	if err != nil {
		t.Fatalf("NewExtractTransform: %v", err)
	}
	rec := core.Record{Data: map[string]any{"material_name": "ABC", "mes_optional_parts": "X1"}}
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if got := out.Data["material_no"]; got != "ABC.X1" {
		t.Fatalf("got=%v want ABC.X1", got)
	}
}

func TestExtractMultipleRules(t *testing.T) {
	tr, err := NewExtractTransform(map[string]any{
		"rules": []any{
			map[string]any{"target": "a", "pattern": "^(.)", "group": 1},
			map[string]any{"target": "b", "template": "{{.a}}-{{.a}}"},
		},
	})
	if err != nil {
		t.Fatalf("NewExtractTransform: %v", err)
	}
	rec := core.Record{Data: map[string]any{"a": "xyz"}}
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Data["a"] != "x" {
		t.Fatalf("a=%v want x", out.Data["a"])
	}
	if out.Data["b"] != "x-x" {
		t.Fatalf("b=%v want x-x", out.Data["b"])
	}
}

func TestExtractValidateErrors(t *testing.T) {
	cases := []struct {
		name   string
		config map[string]any
	}{
		{"no rules", map[string]any{}},
		{"empty rules", map[string]any{"rules": []any{}}},
		{"missing target", map[string]any{"rules": []any{map[string]any{"pattern": "x"}}}},
		{"bad regex", map[string]any{"rules": []any{map[string]any{"target": "t", "pattern": "("}}}},
		{"group out of range", map[string]any{"rules": []any{map[string]any{"target": "t", "pattern": "^x$", "group": 5}}}},
		{"no pattern or template", map[string]any{"rules": []any{map[string]any{"target": "t"}}}},
		{"both pattern and template", map[string]any{"rules": []any{map[string]any{"target": "t", "pattern": "x", "template": "{{.t}}"}}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := NewExtractTransform(c.config); err == nil {
				t.Fatalf("expected error for %s", c.name)
			}
		})
	}
}

func TestExtractRegistryLookup(t *testing.T) {
	if !registry.HasTransform("extract") {
		t.Fatalf("extract not registered")
	}
	tr, err := registry.BuildTransform("extract", map[string]any{
		"rules": []any{
			map[string]any{"target": "v", "pattern": "^(.+)", "group": 1},
		},
	})
	if err != nil {
		t.Fatalf("factory: %v", err)
	}
	rec := core.Record{Data: map[string]any{"v": "hello"}}
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Data["v"] != "hello" {
		t.Fatalf("got %v", out.Data["v"])
	}
}
