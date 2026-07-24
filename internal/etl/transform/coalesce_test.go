package transform

import (
	"context"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func TestCoalesceFirstNonEmpty(t *testing.T) {
	tr, err := NewCoalesceTransform(map[string]any{
		"fields":       []interface{}{"nickname", "username", "email"},
		"target_field": "display_name",
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	out, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{
		"nickname": "",
		"username": "ada",
		"email":    "ada@example.com",
	}})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out.Data["display_name"] != "ada" {
		t.Fatalf("display_name=%v", out.Data["display_name"])
	}
}

func TestCoalesceSkipsNilAndZero(t *testing.T) {
	tr, err := NewCoalesceTransform(map[string]any{
		"fields":       []interface{}{"a", "b", "c"},
		"target_field": "out",
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	out, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{
		"a": nil,
		"b": 0,
		"c": "ok",
	}})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out.Data["out"] != "ok" {
		t.Fatalf("out=%v", out.Data["out"])
	}

	// All empty → target not set.
	out2, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"a": "", "b": 0}})
	if err != nil {
		t.Fatalf("apply2: %v", err)
	}
	if _, ok := out2.Data["out"]; ok {
		t.Fatalf("expected no target, got %#v", out2.Data)
	}
}

func TestCoalesceConfigAndRegistry(t *testing.T) {
	if _, err := NewCoalesceTransform(map[string]any{
		"fields": []interface{}{"a"},
	}); err == nil {
		t.Fatal("expected target_field required")
	}
	if _, err := NewCoalesceTransform(map[string]any{
		"target_field": "out",
	}); err == nil {
		t.Fatal("expected fields required")
	}
	tr, err := registry.BuildTransform("coalesce", map[string]any{
		"fields":       []interface{}{"x"},
		"target_field": "y",
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	out, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"x": "v"}})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out.Data["y"] != "v" {
		t.Fatalf("y=%v", out.Data["y"])
	}
}
