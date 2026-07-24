package transform

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func TestCastHappyPath(t *testing.T) {
	tr, err := NewCastTransform(map[string]any{
		"casts": map[string]any{
			"price":  "to_float",
			"active": "to_bool",
			"qty":    "to_int",
			"tags":   "to_array",
			"meta":   "to_json",
			"when":   "to_timestamp",
			"day":    "to_date",
			"label":  "to_string",
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	rec := core.Record{Data: map[string]any{
		"price":  "12.5",
		"active": "true",
		"qty":    "7",
		"tags":   "a,b,c",
		"meta":   map[string]any{"k": 1},
		"when":   "2024-02-03T04:05:06Z",
		"day":    "2024-02-03",
		"label":  99,
	}}
	out, err := tr.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if out.Data["price"].(float64) != 12.5 {
		t.Fatalf("price=%v", out.Data["price"])
	}
	if out.Data["active"].(bool) != true {
		t.Fatalf("active=%v", out.Data["active"])
	}
	if out.Data["qty"].(int64) != 7 {
		t.Fatalf("qty=%v", out.Data["qty"])
	}
	tags := out.Data["tags"].([]any)
	if len(tags) != 3 || tags[0] != "a" {
		t.Fatalf("tags=%v", tags)
	}
	if out.Data["meta"].(string) != `{"k":1}` {
		t.Fatalf("meta=%v", out.Data["meta"])
	}
	ts := out.Data["when"].(time.Time)
	if ts.Year() != 2024 || ts.Month() != 2 || ts.Day() != 3 {
		t.Fatalf("when=%v", ts)
	}
	day := out.Data["day"].(time.Time)
	if day.Hour() != 0 || day.Day() != 3 {
		t.Fatalf("day=%v", day)
	}
	if out.Data["label"].(string) != "99" {
		t.Fatalf("label=%v", out.Data["label"])
	}
}

func TestCastOnErrorModes(t *testing.T) {
	// null (default): bad value becomes nil
	tr, err := NewCastTransform(map[string]any{
		"casts": map[string]any{"n": "to_int"},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	out, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"n": "nope"}})
	if err != nil {
		t.Fatalf("null mode err=%v", err)
	}
	if out.Data["n"] != nil {
		t.Fatalf("null mode n=%v", out.Data["n"])
	}

	// skip: drop record
	trSkip, err := NewCastTransform(map[string]any{
		"casts":    map[string]any{"n": "to_int"},
		"on_error": "skip",
	})
	if err != nil {
		t.Fatalf("new skip: %v", err)
	}
	_, err = trSkip.Apply(context.Background(), core.Record{Data: map[string]any{"n": "nope"}})
	if !errors.Is(err, core.ErrRecordFiltered) {
		t.Fatalf("skip mode err=%v", err)
	}

	// fail: return error
	trFail, err := NewCastTransform(map[string]any{
		"casts":    map[string]any{"n": "to_int"},
		"on_error": "fail",
	})
	if err != nil {
		t.Fatalf("new fail: %v", err)
	}
	_, err = trFail.Apply(context.Background(), core.Record{Data: map[string]any{"n": "nope"}})
	if err == nil {
		t.Fatal("fail mode expected error")
	}
}

func TestCastMissingFieldAndRegistry(t *testing.T) {
	tr, err := registry.BuildTransform("cast", map[string]any{
		"casts": map[string]any{"price": "to_float"},
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	out, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"other": 1}})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	if _, ok := out.Data["price"]; ok {
		t.Fatalf("missing field should stay missing: %#v", out.Data)
	}
	if _, err := NewCastTransform(map[string]any{}); err == nil {
		t.Fatal("expected casts required")
	}
	if _, err := NewCastTransform(map[string]any{
		"casts": map[string]any{"x": "to_uuid"},
	}); err == nil {
		t.Fatal("expected unsupported type error")
	}
}
