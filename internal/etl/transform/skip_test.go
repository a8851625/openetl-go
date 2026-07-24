package transform

import (
	"context"
	"errors"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func TestSkipDropsThenPasses(t *testing.T) {
	tr, err := NewSkipTransform(map[string]any{"count": 2})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := tr.Apply(ctx, core.Record{Data: map[string]any{"i": i}}); !errors.Is(err, core.ErrRecordFiltered) {
			t.Fatalf("skip %d: %v", i, err)
		}
	}
	out, err := tr.Apply(ctx, core.Record{Data: map[string]any{"i": 2}})
	if err != nil {
		t.Fatalf("pass: %v", err)
	}
	if out.Data["i"] != 2 {
		t.Fatalf("out=%v", out.Data)
	}
	if tr.Skipped() != 2 {
		t.Fatalf("skipped=%d", tr.Skipped())
	}
}

func TestSkipZeroPassesAll(t *testing.T) {
	tr, err := NewSkipTransform(map[string]any{"count": 0})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"i": 1}}); err != nil {
		t.Fatalf("pass: %v", err)
	}
}

func TestSkipRegistryAndValidation(t *testing.T) {
	if _, err := NewSkipTransform(map[string]any{}); err == nil {
		t.Fatal("expected count required")
	}
	if _, err := NewSkipTransform(map[string]any{"count": -3}); err == nil {
		t.Fatal("expected count >= 0")
	}
	tr, err := registry.BuildTransform("skip", map[string]any{"count": float64(1)})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if tr.Name() != "skip" {
		t.Fatalf("name=%q", tr.Name())
	}
}
