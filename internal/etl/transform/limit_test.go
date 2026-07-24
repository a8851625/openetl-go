package transform

import (
	"context"
	"errors"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func TestLimitPassesThenDrops(t *testing.T) {
	tr, err := NewLimitTransform(map[string]any{"count": 2})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if _, err := tr.Apply(ctx, core.Record{Data: map[string]any{"i": i}}); err != nil {
			t.Fatalf("pass %d: %v", i, err)
		}
	}
	if _, err := tr.Apply(ctx, core.Record{Data: map[string]any{"i": 2}}); !errors.Is(err, core.ErrRecordFiltered) {
		t.Fatalf("third: %v", err)
	}
	if tr.Passed() != 2 {
		t.Fatalf("passed=%d", tr.Passed())
	}
}

func TestLimitZeroAndFloatConfig(t *testing.T) {
	tr, err := NewLimitTransform(map[string]any{"count": float64(0)})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if _, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"i": 1}}); !errors.Is(err, core.ErrRecordFiltered) {
		t.Fatalf("zero limit: %v", err)
	}
}

func TestLimitRegistryAndValidation(t *testing.T) {
	if _, err := NewLimitTransform(map[string]any{}); err == nil {
		t.Fatal("expected count required")
	}
	if _, err := NewLimitTransform(map[string]any{"count": -1}); err == nil {
		t.Fatal("expected count >= 0")
	}
	tr, err := registry.BuildTransform("limit", map[string]any{"count": 1})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if tr.Name() != "limit" {
		t.Fatalf("name=%q", tr.Name())
	}
}
