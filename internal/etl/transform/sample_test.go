package transform

import (
	"context"
	"errors"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func TestSampleRateExtremes(t *testing.T) {
	ctx := context.Background()

	none, err := NewSampleTransform(map[string]any{"rate": 0.0})
	if err != nil {
		t.Fatalf("new 0: %v", err)
	}
	if _, err := none.Apply(ctx, core.Record{Data: map[string]any{"i": 1}}); !errors.Is(err, core.ErrRecordFiltered) {
		t.Fatalf("rate=0: %v", err)
	}

	all, err := NewSampleTransform(map[string]any{"rate": 1.0})
	if err != nil {
		t.Fatalf("new 1: %v", err)
	}
	if _, err := all.Apply(ctx, core.Record{Data: map[string]any{"i": 1}}); err != nil {
		t.Fatalf("rate=1: %v", err)
	}
}

func TestSampleSeedDeterministic(t *testing.T) {
	cfg := map[string]any{"rate": 0.5, "seed": 42}
	a, err := NewSampleTransform(cfg)
	if err != nil {
		t.Fatalf("new a: %v", err)
	}
	b, err := NewSampleTransform(cfg)
	if err != nil {
		t.Fatalf("new b: %v", err)
	}
	ctx := context.Background()
	var patternA, patternB []bool
	for i := 0; i < 32; i++ {
		_, errA := a.Apply(ctx, core.Record{Data: map[string]any{"i": i}})
		_, errB := b.Apply(ctx, core.Record{Data: map[string]any{"i": i}})
		patternA = append(patternA, errA == nil)
		patternB = append(patternB, errB == nil)
		if (errA == nil) != (errB == nil) {
			t.Fatalf("seed mismatch at %d: a=%v b=%v", i, errA, errB)
		}
		if errA != nil && !errors.Is(errA, core.ErrRecordFiltered) {
			t.Fatalf("unexpected err: %v", errA)
		}
	}
	// Sanity: not all kept/dropped at rate 0.5 over 32 draws.
	kept := 0
	for _, k := range patternA {
		if k {
			kept++
		}
	}
	if kept == 0 || kept == 32 {
		t.Fatalf("unexpected keep count %d for rate=0.5 seed=42 pattern=%v", kept, patternA)
	}
	_ = patternB
}

func TestSampleRegistryAndValidation(t *testing.T) {
	if _, err := NewSampleTransform(map[string]any{}); err == nil {
		t.Fatal("expected rate required")
	}
	if _, err := NewSampleTransform(map[string]any{"rate": 1.5}); err == nil {
		t.Fatal("expected rate range error")
	}
	if _, err := NewSampleTransform(map[string]any{"rate": -0.1}); err == nil {
		t.Fatal("expected rate range error")
	}
	tr, err := registry.BuildTransform("sample", map[string]any{"rate": 0.25, "seed": float64(7)})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if tr.Name() != "sample" {
		t.Fatalf("name=%q", tr.Name())
	}
}
