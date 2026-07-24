package transform

import (
	"context"
	"fmt"
	"math/rand"
	"sync"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("sample", func(config map[string]any) (core.Transform, error) {
		return NewSampleTransform(config)
	})
}

// SampleTransform keeps each record with probability rate (0.0–1.0).
// An optional seed makes sampling deterministic across runs.
//
// YAML:
//
//	transforms:
//	  - type: sample
//	    config:
//	      rate: 0.1
//	      seed: 42
type SampleTransform struct {
	rate float64
	rng  *rand.Rand
	mu   sync.Mutex
}

func NewSampleTransform(config map[string]any) (*SampleTransform, error) {
	raw, ok := config["rate"]
	if !ok {
		return nil, fmt.Errorf("sample: rate is required")
	}
	rate, ok := toFloat64Config(raw)
	if !ok {
		return nil, fmt.Errorf("sample: rate must be a number")
	}
	if rate < 0 || rate > 1 {
		return nil, fmt.Errorf("sample: rate must be between 0.0 and 1.0")
	}

	t := &SampleTransform{rate: rate}
	if v, ok := config["seed"]; ok {
		seed, ok := toInt64Config(v)
		if !ok {
			return nil, fmt.Errorf("sample: seed must be an integer")
		}
		t.rng = rand.New(rand.NewSource(seed))
	} else {
		t.rng = rand.New(rand.NewSource(rand.Int63()))
	}
	return t, nil
}

func toFloat64Config(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int32:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}

func toInt64Config(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int32:
		return int64(n), true
	case int64:
		return n, true
	case float64:
		return int64(n), true
	case float32:
		return int64(n), true
	default:
		return 0, false
	}
}

func (t *SampleTransform) Name() string { return "sample" }

func (t *SampleTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	_ = ctx
	if t.rate <= 0 {
		return rec, core.ErrRecordFiltered
	}
	if t.rate >= 1 {
		return rec, nil
	}
	t.mu.Lock()
	keep := t.rng.Float64() < t.rate
	t.mu.Unlock()
	if !keep {
		return rec, core.ErrRecordFiltered
	}
	return rec, nil
}
