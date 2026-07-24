package transform

import (
	"context"
	"fmt"
	"sync"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("skip", func(config map[string]any) (core.Transform, error) {
		return NewSkipTransform(config)
	})
}

// SkipTransform discards the first count records and passes the rest.
//
// YAML:
//
//	transforms:
//	  - type: skip
//	    config:
//	      count: 10
type SkipTransform struct {
	count int

	mu      sync.Mutex
	skipped int
}

func NewSkipTransform(config map[string]any) (*SkipTransform, error) {
	raw, ok := config["count"]
	if !ok {
		return nil, fmt.Errorf("skip: count is required")
	}
	n, ok := toInt(raw)
	if !ok {
		return nil, fmt.Errorf("skip: count must be an integer")
	}
	if n < 0 {
		return nil, fmt.Errorf("skip: count must be >= 0")
	}
	return &SkipTransform{count: n}, nil
}

func (t *SkipTransform) Name() string { return "skip" }

func (t *SkipTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.skipped < t.count {
		t.skipped++
		return rec, core.ErrRecordFiltered
	}
	return rec, nil
}

// Skipped returns how many records have been discarded so far (tests).
func (t *SkipTransform) Skipped() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.skipped
}
