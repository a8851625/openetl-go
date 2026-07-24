package transform

import (
	"context"
	"fmt"
	"sync"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("limit", func(config map[string]any) (core.Transform, error) {
		return NewLimitTransform(config)
	})
}

// LimitTransform passes at most count records and filters the rest.
// Useful for debugging or sampling the head of a stream.
//
// YAML:
//
//	transforms:
//	  - type: limit
//	    config:
//	      count: 1000
type LimitTransform struct {
	count int

	mu      sync.Mutex
	passed  int
}

func NewLimitTransform(config map[string]any) (*LimitTransform, error) {
	raw, ok := config["count"]
	if !ok {
		return nil, fmt.Errorf("limit: count is required")
	}
	n, ok := toInt(raw)
	if !ok {
		return nil, fmt.Errorf("limit: count must be an integer")
	}
	if n < 0 {
		return nil, fmt.Errorf("limit: count must be >= 0")
	}
	return &LimitTransform{count: n}, nil
}

func (t *LimitTransform) Name() string { return "limit" }

func (t *LimitTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	_ = ctx
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.passed >= t.count {
		return rec, core.ErrRecordFiltered
	}
	t.passed++
	return rec, nil
}

// Passed returns how many records have been allowed through (tests).
func (t *LimitTransform) Passed() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.passed
}
