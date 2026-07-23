package transform

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	registry.RegisterTransform("distinct", func(config map[string]any) (core.Transform, error) {
		return NewDistinctTransform(config)
	})
}

// DistinctTransform drops records whose key (selected fields, or full payload)
// has already been observed in this process. This first version is in-memory
// only; StateSnapshotter / Flusher hooks are reserved for a future durable
// backend so checkpoint restarts can restore the seen set.
//
// YAML:
//
//	transforms:
//	  - type: distinct
//	    config:
//	      fields: ["id", "event_type"]
type DistinctTransform struct {
	fields []string

	mu   sync.Mutex
	seen map[string]struct{}

	// Extension points for durable state (reserved, currently unused).
	// Future work can wire a state.Store and implement SnapshotState properly.
	pipeline string
	node     string
}

func NewDistinctTransform(config map[string]any) (*DistinctTransform, error) {
	fields, err := stringSliceConfig(config, "fields")
	if err != nil {
		return nil, fmt.Errorf("distinct: %w", err)
	}
	t := &DistinctTransform{
		fields:   fields,
		seen:     make(map[string]struct{}),
		pipeline: "default",
		node:     "distinct",
	}
	if v, ok := config["state_pipeline"].(string); ok && v != "" {
		t.pipeline = v
	}
	if v, ok := config["state_node"].(string); ok && v != "" {
		t.node = v
	}
	return t, nil
}

func (t *DistinctTransform) Name() string { return "distinct" }

func (t *DistinctTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	_ = ctx
	key := t.keyFor(rec)

	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.seen[key]; ok {
		return rec, core.ErrRecordFiltered
	}
	t.seen[key] = struct{}{}
	return rec, nil
}

func (t *DistinctTransform) keyFor(rec core.Record) string {
	if len(t.fields) == 0 {
		// Full-record distinct: stable key over all fields sorted by name.
		if rec.Data == nil {
			return ""
		}
		names := make([]string, 0, len(rec.Data))
		for k := range rec.Data {
			names = append(names, k)
		}
		sort.Strings(names)
		parts := make([]string, 0, len(names))
		for _, k := range names {
			parts = append(parts, k+"="+fmt.Sprintf("%v", rec.Data[k]))
		}
		return strings.Join(parts, "\x1f")
	}
	parts := make([]string, 0, len(t.fields))
	for _, f := range t.fields {
		if rec.Data == nil {
			parts = append(parts, f+"=")
			continue
		}
		parts = append(parts, f+"="+fmt.Sprintf("%v", rec.Data[f]))
	}
	return strings.Join(parts, "\x1f")
}

// Flush is reserved for a future durable/windowed distinct implementation.
// The process-local version has nothing buffered to emit.
func (t *DistinctTransform) Flush(ctx context.Context) ([]core.Record, error) {
	_ = ctx
	return nil, nil
}

// SnapshotState is reserved for durable distinct state. The process-local
// version reports no snapshot so checkpoint envelopes stay empty for this node.
func (t *DistinctTransform) SnapshotState(ctx context.Context) (node string, version string, ok bool, err error) {
	_ = ctx
	return t.node, "", false, nil
}

// SeenCount exposes the current in-process distinct key count (tests / metrics).
func (t *DistinctTransform) SeenCount() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return len(t.seen)
}
