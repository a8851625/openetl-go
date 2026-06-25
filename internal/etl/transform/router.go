package transform

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
	"github.com/a8851625/openetl-go/internal/etl/state"
)

func init() {
	registry.RegisterTransform("router", func(config map[string]any) (core.Transform, error) {
		return NewRouterTransform(config)
	})
	registry.RegisterTransform("fanout", func(config map[string]any) (core.Transform, error) {
		return &FanoutTransform{}, nil
	})
}

// ════════════════════════════════════════════════════════════════════
// Router — Field-value-based conditional routing
// ════════════════════════════════════════════════════════════════════

// RouterTransform evaluates routing rules and sets a routing tag on the
// record's metadata. The DAG executor's edge conditions then route the
// record to the appropriate downstream node based on this tag.
//
// This is a "soft router" — it doesn't split the stream itself, but marks
// each record with a route destination that downstream edge conditions
// can match on.
//
// Config:
//
//	field: "region"              // field to evaluate
//	routes:
//	  cn: "china_sink"           // if field=="cn", route to china_sink
//	  us: "america_sink"
//	  eu: "europe_sink"
//	default: "fallback_sink"     // if no rule matches
type RouterTransform struct {
	field        string
	routes       map[string]string // field value → route tag
	defaultRoute string
}

func NewRouterTransform(config map[string]any) (*RouterTransform, error) {
	t := &RouterTransform{
		routes: make(map[string]string),
	}
	if v, ok := config["field"].(string); ok {
		t.field = v
	}
	if v, ok := config["default"].(string); ok {
		t.defaultRoute = v
	}
	if rawRoutes, ok := config["routes"].(map[string]any); ok {
		for k, v := range rawRoutes {
			t.routes[k] = fmt.Sprintf("%v", v)
		}
	}
	if t.field == "" {
		return nil, fmt.Errorf("router: field is required")
	}
	return t, nil
}

func (t *RouterTransform) Name() string { return "router" }

func (t *RouterTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	if rec.Data == nil {
		return rec, nil
	}

	val, ok := rec.Data[t.field]
	if !ok {
		if t.defaultRoute != "" {
			rec.Metadata.Route = t.defaultRoute // dedicated route field, preserves Source provenance (TF-5)
		}
		return rec, nil
	}

	valStr := fmt.Sprintf("%v", val)
	if route, found := t.routes[valStr]; found {
		rec.Metadata.Route = route
	} else if t.defaultRoute != "" {
		rec.Metadata.Route = t.defaultRoute
	}

	return rec, nil
}

// ════════════════════════════════════════════════════════════════════
// Fanout — 1-to-N broadcast marker (no-op transform)
// ════════════════════════════════════════════════════════════════════

// FanoutTransform is a no-op marker node. In the DAG executor, a fanout
// node with multiple outgoing edges automatically clones records to ALL
// downstream nodes. This transform itself does nothing — it's the DAG
// topology (multiple edges from this node) that creates the fan-out effect.
//
// Usage in YAML:
//
//	nodes:
//	  - id: broadcast
//	    kind: fanout
//	    plugin: fanout
//	edges:
//	  - from: broadcast, to: clickhouse_sink
//	  - from: broadcast, to: mysql_sink
//	  - from: broadcast, to: kafka_sink
type FanoutTransform struct{}

func (f *FanoutTransform) Name() string { return "fanout" }

func (f *FanoutTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	return rec, nil // pass-through; DAG executor handles cloning
}

// ════════════════════════════════════════════════════════════════════
// Deduplicator — Remove duplicate records by key
// ════════════════════════════════════════════════════════════════════

// DeduplicatorTransform removes duplicate records based on a composite key.
// Uses a bounded LRU cache to track seen keys within a window.
//
// Config:
//
//	keys: ["order_id", "product_id"]   // composite dedup key
//	window_size: 10000                  // cache size (records)
type DeduplicatorTransform struct {
	keys       []string
	windowSize int
	cache      []string // ring buffer of seen keys
	cacheMap   map[string]bool
	pos        int
	store      state.Store
	stateOwner bool
	pipeline   string
	node       string
	ttl        time.Duration

	processed              int64
	passed                 int64
	duplicateDropped       int64
	memoryDuplicateDropped int64
	stateDuplicateDropped  int64
	evictedKeys            int64

	// mu guards cache/cacheMap/pos. Apply is invoked concurrently by the DAG
	// executor and ParallelRunner (parallel.go); an unlocked map is a fatal
	// `concurrent map read and map write` (TF-6). Sibling join.go uses an
	// RWMutex for the same reason.
	mu sync.Mutex
}

// dedupKeySep is the separator used when building composite keys from
// multiple record fields. It uses a unit separator (\x1f) which is
// extremely unlikely to appear in field values, preventing key
// collisions that the previous "|" separator could cause.
const dedupKeySep = "\x1f"

func init() {
	registry.RegisterTransform("deduplicate", func(config map[string]any) (core.Transform, error) {
		return NewDeduplicatorTransform(config)
	})
}

// toInt coerces int-like config values (int or float64, the latter being
// what JSON decoding produces) into an int. JSON configs decode numbers
// as float64, which would panic under a bare .(int) assertion.
func toInt(v any) (int, bool) {
	switch n := v.(type) {
	case int:
		return n, true
	case int32:
		return int(n), true
	case int64:
		return int(n), true
	case float64:
		return int(n), true
	case float32:
		return int(n), true
	default:
		return 0, false
	}
}

func NewDeduplicatorTransform(config map[string]any) (*DeduplicatorTransform, error) {
	t := &DeduplicatorTransform{
		windowSize: 10000,
		cacheMap:   make(map[string]bool),
		cache:      make([]string, 0, 10000),
		pipeline:   "default",
		node:       "deduplicate",
	}
	if rawKeys, ok := config["keys"].([]any); ok {
		for _, k := range rawKeys {
			if ks, ok := k.(string); ok {
				t.keys = append(t.keys, ks)
			}
		}
	}
	// window_size may arrive as int (YAML/Go consumers) or float64
	// (JSON configs). Handle both rather than asserting a single type.
	if v, ok := config["window_size"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			t.windowSize = n
			t.cache = make([]string, 0, n)
		}
	}
	if v, ok := config["state_pipeline"].(string); ok && v != "" {
		t.pipeline = v
	}
	if v, ok := config["state_node"].(string); ok && v != "" {
		t.node = v
	}
	if v, ok := config["state_ttl_seconds"]; ok {
		if n, ok := toInt(v); ok && n > 0 {
			t.ttl = time.Duration(n) * time.Second
		}
	}
	if backend, ok := config["state_backend"].(string); ok && backend != "" {
		switch strings.ToLower(backend) {
		case "sqlite":
			path, _ := config["state_path"].(string)
			if path == "" {
				path = "./data/etl-state.db"
			}
			store, err := state.NewSQLiteStore(path)
			if err != nil {
				return nil, fmt.Errorf("deduplicate: open state store: %w", err)
			}
			t.store = store
			t.stateOwner = true
		default:
			return nil, fmt.Errorf("deduplicate: unsupported state_backend %q", backend)
		}
	}
	if len(t.keys) == 0 {
		if t.stateOwner && t.store != nil {
			_ = t.store.Close()
		}
		return nil, fmt.Errorf("deduplicate: keys is required")
	}
	return t, nil
}

func (t *DeduplicatorTransform) Name() string { return "deduplicate" }

// WithStateStore wires a shared state backend into the deduplicator. It is used
// by tests today and is the narrow injection point for future runner-managed
// stateful transform wiring.
func (t *DeduplicatorTransform) WithStateStore(store state.Store, pipeline, node string, ttl time.Duration) *DeduplicatorTransform {
	t.store = store
	t.stateOwner = false
	if pipeline != "" {
		t.pipeline = pipeline
	}
	if node != "" {
		t.node = node
	}
	t.ttl = ttl
	return t
}

func (t *DeduplicatorTransform) Apply(ctx context.Context, rec core.Record) (core.Record, error) {
	atomic.AddInt64(&t.processed, 1)
	if rec.Data == nil {
		atomic.AddInt64(&t.passed, 1)
		return rec, nil
	}

	// Build composite key.
	var parts []string
	for _, k := range t.keys {
		parts = append(parts, fmt.Sprintf("%v", rec.Data[k]))
	}
	compositeKey := strings.Join(parts, dedupKeySep)

	// Check-and-update the seen-key cache atomically. Apply runs concurrently
	// across goroutines (DAG executor, ParallelRunner); without this lock the
	// map operations race and crash (TF-6).
	t.mu.Lock()
	defer t.mu.Unlock()

	// Check if seen.
	if t.cacheMap[compositeKey] {
		atomic.AddInt64(&t.duplicateDropped, 1)
		atomic.AddInt64(&t.memoryDuplicateDropped, 1)
		return rec, core.ErrRecordFiltered // drop duplicate
	}
	if t.store != nil {
		if _, ok, err := t.store.Get(ctx, t.pipeline, t.node, compositeKey); err != nil {
			return rec, fmt.Errorf("deduplicate: get state: %w", err)
		} else if ok {
			if t.addKeyLocked(compositeKey) {
				atomic.AddInt64(&t.evictedKeys, 1)
			}
			atomic.AddInt64(&t.duplicateDropped, 1)
			atomic.AddInt64(&t.stateDuplicateDropped, 1)
			return rec, core.ErrRecordFiltered
		}
		if err := t.store.Set(ctx, t.pipeline, t.node, compositeKey, []byte("1"), t.ttl); err != nil {
			return rec, fmt.Errorf("deduplicate: set state: %w", err)
		}
	}

	// Add to cache.
	if t.addKeyLocked(compositeKey) {
		atomic.AddInt64(&t.evictedKeys, 1)
	}
	atomic.AddInt64(&t.passed, 1)
	return rec, nil
}

func (t *DeduplicatorTransform) addKeyLocked(compositeKey string) bool {
	t.cacheMap[compositeKey] = true
	if len(t.cache) < t.windowSize {
		t.cache = append(t.cache, compositeKey)
		return false
	}
	// Evict oldest entry (ring buffer).
	old := t.cache[t.pos]
	delete(t.cacheMap, old)
	t.cache[t.pos] = compositeKey
	t.pos = (t.pos + 1) % t.windowSize
	return true
}

func (t *DeduplicatorTransform) Close() error {
	if t.stateOwner && t.store != nil {
		return t.store.Close()
	}
	return nil
}

func (t *DeduplicatorTransform) SnapshotState(ctx context.Context) (string, string, bool, error) {
	if t.store == nil {
		return "", "", false, nil
	}
	snap, err := t.store.Snapshot(ctx, t.pipeline, t.node)
	if err != nil {
		return t.node, "", false, fmt.Errorf("deduplicate: snapshot state: %w", err)
	}
	if snap == nil || len(snap.Entries) == 0 {
		return t.node, "", false, nil
	}
	return t.node, snap.Version, true, nil
}

func (t *DeduplicatorTransform) StateMetrics(ctx context.Context) (core.StateMetrics, bool, error) {
	return stateMetrics(ctx, t.store, t.pipeline, t.node, "deduplicate")
}

func (t *DeduplicatorTransform) TransformMetrics() core.TransformMetrics {
	return core.TransformMetrics{
		Node:      t.node,
		Transform: t.Name(),
		Counters: map[string]int64{
			"processed":                atomic.LoadInt64(&t.processed),
			"passed":                   atomic.LoadInt64(&t.passed),
			"duplicate_dropped":        atomic.LoadInt64(&t.duplicateDropped),
			"memory_duplicate_dropped": atomic.LoadInt64(&t.memoryDuplicateDropped),
			"state_duplicate_dropped":  atomic.LoadInt64(&t.stateDuplicateDropped),
			"evicted_keys":             atomic.LoadInt64(&t.evictedKeys),
		},
	}
}
