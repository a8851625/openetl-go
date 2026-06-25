package transform

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/state"
)

// TestDeduplicatorDropsKnownKey verifies basic dedup semantics: the first
// occurrence of a key passes, subsequent occurrences are filtered.
func TestDeduplicatorDropsKnownKey(t *testing.T) {
	dt, err := NewDeduplicatorTransform(map[string]any{
		"keys":        []any{"id"},
		"window_size": 100,
	})
	if err != nil {
		t.Fatalf("NewDeduplicatorTransform: %v", err)
	}

	mk := func(id int) core.Record {
		return core.Record{Data: map[string]any{"id": id}}
	}

	if _, err := dt.Apply(context.Background(), mk(1)); err != nil {
		t.Fatalf("first Apply id=1 err = %v, want nil (pass)", err)
	}
	if _, err := dt.Apply(context.Background(), mk(2)); err != nil {
		t.Fatalf("first Apply id=2 err = %v, want nil (pass)", err)
	}
	if _, err := dt.Apply(context.Background(), mk(1)); err != core.ErrRecordFiltered {
		t.Fatalf("second Apply id=1 err = %v, want ErrRecordFiltered (duplicate)", err)
	}

	metrics := dt.TransformMetrics()
	if metrics.Transform != "deduplicate" {
		t.Fatalf("transform metric name = %q, want deduplicate", metrics.Transform)
	}
	if metrics.Counters["processed"] != 3 || metrics.Counters["passed"] != 2 ||
		metrics.Counters["duplicate_dropped"] != 1 || metrics.Counters["memory_duplicate_dropped"] != 1 {
		t.Fatalf("deduplicate counters = %#v", metrics.Counters)
	}
}

// TestDeduplicatorConcurrentSafe guards TF-6: Apply is invoked concurrently by
// the DAG executor and ParallelRunner. Without the mutex, this test crashes
// under -race with `fatal error: concurrent map read and map write`.
func TestDeduplicatorConcurrentSafe(t *testing.T) {
	dt, err := NewDeduplicatorTransform(map[string]any{
		"keys":        []any{"id"},
		"window_size": 500,
	})
	if err != nil {
		t.Fatalf("NewDeduplicatorTransform: %v", err)
	}

	const goroutines = 16
	const perG = 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(off int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				// Overlapping key ranges across goroutines force cache-map
				// contention and ring-buffer eviction under concurrency.
				rec := core.Record{Data: map[string]any{"id": (off + i) % 300}}
				_, _ = dt.Apply(context.Background(), rec)
			}
		}(g * 7)
	}
	wg.Wait()
}

func TestDeduplicatorStateStoreSurvivesRestart(t *testing.T) {
	store := state.NewMemoryStore()
	ctx := context.Background()

	first, err := NewDeduplicatorTransform(map[string]any{
		"keys":        []any{"id"},
		"window_size": 100,
	})
	if err != nil {
		t.Fatalf("NewDeduplicatorTransform first: %v", err)
	}
	first.WithStateStore(store, "pipe-wide", "dedup-orders", 0)

	rec := core.Record{Data: map[string]any{"id": "o-1"}}
	if _, err := first.Apply(ctx, rec); err != nil {
		t.Fatalf("first Apply err = %v, want nil", err)
	}

	restarted, err := NewDeduplicatorTransform(map[string]any{
		"keys":        []any{"id"},
		"window_size": 100,
	})
	if err != nil {
		t.Fatalf("NewDeduplicatorTransform restarted: %v", err)
	}
	restarted.WithStateStore(store, "pipe-wide", "dedup-orders", 0)

	if _, err := restarted.Apply(ctx, rec); err != core.ErrRecordFiltered {
		t.Fatalf("restarted Apply err = %v, want ErrRecordFiltered", err)
	}

	counters := restarted.TransformMetrics().Counters
	if counters["duplicate_dropped"] != 1 || counters["state_duplicate_dropped"] != 1 {
		t.Fatalf("state-backed duplicate counters = %#v", counters)
	}
}

func TestDeduplicatorMetricsTrackEvictedKeys(t *testing.T) {
	dt, err := NewDeduplicatorTransform(map[string]any{
		"keys":        []any{"id"},
		"window_size": 1,
	})
	if err != nil {
		t.Fatalf("NewDeduplicatorTransform: %v", err)
	}

	if _, err := dt.Apply(context.Background(), core.Record{Data: map[string]any{"id": "a"}}); err != nil {
		t.Fatalf("first Apply err = %v", err)
	}
	if _, err := dt.Apply(context.Background(), core.Record{Data: map[string]any{"id": "b"}}); err != nil {
		t.Fatalf("second Apply err = %v", err)
	}

	counters := dt.TransformMetrics().Counters
	if counters["evicted_keys"] != 1 {
		t.Fatalf("evicted_keys = %d, want 1 (counters=%#v)", counters["evicted_keys"], counters)
	}
}

func TestDeduplicatorStateTTLAllowsKeyAfterExpiry(t *testing.T) {
	store := state.NewMemoryStore()
	ctx := context.Background()

	dt, err := NewDeduplicatorTransform(map[string]any{
		"keys":        []any{"id"},
		"window_size": 1,
	})
	if err != nil {
		t.Fatalf("NewDeduplicatorTransform: %v", err)
	}
	dt.WithStateStore(store, "pipe-wide", "dedup-orders", 20*time.Millisecond)

	rec := core.Record{Data: map[string]any{"id": "o-ttl"}}
	if _, err := dt.Apply(ctx, rec); err != nil {
		t.Fatalf("first Apply err = %v, want nil", err)
	}

	// Evict the original key from the process-local ring so this verifies the
	// shared state TTL rather than only the in-memory cache.
	if _, err := dt.Apply(ctx, core.Record{Data: map[string]any{"id": "other"}}); err != nil {
		t.Fatalf("evicting Apply err = %v, want nil", err)
	}
	if _, err := dt.Apply(ctx, rec); err != core.ErrRecordFiltered {
		t.Fatalf("pre-expiry Apply err = %v, want ErrRecordFiltered", err)
	}

	time.Sleep(30 * time.Millisecond)
	if _, err := dt.Apply(ctx, core.Record{Data: map[string]any{"id": "other-2"}}); err != nil {
		t.Fatalf("second evicting Apply err = %v, want nil", err)
	}
	if _, err := dt.Apply(ctx, rec); err != nil {
		t.Fatalf("post-expiry Apply err = %v, want nil", err)
	}
}
