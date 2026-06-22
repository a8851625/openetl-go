package transform

import (
	"context"
	"sync"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
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
