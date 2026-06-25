package sink

import (
	"sync"
	"testing"
)

func TestNextVersionMonotonic(t *testing.T) {
	s := &ClickHouseSink{}
	var last int64
	for i := 0; i < 100_000; i++ {
		v := s.nextVersion()
		if v <= last {
			t.Fatalf("version regression at iteration %d: %d <= %d", i, v, last)
		}
		last = v
	}
}

func TestNextVersionConcurrent(t *testing.T) {
	s := &ClickHouseSink{}
	var wg sync.WaitGroup
	results := make(chan int64, 10000)

	// Launch 10 goroutines, each generating 1000 versions.
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				results <- s.nextVersion()
			}
		}()
	}
	wg.Wait()
	close(results)

	// Verify all values are unique (strictly monotonic across all goroutines).
	seen := make(map[int64]bool)
	for v := range results {
		if seen[v] {
			t.Fatalf("duplicate version: %d", v)
		}
		seen[v] = true
	}
	if len(seen) != 10000 {
		t.Fatalf("expected 10000 unique versions, got %d", len(seen))
	}
}

func TestClickHouseSchemaDriftBoolCompatibility(t *testing.T) {
	enabled, err := NewClickHouseSink(map[string]any{"schema_drift": true})
	if err != nil {
		t.Fatalf("NewClickHouseSink(true): %v", err)
	}
	if enabled.schemaDrift != "add_columns" {
		t.Fatalf("schemaDrift = %q, want add_columns", enabled.schemaDrift)
	}

	disabled, err := NewClickHouseSink(map[string]any{"schema_drift": false})
	if err != nil {
		t.Fatalf("NewClickHouseSink(false): %v", err)
	}
	if disabled.schemaDrift != "ignore" {
		t.Fatalf("schemaDrift = %q, want ignore", disabled.schemaDrift)
	}
}
