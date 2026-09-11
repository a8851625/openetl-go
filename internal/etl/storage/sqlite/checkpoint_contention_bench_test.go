package sqlite_test

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

// TestBenchmarkSQLiteCheckpointStoreContention is a direct SQL-store
// microbenchmark. Workers below call SaveCheckpoint in a tight loop; they are
// NOT streaming pipelines and omit source/sink, lifecycle and fencing costs.
// Its results do not satisfy IT-3/T3.6's pipeline-count or queueing acceptance.
//
// The output (one line per concurrency level: p50/p95/max ms and ops/s) is
// useful for investigating this storage primitive in isolation. Actual
// pipeline capacity evidence is produced by hack/bench-capacity.sh.
func TestBenchmarkSQLiteCheckpointStoreContention(t *testing.T) {
	if testing.Short() {
		t.Skip("benchmark curve; run explicitly")
	}
	ctx := context.Background()
	for _, workers := range []int{1, 2, 4, 8, 16, 32} {
		st, err := sqlite.New(filepath.Join(t.TempDir(), fmt.Sprintf("bench-%d.db", workers)))
		if err != nil {
			t.Fatalf("NewSQLite(%d): %v", workers, err)
		}
		func() {
			defer st.Close()
			const opsPerWorker = 200
			latencies := make([]time.Duration, workers*opsPerWorker)
			var wg sync.WaitGroup
			var mu sync.Mutex
			start := time.Now()
			for w := 0; w < workers; w++ {
				wg.Add(1)
				go func(w int) {
					defer wg.Done()
					job := fmt.Sprintf("pipe-%d", w)
					for i := 0; i < opsPerWorker; i++ {
						rec := &storage.CheckpointRecord{
							JobName:   job,
							Source:    "mysql_cdc",
							Position:  []byte(fmt.Sprintf(`{"offset":%d,"worker":%d}`, i, w)),
							Timestamp: time.Now(),
						}
						t0 := time.Now()
						if err := st.SaveCheckpoint(ctx, rec); err != nil {
							t.Errorf("save: %v", err)
							return
						}
						mu.Lock()
						latencies[w*opsPerWorker+i] = time.Since(t0)
						mu.Unlock()
					}
				}(w)
			}
			wg.Wait()
			elapsed := time.Since(start)

			for i := 1; i < len(latencies); i++ {
				for j := i; j > 0 && latencies[j] < latencies[j-1]; j-- {
					latencies[j], latencies[j-1] = latencies[j-1], latencies[j]
				}
			}
			pct := func(p float64) float64 {
				idx := int(float64(len(latencies)-1) * p)
				return float64(latencies[idx].Microseconds()) / 1000.0
			}
			ops := float64(len(latencies)) / elapsed.Seconds()
			t.Logf("curve: concurrency=%d p50=%.2fms p95=%.2fms max=%.2fms ops_per_sec=%.0f",
				workers, pct(0.50), pct(0.95), float64(latencies[len(latencies)-1].Microseconds())/1000.0, ops)
		}()
	}
}
