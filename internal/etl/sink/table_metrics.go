package sink

import (
	"sync"
	"sync/atomic"
	"time"
)

// tableMetricsSet tracks per-target-table write counters so multi-table
// fan-out pipelines can see which table drags a batch (GAP-6; generalized
// from the ClickHouse sink's TableWriteStats).
type tableMetricsSet struct {
	mu sync.RWMutex
	m  map[string]*tableWriteMetrics
}

// tableWriteMetrics tracks per-table write counters.
type tableWriteMetrics struct {
	rowsWritten    int64
	batchesSent    int64
	writeLatencyNs int64
	writeErrors    int64
}

func newTableMetricsSet() *tableMetricsSet {
	return &tableMetricsSet{m: map[string]*tableWriteMetrics{}}
}

// record updates the per-table counters.
func (t *tableMetricsSet) record(table string, rows int, latency time.Duration, failed bool) {
	if table == "" {
		return
	}
	t.mu.Lock()
	m, ok := t.m[table]
	if !ok {
		m = &tableWriteMetrics{}
		t.m[table] = m
	}
	t.mu.Unlock()
	atomic.AddInt64(&m.rowsWritten, int64(rows))
	atomic.AddInt64(&m.batchesSent, 1)
	atomic.AddInt64(&m.writeLatencyNs, latency.Nanoseconds())
	if failed {
		atomic.AddInt64(&m.writeErrors, 1)
	}
}

// TableWriteStats is a snapshot of per-table write metrics.
type TableWriteStats struct {
	Table          string  `json:"table"`
	RowsWritten    int64   `json:"rows_written"`
	BatchesSent    int64   `json:"batches_sent"`
	WriteLatencyMs float64 `json:"write_latency_ms"`
	Errors         int64   `json:"errors"`
}

// snapshot returns sorted-by-table stats.
func (t *tableMetricsSet) snapshot() []TableWriteStats {
	t.mu.RLock()
	defer t.mu.RUnlock()
	out := make([]TableWriteStats, 0, len(t.m))
	for tbl, m := range t.m {
		wl := float64(0)
		if b := atomic.LoadInt64(&m.batchesSent); b > 0 {
			wl = float64(atomic.LoadInt64(&m.writeLatencyNs)) / float64(b) / 1e6
		}
		out = append(out, TableWriteStats{
			Table:          tbl,
			RowsWritten:    atomic.LoadInt64(&m.rowsWritten),
			BatchesSent:    atomic.LoadInt64(&m.batchesSent),
			WriteLatencyMs: wl,
			Errors:         atomic.LoadInt64(&m.writeErrors),
		})
	}
	// deterministic order
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1].Table > out[j].Table; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}

// TableWriteStats returns per-table write counters (multi-table fan-out
// observability; exposed for API/logging consumers).
func (s *MySQLSink) TableWriteStats() []TableWriteStats { return s.tableMetrics.snapshot() }

// TableWriteStats returns per-table write counters for the postgres sink.
func (s *PostgresSink) TableWriteStats() []TableWriteStats { return s.tableMetrics.snapshot() }
