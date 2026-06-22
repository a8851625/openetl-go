package sink

import (
	"sync/atomic"
	"time"

	"openetl-go/internal/etl/core"
)

// sinkCounters tracks per-sink write metrics. Sinks embed it and call
// recordMetrics after a successful Write; SinkMetrics() is provided by calling
// metricsFor(name). This lets every sink expose SinkMetricsProvider (P4-20,
// SK-4) without duplicating the counter plumbing.
type sinkCounters struct {
	rowsWritten    int64
	batchesSent    int64
	writeLatencyNs int64
	writeErrors    int64
}

// recordMetrics updates write counters after a batch. rows=len(records written),
// latency=wall time of the write. Safe for concurrent use.
func (c *sinkCounters) recordMetrics(rows int, latency time.Duration) {
	atomic.AddInt64(&c.rowsWritten, int64(rows))
	atomic.AddInt64(&c.batchesSent, 1)
	atomic.AddInt64(&c.writeLatencyNs, latency.Nanoseconds())
}

// recordError increments the error counter (call on write failure).
func (c *sinkCounters) recordError() { atomic.AddInt64(&c.writeErrors, 1) }

// metricsFor returns a snapshot of the counters as core.SinkMetrics.
func (c *sinkCounters) metricsFor(name string) core.SinkMetrics {
	wl := float64(0)
	if b := atomic.LoadInt64(&c.batchesSent); b > 0 {
		wl = float64(atomic.LoadInt64(&c.writeLatencyNs)) / float64(b) / 1e6 // ns→ms
	}
	return core.SinkMetrics{
		SinkName:     name,
		RowsWritten:  atomic.LoadInt64(&c.rowsWritten),
		BatchesSent:  atomic.LoadInt64(&c.batchesSent),
		WriteLatency: wl,
		Errors:       atomic.LoadInt64(&c.writeErrors),
	}
}
