package telemetry

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"
)

type Counter struct {
	value int64
}

func (c *Counter) Inc()         { atomic.AddInt64(&c.value, 1) }
func (c *Counter) Add(n int64)  { atomic.AddInt64(&c.value, n) }
func (c *Counter) Value() int64 { return atomic.LoadInt64(&c.value) }
func (c *Counter) Reset()       { atomic.StoreInt64(&c.value, 0) }

type MetricsRegistry struct {
	mu       sync.RWMutex
	counters map[string]*Counter
}

var DefaultRegistry = &MetricsRegistry{counters: map[string]*Counter{}}

func (r *MetricsRegistry) Counter(name string) *Counter {
	r.mu.RLock()
	if c, ok := r.counters[name]; ok {
		r.mu.RUnlock()
		return c
	}
	r.mu.RUnlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if c, ok := r.counters[name]; ok {
		return c
	}
	c := &Counter{}
	r.counters[name] = c
	return c
}

func (r *MetricsRegistry) Snapshot() map[string]int64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	snap := map[string]int64{}
	for name, c := range r.counters {
		snap[name] = c.Value()
	}
	return snap
}

func (r *MetricsRegistry) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(r.Snapshot())
}

type PipelineMetrics struct {
	Name                 string     `json:"name"`
	Status               string     `json:"status"`
	RecordsRead          int64      `json:"records_read"`
	RecordsWritten       int64      `json:"records_written"`
	RecordsFailed        int64      `json:"records_failed"`
	RecordsDLQ           int64      `json:"records_dlq"`
	DLQFileCount         int        `json:"dlq_file_count"`
	DLQReplayCount       int64      `json:"dlq_replay_count"`
	DLQDeleteCount       int64      `json:"dlq_delete_count"`
	LastError            string     `json:"last_error,omitempty"`
	LastCheckpoint       time.Time  `json:"last_checkpoint"`
	CheckpointAgeSeconds int64      `json:"checkpoint_age_seconds"`
	SourceReadLatencyMs  float64    `json:"source_read_latency_ms"`
	SinkWriteLatencyMs   float64    `json:"sink_write_latency_ms"`
	LastBatchSize        int        `json:"last_batch_size"`
	AvgBatchSize         int64      `json:"avg_batch_size"`
	BatchCount           int64      `json:"batch_count"`
	CDCLagMs             int64      `json:"cdc_lag_ms,omitempty"`
	BackpressureDepth    int        `json:"backpressure_depth"`
	BackpressureCapacity int        `json:"backpressure_capacity"`
	StartedAt            *time.Time `json:"started_at,omitempty"`
	Uptime               string     `json:"uptime"`
	// CircuitBreakerState: 0=closed, 1=open, 2=half_open
	CircuitBreakerState int `json:"circuit_breaker_state"`
	// Per-sink metrics: sink name → metrics snapshot
	SinkMetrics []SinkMetric `json:"sink_metrics,omitempty"`
}

// SinkMetric provides per-sink write metrics for Prometheus exposure.
type SinkMetric struct {
	SinkName     string  `json:"sink_name"`
	RowsWritten  int64   `json:"rows_written"`
	BatchesSent  int64   `json:"batches_sent"`
	WriteLatency float64 `json:"write_latency_ms"`
	Errors       int64   `json:"errors"`
}

func MetricsHandler(getMetrics func() []PipelineMetrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		metrics := getMetrics()
		// Ensure empty result serializes as [] not null — the frontend calls
		// .find()/.map() on `pipelines` and a JSON null crashes the SPA
		// ("Cannot read properties of null (reading 'find')").
		if metrics == nil {
			metrics = []PipelineMetrics{}
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]any{
			"pipelines": metrics,
			"timestamp": time.Now(),
		})
	}
}

func HealthHandler(getStatus func() map[string]string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		status := getStatus()
		w.Header().Set("Content-Type", "application/json")
		status["timestamp"] = time.Now().Format(time.RFC3339)

		// If any component is unhealthy, return 503 so load balancers
		// and kube probes can react appropriately.
		overall := status["status"]
		if overall != "ok" && overall != "" {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		json.NewEncoder(w).Encode(status)
	}
}

func PrometheusHandler(getMetrics func() []PipelineMetrics) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		w.Write([]byte(`# HELP etl_records_read_total Total records read from sources.
# TYPE etl_records_read_total counter
# HELP etl_records_written_total Total records written to sinks.
# TYPE etl_records_written_total counter
# HELP etl_records_failed_total Total records that failed to write.
# TYPE etl_records_failed_total counter
# HELP etl_records_dlq_total Total records sent to dead-letter queue.
# TYPE etl_records_dlq_total counter
# HELP etl_dlq_file_count Number of dead-letter queue files.
# TYPE etl_dlq_file_count gauge
# HELP etl_dlq_replay_total Number of DLQ records replayed.
# TYPE etl_dlq_replay_total counter
# HELP etl_dlq_delete_total Number of DLQ records deleted.
# TYPE etl_dlq_delete_total counter
# HELP etl_checkpoint_age_seconds Age of the last committed checkpoint.
# TYPE etl_checkpoint_age_seconds gauge
# HELP etl_source_read_latency_ms Source read latency in milliseconds (average).
# TYPE etl_source_read_latency_ms gauge
# HELP etl_source_read_latency_ms_sum Cumulative source read latency in milliseconds.
# TYPE etl_source_read_latency_ms_sum counter
# HELP etl_source_read_latency_ms_count Number of source read latency samples.
# TYPE etl_source_read_latency_ms_count counter
# HELP etl_sink_write_latency_ms Sink write latency in milliseconds (average).
# TYPE etl_sink_write_latency_ms gauge
# HELP etl_sink_write_latency_ms_sum Cumulative sink write latency in milliseconds.
# TYPE etl_sink_write_latency_ms_sum counter
# HELP etl_sink_write_latency_ms_count Number of sink write latency samples.
# TYPE etl_sink_write_latency_ms_count counter
# HELP etl_last_batch_size Size of the most recent batch.
# TYPE etl_last_batch_size gauge
# HELP etl_avg_batch_size Average batch size.
# TYPE etl_avg_batch_size gauge
# HELP etl_batch_count_total Total number of batches processed.
# TYPE etl_batch_count_total counter
# HELP etl_cdc_lag_ms CDC replication lag in milliseconds.
# TYPE etl_cdc_lag_ms gauge
# HELP etl_backpressure_depth Number of records currently buffered in the backpressure channel.
# TYPE etl_backpressure_depth gauge
# HELP etl_backpressure_capacity Total capacity of the backpressure channel.
# TYPE etl_backpressure_capacity gauge
# HELP etl_circuit_breaker_state Circuit breaker state (0=closed, 1=open, 2=half_open).
# TYPE etl_circuit_breaker_state gauge
# HELP etl_sink_rows_written_total Total rows written per sink.
# TYPE etl_sink_rows_written_total counter
# HELP etl_sink_batches_sent_total Total batches sent per sink.
# TYPE etl_sink_batches_sent_total counter
# HELP etl_sink_write_latency_ms_per_sink Sink write latency per sink in milliseconds (average).
# TYPE etl_sink_write_latency_ms_per_sink gauge
`))
		for _, m := range getMetrics() {
			fmt.Fprintf(w, "etl_records_read_total{pipeline=\"%s\"} %d\n", m.Name, m.RecordsRead)
			fmt.Fprintf(w, "etl_records_written_total{pipeline=\"%s\"} %d\n", m.Name, m.RecordsWritten)
			fmt.Fprintf(w, "etl_records_failed_total{pipeline=\"%s\"} %d\n", m.Name, m.RecordsFailed)
			fmt.Fprintf(w, "etl_records_dlq_total{pipeline=\"%s\"} %d\n", m.Name, m.RecordsDLQ)
			fmt.Fprintf(w, "etl_dlq_file_count{pipeline=\"%s\"} %d\n", m.Name, m.DLQFileCount)
			fmt.Fprintf(w, "etl_dlq_replay_total{pipeline=\"%s\"} %d\n", m.Name, m.DLQReplayCount)
			fmt.Fprintf(w, "etl_dlq_delete_total{pipeline=\"%s\"} %d\n", m.Name, m.DLQDeleteCount)
			fmt.Fprintf(w, "etl_checkpoint_age_seconds{pipeline=\"%s\"} %d\n", m.Name, m.CheckpointAgeSeconds)
			fmt.Fprintf(w, "etl_source_read_latency_ms{pipeline=\"%s\"} %.2f\n", m.Name, m.SourceReadLatencyMs)
			fmt.Fprintf(w, "etl_sink_write_latency_ms{pipeline=\"%s\"} %.2f\n", m.Name, m.SinkWriteLatencyMs)
			fmt.Fprintf(w, "etl_last_batch_size{pipeline=\"%s\"} %d\n", m.Name, m.LastBatchSize)
			fmt.Fprintf(w, "etl_avg_batch_size{pipeline=\"%s\"} %d\n", m.Name, m.AvgBatchSize)
			fmt.Fprintf(w, "etl_batch_count_total{pipeline=\"%s\"} %d\n", m.Name, m.BatchCount)
			if m.CDCLagMs > 0 {
				fmt.Fprintf(w, "etl_cdc_lag_ms{pipeline=\"%s\"} %d\n", m.Name, m.CDCLagMs)
			}
			fmt.Fprintf(w, "etl_backpressure_depth{pipeline=\"%s\"} %d\n", m.Name, m.BackpressureDepth)
			fmt.Fprintf(w, "etl_backpressure_capacity{pipeline=\"%s\"} %d\n", m.Name, m.BackpressureCapacity)
			fmt.Fprintf(w, "etl_circuit_breaker_state{pipeline=\"%s\"} %d\n", m.Name, m.CircuitBreakerState)
			// Per-sink metrics
			for _, sm := range m.SinkMetrics {
				fmt.Fprintf(w, "etl_sink_rows_written_total{pipeline=\"%s\",sink=\"%s\"} %d\n", m.Name, sm.SinkName, sm.RowsWritten)
				fmt.Fprintf(w, "etl_sink_batches_sent_total{pipeline=\"%s\",sink=\"%s\"} %d\n", m.Name, sm.SinkName, sm.BatchesSent)
				fmt.Fprintf(w, "etl_sink_write_latency_ms_per_sink{pipeline=\"%s\",sink=\"%s\"} %.2f\n", m.Name, sm.SinkName, sm.WriteLatency)
			}
		}
	}
}
