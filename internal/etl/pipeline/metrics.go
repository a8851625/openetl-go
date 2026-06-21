package pipeline

import (
	"context"
	"fmt"
	"sync"
	"time"

	"openetl-go/internal/etl/core"
)

type MetricsHooks struct {
	mu              sync.Mutex
	SourceReadNanos int64
	SinkWriteNanos  int64
	LastBatchSize   int
	BatchCount      int64
	TotalBatchSize  int64
	SourceReadCount int64
	CDCLagMs        int64
}

func (m *MetricsHooks) RecordSourceRead(start time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SourceReadNanos += time.Since(start).Nanoseconds()
	m.SourceReadCount++
}

func (m *MetricsHooks) RecordSinkWrite(start time.Time, batchSize int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.SinkWriteNanos += time.Since(start).Nanoseconds()
	m.LastBatchSize = batchSize
	m.BatchCount++
	m.TotalBatchSize += int64(batchSize)
}

func (m *MetricsHooks) Snapshot() MetricsSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()
	var avgBatchSize int64
	if m.BatchCount > 0 {
		avgBatchSize = m.TotalBatchSize / m.BatchCount
	}
	var avgReadMs float64
	if m.SourceReadCount > 0 {
		avgReadMs = float64(m.SourceReadNanos) / float64(m.SourceReadCount) / 1e6
	}
	var avgWriteMs float64
	if m.BatchCount > 0 {
		avgWriteMs = float64(m.SinkWriteNanos) / float64(m.BatchCount) / 1e6
	}
	return MetricsSnapshot{
		SourceReadLatencyMs: avgReadMs,
		SinkWriteLatencyMs:  avgWriteMs,
		LastBatchSize:       m.LastBatchSize,
		AvgBatchSize:        avgBatchSize,
		BatchCount:          m.BatchCount,
		CDCLagMs:            m.CDCLagMs,
	}
}

type MetricsSnapshot struct {
	SourceReadLatencyMs  float64 `json:"source_read_latency_ms"`
	SinkWriteLatencyMs   float64 `json:"sink_write_latency_ms"`
	LastBatchSize        int     `json:"last_batch_size"`
	AvgBatchSize         int64   `json:"avg_batch_size"`
	BatchCount           int64   `json:"batch_count"`
	CDCLagMs             int64   `json:"cdc_lag_ms,omitempty"`
	BackpressureDepth    int     `json:"backpressure_depth"`
	BackpressureCapacity int     `json:"backpressure_capacity"`
}

type SinkWriteHook struct {
	Hooks *MetricsHooks
	Sink  core.Sink
}

func (s *SinkWriteHook) Open(ctx context.Context) error {
	return s.Sink.Open(ctx)
}

func (s *SinkWriteHook) Write(ctx context.Context, records []core.Record) error {
	start := time.Now()
	err := s.Sink.Write(ctx, records)
	s.Hooks.RecordSinkWrite(start, len(records))
	return err
}

func (s *SinkWriteHook) Close() error { return s.Sink.Close() }

func (s *SinkWriteHook) Name() string { return s.Sink.Name() }

func CheckIdempotencyCompatibility(sourceType string, sinkType string, sinkConfig map[string]any) []string {
	var warnings []string
	cdcSources := map[string]bool{
		"mysql_cdc":          true,
		"mysql_snapshot_cdc": true,
		"postgres_cdc":       true,
		"kafka":              true,
	}
	nonIdempotentSinks := map[string]bool{
		"file_sink": true,
		"s3":        true,
	}
	appendOnlySinks := map[string]bool{
		"file_sink":     true,
		"s3":            true,
		"kafka":         true,
		"elasticsearch": true,
		"es":            true,
	}

	if cdcSources[sourceType] && nonIdempotentSinks[sinkType] {
		warnings = append(warnings, fmt.Sprintf(
			"source %s may re-deliver records on restart; sink %s does not deduplicate – risk of duplicates",
			sourceType, sinkType,
		))
	}

	if sourceType == "mysql_batch" && (sinkType == "mysql" || sinkType == "doris") {
		batchMode := ""
		if v, ok := sinkConfig["batch_mode"]; ok {
			batchMode, _ = v.(string)
		}
		if batchMode != "upsert" && batchMode != "replace" {
			warnings = append(warnings, fmt.Sprintf(
				"mysql_batch source with %s sink in insert mode may cause issues on re-run; consider batch_mode: upsert",
				sinkType,
			))
		}
	}

	if cdcSources[sourceType] && appendOnlySinks[sinkType] {
		warnings = append(warnings, fmt.Sprintf(
			"CDC source %s with append-only sink %s: UPDATE/DELETE operations will be written as plain rows, not applied as mutations",
			sourceType, sinkType,
		))
	}

	return warnings
}
