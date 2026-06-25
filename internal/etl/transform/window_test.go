package transform

import (
	"context"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/state"
)

func TestWindowStateStoreRestoresBufferedAggregates(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()
	base := time.Now().Truncate(time.Hour)

	seed, err := NewWindowTransform(map[string]any{
		"window_size_seconds": 3600,
		"group_by":            []any{"region"},
		"aggregates": map[string]any{
			"order_count": map[string]any{"func": "count"},
			"total":       map[string]any{"func": "sum", "field": "amount"},
		},
	})
	if err != nil {
		t.Fatalf("NewWindowTransform seed: %v", err)
	}
	seed.WithStateStore(store, "wide-pipe", "window-orders", time.Hour)
	_, err = seed.Apply(ctx, core.Record{
		Data:     map[string]any{"region": "east", "amount": 10},
		Metadata: core.Metadata{Timestamp: base.Add(5 * time.Minute)},
	})
	if err != core.ErrRecordFiltered {
		t.Fatalf("seed Apply err = %v, want ErrRecordFiltered", err)
	}

	restarted, err := NewWindowTransform(map[string]any{
		"window_size_seconds": 3600,
		"group_by":            []any{"region"},
		"aggregates": map[string]any{
			"order_count": map[string]any{"func": "count"},
			"total":       map[string]any{"func": "sum", "field": "amount"},
		},
	})
	if err != nil {
		t.Fatalf("NewWindowTransform restarted: %v", err)
	}
	restarted.WithStateStore(store, "wide-pipe", "window-orders", time.Hour)
	_, err = restarted.Apply(ctx, core.Record{
		Data:     map[string]any{"region": "east", "amount": 5},
		Metadata: core.Metadata{Timestamp: base.Add(10 * time.Minute)},
	})
	if err != core.ErrRecordFiltered {
		t.Fatalf("restarted Apply err = %v, want ErrRecordFiltered", err)
	}

	out, err := restarted.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("Flush emitted %d records, want 1: %#v", len(out), out)
	}
	if out[0].Data["region"] != "east" || out[0].Data["order_count"] != int64(2) || out[0].Data["total"] != float64(15) {
		t.Fatalf("restored aggregate mismatch: %#v", out[0].Data)
	}
}

func TestWindowStateStoreTTLExpiryPreventsRestore(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()

	seed, err := NewWindowTransform(map[string]any{
		"window_size_seconds": 3600,
		"aggregates": map[string]any{
			"order_count": map[string]any{"func": "count"},
		},
	})
	if err != nil {
		t.Fatalf("NewWindowTransform seed: %v", err)
	}
	seed.WithStateStore(store, "wide-pipe", "window-orders", 20*time.Millisecond)
	_, err = seed.Apply(ctx, core.Record{Data: map[string]any{"amount": 10}})
	if err != core.ErrRecordFiltered {
		t.Fatalf("seed Apply err = %v, want ErrRecordFiltered", err)
	}
	time.Sleep(30 * time.Millisecond)

	restarted, err := NewWindowTransform(map[string]any{
		"window_size_seconds": 3600,
		"aggregates": map[string]any{
			"order_count": map[string]any{"func": "count"},
		},
	})
	if err != nil {
		t.Fatalf("NewWindowTransform restarted: %v", err)
	}
	restarted.WithStateStore(store, "wide-pipe", "window-orders", 20*time.Millisecond)
	out, err := restarted.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if len(out) != 0 {
		t.Fatalf("expired state restored unexpectedly: %#v", out)
	}
}

func TestWindowTransformMetricsTrackEmittedAggregates(t *testing.T) {
	ctx := context.Background()
	window, err := NewWindowTransform(map[string]any{
		"window_size_seconds": 1,
		"state_node":          "window-orders",
		"group_by":            []any{"region"},
		"aggregates": map[string]any{
			"order_count": map[string]any{"func": "count"},
		},
	})
	if err != nil {
		t.Fatalf("NewWindowTransform: %v", err)
	}

	out, err := window.ApplyBatch(ctx, []core.Record{{
		Data:     map[string]any{"region": "east"},
		Metadata: core.Metadata{Timestamp: time.Now().Add(-2 * time.Second)},
	}})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("ApplyBatch emitted %d records, want 1", len(out))
	}

	metrics := window.TransformMetrics()
	if metrics.Node != "window-orders" || metrics.Transform != "window" {
		t.Fatalf("unexpected metric identity: %#v", metrics)
	}
	if metrics.Counters["accumulated"] != 1 || metrics.Counters["emitted_records"] != 1 || metrics.Counters["emitted_windows"] != 1 {
		t.Fatalf("window counters = %#v, want accumulated/emitted records/windows", metrics.Counters)
	}
}

func TestWindowTransformMetricsTrackLateDroppedRecords(t *testing.T) {
	ctx := context.Background()
	base := time.Now().Truncate(time.Hour)
	window, err := NewWindowTransform(map[string]any{
		"window_size_seconds":      3600,
		"allowed_lateness_seconds": 5,
		"aggregates": map[string]any{
			"order_count": map[string]any{"func": "count"},
		},
	})
	if err != nil {
		t.Fatalf("NewWindowTransform: %v", err)
	}

	_, err = window.ApplyBatch(ctx, []core.Record{
		{Data: map[string]any{"id": 1}, Metadata: core.Metadata{Timestamp: base}},
		{Data: map[string]any{"id": 2}, Metadata: core.Metadata{Timestamp: base.Add(-10 * time.Second)}},
	})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}

	metrics := window.TransformMetrics()
	if metrics.Counters["accumulated"] != 1 || metrics.Counters["late_dropped"] != 1 {
		t.Fatalf("window counters after late record = %#v, want one accumulated and one late drop", metrics.Counters)
	}

	out, err := window.Flush(ctx)
	if err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if len(out) != 1 {
		t.Fatalf("Flush emitted %d records, want 1", len(out))
	}
	metrics = window.TransformMetrics()
	if metrics.Counters["flushed_records"] != 1 || metrics.Counters["emitted_records"] != 1 {
		t.Fatalf("window counters after flush = %#v, want flushed/emitted record", metrics.Counters)
	}
}
