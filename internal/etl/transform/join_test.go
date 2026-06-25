package transform

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/state"
)

func TestJoinStateStoreRestoresBufferedRecords(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()

	seed, err := NewJoinTransform(map[string]any{
		"join_type":       "left",
		"join_key":        "user_id",
		"join_window_sec": 60,
		"join_fields":     []any{"amount", "status"},
		"join_prefix":     "prev_",
	})
	if err != nil {
		t.Fatalf("NewJoinTransform seed: %v", err)
	}
	seed.WithStateStore(store, "wide-pipe", "join-orders", time.Hour)
	if _, err := seed.Apply(ctx, core.Record{Data: map[string]any{
		"user_id": "u-1",
		"amount":  100,
		"status":  "paid",
	}}); err != nil {
		t.Fatalf("seed Apply: %v", err)
	}

	restarted, err := NewJoinTransform(map[string]any{
		"join_type":       "left",
		"join_key":        "user_id",
		"join_window_sec": 60,
		"join_fields":     []any{"amount", "status"},
		"join_prefix":     "prev_",
	})
	if err != nil {
		t.Fatalf("NewJoinTransform restarted: %v", err)
	}
	restarted.WithStateStore(store, "wide-pipe", "join-orders", time.Hour)

	rec, err := restarted.Apply(ctx, core.Record{Data: map[string]any{
		"user_id": "u-1",
		"amount":  200,
		"status":  "refund",
	}})
	if err != nil {
		t.Fatalf("restarted Apply: %v", err)
	}
	if rec.Data["prev_amount"] != float64(100) || rec.Data["prev_status"] != "paid" {
		t.Fatalf("restored join fields missing: %#v", rec.Data)
	}
}

func TestJoinTransformMetricsTrackHitsAndMisses(t *testing.T) {
	ctx := context.Background()
	join, err := NewJoinTransform(map[string]any{
		"join_type":       "left",
		"join_key":        "user_id",
		"join_window_sec": 60,
		"join_fields":     []any{"amount"},
		"join_prefix":     "prev_",
		"state_node":      "join-orders",
	})
	if err != nil {
		t.Fatalf("NewJoinTransform: %v", err)
	}

	if _, err := join.Apply(ctx, core.Record{Data: map[string]any{"user_id": "u-1", "amount": 100}}); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if _, err := join.Apply(ctx, core.Record{Data: map[string]any{"user_id": "u-1", "amount": 200}}); err != nil {
		t.Fatalf("second Apply: %v", err)
	}

	metrics := join.TransformMetrics()
	if metrics.Node != "join-orders" || metrics.Transform != "join" {
		t.Fatalf("unexpected metric identity: %#v", metrics)
	}
	if metrics.Counters["miss"] != 1 || metrics.Counters["hit"] != 1 {
		t.Fatalf("join counters = %#v, want one miss and one hit", metrics.Counters)
	}
}

func TestJoinTransformMetricsTrackInnerMissPolicies(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		onMiss  string
		counter string
	}{
		{name: "drop", onMiss: "drop", counter: "miss_dropped"},
		{name: "dlq", onMiss: "dlq", counter: "miss_dlq"},
		{name: "error", onMiss: "error", counter: "miss_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			join, err := NewJoinTransform(map[string]any{
				"join_type":   "inner",
				"join_key":    "user_id",
				"join_fields": []any{"amount"},
				"on_miss":     tc.onMiss,
			})
			if err != nil {
				t.Fatalf("NewJoinTransform: %v", err)
			}

			_, err = join.Apply(ctx, core.Record{Data: map[string]any{"user_id": "missing"}})
			if tc.onMiss == "drop" {
				if !errors.Is(err, core.ErrRecordFiltered) {
					t.Fatalf("drop err = %v, want ErrRecordFiltered", err)
				}
			} else if err == nil {
				t.Fatalf("%s err = nil, want error", tc.onMiss)
			}

			counters := join.TransformMetrics().Counters
			if counters["miss"] != 1 || counters[tc.counter] != 1 {
				t.Fatalf("counters = %#v, want miss and %s", counters, tc.counter)
			}
		})
	}
}

func TestJoinTransformRejectsWhenBufferedKeyLimitExceeded(t *testing.T) {
	ctx := context.Background()
	join, err := NewJoinTransform(map[string]any{
		"join_type":         "left",
		"join_key":          "user_id",
		"join_fields":       []any{"amount"},
		"max_buffered_keys": 1,
	})
	if err != nil {
		t.Fatalf("NewJoinTransform: %v", err)
	}

	if _, err := join.Apply(ctx, core.Record{Data: map[string]any{"user_id": "u-1", "amount": 100}}); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if _, err := join.Apply(ctx, core.Record{Data: map[string]any{"user_id": "u-2", "amount": 200}}); err == nil {
		t.Fatal("second Apply err = nil, want state key limit error")
	}

	counters := join.TransformMetrics().Counters
	if counters["state_limit_exceeded"] != 1 {
		t.Fatalf("state_limit_exceeded = %d, want 1 (counters=%#v)", counters["state_limit_exceeded"], counters)
	}
}

func TestJoinTransformRejectsWhenBufferedRecordLimitExceeded(t *testing.T) {
	ctx := context.Background()
	join, err := NewJoinTransform(map[string]any{
		"join_type":            "left",
		"join_key":             "user_id",
		"join_fields":          []any{"amount"},
		"max_buffered_records": 1,
	})
	if err != nil {
		t.Fatalf("NewJoinTransform: %v", err)
	}

	if _, err := join.Apply(ctx, core.Record{Data: map[string]any{"user_id": "u-1", "amount": 100}}); err != nil {
		t.Fatalf("first Apply: %v", err)
	}
	if _, err := join.Apply(ctx, core.Record{Data: map[string]any{"user_id": "u-1", "amount": 200}}); err == nil {
		t.Fatal("second Apply err = nil, want state record limit error")
	}

	counters := join.TransformMetrics().Counters
	if counters["state_limit_exceeded"] != 1 {
		t.Fatalf("state_limit_exceeded = %d, want 1 (counters=%#v)", counters["state_limit_exceeded"], counters)
	}
}

func TestJoinStateStoreTTLExpiryPreventsRestore(t *testing.T) {
	ctx := context.Background()
	store := state.NewMemoryStore()

	seed, err := NewJoinTransform(map[string]any{
		"join_type":       "left",
		"join_key":        "user_id",
		"join_window_sec": 60,
		"join_fields":     []any{"amount"},
		"join_prefix":     "prev_",
	})
	if err != nil {
		t.Fatalf("NewJoinTransform seed: %v", err)
	}
	seed.WithStateStore(store, "wide-pipe", "join-orders", 20*time.Millisecond)
	if _, err := seed.Apply(ctx, core.Record{Data: map[string]any{
		"user_id": "u-ttl",
		"amount":  100,
	}}); err != nil {
		t.Fatalf("seed Apply: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	restarted, err := NewJoinTransform(map[string]any{
		"join_type":       "left",
		"join_key":        "user_id",
		"join_window_sec": 60,
		"join_fields":     []any{"amount"},
		"join_prefix":     "prev_",
	})
	if err != nil {
		t.Fatalf("NewJoinTransform restarted: %v", err)
	}
	restarted.WithStateStore(store, "wide-pipe", "join-orders", 20*time.Millisecond)

	rec, err := restarted.Apply(ctx, core.Record{Data: map[string]any{
		"user_id": "u-ttl",
		"amount":  200,
	}})
	if err != nil {
		t.Fatalf("restarted Apply: %v", err)
	}
	if rec.Data["prev_amount"] != nil {
		t.Fatalf("expired join state restored unexpectedly: %#v", rec.Data)
	}
}
