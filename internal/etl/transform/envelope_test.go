package transform

import (
	"context"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestEnvelopeNormalizeDebeziumUpdate(t *testing.T) {
	tr, err := NewEnvelopeNormalizeTransform(nil)
	if err != nil {
		t.Fatalf("NewEnvelopeNormalizeTransform: %v", err)
	}

	rec, err := tr.Apply(context.Background(), core.Record{
		Data: map[string]any{
			"payload": map[string]any{
				"op":    "u",
				"ts_ms": float64(1710000000123),
				"source": map[string]any{
					"table": "orders",
				},
				"before": map[string]any{"id": float64(1), "amount": float64(9)},
				"after":  map[string]any{"id": float64(1), "amount": float64(12)},
			},
		},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if rec.Operation != core.OpUpdate {
		t.Fatalf("Operation = %s, want UPDATE", rec.Operation)
	}
	if rec.Metadata.Table != "orders" {
		t.Fatalf("Metadata.Table = %q, want orders", rec.Metadata.Table)
	}
	if rec.Data["amount"] != float64(12) {
		t.Fatalf("amount = %v, want 12", rec.Data["amount"])
	}
	if rec.Data["_op"] != string(core.OpUpdate) || rec.Data["_source_table"] != "orders" {
		t.Fatalf("metadata fields missing: %#v", rec.Data)
	}
	if rec.Before["amount"] != float64(9) {
		t.Fatalf("Before amount = %v, want 9", rec.Before["amount"])
	}
	if rec.Metadata.Timestamp.IsZero() {
		t.Fatal("Metadata.Timestamp is zero")
	}
}

func TestEnvelopeNormalizePlainJSONPassesThrough(t *testing.T) {
	tr, err := NewEnvelopeNormalizeTransform(map[string]any{"keep_metadata": false})
	if err != nil {
		t.Fatalf("NewEnvelopeNormalizeTransform: %v", err)
	}

	rec, err := tr.Apply(context.Background(), core.Record{
		Operation: core.OpInsert,
		Data:      map[string]any{"id": "o1", "amount": 10},
	})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if rec.Data["id"] != "o1" || rec.Data["_op"] != nil {
		t.Fatalf("plain JSON changed unexpectedly: %#v", rec.Data)
	}
}
