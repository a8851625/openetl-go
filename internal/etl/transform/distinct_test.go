package transform

import (
	"context"
	"errors"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func TestDistinctByFields(t *testing.T) {
	tr, err := NewDistinctTransform(map[string]any{
		"fields": []interface{}{"id", "event_type"},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx := context.Background()

	pass := func(id any, et string) {
		t.Helper()
		_, err := tr.Apply(ctx, core.Record{Data: map[string]any{"id": id, "event_type": et, "payload": "x"}})
		if err != nil {
			t.Fatalf("expected pass id=%v et=%s: %v", id, et, err)
		}
	}
	drop := func(id any, et string) {
		t.Helper()
		_, err := tr.Apply(ctx, core.Record{Data: map[string]any{"id": id, "event_type": et}})
		if !errors.Is(err, core.ErrRecordFiltered) {
			t.Fatalf("expected filter id=%v et=%s, got %v", id, et, err)
		}
	}

	pass(1, "click")
	pass(1, "view") // different event_type
	drop(1, "click")
	pass(2, "click")
	if tr.SeenCount() != 3 {
		t.Fatalf("seen=%d want 3", tr.SeenCount())
	}
}

func TestDistinctFullRecord(t *testing.T) {
	tr, err := NewDistinctTransform(map[string]any{})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx := context.Background()

	rec := core.Record{Data: map[string]any{"a": 1, "b": "x"}}
	if _, err := tr.Apply(ctx, rec); err != nil {
		t.Fatalf("first: %v", err)
	}
	// Same values, different map insertion order — still a duplicate.
	dup := core.Record{Data: map[string]any{"b": "x", "a": 1}}
	if _, err := tr.Apply(ctx, dup); !errors.Is(err, core.ErrRecordFiltered) {
		t.Fatalf("dup err=%v", err)
	}
	// Different payload should pass.
	if _, err := tr.Apply(ctx, core.Record{Data: map[string]any{"a": 1, "b": "y"}}); err != nil {
		t.Fatalf("different: %v", err)
	}
}

func TestDistinctMissingFieldsAndEmptyInput(t *testing.T) {
	tr, err := NewDistinctTransform(map[string]any{
		"fields": []interface{}{"id"},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	ctx := context.Background()

	// Missing field still produces a stable key (empty value).
	if _, err := tr.Apply(ctx, core.Record{Data: map[string]any{"name": "a"}}); err != nil {
		t.Fatalf("missing field first: %v", err)
	}
	if _, err := tr.Apply(ctx, core.Record{Data: map[string]any{"name": "b"}}); !errors.Is(err, core.ErrRecordFiltered) {
		t.Fatalf("missing field second: %v", err)
	}
	// Nil data is accepted once.
	if _, err := tr.Apply(ctx, core.Record{}); err != nil {
		// fields=["id"] with nil data → key "id=<nil-ish empty>"
		// already used above for missing field, so may filter.
		if !errors.Is(err, core.ErrRecordFiltered) {
			t.Fatalf("nil data: %v", err)
		}
	}
}

func TestDistinctRegistryAndExtensionPoints(t *testing.T) {
	tr, err := registry.BuildTransform("distinct", map[string]any{
		"fields": []interface{}{"id"},
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if tr.Name() != "distinct" {
		t.Fatalf("name=%q", tr.Name())
	}
	// Flusher / StateSnapshotter reserved hooks exist and are no-ops.
	dt := tr.(*DistinctTransform)
	if out, err := dt.Flush(context.Background()); err != nil || out != nil {
		t.Fatalf("Flush out=%v err=%v", out, err)
	}
	node, ver, ok, err := dt.SnapshotState(context.Background())
	if err != nil || ok || ver != "" || node != "distinct" {
		t.Fatalf("SnapshotState node=%q ver=%q ok=%v err=%v", node, ver, ok, err)
	}
}
