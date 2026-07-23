package transform

import (
	"context"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func TestSortAscendingAndDescending(t *testing.T) {
	tr, err := NewSortTransform(map[string]any{
		"fields": []any{
			map[string]any{"name": "score", "order": "desc"},
			map[string]any{"name": "id", "order": "asc"},
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	in := []core.Record{
		{Data: map[string]any{"id": 2, "score": 10}},
		{Data: map[string]any{"id": 1, "score": 30}},
		{Data: map[string]any{"id": 3, "score": 30}},
		{Data: map[string]any{"id": 4, "score": 20}},
	}
	out, err := tr.ApplyBatch(context.Background(), in)
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	wantIDs := []int{1, 3, 4, 2} // score desc, then id asc
	for i, id := range wantIDs {
		got := out[i].Data["id"]
		// numeric compare via toFloat-compatible types
		if int(got.(int)) != id {
			t.Fatalf("pos %d id=%v want %d; full=%v", i, got, id, out[i].Data)
		}
	}
}

func TestSortStringAndNil(t *testing.T) {
	tr, err := NewSortTransform(map[string]any{
		"fields": []any{
			map[string]any{"name": "name", "order": "asc"},
		},
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	out, err := tr.ApplyBatch(context.Background(), []core.Record{
		{Data: map[string]any{"name": "bob"}},
		{Data: map[string]any{"name": nil}},
		{Data: map[string]any{"name": "alice"}},
		{Data: map[string]any{}}, // missing field → nil
	})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	// nil first, then alice, bob
	if out[0].Data["name"] != nil && out[0].Data["name"] != "" {
		// missing/nil both sort first; either is fine as long as non-nil strings follow.
	}
	names := []string{}
	for _, r := range out {
		if r.Data["name"] == nil {
			names = append(names, "")
			continue
		}
		names = append(names, r.Data["name"].(string))
	}
	// first two empty/nil, then alice, bob
	if names[len(names)-2] != "alice" || names[len(names)-1] != "bob" {
		t.Fatalf("names=%v", names)
	}
}

func TestSortMaxBuffer(t *testing.T) {
	tr, err := NewSortTransform(map[string]any{
		"fields": []any{
			map[string]any{"name": "id", "order": "asc"},
		},
		"max_buffer": 2,
	})
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	_, err = tr.ApplyBatch(context.Background(), []core.Record{
		{Data: map[string]any{"id": 1}},
		{Data: map[string]any{"id": 2}},
		{Data: map[string]any{"id": 3}},
	})
	if err == nil || !strings.Contains(err.Error(), "max_buffer") {
		t.Fatalf("expected max_buffer error, got %v", err)
	}
}

func TestSortEmptyBatchAndRegistry(t *testing.T) {
	tr, err := registry.BuildTransform("sort", map[string]any{
		"fields": []any{
			map[string]any{"name": "id", "order": "asc"},
		},
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	bt := tr.(core.BatchTransform)
	out, err := bt.ApplyBatch(context.Background(), nil)
	if err != nil || out != nil {
		t.Fatalf("empty batch out=%v err=%v", out, err)
	}
	// Config validation: missing fields.
	if _, err := NewSortTransform(map[string]any{}); err == nil {
		t.Fatal("expected fields required error")
	}
}
