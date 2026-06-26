//go:build cgo

package transform

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestTSTransformApplyBatchExpandsArrayResults(t *testing.T) {
	transform, err := NewTSTransform(map[string]any{
		"script": `
function transform(record) {
  return record.data.items.map(function(item) {
    return {
      data: {
        order_id: record.data.id,
        sku: item.sku,
        qty: item.qty
      },
      metadata: {
        table: "order_items"
      }
    };
  });
}
`,
	})
	if err != nil {
		t.Fatalf("NewTSTransform: %v", err)
	}
	defer transform.Close()

	in := core.Record{
		Operation: core.OpInsert,
		Data: map[string]any{
			"id": "order-1",
			"items": []any{
				map[string]any{"sku": "A", "qty": 2},
				map[string]any{"sku": "B", "qty": 3},
			},
		},
		Metadata: core.Metadata{Source: "kafka", Table: "orders", Offset: 42},
	}
	out, err := transform.ApplyBatch(context.Background(), []core.Record{in})
	if err != nil {
		t.Fatalf("ApplyBatch: %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("outputs = %d, want 2: %#v", len(out), out)
	}
	if out[0].Data["order_id"] != "order-1" || out[0].Data["sku"] != "A" || out[0].Data["qty"] != float64(2) {
		t.Fatalf("first output data = %#v", out[0].Data)
	}
	if out[1].Data["order_id"] != "order-1" || out[1].Data["sku"] != "B" || out[1].Data["qty"] != float64(3) {
		t.Fatalf("second output data = %#v", out[1].Data)
	}
	if out[0].Operation != core.OpInsert || out[0].Metadata.Source != "kafka" || out[0].Metadata.Table != "order_items" || out[0].Metadata.Offset != 42 {
		t.Fatalf("first output envelope = %#v", out[0])
	}
}

func TestTSTransformApplyBatchReportsPartialErrors(t *testing.T) {
	transform, err := NewTSTransform(map[string]any{
		"script": `
function transform(record) {
  if (record.data.bad) {
    throw new Error("bad payload");
  }
  return { data: { id: record.data.id } };
}
`,
	})
	if err != nil {
		t.Fatalf("NewTSTransform: %v", err)
	}
	defer transform.Close()

	out, err := transform.ApplyBatch(context.Background(), []core.Record{
		{Data: map[string]any{"id": 1}},
		{Data: map[string]any{"id": 2, "bad": true}},
	})
	if len(out) != 1 || out[0].Data["id"] != float64(1) {
		t.Fatalf("outputs = %#v, want one survivor id=1", out)
	}
	var partial core.PartialTransformError
	if !errors.As(err, &partial) {
		t.Fatalf("ApplyBatch error = %T %v, want core.PartialTransformError", err, err)
	}
	failures := partial.FailedRecords()
	if len(failures) != 1 || failures[0].Record.Data["id"] != 2 {
		t.Fatalf("failures = %#v, want input id=2", failures)
	}
	var classified core.ClassifiedError
	if !errors.As(failures[0].Err, &classified) || classified.Class != core.ErrorClassData {
		t.Fatalf("failure error = %T %v, want data-classified error", failures[0].Err, failures[0].Err)
	}
}

func TestTSTransformApplyCallsScriptOnce(t *testing.T) {
	transform, err := NewTSTransform(map[string]any{
		"script": `
var calls = 0;
function transform(record) {
  calls++;
  return { data: { id: record.data.id, calls: calls } };
}
`,
	})
	if err != nil {
		t.Fatalf("NewTSTransform: %v", err)
	}
	defer transform.Close()

	out, err := transform.Apply(context.Background(), core.Record{Data: map[string]any{"id": "a"}})
	if err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if out.Data["calls"] != float64(1) {
		t.Fatalf("calls = %#v, want 1; output = %#v", out.Data["calls"], out)
	}
}

func TestTSTransformApplyRejectsDirectMultiOutput(t *testing.T) {
	transform, err := NewTSTransform(map[string]any{
		"script": `function transform(record) { return [{ data: { id: 1 } }, { data: { id: 2 } }]; }`,
	})
	if err != nil {
		t.Fatalf("NewTSTransform: %v", err)
	}
	defer transform.Close()

	_, err = transform.Apply(context.Background(), core.Record{Data: map[string]any{"id": "a"}})
	if err == nil || !strings.Contains(err.Error(), "use batch execution") {
		t.Fatalf("Apply error = %v, want direct multi-output rejection", err)
	}
}
