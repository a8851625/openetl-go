package transform

import (
	"context"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func TestRenameDropAddAndTypeConvert(t *testing.T) {
	rec := core.Record{Data: map[string]any{"old": "42", "drop": "x"}}

	renamed, err := (&RenameTransform{mappings: map[string]string{"old": "new"}}).Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("rename error = %v", err)
	}
	if renamed.Data["new"] != "42" || renamed.Data["old"] != nil {
		t.Fatalf("rename result = %#v", renamed.Data)
	}

	dropped, err := (&DropFieldTransform{fields: []string{"drop"}}).Apply(context.Background(), renamed)
	if err != nil {
		t.Fatalf("drop error = %v", err)
	}
	if _, ok := dropped.Data["drop"]; ok {
		t.Fatalf("drop result = %#v", dropped.Data)
	}

	added, err := (&AddFieldTransform{field: "etl", value: "ok"}).Apply(context.Background(), dropped)
	if err != nil {
		t.Fatalf("add error = %v", err)
	}
	if added.Data["etl"] != "ok" {
		t.Fatalf("add result = %#v", added.Data)
	}

	converted, err := (&TypeConvertTransform{conversions: map[string]string{"new": "int64"}}).Apply(context.Background(), added)
	if err != nil {
		t.Fatalf("convert error = %v", err)
	}
	if converted.Data["new"] != int64(42) {
		t.Fatalf("convert result = %#v", converted.Data)
	}
}

func TestProjectTransformProjectsAliasesConstantsAndTimeFormats(t *testing.T) {
	eventTime := time.Date(2024, 2, 3, 4, 5, 6, 789000000, time.UTC)
	rec := core.Record{
		Operation: core.OpUpdate,
		Data: map[string]any{
			"id":         "42",
			"name":       "Ada",
			"event_time": eventTime,
			"created_at": "2024-02-03T04:05:06Z",
			"drop":       "x",
		},
		Before:   map[string]any{"name": "old"},
		Metadata: core.Metadata{Source: "mysql", Table: "users"},
	}

	projected, err := (&ProjectTransform{
		fields: []string{"id", "event_time"},
		mappings: map[string]string{
			"name":       "customer_name",
			"created_at": "dt",
		},
		constants: map[string]any{
			"source_system": "crm",
		},
		timeFormats: map[string]string{
			"event_time": "unix_ms",
			"dt":         "2006-01-02",
		},
	}).Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("project error = %v", err)
	}

	wantEventTime := eventTime.UnixMilli()
	if projected.Data["id"] != "42" ||
		projected.Data["customer_name"] != "Ada" ||
		projected.Data["event_time"] != wantEventTime ||
		projected.Data["dt"] != "2024-02-03" ||
		projected.Data["source_system"] != "crm" {
		t.Fatalf("project result = %#v", projected.Data)
	}
	for _, field := range []string{"name", "created_at", "drop"} {
		if _, ok := projected.Data[field]; ok {
			t.Fatalf("project retained %q in %#v", field, projected.Data)
		}
	}
	if projected.Operation != rec.Operation || projected.Metadata.Source != rec.Metadata.Source || projected.Metadata.Table != rec.Metadata.Table || projected.Before["name"] != "old" {
		t.Fatalf("project did not preserve record envelope: %#v", projected)
	}
}

func TestProjectTransformKeepUnmappedRenamesMappedFields(t *testing.T) {
	rec := core.Record{Data: map[string]any{"id": 1, "name": "Ada", "status": "active"}}

	projected, err := (&ProjectTransform{
		mappings:     map[string]string{"name": "customer_name"},
		keepUnmapped: true,
	}).Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("project error = %v", err)
	}

	if projected.Data["id"] != 1 || projected.Data["status"] != "active" || projected.Data["customer_name"] != "Ada" {
		t.Fatalf("project keep_unmapped result = %#v", projected.Data)
	}
	if _, ok := projected.Data["name"]; ok {
		t.Fatalf("project keep_unmapped retained mapped source: %#v", projected.Data)
	}
}

func TestSelectFieldsAliasBuildsProjectTransform(t *testing.T) {
	transform, err := registry.BuildTransform("select_fields", map[string]any{
		"fields":   []interface{}{"id"},
		"mappings": map[string]string{"name": "customer_name"},
		"constants": map[string]string{
			"source_system": "crm",
		},
	})
	if err != nil {
		t.Fatalf("build select_fields error = %v", err)
	}
	if transform.Name() != "select_fields" {
		t.Fatalf("transform name = %q, want select_fields", transform.Name())
	}

	rec := core.Record{Data: map[string]any{"id": 1, "name": "Ada", "drop": true}}
	projected, err := transform.Apply(context.Background(), rec)
	if err != nil {
		t.Fatalf("select_fields apply error = %v", err)
	}
	if projected.Data["id"] != 1 ||
		projected.Data["customer_name"] != "Ada" ||
		projected.Data["source_system"] != "crm" ||
		len(projected.Data) != 3 {
		t.Fatalf("select_fields result = %#v", projected.Data)
	}
}

func TestProjectTransformReturnsTimeParseError(t *testing.T) {
	rec := core.Record{Data: map[string]any{"event_time": "not-a-time"}}

	_, err := (&ProjectTransform{
		fields:      []string{"event_time"},
		timeFormats: map[string]string{"event_time": time.RFC3339},
	}).Apply(context.Background(), rec)
	if err == nil {
		t.Fatal("project time parse error = nil, want error")
	}
}
