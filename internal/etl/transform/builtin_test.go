package transform

import (
	"context"
	"testing"

	"openetl-go/internal/etl/core"
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

func TestLuaTransform(t *testing.T) {
	tr, err := NewLuaTransform(`record.full_name = record.first .. " " .. record.last`)
	if err != nil {
		t.Fatalf("NewLuaTransform error = %v", err)
	}
	rec, err := tr.Apply(context.Background(), core.Record{Data: map[string]any{"first": "Ada", "last": "Lovelace"}})
	if err != nil {
		t.Fatalf("Apply error = %v", err)
	}
	if rec.Data["full_name"] != "Ada Lovelace" {
		t.Fatalf("full_name = %#v", rec.Data["full_name"])
	}
}
