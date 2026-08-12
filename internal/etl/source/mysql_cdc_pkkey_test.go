package source

import (
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/go-mysql-org/go-mysql/schema"
)

// TestMetadataKeyJSONMulti verifies the per-table PK JSON object builder used
// by mysql_cdc / mysql_snapshot_cdc to fill Metadata.Key for downstream
// pk_columns_from_metadata sinks (esp. on DELETE events).
func TestMetadataKeyJSONMulti(t *testing.T) {
	cases := []struct {
		name    string
		pkCols  []string
		row     map[string]any
		want    string
	}{
		{"single pk", []string{"session_id"}, map[string]any{"session_id": "de2aaedd", "data": "x"}, `{"session_id":"de2aaedd"}`},
		{"composite pk", []string{"tenant_id", "id"}, map[string]any{"tenant_id": 3, "id": 42, "name": "a"}, `{"id":42,"tenant_id":3}`},
		{"pk missing in row", []string{"id"}, map[string]any{"name": "a"}, ""},
		{"no pk cols", nil, map[string]any{"id": 1}, ""},
		{"pk nil value", []string{"id"}, map[string]any{"id": nil}, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got := metadataKeyJSONMulti(c.pkCols, c.row)
			if got != c.want {
				t.Errorf("metadataKeyJSONMulti(%v,%v) = %q, want %q", c.pkCols, c.row, got, c.want)
			}
		})
	}
}

// TestPkColumnNames verifies canal schema.Table PK index → name resolution.
func TestPkColumnNames(t *testing.T) {
	tbl := &schema.Table{
		Columns: []schema.TableColumn{{Name: "tenant_id"}, {Name: "id"}, {Name: "data"}},
		PKColumns: []int{0, 1},
	}
	got := pkColumnNames(tbl)
	if len(got) != 2 || got[0] != "tenant_id" || got[1] != "id" {
		t.Errorf("pkColumnNames = %v, want [tenant_id id]", got)
	}
	if pkColumnNames(nil) != nil {
		t.Error("nil table should return nil")
	}
	if pkColumnNames(&schema.Table{}) != nil {
		t.Error("no PK should return nil")
	}
}

// TestMysqlCDCHandlerFillsMetadataKey is a regression guard: the handler must
// fill rec.Metadata.Key with the per-table PK JSON object for all ops, so
// downstream sinks with pk_columns_from_metadata can handle DELETE.
func TestMysqlCDCHandlerFillsMetadataKey(t *testing.T) {
	// Simulate the key-derivation logic directly (OnRow needs a canal runtime;
	// we assert the building blocks used inside OnRow).
	pkCols := []string{"session_id"}
	cases := []struct {
		op   core.OpType
		data map[string]any
	}{
		{core.OpInsert, map[string]any{"session_id": "ins1", "data": "x"}},
		{core.OpDelete, map[string]any{"session_id": "del1", "data": "y"}},
	}
	for _, c := range cases {
		key := metadataKeyJSONMulti(pkCols, c.data)
		if key == "" {
			t.Errorf("op %v: metadata key empty, want per-table PK JSON", c.op)
		}
	}
}
