package sink

import (
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// GAP-3: postgres sink pk_columns_from_metadata derives per-table PKs from
// JSON-object Metadata.Key (multi-table fan-out), falling back to static
// pk_columns then id.
func TestPostgresDerivePKFromMetadataShared(t *testing.T) {
	records := []core.Record{
		{Operation: core.OpInsert, Data: map[string]any{"other": 1}, Metadata: core.Metadata{Table: "other_table", Key: `{"x":1}`}},
		{Operation: core.OpDelete, Data: map[string]any{}, Metadata: core.Metadata{Table: "surcharge", Key: `{"strategy_id":24,"service_city_id":2}`}},
		{Operation: core.OpInsert, Data: map[string]any{"id": 1}, Metadata: core.Metadata{Table: "nokey"}},
		{Operation: core.OpInsert, Data: map[string]any{"id": 1}, Metadata: core.Metadata{Table: "scalarkey", Key: `"plain-string-key"`}},
	}
	if pk := derivePKFromMetadataShared("surcharge", records); len(pk) != 2 || pk[0] != "service_city_id" || pk[1] != "strategy_id" {
		t.Fatalf("composite pk = %v", pk)
	}
	if pk := derivePKFromMetadataShared("nokey", records); pk != nil {
		t.Fatalf("no-key table = %v, want nil", pk)
	}
	if pk := derivePKFromMetadataShared("scalarkey", records); pk != nil {
		t.Fatalf("scalar key = %v, want nil", pk)
	}
}

// Config parsing: pk_columns_from_metadata accepted.
func TestPostgresPKFromMetadataConfig(t *testing.T) {
	s, err := NewPostgresSink(map[string]any{
		"host": "pg", "database": "d", "table": "t",
		"pk_columns": []any{"a"}, "pk_columns_from_metadata": true,
	})
	if err != nil {
		t.Fatalf("config: %v", err)
	}
	if !s.pkColumnsFromMetadata || len(s.pkColumns) != 1 {
		t.Fatalf("fields = %v/%v", s.pkColumnsFromMetadata, s.pkColumns)
	}
	s2, _ := NewPostgresSink(map[string]any{"host": "pg", "database": "d", "table": "t"})
	if s2.pkColumnsFromMetadata {
		t.Fatal("default must be off")
	}
}
