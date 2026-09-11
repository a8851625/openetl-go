package sink

import "testing"

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
