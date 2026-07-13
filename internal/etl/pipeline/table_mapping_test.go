package pipeline

import (
	"context"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestMapTableTemplateAndRules(t *testing.T) {
	tm := &TableMapping{
		Template: "ods_{source_db}__{source_table}",
		Rules: map[string]string{
			"products": "ods_products_special",
		},
	}
	if got := tm.MapTableWithDB("dzh3136_go", "customers"); got != "ods_dzh3136_go__customers" {
		t.Fatalf("template map = %q", got)
	}
	if got := tm.MapTableWithDB("dzh3136_go", "products"); got != "ods_products_special" {
		t.Fatalf("rule map = %q", got)
	}
}

func TestTableMappingProcessorPreservesSourceFields(t *testing.T) {
	p := NewTableMappingProcessor(&TableMapping{
		RegexPatterns: []TableRegexPattern{
			{Pattern: "^(.*)$", Replacement: "ods_$1"},
		},
	})
	rec := core.Record{
		Operation: core.OpInsert,
		Data:      map[string]any{"id": 1},
		Metadata:  core.Metadata{Database: "src_db", Table: "orders"},
	}
	out, err := p.Process(context.Background(), rec)
	if err != nil {
		t.Fatalf("Process: %v", err)
	}
	if out.Metadata.Table != "ods_orders" {
		t.Fatalf("mapped table = %q", out.Metadata.Table)
	}
	if out.Data["_source_table"] != "orders" {
		t.Fatalf("_source_table = %#v", out.Data["_source_table"])
	}
	if out.Data["_source_database"] != "src_db" {
		t.Fatalf("_source_database = %#v", out.Data["_source_database"])
	}
}

func TestBuildProcessorsIncludesTableMapping(t *testing.T) {
	spec := &Spec{
		Name: "demo",
		TableMapping: &TableMapping{
			Template: "tgt_{source_table}",
		},
	}
	chain := BuildProcessors(spec)
	if len(chain) != 1 {
		t.Fatalf("chain len = %d, want 1", len(chain))
	}
	if chain[0].Name() != "table_mapping" {
		t.Fatalf("processor = %q", chain[0].Name())
	}
}
