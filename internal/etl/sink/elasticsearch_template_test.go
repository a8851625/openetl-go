package sink

import (
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// GAP-4: index_template {table}/{db} substitution with loud failure on
// missing metadata; static-index behavior unchanged when unset.
func TestElasticsearchResolveIndexTemplate(t *testing.T) {
	s := &ElasticsearchSink{name: "es", indexTemplate: "ods_{table}"}
	rec := core.Record{Metadata: core.Metadata{Table: "Orders", Database: "shop"}}
	idx, err := s.resolveIndex(rec)
	if err != nil || idx != "ods_Orders" {
		t.Fatalf("template resolve = %q, %v", idx, err)
	}

	s2 := &ElasticsearchSink{name: "es", indexTemplate: "{db}_{table}"}
	if idx, _ := s2.resolveIndex(rec); idx != "shop_Orders" {
		t.Fatalf("db template = %q", idx)
	}

	// missing table metadata -> error, never a literal "{table}" index
	if _, err := s.resolveIndex(core.Record{Metadata: core.Metadata{}}); err == nil {
		t.Fatal("missing table metadata must fail loudly")
	}

	// pre-existing behavior when template unset
	s3 := &ElasticsearchSink{name: "es", index: "fixed"}
	if idx, _ := s3.resolveIndex(rec); idx != "fixed" {
		t.Fatalf("static index = %q", idx)
	}
	s4 := &ElasticsearchSink{name: "es"}
	if idx, _ := s4.resolveIndex(rec); idx != "orders" {
		t.Fatalf("table fallback = %q", idx)
	}
}
