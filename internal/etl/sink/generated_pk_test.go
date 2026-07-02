package sink

import (
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func resetPKMetaCache() {
	pkMetaCache.mu.Lock()
	pkMetaCache.pkBy = map[string][]string{}
	pkMetaCache.mu.Unlock()
}

func TestDerivePKFromMetadata(t *testing.T) {
	resetPKMetaCache()
	s := &MySQLSink{pkColumnsFromMetadata: true}
	records := []core.Record{
		{Metadata: core.Metadata{Table: "orders", Key: `{"order_id": 123}`}},
		{Metadata: core.Metadata{Table: "orders", Key: `{"order_id": 456}`}},
	}
	pk := s.derivePKFromMetadata("orders", records)
	if len(pk) != 1 || pk[0] != "order_id" {
		t.Fatalf("got pk=%v, want [order_id]", pk)
	}
}

func TestDerivePKFromMetadataComposite(t *testing.T) {
	resetPKMetaCache()
	s := &MySQLSink{pkColumnsFromMetadata: true}
	records := []core.Record{
		{Metadata: core.Metadata{Table: "t", Key: `{"tenant_id":"x","seq":5}`}},
	}
	pk := s.derivePKFromMetadata("t", records)
	if len(pk) != 2 {
		t.Fatalf("got pk=%v, want 2 cols", pk)
	}
}

func TestDerivePKFromMetadataEmptyKeyFallsBack(t *testing.T) {
	resetPKMetaCache()
	s := &MySQLSink{pkColumnsFromMetadata: true, pkColumns: []string{"id"}}
	records := []core.Record{
		{Metadata: core.Metadata{Table: "t"}}, // no key
	}
	pk := s.derivePKFromMetadata("t", records)
	if pk != nil {
		t.Fatalf("expected nil when no usable key, got %v", pk)
	}
}

func TestDerivePKFromMetadataCachedAcrossCalls(t *testing.T) {
	resetPKMetaCache()
	s := &MySQLSink{pkColumnsFromMetadata: true}
	records := []core.Record{
		{Metadata: core.Metadata{Table: "t", Key: `{"id":1}`}},
	}
	first := s.derivePKFromMetadata("t", records)
	second := s.derivePKFromMetadata("t", nil) // no records second time
	if len(first) == 0 || len(second) == 0 || first[0] != second[0] {
		t.Fatalf("cache miss: first=%v second=%v", first, second)
	}
}

func TestGeneratedColumnsCacheRoundtrip(t *testing.T) {
	c := core.NewSchemaCache()
	gen, ok := c.GeneratedColumns("db.t")
	if ok {
		t.Fatalf("unexpected cache hit on empty cache")
	}
	c.SetGeneratedColumns("db.t", map[string]bool{"computed": true})
	gen, ok = c.GeneratedColumns("db.t")
	if !ok {
		t.Fatalf("missing cache after set")
	}
	if !gen["computed"] {
		t.Fatalf("missing 'computed' in cached set")
	}
}
