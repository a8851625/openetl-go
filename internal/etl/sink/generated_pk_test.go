package sink

import (
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

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
