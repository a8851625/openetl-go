package sink

import (
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// TestDorisResolveTableStatic verifies the legacy static-table behavior is
// preserved when table_template is not configured: the configured table wins,
// falling back to record metadata table.
func TestDorisResolveTableStatic(t *testing.T) {
	s := &DorisSink{name: "doris", table: "orders"}

	got, err := s.resolveTable(core.Record{Metadata: core.Metadata{Table: "irrelevant"}})
	if err != nil {
		t.Fatalf("resolveTable: %v", err)
	}
	if got != "orders" {
		t.Errorf("static table = %q, want orders", got)
	}

	// Empty static table falls back to record metadata table.
	s2 := &DorisSink{name: "doris"}
	got2, err := s2.resolveTable(core.Record{Metadata: core.Metadata{Table: "users"}})
	if err != nil {
		t.Fatalf("resolveTable fallback: %v", err)
	}
	if got2 != "users" {
		t.Errorf("metadata fallback = %q, want users", got2)
	}
}

// TestDorisResolveTableTemplate verifies table_template substitution from
// record metadata. This is the core of multi-table fan-out (e.g. a kafka
// source with format=envelope feeding multiple Doris tables from one sink).
func TestDorisResolveTableTemplate(t *testing.T) {
	cases := []struct {
		name     string
		template string
		rec      core.Record
		want     string
		wantErr  bool
	}{
		{
			name:     "table placeholder",
			template: "ods_{table}",
			rec:      core.Record{Metadata: core.Metadata{Table: "orders"}},
			want:     "ods_orders",
		},
		{
			name:     "db and table placeholders",
			template: "{db}_{table}",
			rec:      core.Record{Metadata: core.Metadata{Database: "shop", Table: "orders"}},
			want:     "shop_orders",
		},
		{
			name:     "literal template",
			template: "all_events",
			rec:      core.Record{Metadata: core.Metadata{Table: "orders"}},
			want:     "all_events",
		},
		{
			name:     "missing table metadata",
			template: "ods_{table}",
			rec:      core.Record{Metadata: core.Metadata{}},
			wantErr:  true,
		},
		{
			name:     "missing db metadata",
			template: "{db}_{table}",
			rec:      core.Record{Metadata: core.Metadata{Table: "orders"}},
			wantErr:  true,
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := &DorisSink{name: "doris", tableTemplate: c.template}
			got, err := s.resolveTable(c.rec)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error, got %q", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("resolveTable: %v", err)
			}
			if got != c.want {
				t.Errorf("got %q, want %q", got, c.want)
			}
		})
	}
}

// TestDorisResolveTableTemplateTakesPrecedenceOverStatic ensures that when
// both table and table_template are configured, the template wins (it is the
// more specific multi-table intent). This matches the kafka sink's
// topic_template precedence over static topic.
func TestDorisResolveTableTemplateTakesPrecedenceOverStatic(t *testing.T) {
	s := &DorisSink{name: "doris", table: "fallback", tableTemplate: "ods_{table}"}
	got, err := s.resolveTable(core.Record{Metadata: core.Metadata{Table: "orders"}})
	if err != nil {
		t.Fatalf("resolveTable: %v", err)
	}
	if got != "ods_orders" {
		t.Errorf("got %q, template should win over static table", got)
	}
}

// TestDorisConfigParsesTableTemplate verifies NewDorisSink reads table_template
// from the config map.
func TestDorisConfigParsesTableTemplate(t *testing.T) {
	s, err := NewDorisSink(map[string]any{
		"host":          "fe",
		"database":      "ods",
		"table_template": "ods_{table}",
	})
	if err != nil {
		t.Fatalf("NewDorisSink: %v", err)
	}
	if s.tableTemplate != "ods_{table}" {
		t.Errorf("tableTemplate = %q, want ods_{table}", s.tableTemplate)
	}
}
