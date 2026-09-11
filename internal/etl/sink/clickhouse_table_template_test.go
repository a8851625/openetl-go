package sink

import (
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// TestClickHouseResolveTableStatic verifies the legacy static-table behavior
// is preserved when table_template is not configured: the configured table
// wins, falling back to record metadata table.
func TestClickHouseResolveTableStatic(t *testing.T) {
	s := &ClickHouseSink{name: "clickhouse", table: "orders"}

	got, err := s.resolveTable(core.Record{Metadata: core.Metadata{Table: "irrelevant"}})
	if err != nil {
		t.Fatalf("resolveTable: %v", err)
	}
	if got != "orders" {
		t.Errorf("static table = %q, want orders", got)
	}

	s2 := &ClickHouseSink{name: "clickhouse"}
	got2, err := s2.resolveTable(core.Record{Metadata: core.Metadata{Table: "users"}})
	if err != nil {
		t.Fatalf("resolveTable fallback: %v", err)
	}
	if got2 != "users" {
		t.Errorf("metadata fallback = %q, want users", got2)
	}
}

// TestClickHouseResolveTableTemplate verifies table_template substitution
// from record metadata — the core of multi-table fan-out (e.g. a kafka source
// with format=envelope feeding multiple ClickHouse tables from one sink).
func TestClickHouseResolveTableTemplate(t *testing.T) {
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
			s := &ClickHouseSink{name: "clickhouse", tableTemplate: c.template}
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

// TestClickHouseResolveTableTemplateTakesPrecedenceOverStatic ensures that
// when both table and table_template are configured, the template wins.
func TestClickHouseResolveTableTemplateTakesPrecedenceOverStatic(t *testing.T) {
	s := &ClickHouseSink{name: "clickhouse", table: "fallback", tableTemplate: "ods_{table}"}
	got, err := s.resolveTable(core.Record{Metadata: core.Metadata{Table: "orders"}})
	if err != nil {
		t.Fatalf("resolveTable: %v", err)
	}
	if got != "ods_orders" {
		t.Errorf("got %q, template should win over static table", got)
	}
}

// TestClickHouseConfigParsesMultiTableFields verifies NewClickHouseSink reads
// table_template and pk_columns_from_metadata from the config map.
func TestClickHouseConfigParsesMultiTableFields(t *testing.T) {
	s, err := NewClickHouseSink(map[string]any{
		"host":                     "ch",
		"database":                 "ods",
		"table_template":           "ods_{table}",
		"pk_columns_from_metadata": true,
	})
	if err != nil {
		t.Fatalf("NewClickHouseSink: %v", err)
	}
	if s.tableTemplate != "ods_{table}" {
		t.Errorf("tableTemplate = %q, want ods_{table}", s.tableTemplate)
	}
	if !s.pkColumnsFromMetadata {
		t.Error("pkColumnsFromMetadata = false, want true")
	}
}

// TestClickHousePKColumnsByTable derives heterogeneous per-table key columns
// from envelope metadata keys (orders.order_id BIGINT vs users.user_no
// VARCHAR) routed through a table template.
func TestClickHousePKColumnsByTable(t *testing.T) {
	s := &ClickHouseSink{
		name:                  "clickhouse",
		database:              "ods",
		tableTemplate:         "ods_{table}",
		pkColumnsFromMetadata: true,
	}
	records := []core.Record{
		{
			Operation: core.OpInsert,
			Data:      map[string]any{"order_id": int64(1)},
			Metadata:  core.Metadata{Table: "orders", Key: `{"order_id":1}`, PrimaryKeyColumns: []string{"order_id"}},
		},
		{
			Operation: core.OpInsert,
			Data:      map[string]any{"user_no": "u1"},
			Metadata:  core.Metadata{Table: "users", Key: `{"user_no":"u1"}`, PrimaryKeyColumns: []string{"user_no"}},
		},
	}
	pkByTable, err := s.pkColumnsByTable(records)
	if err != nil {
		t.Fatalf("pkColumnsByTable: %v", err)
	}
	// Target table key (from template) and source metadata key both resolve.
	if got := pkByTable["ods_orders"]; !sameIdentifierSet(got, []string{"order_id"}) {
		t.Errorf("ods_orders pk = %v, want [order_id]", got)
	}
	if got := pkByTable["users"]; !sameIdentifierSet(got, []string{"user_no"}) {
		t.Errorf("users pk = %v, want [user_no]", got)
	}
}

// TestClickHousePKColumnsByTableRejectsKeyChange verifies a batch whose key
// columns change for the same table is rejected instead of mixing schemes.
func TestClickHousePKColumnsByTableRejectsKeyChange(t *testing.T) {
	s := &ClickHouseSink{name: "clickhouse", database: "ods", pkColumnsFromMetadata: true}
	records := []core.Record{
		{
			Operation: core.OpInsert,
			Data:      map[string]any{"order_id": int64(1)},
			Metadata:  core.Metadata{Table: "orders", Key: `{"order_id":1}`, PrimaryKeyColumns: []string{"order_id"}},
		},
		{
			Operation: core.OpInsert,
			Data:      map[string]any{"order_id": int64(2), "tenant_id": int64(9)},
			Metadata:  core.Metadata{Table: "orders", Key: `{"order_id":2,"tenant_id":9}`, PrimaryKeyColumns: []string{"order_id", "tenant_id"}},
		},
	}
	if _, err := s.pkColumnsByTable(records); err == nil {
		t.Fatal("expected error for changed key columns within one batch")
	}
}

// TestClickHousePKColumnsByTableRejectsEmptyKeyDespiteStaticFallback verifies
// ordinary metadata-PK traffic cannot use static pk_columns to guess identity.
func TestClickHousePKColumnsByTableRejectsEmptyKeyDespiteStaticFallback(t *testing.T) {
	s := &ClickHouseSink{name: "clickhouse", database: "ods", pkColumnsFromMetadata: true, pkColumns: []string{"strategy_id", "service_city_id"}}
	records := []core.Record{
		{
			Operation: core.OpInsert,
			Data:      map[string]any{"strategy_id": int64(24), "service_city_id": int64(2)},
			Metadata:  core.Metadata{Table: "surcharge", PrimaryKeyColumns: []string{"strategy_id", "service_city_id"}},
		},
	}
	if _, err := s.pkColumnsByTable(records); err == nil || core.ClassifyError(err) != core.ErrorClassData || !strings.Contains(err.Error(), string(core.RecordIdentityReasonKeyMissing)) {
		t.Fatalf("empty metadata key error=%v, want fail-closed data/key_missing", err)
	}
}

// TestClickHouseCompactUsesPerTablePK verifies batch compaction keys come from
// metadata-derived per-table PKs so mixed tables collapse on their own keys.
func TestClickHouseCompactUsesPerTablePK(t *testing.T) {
	s := &ClickHouseSink{name: "clickhouse", database: "ods", tableTemplate: "ods_{table}", pkColumnsFromMetadata: true}
	records := []core.Record{
		{
			Operation: core.OpInsert,
			Data:      map[string]any{"order_id": int64(1), "amount": 11.0},
			Metadata:  core.Metadata{Table: "orders", Key: `{"order_id":1}`, PrimaryKeyColumns: []string{"order_id"}},
		},
		{
			Operation: core.OpInsert,
			Data:      map[string]any{"user_no": "u1", "city": "sz"},
			Metadata:  core.Metadata{Table: "users", Key: `{"user_no":"u1"}`, PrimaryKeyColumns: []string{"user_no"}},
		},
		{
			Operation: core.OpUpdate,
			Before:    map[string]any{"order_id": int64(1), "amount": 11.0},
			Data:      map[string]any{"order_id": int64(1), "amount": 15.0},
			Metadata: core.Metadata{Table: "orders", Key: `{"order_id":1}`, PrimaryKeyColumns: []string{"order_id"},
				FormatContractID: core.FormatContractOpenETLEnvelopeV1},
		},
	}
	pkByTable, err := s.pkColumnsByTable(records)
	if err != nil {
		t.Fatalf("pkColumnsByTable: %v", err)
	}
	s.pkByTable = pkByTable
	compacted := CompactRecordsByPK(records, func(table string) []string {
		if pk, ok := s.pkByTable[table]; ok {
			return pk
		}
		if len(s.pkColumns) > 0 {
			return s.pkColumns
		}
		return []string{"id"}
	})
	if len(compacted) != 2 {
		t.Fatalf("compacted = %d records, want 2 (orders collapsed onto its final update)", len(compacted))
	}
	var ordersFinal *core.Record
	for i := range compacted {
		if compacted[i].Metadata.Table == "orders" {
			ordersFinal = &compacted[i]
		}
	}
	if ordersFinal == nil || ordersFinal.Operation != core.OpUpdate {
		t.Fatalf("orders record missing or not collapsed to final update: %+v", compacted)
	}
}
