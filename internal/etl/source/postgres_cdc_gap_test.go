package source

import (
	"encoding/json"
	"testing"
)

// GAP-1 regression: pgCatalog PK cache + recordKey derivation incl. composite
// PKs, and columnTypes from RELATION-learned OIDs.
func TestPGCatalogRecordKeyComposite(t *testing.T) {
	c := newPGCatalog()
	c.setTablePKs("surcharge_strategy_to_service_city", []string{"strategy_id", "service_city_id"})
	r := &pgCDCReader{catalog: c}

	data := map[string]any{"strategy_id": int64(24), "service_city_id": int64(2), "note": "x"}
	key := r.recordKey("surcharge_strategy_to_service_city", data, nil)
	if key == "" {
		t.Fatal("composite key not derived")
	}
	var obj map[string]any
	if err := json.Unmarshal([]byte(key), &obj); err != nil {
		t.Fatalf("key not JSON object: %v", err)
	}
	if len(obj) != 2 || obj["strategy_id"] == nil || obj["service_city_id"] == nil {
		t.Fatalf("key = %v", obj)
	}

	// UPDATE prefers before-image
	before := map[string]any{"strategy_id": int64(99), "service_city_id": int64(8)}
	key2 := r.recordKey("surcharge_strategy_to_service_city", data, before)
	json.Unmarshal([]byte(key2), &obj)
	if obj["strategy_id"].(float64) != 99 {
		t.Fatalf("update must prefer before image, got %v", obj)
	}

	// unknown table -> empty (no key), pre-existing behavior
	if k := r.recordKey("nope", data, nil); k != "" {
		t.Fatalf("unknown table must yield empty key, got %q", k)
	}
}

func TestPGCatalogColumnTypes(t *testing.T) {
	c := newPGCatalog()
	c.setRelation(42, "orders", []pgColumnInfo{
		{Name: "id", TypeOID: 20},       // int8 -> bigint
		{Name: "name", TypeOID: 1043},   // varchar
		{Name: "amount", TypeOID: 1700}, // numeric
		{Name: "ts", TypeOID: 1114},     // timestamp
		{Name: "flag", TypeOID: 16},     // bool
	})
	ct := c.columnTypes(42)
	if ct["id"] != "bigint" || ct["name"] != "varchar" || ct["amount"] != "numeric" || ct["ts"] != "timestamp" || ct["flag"] != "boolean" {
		t.Fatalf("columnTypes = %v", ct)
	}
	if c.columnTypes(999) != nil {
		t.Fatal("unknown relation must yield nil")
	}
	if pgTypeName(12345) != "oid_12345" {
		t.Fatalf("unknown oid fallback = %q", pgTypeName(12345))
	}
}
