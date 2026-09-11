package sink

import (
	"strings"
	"testing"
)

func TestBuildPgCreateTableDDLDerivedPK(t *testing.T) {
	// GAP-3: auto-created tables must carry the derived PRIMARY KEY so the
	// upsert path's ON CONFLICT(per-table pk) has a matching constraint
	// (SQLSTATE 42P10 otherwise). The legacy "id BIGSERIAL" heuristic only
	// applies when no PK is known.
	resolver := func(column string, _ any, _ map[string]string) string { return "text" }
	base := map[string]any{}

	ddl := buildPgCreateTableDDL("public", "ods_orders",
		[]string{"order_id", "amount", "run"}, base, []string{"order_id"}, resolver)
	if !strings.Contains(ddl, `PRIMARY KEY ("order_id")`) {
		t.Errorf("derived PK missing from DDL: %s", ddl)
	}
	if strings.Contains(ddl, "BIGSERIAL") {
		t.Errorf("BIGSERIAL heuristic must not apply when derived PK exists: %s", ddl)
	}

	ddl = buildPgCreateTableDDL("public", "t2",
		[]string{"id", "name"}, base, nil, resolver)
	if !strings.Contains(ddl, `"id" BIGSERIAL PRIMARY KEY`) {
		t.Errorf("legacy id heuristic lost: %s", ddl)
	}

	// PK column not present in columns must be ignored -> fall back to the
	// legacy id heuristic instead of emitting a bogus constraint.
	ddl = buildPgCreateTableDDL("public", "t3",
		[]string{"id", "name"}, base, []string{"ghost"}, resolver)
	if !strings.Contains(ddl, `"id" BIGSERIAL PRIMARY KEY`) {
		t.Errorf("legacy heuristic lost when derived PK invalid: %s", ddl)
	}
}
