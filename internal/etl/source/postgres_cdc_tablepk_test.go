package source

import (
	"context"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// TestLoadTablePKsQueryShape pins the primary-key discovery contract that a
// real PostgreSQL 16 instance enforces. The query must reference only aliases
// present in its FROM clause (the previous `rc.relname` selected an unknown
// alias and every lookup failed, leaving records without a metadata key).
func TestLoadTablePKsQueryShape(t *testing.T) {
	src := &PostgresCDCSource{tables: []string{"pg_orders", "analytics.events"}}
	reader := &pgCDCReader{source: src}

	query, args := reader.tablePKQueryForTest()
	for _, alias := range []string{"rc.", "bogus."} {
		if contains(query, alias) {
			t.Fatalf("query selects unknown alias %q: %s", alias, query)
		}
	}
	for _, want := range []string{
		"n.nspname || '.' || c.relname",
		"FROM pg_index x",
		"WHERE x.indisprimary",
		"array_position(x.indkey::int2[], a.attnum::int2)",
		"(n.nspname, c.relname) IN",
	} {
		if !contains(query, want) {
			t.Fatalf("query missing %q: %s", want, query)
		}
	}
	if len(args) != 4 {
		t.Fatalf("expected schema+table args for 2 tables, got %d (%v)", len(args), args)
	}
}

func TestCatalogPrimaryKeysLookup(t *testing.T) {
	catalog := newPGCatalog()
	catalog.setTablePKs("pg_orders", []string{"id"})
	if got := catalog.primaryKeys("pg_orders"); len(got) != 1 || got[0] != "id" {
		t.Fatalf("primaryKeys(pg_orders) = %v", got)
	}
	if got := catalog.primaryKeys("missing"); got != nil {
		t.Fatalf("primaryKeys(missing) = %v, want nil", got)
	}
}

func contains(haystack, needle string) bool {
	return len(needle) == 0 || (len(haystack) >= len(needle) && indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}

// pgTuple builds one pgoutput tuple-data field.
func pgTupleField(tag byte, value string) []byte {
	out := []byte{tag}
	if tag == 't' {
		n := len(value)
		out = append(out, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
		out = append(out, []byte(value)...)
	}
	return out
}

// TestParseUpdateKeyTupleNamesColumnsFromReplicaIdentity pins the 'K' old-tuple
// decoding: pgoutput publishes only replica-identity columns there, so they must
// be named from the relation's key columns rather than the first N columns.
// Before this fix the before image was empty and a metadata-PK sink rejected the
// UPDATE as an incomplete identity.
func TestParseUpdateKeyTupleNamesColumnsFromReplicaIdentity(t *testing.T) {
	reader := &pgCDCReader{
		source:  &PostgresCDCSource{name: "pg", database: "db"},
		records: make(chan core.Record, 4),
		catalog: newPGCatalog(),
		done:    make(chan struct{}),
		ctx:     context.Background(),
	}
	const relID uint32 = 7
	reader.catalog.setRelation(relID, "pg_orders", []pgColumnInfo{
		{Name: "id", TypeOID: 20, Key: true},
		{Name: "name", TypeOID: 25},
		{Name: "amount", TypeOID: 23},
	})

	msg := []byte{byte(relID >> 24), byte(relID >> 16), byte(relID >> 8), byte(relID)}
	msg = append(msg, 'K')
	msg = append(msg, 0, 1) // one key column in the old tuple
	msg = append(msg, pgTupleField('t', "1")...)
	msg = append(msg, 'N')
	msg = append(msg, 0, 3) // three columns in the new tuple
	msg = append(msg, pgTupleField('t', "1")...)
	msg = append(msg, pgTupleField('t', "alpha-v2")...)
	msg = append(msg, pgTupleField('t', "11")...)

	if rest := reader.parseUpdateMsg(msg, "0/1234"); len(rest) != 0 {
		t.Fatalf("parseUpdateMsg left %d unconsumed bytes", len(rest))
	}
	select {
	case rec := <-reader.records:
		if rec.Operation != core.OpUpdate {
			t.Fatalf("operation = %v", rec.Operation)
		}
		if got, want := rec.Before["id"], int64(1); got != want {
			t.Fatalf("before.id = %v, want %v", got, want)
		}
		if _, ok := rec.Before["name"]; ok {
			t.Fatalf("key tuple must not invent non-key columns: %v", rec.Before)
		}
		if rec.Metadata.BeforeImageState != core.BeforeImageStatePartial {
			t.Fatalf("BeforeImageState = %q, want partial", rec.Metadata.BeforeImageState)
		}
		if rec.Data["name"] != "alpha-v2" {
			t.Fatalf("new image = %v", rec.Data)
		}
	default:
		t.Fatal("no record emitted")
	}
}

// TestParseUpdateFullOldTupleMarksFullBeforeImage covers the 'O' (REPLICA
// IDENTITY FULL) case, where the old tuple carries every column.
func TestParseUpdateFullOldTupleMarksFullBeforeImage(t *testing.T) {
	reader := &pgCDCReader{
		source:  &PostgresCDCSource{name: "pg", database: "db"},
		records: make(chan core.Record, 4),
		catalog: newPGCatalog(),
		done:    make(chan struct{}),
		ctx:     context.Background(),
	}
	const relID uint32 = 9
	reader.catalog.setRelation(relID, "pg_orders", []pgColumnInfo{
		{Name: "id", TypeOID: 20},
		{Name: "name", TypeOID: 25},
	})

	msg := []byte{byte(relID >> 24), byte(relID >> 16), byte(relID >> 8), byte(relID)}
	msg = append(msg, 'O')
	msg = append(msg, 0, 2)
	msg = append(msg, pgTupleField('t', "5")...)
	msg = append(msg, pgTupleField('t', "old")...)
	msg = append(msg, 'N')
	msg = append(msg, 0, 2)
	msg = append(msg, pgTupleField('t', "5")...)
	msg = append(msg, pgTupleField('t', "new")...)

	reader.parseUpdateMsg(msg, "0/5678")
	select {
	case rec := <-reader.records:
		if rec.Metadata.BeforeImageState != core.BeforeImageStateFull {
			t.Fatalf("BeforeImageState = %q, want full", rec.Metadata.BeforeImageState)
		}
		if rec.Before["name"] != "old" || rec.Data["name"] != "new" {
			t.Fatalf("before=%v data=%v", rec.Before, rec.Data)
		}
	default:
		t.Fatal("no record emitted")
	}
}

// TestParseRelationMarksReplicaIdentityFlags pins the relation-message flag
// parsing that the key-tuple decoding depends on.
func TestParseRelationMarksReplicaIdentityFlags(t *testing.T) {
	reader := &pgCDCReader{catalog: newPGCatalog()}
	const relID uint32 = 11
	msg := []byte{byte(relID >> 24), byte(relID >> 16), byte(relID >> 8), byte(relID)}
	msg = append(msg, []byte("public\x00pg_orders\x00")...)
	msg = append(msg, 'd')
	msg = append(msg, 0, 2)
	for _, col := range []struct {
		name  string
		flags byte
		oid   uint32
	}{{"id", 1, 20}, {"name", 0, 25}} {
		msg = append(msg, col.flags)
		msg = append(msg, []byte(col.name+"\x00")...)
		msg = append(msg, byte(col.oid>>24), byte(col.oid>>16), byte(col.oid>>8), byte(col.oid))
		msg = append(msg, 0, 0, 0, 0) // type modifier
	}
	reader.parseRelationMsg(msg)

	keys := reader.catalog.keyColumns(relID)
	if len(keys) != 1 || keys[0].Name != "id" {
		t.Fatalf("keyColumns = %v, want [id]", keys)
	}
	if got := reader.catalog.columnTypes(relID); len(got) != 2 {
		t.Fatalf("columnTypes = %v", got)
	}
}

// TestParseUpdateWithoutOldTupleReconstructsKeyBeforeImage pins the replica
// identity 'd' case: pgoutput sends 'N' for the old tuple when the key did not
// change, and the reader must still hand a metadata-PK sink a usable key-only
// before image instead of failing identity as update_before_image_missing.
func TestParseUpdateWithoutOldTupleReconstructsKeyBeforeImage(t *testing.T) {
	reader := &pgCDCReader{
		source:  &PostgresCDCSource{name: "pg", database: "db"},
		records: make(chan core.Record, 4),
		catalog: newPGCatalog(),
		done:    make(chan struct{}),
		ctx:     context.Background(),
	}
	const relID uint32 = 21
	reader.catalog.setRelation(relID, "pg_orders", []pgColumnInfo{
		{Name: "id", TypeOID: 20, Key: true},
		{Name: "name", TypeOID: 25},
		{Name: "amount", TypeOID: 23},
	})
	reader.catalog.setTablePKs("pg_orders", []string{"id"})

	msg := []byte{byte(relID >> 24), byte(relID >> 16), byte(relID >> 8), byte(relID)}
	msg = append(msg, 'N')
	msg = append(msg, 0, 3)
	msg = append(msg, pgTupleField('t', "1")...)
	msg = append(msg, pgTupleField('t', "alpha-v2")...)
	msg = append(msg, pgTupleField('t', "11")...)

	reader.parseUpdateMsg(msg, "0/9abc")
	select {
	case rec := <-reader.records:
		if rec.Metadata.BeforeImageState != core.BeforeImageStatePartial {
			t.Fatalf("BeforeImageState = %q, want partial", rec.Metadata.BeforeImageState)
		}
		if got := rec.Before["id"]; got != int64(1) {
			t.Fatalf("before.id = %v, want 1", got)
		}
		if _, ok := rec.Before["name"]; ok {
			t.Fatalf("reconstructed before image must stay key-only: %v", rec.Before)
		}
		if rec.Metadata.Key != `{"id":1}` {
			t.Fatalf("metadata key = %q", rec.Metadata.Key)
		}
	default:
		t.Fatal("no record emitted")
	}
}

// TestHandleTruncateMsgFailsClosed pins the pgoutput TRUNCATE wire format
// (Int32 relation count, Int8 flags, then Int32 relation IDs). The previous
// parser read the count one byte late, so real TRUNCATE frames always looked
// truncated and were silently dropped instead of failing closed.
func TestHandleTruncateMsgFailsClosed(t *testing.T) {
	reader := &pgCDCReader{
		source:  &PostgresCDCSource{onTruncate: "error"},
		errors:  make(chan error, 4),
		done:    make(chan struct{}),
		ctx:     context.Background(),
		catalog: newPGCatalog(),
	}
	body := []byte{0, 0, 0, 1, 0, 0, 0, 0, 42}
	if rest := reader.handleTruncateMsg(body); len(rest) != 0 {
		t.Fatalf("handleTruncateMsg left %d bytes", len(rest))
	}
	select {
	case err := <-reader.errors:
		if err == nil {
			t.Fatal("expected a TRUNCATE error")
		}
	default:
		t.Fatal("TRUNCATE did not surface an error (policy error must fail closed)")
	}
	select {
	case <-reader.done:
	default:
		t.Fatal("TRUNCATE did not stop the stream")
	}
}

// TestHandleTruncateMsgSkipPolicyContinues covers on_truncate=skip: the message
// is consumed and logged but the stream keeps running.
func TestHandleTruncateMsgSkipPolicyContinues(t *testing.T) {
	reader := &pgCDCReader{
		source:  &PostgresCDCSource{onTruncate: "skip"},
		errors:  make(chan error, 4),
		done:    make(chan struct{}),
		ctx:     context.Background(),
		catalog: newPGCatalog(),
	}
	body := []byte{0, 0, 0, 2, 1, 0, 0, 0, 7, 0, 0, 0, 8}
	if rest := reader.handleTruncateMsg(body); len(rest) != 0 {
		t.Fatalf("handleTruncateMsg left %d bytes", len(rest))
	}
	select {
	case err := <-reader.errors:
		t.Fatalf("skip policy must not error, got %v", err)
	default:
	}
	select {
	case <-reader.done:
		t.Fatal("skip policy must not stop the stream")
	default:
	}
}

// TestHandleTruncateMsgRejectsTruncatedFrame pins that a malformed frame aborts
// parsing instead of mis-skipping into the next message.
func TestHandleTruncateMsgRejectsTruncatedFrame(t *testing.T) {
	reader := &pgCDCReader{
		source:  &PostgresCDCSource{onTruncate: "skip"},
		errors:  make(chan error, 4),
		done:    make(chan struct{}),
		ctx:     context.Background(),
		catalog: newPGCatalog(),
	}
	// Claims 3 relations but only carries one.
	body := []byte{0, 0, 0, 3, 0, 0, 0, 0, 42}
	if rest := reader.handleTruncateMsg(body); len(rest) != 0 {
		t.Fatalf("truncated frame must consume the rest, left %d", len(rest))
	}
}
