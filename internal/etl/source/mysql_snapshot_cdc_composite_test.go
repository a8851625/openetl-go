package source

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/a8851625/openetl-go/internal/etl/core"
)

// --- T6.1: row-value comparison SQL construction -------------------------

func TestCompositeCursorQuery(t *testing.T) {
	cases := []struct {
		name     string
		table    string
		cols     []string
		lastVals []any
		limit    int
		wantQ    string
		wantArgs []any
	}{
		{
			name:     "first page two int columns",
			table:    "category_description",
			cols:     []string{"category_id", "language_id"},
			lastVals: nil,
			limit:    2,
			wantQ:    "SELECT * FROM `category_description` ORDER BY `category_id`,`language_id` LIMIT 2",
			wantArgs: nil,
		},
		{
			name:     "paged two int columns",
			table:    "category_description",
			cols:     []string{"category_id", "language_id"},
			lastVals: []any{int64(101), int64(1)},
			limit:    2,
			wantQ:    "SELECT * FROM `category_description` WHERE (`category_id`,`language_id`) > (?,?) ORDER BY `category_id`,`language_id` LIMIT 2",
			wantArgs: []any{int64(101), int64(1)},
		},
		{
			name:     "paged int plus varchar column",
			table:    "entity_map",
			cols:     []string{"entity_id", "code"},
			lastVals: []any{float64(7), "en"},
			limit:    100,
			wantQ:    "SELECT * FROM `entity_map` WHERE (`entity_id`,`code`) > (?,?) ORDER BY `entity_id`,`code` LIMIT 100",
			wantArgs: []any{float64(7), "en"},
		},
		{
			name:     "three column key",
			table:    "acl",
			cols:     []string{"a", "b", "c"},
			lastVals: []any{int64(1), int64(2), int64(3)},
			limit:    50,
			wantQ:    "SELECT * FROM `acl` WHERE (`a`,`b`,`c`) > (?,?,?) ORDER BY `a`,`b`,`c` LIMIT 50",
			wantArgs: []any{int64(1), int64(2), int64(3)},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			q, args := compositeCursorQuery(c.table, c.cols, c.lastVals, c.limit)
			if q != c.wantQ {
				t.Fatalf("query = %q, want %q", q, c.wantQ)
			}
			if len(args) != len(c.wantArgs) {
				t.Fatalf("args = %v, want %v", args, c.wantArgs)
			}
			for i := range args {
				if args[i] != c.wantArgs[i] {
					t.Fatalf("args[%d] = %v (%T), want %v (%T)", i, args[i], args[i], c.wantArgs[i], c.wantArgs[i])
				}
			}
		})
	}
}

// TestCompositeCursorQueryParameterOrderMatchesOrderBy guards the plan.md
// delivery constraint: parameter order must equal the ORDER BY column order.
func TestCompositeCursorQueryParameterOrderMatchesOrderBy(t *testing.T) {
	cols := []string{"entity_id", "language_id", "store_id"}
	q, args := compositeCursorQuery("t", cols, []any{int64(9), int64(2), int64(4)}, 10)
	if !strings.Contains(q, "ORDER BY `entity_id`,`language_id`,`store_id`") {
		t.Fatalf("ORDER BY column order wrong: %q", q)
	}
	if !strings.Contains(q, "WHERE (`entity_id`,`language_id`,`store_id`) > (?,?,?)") {
		t.Fatalf("row-value predicate column order wrong: %q", q)
	}
	if len(args) != 3 || args[0] != int64(9) || args[1] != int64(2) || args[2] != int64(4) {
		t.Fatalf("args = %v, want [9 2 4]", args)
	}
}

func TestCompositeCursorOrderable(t *testing.T) {
	yes := []string{"tinyint", "smallint", "mediumint", "int", "integer", "bigint", "char", "varchar", "binary", "varbinary", "date", "datetime", "timestamp"}
	for _, dt := range yes {
		if !compositeCursorOrderable(dt) {
			t.Errorf("compositeCursorOrderable(%q) = false, want true", dt)
		}
	}
	no := []string{"blob", "text", "json", "decimal", "float", "double", "enum", "set", "bit", "year", "geometry", ""}
	for _, dt := range no {
		if compositeCursorOrderable(dt) {
			t.Errorf("compositeCursorOrderable(%q) = true, want false", dt)
		}
	}
}

// --- T6.1: composite tuple comparison -------------------------------------

func TestCompositeTupleGreater(t *testing.T) {
	i := func(n int64) any { return n }
	cases := []struct {
		name string
		a, b []any
		want bool
	}{
		{"equal int pair", []any{i(1), i(2)}, []any{i(1), i(2)}, false},
		{"second column decides", []any{i(1), i(3)}, []any{i(1), i(2)}, true},
		{"first column decides gt", []any{i(2), i(0)}, []any{i(1), i(9)}, true},
		{"first column decides lt", []any{i(0), i(9)}, []any{i(1), i(0)}, false},
		{"prefix equal shorter b", []any{i(1)}, []any{i(1), i(2)}, false},
		{"string second column", []any{i(5), "b"}, []any{i(5), "a"}, true},
		{"string second column eq", []any{i(5), "b"}, []any{i(5), "b"}, false},
		{"mixed int string vs float", []any{float64(3), "x"}, []any{i(3), "x"}, false},
		{"large int64 exact", []any{i(9223372036854775807), i(1)}, []any{i(9223372036854775806), i(9)}, true},
		{"large uint64 exact", []any{uint64(18446744073709551615)}, []any{uint64(18446744073709551614)}, true},
		{"datetime string form", []any{"2026-01-02 00:00:00.000000", i(1)}, []any{"2026-01-01 23:59:59.999999", i(9)}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := compositeTupleGreater(c.a, c.b); got != c.want {
				t.Fatalf("compositeTupleGreater(%v, %v) = %v, want %v", c.a, c.b, got, c.want)
			}
		})
	}
}

func TestCompositeTupleCursorString(t *testing.T) {
	data := map[string]any{"entity_id": int64(7), "language_id": int64(1), "name": "x"}
	got := compositeTupleCursorString([]string{"entity_id", "language_id"}, data)
	want := `["7","1"]`
	if got != want {
		t.Fatalf("cursor = %q, want %q", got, want)
	}
	// A NULL key column yields no cursor rather than a bogus tuple.
	nilData := map[string]any{"entity_id": nil, "language_id": int64(1)}
	if got := compositeTupleCursorString([]string{"entity_id", "language_id"}, nilData); got != "" {
		t.Fatalf("nil key cursor = %q, want empty", got)
	}
}

func TestSnapshotMetadataKeyComposite(t *testing.T) {
	rpk := resolvedPK{column: "category_id", columns: []string{"category_id", "language_id"}, kind: pkKindComposite}
	data := map[string]any{"category_id": int64(42), "language_id": int64(1), "name": "Tools"}
	key := snapshotMetadataKey(rpk, data)
	// Same JSON shape as the CDC phase's metadataKeyJSONMulti so downstream
	// RMT merges line up across the snapshot->CDC handoff.
	want := `{"category_id":42,"language_id":1}`
	if key != want {
		t.Fatalf("key = %q, want %q", key, want)
	}
	cols := snapshotPrimaryKeyColumns(rpk)
	if len(cols) != 2 || cols[0] != "category_id" || cols[1] != "language_id" {
		t.Fatalf("primaryKeyColumns = %v", cols)
	}
	// Single-column kinds keep the legacy declaration.
	single := snapshotPrimaryKeyColumns(resolvedPK{column: "id", kind: pkKindNumeric})
	if len(single) != 1 || single[0] != "id" {
		t.Fatalf("single primaryKeyColumns = %v", single)
	}
}

// --- T6.2: checkpoint serialization / restore / legacy compat -------------

func newCompositeCheckpointReader() *snapshotCDCReader {
	return &snapshotCDCReader{
		source: &MySQLSnapshotCDCSource{
			name:     "mysql_snapshot_cdc",
			table:    "category_description",
			database: "shop",
		},
		phase:                "snapshot",
		checkpointPhase:      "snapshot",
		checkpointFile:       "binlog.000001",
		checkpointPos:        4,
		snapshotHandoffFile:  "binlog.000001",
		snapshotHandoffPos:   4,
		snapshotHandoffValid: true,
		tableLastIDs:         map[string]int64{},
		tableLastStr:         map[string]string{},
		tableLastComposite:   map[string][]any{},
		resolvedPKs: map[string]resolvedPK{
			"category_description": {column: "category_id", columns: []string{"category_id", "language_id"}, kind: pkKindComposite},
		},
	}
}

func TestSnapshotCheckpointPersistsCompositeCursorAfterDurableAck(t *testing.T) {
	r := newCompositeCheckpointReader()
	rec := core.Record{
		Data: map[string]any{"category_id": int64(101), "language_id": int64(2)},
		Metadata: core.Metadata{
			Table: "category_description",
			// Composite snapshot rows also carry the multi-column PK metadata
			// (T6.3 seam): the checkpoint builder must consume Data columns.
			PrimaryKeyColumns: []string{"category_id", "language_id"},
		},
	}
	cp, err := r.CheckpointForRecords(context.Background(), []core.Record{rec})
	if err != nil {
		t.Fatalf("CheckpointForRecords: %v", err)
	}
	var pos snapshotCDCPosition
	if err := json.Unmarshal(cp.Position, &pos); err != nil {
		t.Fatalf("decode checkpoint: %v", err)
	}
	raw, ok := pos.LastCompositeCursors["category_description"]
	if !ok {
		t.Fatalf("composite cursor missing: %+v", pos)
	}
	var tuple []json.RawMessage
	if err := json.Unmarshal(raw, &tuple); err != nil || len(tuple) != 2 {
		t.Fatalf("composite tuple = %s: %v", raw, err)
	}
	// Durable state unchanged before ack.
	if len(r.tableLastComposite["category_description"]) != 0 {
		t.Fatalf("durable composite cursor mutated before AckCheckpoint: %v", r.tableLastComposite)
	}
	if err := r.AckCheckpoint(context.Background(), cp); err != nil {
		t.Fatalf("AckCheckpoint: %v", err)
	}
	acked := r.tableLastComposite["category_description"]
	if len(acked) != 2 || acked[0] != float64(101) || acked[1] != float64(2) {
		t.Fatalf("durable composite cursor after ack = %v (%T)", acked, acked)
	}
}

func TestSnapshotCheckpointCompositeCursorOnlyAdvancesForward(t *testing.T) {
	// An out-of-order (replayed) record must not rewind the durable cursor.
	r := newCompositeCheckpointReader()
	r.tableLastComposite["category_description"] = []any{float64(200), float64(5)}
	rec := core.Record{
		Data:     map[string]any{"category_id": int64(101), "language_id": int64(2)},
		Metadata: core.Metadata{Table: "category_description"},
	}
	cp, err := r.CheckpointForRecords(context.Background(), []core.Record{rec})
	if err != nil {
		t.Fatalf("CheckpointForRecords: %v", err)
	}
	var pos snapshotCDCPosition
	if err := json.Unmarshal(cp.Position, &pos); err != nil {
		t.Fatalf("decode: %v", err)
	}
	var tuple []float64
	raw := pos.LastCompositeCursors["category_description"]
	if err := json.Unmarshal(raw, &tuple); err != nil {
		t.Fatalf("tuple decode: %v", err)
	}
	if len(tuple) != 2 || tuple[0] != 200 || tuple[1] != 5 {
		t.Fatalf("cursor rewound: %v", tuple)
	}
}

func TestSnapshotCheckpointLegacyWithoutCompositeFieldRoundTrips(t *testing.T) {
	// A legacy checkpoint (no last_composite_cursors) must decode cleanly and
	// produce an identical position when re-marshaled (byte-level omitempty).
	legacy := []byte(`{"phase":"snapshot","last_id":7,"last_ids":{"orders":7},"file":"binlog.000001","pos":4}`)
	pos, err := decodeSnapshotCDCCheckpointPosition(legacy)
	if err != nil {
		t.Fatalf("decode legacy: %v", err)
	}
	if len(pos.LastCompositeCursors) != 0 {
		t.Fatalf("legacy composite cursors = %v, want empty", pos.LastCompositeCursors)
	}
	out, err := json.Marshal(pos)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var a, b map[string]json.RawMessage
	_ = json.Unmarshal(legacy, &a)
	_ = json.Unmarshal(out, &b)
	for k := range a {
		if _, ok := b[k]; !ok {
			t.Fatalf("field %q lost in round trip: %s", k, out)
		}
	}
	if _, present := b["last_composite_cursors"]; present {
		t.Fatalf("last_composite_cursors must stay omitted for legacy positions: %s", out)
	}
}

func TestSnapshotCheckpointCompositeRestoreIntoReader(t *testing.T) {
	// A checkpoint with a composite cursor restores into the reader's durable
	// and read-ahead maps so a restart resumes mid-table without replaying.
	cpPos := snapshotCDCPosition{
		Phase: "snapshot",
		LastCompositeCursors: map[string]json.RawMessage{
			"category_description": json.RawMessage(`[101,2]`),
		},
		File: "binlog.000002",
		Pos:  120,
	}
	raw, err := json.Marshal(cpPos)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	pos, err := decodeSnapshotCDCCheckpointPosition(raw)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	r := newCompositeCheckpointReader()
	// Simulate the Open() restore seam for composite cursors.
	if len(pos.LastCompositeCursors) > 0 {
		restored := make(map[string][]any)
		for table, rawTuple := range pos.LastCompositeCursors {
			var arr []json.RawMessage
			if err := json.Unmarshal(rawTuple, &arr); err != nil || len(arr) == 0 {
				continue
			}
			tuple := make([]any, len(arr))
			for i, el := range arr {
				var s string
				if err := json.Unmarshal(el, &s); err == nil {
					tuple[i] = s
					continue
				}
				var f float64
				if err := json.Unmarshal(el, &f); err == nil {
					tuple[i] = f
				}
			}
			restored[table] = tuple
		}
		r.tableLastComposite = restored
	}
	r.snapshotReadComposite = cloneCompositeCursorMap(r.tableLastComposite)
	tuple := r.getSnapshotReadComposite("category_description", 2)
	if tuple == nil || tuple[0] != float64(101) || tuple[1] != float64(2) {
		t.Fatalf("restored read cursor = %v", tuple)
	}
	// Wrong column-count tuples (schema drift) must not satisfy the cursor.
	if bad := r.getSnapshotReadComposite("category_description", 3); bad != nil {
		t.Fatalf("mismatched col-count tuple = %v, want nil", bad)
	}
	// The paged query must use the restored tuple.
	q, args := compositeCursorQuery("category_description", []string{"category_id", "language_id"}, tuple, 10)
	if !strings.Contains(q, "WHERE (`category_id`,`language_id`) > (?,?)") {
		t.Fatalf("query = %q", q)
	}
	if len(args) != 2 || args[0] != float64(101) || args[1] != float64(2) {
		t.Fatalf("args = %v", args)
	}
}

func TestMarshalCompositeCursorsOmitEmpty(t *testing.T) {
	if out := marshalCompositeCursors(nil); out != nil {
		t.Fatalf("nil in = %v, want nil", out)
	}
	if out := marshalCompositeCursors(map[string][]any{}); out != nil {
		t.Fatalf("empty in = %v, want nil", out)
	}
	in := map[string][]any{"t": {int64(1), "a"}}
	out := marshalCompositeCursors(in)
	if len(out) != 1 || string(out["t"]) != `[1,"a"]` {
		t.Fatalf("out = %v", out)
	}
}

// --- shard degradation -----------------------------------------------------

func TestCompositeCursorDegradesSharding(t *testing.T) {
	// compositeCursorQuery itself never emits a MOD predicate; sharding is
	// disabled at the snapshotTable level for composite keys. Guard the SQL
	// builder so no future change silently reintroduces MOD for composites.
	q, _ := compositeCursorQuery("t", []string{"a", "b"}, []any{int64(1), int64(2)}, 10)
	if strings.Contains(strings.ToUpper(q), "MOD(") {
		t.Fatalf("composite query must not contain MOD sharding: %q", q)
	}
}

// --- snapshotTable composite paging end-to-end against sqlmock ------------

func TestSnapshotTableCompositePaging(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	r := newCompositeCheckpointReader()
	r.db = db
	r.records = make(chan core.Record, 16)
	r.done = make(chan struct{})
	r.source.limit = 2

	rpk := resolvedPK{column: "category_id", columns: []string{"category_id", "language_id"}, kind: pkKindComposite}

	// column types lookup (non-fatal path)
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, column_type FROM information_schema.columns").
		WithArgs("shop", "category_description").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "column_type"}).
			AddRow("category_id", "int(11)").
			AddRow("language_id", "int(11)").
			AddRow("name", "varchar(128)"))

	// Page 1: no cursor yet -> ORDER BY prefix
	mock.ExpectQuery("SELECT \\* FROM `category_description` ORDER BY").
		WillReturnRows(sqlmock.NewRows([]string{"category_id", "language_id", "name"}).
			AddRow(101, 1, "one").
			AddRow(101, 2, "two"))
	// Page 2: cursor (101,2)
	mock.ExpectQuery("SELECT \\* FROM `category_description` WHERE").
		WithArgs(int64(101), int64(2)).
		WillReturnRows(sqlmock.NewRows([]string{"category_id", "language_id", "name"}).
			AddRow(102, 1, "three"))
	// Page 3: cursor (102,1) -> empty, ends the table
	mock.ExpectQuery("SELECT \\* FROM `category_description` WHERE").
		WithArgs(int64(102), int64(1)).
		WillReturnRows(sqlmock.NewRows([]string{"category_id", "language_id", "name"}))

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	mock.ExpectRollback()
	if err := r.snapshotTable(context.Background(), tx, "category_description", rpk); err != nil {
		t.Fatalf("snapshotTable: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback: %v", err)
	}
	close(r.records)
	var records []core.Record
	for rec := range r.records {
		records = append(records, rec)
	}
	if len(records) != 3 {
		t.Fatalf("records = %d, want 3", len(records))
	}
	for _, rec := range records {
		if rec.Metadata.SourcePhase != core.SourcePhaseSnapshot {
			t.Fatalf("phase = %v", rec.Metadata.SourcePhase)
		}
		if len(rec.Metadata.PrimaryKeyColumns) != 2 {
			t.Fatalf("pk columns = %v", rec.Metadata.PrimaryKeyColumns)
		}
		if rec.Metadata.Key == "" {
			t.Fatalf("metadata key missing for %v", rec.Data)
		}
	}
	// Producer read cursor advanced to the last row tuple.
	tuple := r.snapshotReadComposite["category_description"]
	if len(tuple) != 2 || tuple[0] != int64(102) || tuple[1] != int64(1) {
		t.Fatalf("read cursor = %v (%T)", tuple, tuple)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("unmet: %v", err)
	}
}

func TestSnapshotTableCompositeCursorRejectsNullKeyColumn(t *testing.T) {
	// A row whose key column arrives NULL (impossible under a NOT NULL PK,
	// but defensive) must fail loudly instead of silently mispaging.
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	r := newCompositeCheckpointReader()
	r.db = db
	r.records = make(chan core.Record, 4)
	r.done = make(chan struct{})
	r.source.limit = 2
	rpk := resolvedPK{column: "category_id", columns: []string{"category_id", "language_id"}, kind: pkKindComposite}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, column_type FROM information_schema.columns").
		WithArgs("shop", "category_description").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "column_type"}))
	mock.ExpectQuery("SELECT \\* FROM `category_description` ORDER BY").
		WillReturnRows(sqlmock.NewRows([]string{"category_id", "language_id", "name"}).
			AddRow(nil, 1, "bad row"))

	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	err = r.snapshotTable(context.Background(), tx, "category_description", rpk)
	if err == nil || !strings.Contains(err.Error(), "NULL") {
		t.Fatalf("err = %v, want NULL key failure", err)
	}
	_ = tx.Rollback()
}

// --- checkpoint validation of the new field --------------------------------

func TestValidateCheckpointCompositeCursors(t *testing.T) {
	s := &MySQLSnapshotCDCSource{name: "mysql_snapshot_cdc", database: "shop"}
	good := core.Checkpoint{Source: "mysql_snapshot_cdc", Position: []byte(
		`{"phase":"snapshot","file":"mysql-bin.000001","pos":4,"last_composite_cursors":{"t":[1,"a"]}}`)}
	if err := s.ValidateCheckpoint(context.Background(), &good); err != nil {
		t.Fatalf("valid composite checkpoint rejected: %v", err)
	}
	badShape := core.Checkpoint{Source: "mysql_snapshot_cdc", Position: []byte(
		`{"phase":"snapshot","file":"mysql-bin.000001","pos":4,"last_composite_cursors":{"t":{"a":1}}}`)}
	if err := s.ValidateCheckpoint(context.Background(), &badShape); err == nil {
		t.Fatalf("object tuple must be rejected")
	}
	badElement := core.Checkpoint{Source: "mysql_snapshot_cdc", Position: []byte(
		`{"phase":"snapshot","file":"mysql-bin.000001","pos":4,"last_composite_cursors":{"t":[true]}}`)}
	if err := s.ValidateCheckpoint(context.Background(), &badElement); err == nil {
		t.Fatalf("boolean tuple element must be rejected")
	}
}

// _ = time.Time keeps the import list stable if cursorString time cases move.
var _ = time.Now
