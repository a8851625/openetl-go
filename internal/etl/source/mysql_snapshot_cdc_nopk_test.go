package source

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestNewMySQLSnapshotCDCSourceParsesNoPKMode(t *testing.T) {
	base := func(extra map[string]any) map[string]any {
		cfg := map[string]any{
			"host":     "mysql",
			"user":     "etl",
			"database": "shop",
			"tables":   []any{"orders"},
		}
		for k, v := range extra {
			cfg[k] = v
		}
		return cfg
	}
	s, err := NewMySQLSnapshotCDCSource(base(nil))
	if err != nil {
		t.Fatalf("NewMySQLSnapshotCDCSource: %v", err)
	}
	if s.noPKSnapshotMode != noPKSnapshotSkip {
		t.Fatalf("default mode = %q, want skip", s.noPKSnapshotMode)
	}
	s, err = NewMySQLSnapshotCDCSource(base(map[string]any{"no_pk_snapshot": " HASH "}))
	if err != nil {
		t.Fatalf("NewMySQLSnapshotCDCSource(hash): %v", err)
	}
	if s.noPKSnapshotMode != noPKSnapshotHash {
		t.Fatalf("mode = %q, want hash", s.noPKSnapshotMode)
	}
	if _, err := NewMySQLSnapshotCDCSource(base(map[string]any{"no_pk_snapshot": "synthetic"})); err == nil ||
		!strings.Contains(err.Error(), "no_pk_snapshot") {
		t.Fatalf("err = %v, want no_pk_snapshot rejection", err)
	}
}

func TestResolveTablePKsHashModeAcceptsKeylessTable(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := newPKTestSource("shop", []string{"audit_log"}, "", nil, false)
	s.noPKSnapshotMode = noPKSnapshotHash
	expectPKDetection(mock, "shop", "audit_log") // no single-column PK
	expectCompositePK(mock, "shop", "audit_log") // no composite PK either

	out, err := s.resolveTablePKs(context.Background(), db)
	if err != nil {
		t.Fatalf("resolveTablePKs: %v", err)
	}
	if out["audit_log"].kind != pkKindRowHash {
		t.Fatalf("kind = %v, want pkKindRowHash", out["audit_log"].kind)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet: %v", err)
	}
}

func TestResolveTablePKsSkipModeStillFailsClosed(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	s := newPKTestSource("shop", []string{"audit_log"}, "", nil, false)
	expectPKDetection(mock, "shop", "audit_log")
	expectCompositePK(mock, "shop", "audit_log")

	if _, err := s.resolveTablePKs(context.Background(), db); err == nil ||
		!strings.Contains(err.Error(), "no usable primary key") {
		t.Fatalf("err = %v, want fail-closed message in skip mode", err)
	}
}

func TestSnapshotRowHashIsContentStable(t *testing.T) {
	cols := []string{"b", "a"}
	row := map[string]any{"a": int64(1), "b": "x"}

	// Column ordering from information_schema must not change the hash.
	h1 := snapshotRowHash(cols, row)
	h2 := snapshotRowHash([]string{"a", "b"}, row)
	if h1 != h2 {
		t.Fatalf("hash depends on column order: %s vs %s", h1, h2)
	}
	if len(h1) != 64 {
		t.Fatalf("hash = %q, want 64 hex chars", h1)
	}
	// Different content, different hash.
	if h1 == snapshotRowHash(cols, map[string]any{"a": int64(1), "b": "y"}) {
		t.Fatal("hash must change with content")
	}
	// NULL must not collide with the empty string.
	nullHash := snapshotRowHash([]string{"a"}, map[string]any{"a": nil})
	emptyHash := snapshotRowHash([]string{"a"}, map[string]any{"a": ""})
	if nullHash == emptyHash {
		t.Fatal("NULL and empty string must hash differently")
	}
	// The synthetic column itself is excluded so re-reading a sink row (which
	// now carries _row_hash) cannot change the key.
	withSynthetic := map[string]any{"a": int64(1), "b": "x", snapshotRowHashColumn: "ignored"}
	if snapshotRowHash(append(cols, snapshotRowHashColumn), withSynthetic) != h1 {
		t.Fatal("the synthetic key column must be excluded from its own hash")
	}
}

func TestSnapshotTableRowHashEmitsSyntheticKeys(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()

	// Order matters: Begin, then column types, then the table scan.
	mock.ExpectBegin()
	mock.ExpectQuery("SELECT column_name, column_type FROM information_schema.columns").
		WithArgs("shop", "audit_log").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "column_type"}).
			AddRow("a", "bigint").
			AddRow("b", "varchar(8)"))
	mock.ExpectQuery("SELECT \\* FROM `audit_log`").
		WillReturnRows(sqlmock.NewRows([]string{"a", "b"}).
			AddRow(int64(1), "x").
			AddRow(int64(2), "y").
			AddRow(int64(2), "y")) // duplicate content collapses to the same key

	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()

	src := &MySQLSnapshotCDCSource{name: "mysql_snapshot_cdc", database: "shop"}
	r := &snapshotCDCReader{
		source:               src,
		records:              make(chan core.Record, 8),
		snapshotHandoffFile:  "mysql-bin.000001",
		snapshotHandoffPos:   4,
		snapshotHandoffValid: true,
	}
	if err := r.snapshotTableRowHash(context.Background(), tx, "audit_log"); err != nil {
		t.Fatalf("snapshotTableRowHash: %v", err)
	}
	close(r.records)
	var got []core.Record
	for rec := range r.records {
		got = append(got, rec)
	}
	if len(got) != 3 {
		t.Fatalf("records = %d, want 3", len(got))
	}
	seen := map[string]struct{}{}
	for i, rec := range got {
		if rec.Operation != core.OpInsert {
			t.Fatalf("record %d op = %v", i, rec.Operation)
		}
		hash, ok := rec.Data[snapshotRowHashColumn].(string)
		if !ok || len(hash) != 64 {
			t.Fatalf("record %d missing synthetic key: %v", i, rec.Data[snapshotRowHashColumn])
		}
		if len(rec.Metadata.PrimaryKeyColumns) != 1 || rec.Metadata.PrimaryKeyColumns[0] != snapshotRowHashColumn {
			t.Fatalf("record %d primary key columns = %v", i, rec.Metadata.PrimaryKeyColumns)
		}
		if !strings.Contains(rec.Metadata.Key, snapshotRowHashColumn) {
			t.Fatalf("record %d metadata key = %q", i, rec.Metadata.Key)
		}
		if rec.Metadata.SourcePhase != core.SourcePhaseSnapshot {
			t.Fatalf("record %d phase = %q", i, rec.Metadata.SourcePhase)
		}
		if rec.Metadata.SnapshotHandoffFile != "mysql-bin.000001" || rec.Metadata.SnapshotHandoffPos != 4 {
			t.Fatalf("record %d handoff = %s:%d", i, rec.Metadata.SnapshotHandoffFile, rec.Metadata.SnapshotHandoffPos)
		}
		if rec.Metadata.Cursor != hash {
			t.Fatalf("record %d cursor = %q, want the row hash", i, rec.Metadata.Cursor)
		}
		if rec.Metadata.ColumnTypes["a"] != "bigint" {
			t.Fatalf("record %d column types = %v", i, rec.Metadata.ColumnTypes)
		}
		seen[hash] = struct{}{}
	}
	if len(seen) != 2 {
		t.Fatalf("distinct keys = %d, want 2 (identical rows share a key)", len(seen))
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet expectations: %v", err)
	}
}

func TestSnapshotTableRowHashRequiresHandoff(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock: %v", err)
	}
	defer db.Close()
	mock.ExpectBegin()
	tx, err := db.Begin()
	if err != nil {
		t.Fatalf("begin: %v", err)
	}
	defer tx.Rollback()
	r := &snapshotCDCReader{
		source:  &MySQLSnapshotCDCSource{name: "mysql_snapshot_cdc", database: "shop"},
		records: make(chan core.Record, 1),
	}
	if err := r.snapshotTableRowHash(context.Background(), tx, "audit_log"); err == nil ||
		!strings.Contains(err.Error(), "handoff position") {
		t.Fatalf("err = %v, want missing-handoff rejection", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("no query should have run: %v", err)
	}
}

func TestCheckpointFromRecordsHandlesRowHashTables(t *testing.T) {
	// Regression: a keyless snapshot record must produce a checkpoint instead of
	// failing with "cannot determine snapshot cursor", which used to block the
	// checkpoint and fail the whole pipeline.
	src := &MySQLSnapshotCDCSource{name: "mysql_snapshot_cdc", database: "shop", table: "audit_log"}
	r := &snapshotCDCReader{
		source:               src,
		checkpointPhase:      "snapshot",
		checkpointFile:       "mysql-bin.000001",
		checkpointPos:        4,
		snapshotHandoffFile:  "mysql-bin.000001",
		snapshotHandoffPos:   4,
		snapshotHandoffValid: true,
		resolvedPKs:          map[string]resolvedPK{"audit_log": {kind: pkKindRowHash}},
	}
	rec := core.Record{
		Operation: core.OpInsert,
		Data:      map[string]any{"actor": "alice", snapshotRowHashColumn: "abc123"},
		Metadata: core.Metadata{
			Table:               "audit_log",
			SourcePhase:         core.SourcePhaseSnapshot,
			SnapshotHandoffFile: "mysql-bin.000001",
			SnapshotHandoffPos:  4,
			PrimaryKeyColumns:   []string{snapshotRowHashColumn},
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
	if pos.Phase != "snapshot" || pos.File != "mysql-bin.000001" || pos.Pos != 4 {
		t.Fatalf("position = %+v", pos)
	}
	if len(pos.LastIDs) != 0 || len(pos.LastStrs) != 0 || len(pos.LastCompositeCursors) != 0 {
		t.Fatalf("a row-hash table must not persist a cursor: %+v", pos)
	}

	// A row-hash record without the synthetic key is a programming error and
	// must still fail loudly.
	rec.Data = map[string]any{"actor": "alice"}
	if _, err := r.CheckpointForRecords(context.Background(), []core.Record{rec}); err == nil ||
		!strings.Contains(err.Error(), snapshotRowHashColumn) {
		t.Fatalf("err = %v, want missing synthetic key rejection", err)
	}
}
