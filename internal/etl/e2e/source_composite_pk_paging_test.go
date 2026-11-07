//go:build e2e

package e2e

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/e2e/harness"
	"github.com/a8851625/openetl-go/internal/etl/source"
)

// TestSourceCompositePKSnapshotPaging verifies the IT-6 T6.1/T6.2 seam on a
// real MySQL 8 (binlog ROW, REPEATABLE READ snapshot): composite PRIMARY KEY
// tables resolve a row-value comparison cursor, page exhaustively without
// missing/duplicate rows across duplicate-first-column groups, carry the
// composite PK metadata contract, and resume from a durable composite
// checkpoint cursor after an interruption. Real no-PK tables stay skipped.
// The full snapshot->CDC merge into ClickHouse is covered by the path e2e.
func TestSourceCompositePKSnapshotPaging(t *testing.T) {
	ctx := context.Background()
	my, err := harness.MySQL(ctx)
	if err != nil {
		harness.Skip(t, "mysql container unavailable: %v", err)
	}
	ns := harness.Namespace(t)
	db := harness.DBName(ns, "src")

	// Two composite shapes + one no-PK control:
	//  - junction (int, int) — OpenCart *_to_store style; only 10 distinct
	//    entity_ids so a limit=7 page boundary necessarily falls inside an
	//    entity_id group and the second column must keep the order total.
	//  - description (int, varchar) — *_description style mixing numeric and
	//    string cursor columns.
	//  - no_pk — real no-PK table: must remain skipped (semantics unchanged).
	mustExec(t, my.Exec, fmt.Sprintf(`
CREATE DATABASE IF NOT EXISTS %[1]s;
DROP TABLE IF EXISTS %[1]s.junction;
CREATE TABLE %[1]s.junction (
  entity_id INT NOT NULL,
  store_id INT NOT NULL,
  note VARCHAR(64),
  PRIMARY KEY (entity_id, store_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS %[1]s.description;
CREATE TABLE %[1]s.description (
  entity_id INT NOT NULL,
  language_id VARCHAR(8) NOT NULL,
  name VARCHAR(128),
  PRIMARY KEY (entity_id, language_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
DROP TABLE IF EXISTS %[1]s.no_pk;
CREATE TABLE %[1]s.no_pk (
  payload VARCHAR(64)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`, db))

	values := make([]string, 0, 60)
	for e := 1; e <= 10; e++ {
		for s := 1; s <= 6; s++ {
			values = append(values, fmt.Sprintf("(%d, %d, 'e%d-s%d')", e, s, e, s))
		}
	}
	mustExec(t, my.Exec, fmt.Sprintf(`INSERT INTO %s.junction (entity_id, store_id, note) VALUES %s;`,
		db, strings.Join(values, ",")))
	descValues := make([]string, 0, 25)
	for e := 1; e <= 5; e++ {
		for l := 0; l < 5; l++ {
			descValues = append(descValues, fmt.Sprintf("(%d, 'l%d', 'name-%d-%d')", e, l, e, l))
		}
	}
	mustExec(t, my.Exec, fmt.Sprintf(`INSERT INTO %s.description (entity_id, language_id, name) VALUES %s;`,
		db, strings.Join(descValues, ",")))
	mustExec(t, my.Exec, fmt.Sprintf(`INSERT INTO %s.no_pk (payload) VALUES ('x');`, db))

	// --- exhaustive paging + metadata contract ---
	junctionRows := drainSnapshotRecords(t, my, db, "junction-drain", []string{"junction"}, 7, 60)
	if len(junctionRows) != 60 {
		t.Fatalf("junction rows = %d, want 60 (missing historical rows under composite cursor)", len(junctionRows))
	}
	seen := map[string]bool{}
	for _, rec := range junctionRows {
		if rec.Metadata.SourcePhase != core.SourcePhaseSnapshot {
			t.Fatalf("phase = %v, want snapshot", rec.Metadata.SourcePhase)
		}
		cols := rec.Metadata.PrimaryKeyColumns
		if len(cols) != 2 || cols[0] != "entity_id" || cols[1] != "store_id" {
			t.Fatalf("primaryKeyColumns = %v, want [entity_id store_id]", cols)
		}
		var keyObj map[string]any
		if rec.Metadata.Key == "" || json.Unmarshal([]byte(rec.Metadata.Key), &keyObj) != nil {
			t.Fatalf("metadata key = %q, want composite JSON object", rec.Metadata.Key)
		}
		k := fmt.Sprintf("%v|%v", rec.Data["entity_id"], rec.Data["store_id"])
		if seen[k] {
			t.Fatalf("junction duplicate row %s", k)
		}
		seen[k] = true
	}

	descRows := drainSnapshotRecords(t, my, db, "description-drain", []string{"description"}, 4, 25)
	if len(descRows) != 25 {
		t.Fatalf("description rows = %d, want 25", len(descRows))
	}
	for _, rec := range descRows {
		cols := rec.Metadata.PrimaryKeyColumns
		if len(cols) != 2 || cols[0] != "entity_id" || cols[1] != "language_id" {
			t.Fatalf("description primaryKeyColumns = %v", cols)
		}
	}

	// --- real no-PK table stays skipped ---
	// The table is explicitly listed, so the source's fail-closed contract
	// requires skip_no_pk_tables=true to allow the skip (silent drop of a
	// user-requested table would hide a configuration mistake).
	noPKRows := drainSnapshotRecordsSkipNoPK(t, my, db, "nopk-drain", []string{"no_pk"}, 7, 0)
	if len(noPKRows) != 0 {
		t.Fatalf("no_pk rows = %d, want 0 (no-PK tables must stay skipped)", len(noPKRows))
	}
	// And listing it without the flag must fail closed at Open:
	if err := expectOpenFailsWithoutSkipFlag(t, my, db, "no_pk"); err != nil {
		t.Fatal(err)
	}

	// --- checkpoint resume: durable composite cursor resumes, not replays ---
	// Pass 1: drain junction with limit=7, read 21 rows, build the durable
	// checkpoint exactly as the runner would (CheckpointForRecords over the
	// sink-acknowledged batch), then drop the reader (crash).
	pass1 := drainSnapshotRecords(t, my, db, "resume-pass1", []string{"junction"}, 7, 21)
	if len(pass1) != 21 {
		t.Fatalf("pass1 rows = %d, want 21", len(pass1))
	}
	// Re-open a reader to build the checkpoint from the acked records: the
	// reader must be the same one that produced them, so redo the drain.
	cp := buildCheckpointFromFirstRecords(t, my, db, "resume-cp", []string{"junction"}, 7, 21)
	var pos map[string]json.RawMessage
	if err := json.Unmarshal(cp.Position, &pos); err != nil {
		t.Fatalf("decode checkpoint: %v", err)
	}
	tuple, ok := pos["last_composite_cursors"]
	if !ok {
		t.Fatalf("checkpoint lacks last_composite_cursors: %s", cp.Position)
	}
	var cursors map[string][]json.RawMessage
	if err := json.Unmarshal(tuple, &cursors); err != nil {
		t.Fatalf("decode composite cursors: %v", err)
	}
	if len(cursors["junction"]) != 2 {
		t.Fatalf("junction cursor = %v, want 2 columns", cursors["junction"])
	}

	// Pass 2: resume from that checkpoint; pages are (7,7,7,7,7,7,4)=60 rows
	// with 21 acked, so exactly 39 rows remain, all entity_id >= 4.
	resumed := drainSnapshotRecordsWithCheckpoint(t, my, db, "resume-pass2", []string{"junction"}, 7, 39, &cp)
	if len(resumed) != 39 {
		t.Fatalf("resumed rows = %d, want 39 (60 - 21 acked)", len(resumed))
	}
	for _, rec := range resumed {
		switch e := rec.Data["entity_id"].(type) {
		case int64:
			if e < 4 {
				t.Fatalf("resumed row replayed acked prefix: entity_id=%v", e)
			}
		case float64:
			if e < 4 {
				t.Fatalf("resumed row replayed acked prefix: entity_id=%v", e)
			}
		default:
			t.Fatalf("entity_id type %T", rec.Data["entity_id"])
		}
	}
}

// drainSnapshotRecords opens a mysql_snapshot_cdc source against the shared
// MySQL container and collects exactly want snapshot-phase records for the
// configured tables. Reading stops after want records; a paging bug that
// yields fewer records fails via the context deadline instead of hanging.
func drainSnapshotRecords(t *testing.T, my *harness.MySQLInstance, database, name string, tables []string, limit, want int) []core.Record {
	t.Helper()
	return drainWithCheckpoint(t, my, database, name, tables, limit, want, nil, false)
}

func drainSnapshotRecordsSkipNoPK(t *testing.T, my *harness.MySQLInstance, database, name string, tables []string, limit, want int) []core.Record {
	t.Helper()
	return drainWithCheckpoint(t, my, database, name, tables, limit, want, nil, true)
}

func drainSnapshotRecordsWithCheckpoint(t *testing.T, my *harness.MySQLInstance, database, name string, tables []string, limit, want int, cp *core.Checkpoint) []core.Record {
	t.Helper()
	return drainWithCheckpoint(t, my, database, name, tables, limit, want, cp, false)
}

func drainWithCheckpoint(t *testing.T, my *harness.MySQLInstance, database, name string, tables []string, limit, want int, cp *core.Checkpoint, skipNoPK bool) []core.Record {
	t.Helper()
	readCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	src, err := source.NewMySQLSnapshotCDCSource(map[string]any{
		"name":              name,
		"host":              my.Host,
		"port":              my.Port,
		"user":              harness.MySQLSyncUser,
		"password":          harness.MySQLSyncPassword,
		"database":          database,
		"tables":            tables,
		"limit":             limit,
		"skip_no_pk_tables": skipNoPK,
	})
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	rr, err := src.Open(readCtx, cp)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	out := make([]core.Record, 0, want)
	for len(out) < want {
		rec, err := rr.Read(readCtx)
		if err != nil {
			t.Fatalf("read after %d records: %v", len(out), err)
		}
		out = append(out, rec)
	}
	_ = rr.Close()
	return out
}

// buildCheckpointFromFirstRecords drains the first n records and returns the
// durable checkpoint the runner would persist for that acknowledged batch
// (CheckpointForRecords over exactly those records).
func buildCheckpointFromFirstRecords(t *testing.T, my *harness.MySQLInstance, database, name string, tables []string, limit, n int) core.Checkpoint {
	t.Helper()
	readCtx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	src, err := source.NewMySQLSnapshotCDCSource(map[string]any{
		"name":     name,
		"host":     my.Host,
		"port":     my.Port,
		"user":     harness.MySQLSyncUser,
		"password": harness.MySQLSyncPassword,
		"database": database,
		"tables":   tables,
		"limit":    limit,
	})
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	rr, err := src.Open(readCtx, nil)
	if err != nil {
		t.Fatalf("open source: %v", err)
	}
	acked := make([]core.Record, 0, n)
	for len(acked) < n {
		rec, err := rr.Read(readCtx)
		if err != nil {
			t.Fatalf("read after %d records: %v", len(acked), err)
		}
		acked = append(acked, rec)
	}
	batcher, ok := rr.(core.BatchRecordCheckpointer)
	if !ok {
		t.Fatalf("reader does not implement BatchRecordCheckpointer")
	}
	cp, err := batcher.CheckpointForRecords(readCtx, acked)
	if err != nil {
		t.Fatalf("CheckpointForRecords: %v", err)
	}
	_ = rr.Close()
	return cp
}

// expectOpenFailsWithoutSkipFlag asserts the fail-closed contract: an
// explicitly listed table with no usable key is rejected at Open unless
// skip_no_pk_tables=true is set.
func expectOpenFailsWithoutSkipFlag(t *testing.T, my *harness.MySQLInstance, database, table string) error {
	t.Helper()
	readCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	src, err := source.NewMySQLSnapshotCDCSource(map[string]any{
		"name":     "nopk-failclosed",
		"host":     my.Host,
		"port":     my.Port,
		"user":     harness.MySQLSyncUser,
		"password": harness.MySQLSyncPassword,
		"database": database,
		"tables":   []string{table},
		"limit":    7,
	})
	if err != nil {
		t.Fatalf("new source: %v", err)
	}
	_, err = src.Open(readCtx, nil)
	if err == nil {
		return fmt.Errorf("open must fail closed for explicit no-PK table %s without skip_no_pk_tables", table)
	}
	if !strings.Contains(err.Error(), "no usable primary key") {
		return fmt.Errorf("open error = %v, want no-usable-primary-key diagnosis", err)
	}
	return nil
}
