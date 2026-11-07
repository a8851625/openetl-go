package source

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"

	"github.com/gogf/gf/v2/frame/g"
)

// SnapshotTableCoverage describes how one source table will participate in
// the snapshot phase of a mysql_snapshot_cdc pipeline.
type SnapshotTableCoverage struct {
	Table string   `json:"table"`
	Kind  string   `json:"kind"` // numeric | ordered | composite | row_hash | none
	Key   []string `json:"key,omitempty"`
}

// SnapshotCoverage is the preflight-visible snapshot plan: which tables the
// snapshot will page (and with which cursor kind) and which tables will be
// skipped (no usable snapshot key; CDC still applies). Preflight surfaces
// this so "existing data missing" is visible before start, not only in
// runtime logs.
type SnapshotCoverage struct {
	TablesTotal       int                     `json:"tables_total"`
	TablesSnapshotted int                     `json:"tables_snapshotted"`
	TablesRowHash     int                     `json:"tables_snapshotted_by_row_hash"`
	TablesSkipped     int                     `json:"tables_skipped_no_pk"`
	SkippedTables     []string                `json:"skipped_tables,omitempty"`
	Entries           []SnapshotTableCoverage `json:"entries"`
}

// ProbeSnapshotCoverage resolves the snapshot key plan for the given tables
// (or the whole database when tables is ["*"]) using exactly the runtime
// resolution order (explicit pk_columns > global pk_column > single-column
// PRIMARY > composite PRIMARY), so the report cannot drift from behavior.
// The connection is used read-only; the caller owns closing it.
// noPKMode mirrors source config no_pk_snapshot: "hash" turns keyless tables
// into row-hash snapshots instead of skipping them, so the plan must say so.
func ProbeSnapshotCoverage(ctx context.Context, db *sql.DB, database string, tables []string, pkColumn string, pkColumns map[string]string, noPKMode string) (*SnapshotCoverage, error) {
	if db == nil {
		return nil, fmt.Errorf("ProbeSnapshotCoverage: nil db")
	}
	if len(tables) == 1 && tables[0] == "*" {
		listed, err := listMySQLTables(ctx, db, database)
		if err != nil {
			return nil, err
		}
		if len(listed) == 0 {
			return nil, fmt.Errorf("database %s has no base tables", database)
		}
		tables = listed
	}
	cov := &SnapshotCoverage{Entries: make([]SnapshotTableCoverage, 0, len(tables))}
	for _, t := range tables {
		rpk, err := resolveSnapshotTableKey(ctx, db, database, t, pkColumn, pkColumns)
		if err != nil {
			return nil, err
		}
		entry := SnapshotTableCoverage{Table: t}
		if rpk.kind == pkKindNone && strings.EqualFold(strings.TrimSpace(noPKMode), noPKSnapshotHash) {
			entry.Kind = "row_hash"
			entry.Key = []string{snapshotRowHashColumn}
			cov.TablesRowHash++
			cov.TablesSnapshotted++
			cov.Entries = append(cov.Entries, entry)
			continue
		}
		switch rpk.kind {
		case pkKindNumeric:
			entry.Kind = "numeric"
			entry.Key = []string{rpk.column}
			cov.TablesSnapshotted++
		case pkKindOrdered:
			entry.Kind = "ordered"
			entry.Key = []string{rpk.column}
			cov.TablesSnapshotted++
		case pkKindComposite:
			entry.Kind = "composite"
			entry.Key = rpk.columns
			cov.TablesSnapshotted++
		default:
			entry.Kind = "none"
			cov.TablesSkipped++
			cov.SkippedTables = append(cov.SkippedTables, t)
		}
		cov.Entries = append(cov.Entries, entry)
	}
	sort.Strings(cov.SkippedTables)
	g.Log().Debugf(ctx, "mysql_snapshot_cdc coverage probe: %d tables, %d snapshotted, %d skipped (no usable key)", len(tables), cov.TablesSnapshotted, cov.TablesSkipped)
	return cov, nil
}

// SnapshotRowHashColumn is the synthetic key column used for
// no_pk_snapshot=hash tables. Exported so preflight and docs name the same
// column the runtime writes.
const SnapshotRowHashColumn = snapshotRowHashColumn
