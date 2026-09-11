package sink

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func TestClickHouseVersionModeConfig(t *testing.T) {
	defaultSink, err := NewClickHouseSink(map[string]any{})
	if err != nil {
		t.Fatalf("NewClickHouseSink(default): %v", err)
	}
	if defaultSink.versionMode != clickHouseVersionModeSourceOrder || defaultSink.versionCol != "_version" || defaultSink.deleteCol != "_is_deleted" {
		t.Fatalf("source-order defaults = mode=%q version=%q delete=%q", defaultSink.versionMode, defaultSink.versionCol, defaultSink.deleteCol)
	}

	appendSink, err := NewClickHouseSink(map[string]any{"version_mode": "append"})
	if err != nil {
		t.Fatalf("NewClickHouseSink(append): %v", err)
	}
	if appendSink.versionMode != clickHouseVersionModeAppend {
		t.Fatalf("versionMode = %q, want append", appendSink.versionMode)
	}

	for name, config := range map[string]map[string]any{
		"unknown_mode":      {"version_mode": "clock"},
		"empty_version":     {"version_column": ""},
		"empty_delete":      {"delete_column": ""},
		"reserved_conflict": {"version_column": "event_order", "delete_column": "event_order"},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := NewClickHouseSink(config); err == nil {
				t.Fatalf("NewClickHouseSink(%v) succeeded, want error", config)
			}
		})
	}
}

func TestClickHousePrepareUsesSourceOrderAndPreservesOutOfOrderReplay(t *testing.T) {
	sink := mustClickHouseVersionSink(t, map[string]any{"pk_columns": []string{"id"}})
	newer := mysqlCDCClickHouseRecord(core.OpUpdate, "mysql-bin.000009", 900, map[string]any{"id": int64(7), "value": "new"}, map[string]any{"value": "old"})
	older := mysqlCDCClickHouseRecord(core.OpUpdate, "mysql-bin.000008", 800, map[string]any{"id": int64(7), "value": "old"}, map[string]any{"value": "older"})

	prepared, err := sink.prepareDataRecords([]core.Record{newer, older})
	if err != nil {
		t.Fatalf("prepareDataRecords: %v", err)
	}
	if len(prepared) != 2 {
		t.Fatalf("len(prepared) = %d, want both source versions (no arrival-order compaction)", len(prepared))
	}
	newOrder := core.SourceOrder(newer)
	oldOrder := core.SourceOrder(older)
	if got := prepared[0].Data["_version"]; got != newOrder.Version {
		t.Fatalf("new version = %#v, want %d", got, newOrder.Version)
	}
	if got := prepared[1].Data["_version"]; got != oldOrder.Version {
		t.Fatalf("old replay version = %#v, want %d", got, oldOrder.Version)
	}
	if oldOrder.Version >= newOrder.Version {
		t.Fatalf("source order inverted: old=%d new=%d", oldOrder.Version, newOrder.Version)
	}
	if prepared[0].Data["_is_deleted"] != uint8(0) || prepared[1].Data["_is_deleted"] != uint8(0) {
		t.Fatalf("live rows do not carry _is_deleted=0: %#v", prepared)
	}
	if _, exists := newer.Data["_version"]; exists {
		t.Fatal("prepareDataRecords mutated the caller's record")
	}

	// Timestamp changes and a new sink instance cannot affect the version.
	newer.Metadata.Timestamp = time.Unix(1, 0)
	restarted := mustClickHouseVersionSink(t, map[string]any{"pk_columns": []string{"id"}})
	again, err := restarted.prepareDataRecords([]core.Record{newer})
	if err != nil {
		t.Fatalf("prepare after restart: %v", err)
	}
	if again[0].Data["_version"] != newOrder.Version {
		t.Fatalf("version changed across clock/restart: got=%v want=%d", again[0].Data["_version"], newOrder.Version)
	}
}

func TestClickHouseCheckpointResetSnapshotUsesHandoffOrder(t *testing.T) {
	sink := mustClickHouseVersionSink(t, map[string]any{"pk_columns": []string{"id"}})
	priorCDC := mysqlCDCClickHouseRecord(core.OpUpdate, "mysql-bin.000009", 900,
		map[string]any{"id": int64(7), "value": "before-reset"}, map[string]any{"value": "old"})
	resetSnapshot := core.Record{
		Operation: core.OpInsert,
		Data:      map[string]any{"id": int64(7), "value": "after-reset"},
		Metadata: core.Metadata{
			SourceType: core.SourceTypeMySQLSnapshotCDC, SourcePhase: core.SourcePhaseSnapshot,
			Table: "orders", Cursor: "customer-z", CursorKind: core.SourceCursorOrdered,
			SnapshotHandoffFile: "mysql-bin.000010", SnapshotHandoffPos: 4,
		},
	}

	preparedPrior, err := sink.prepareDataRecords([]core.Record{priorCDC})
	if err != nil {
		t.Fatalf("prepare prior CDC: %v", err)
	}
	preparedReset, err := sink.prepareDataRecords([]core.Record{resetSnapshot})
	if err != nil {
		t.Fatalf("prepare reset snapshot: %v", err)
	}
	priorVersion := preparedPrior[0].Data["_version"].(uint64)
	resetVersion := preparedReset[0].Data["_version"].(uint64)
	if resetVersion <= priorVersion {
		t.Fatalf("reset snapshot version %d must supersede prior CDC %d", resetVersion, priorVersion)
	}
}

func TestClickHousePrepareDeleteAndKeyChangeTombstones(t *testing.T) {
	sink := mustClickHouseVersionSink(t, map[string]any{"pk_columns": []string{"tenant_id", "id"}})
	update := mysqlCDCClickHouseRecord(core.OpUpdate, "mysql-bin.000010", 88,
		map[string]any{"tenant_id": "acme", "id": int64(2), "value": "new"},
		map[string]any{"id": int64(1), "value": "old"})
	prepared, err := sink.prepareDataRecords([]core.Record{update})
	if err != nil {
		t.Fatalf("prepare key-changing update: %v", err)
	}
	if len(prepared) != 2 {
		t.Fatalf("len(prepared) = %d, want live row + old-key tombstone", len(prepared))
	}
	live, tombstone := prepared[0], prepared[1]
	if live.Operation != core.OpUpdate || live.Data["id"] != int64(2) || live.Data["_is_deleted"] != uint8(0) {
		t.Fatalf("live row = %#v", live)
	}
	if tombstone.Operation != core.OpDelete || tombstone.Data["tenant_id"] != "acme" || tombstone.Data["id"] != int64(1) || tombstone.Data["_is_deleted"] != uint8(1) {
		t.Fatalf("old-key tombstone = %#v", tombstone)
	}
	if live.Data["_version"] != tombstone.Data["_version"] {
		t.Fatalf("key-change versions differ: live=%v tombstone=%v", live.Data["_version"], tombstone.Data["_version"])
	}

	deleteRecord := mysqlCDCClickHouseRecord(core.OpDelete, "mysql-bin.000011", 99,
		map[string]any{"tenant_id": "acme", "id": int64(2), "value": "new"}, nil)
	deleted, err := sink.prepareDataRecords([]core.Record{deleteRecord})
	if err != nil {
		t.Fatalf("prepare delete: %v", err)
	}
	if len(deleted) != 1 || deleted[0].Data["_is_deleted"] != uint8(1) {
		t.Fatalf("delete tombstone = %#v", deleted)
	}
}

func TestClickHousePrepareNoKeyChangeDoesNotCreateTombstone(t *testing.T) {
	sink := mustClickHouseVersionSink(t, map[string]any{"pk_columns": []string{"id"}})
	record := mysqlCDCClickHouseRecord(core.OpUpdate, "mysql-bin.000010", 100,
		map[string]any{"id": int64(5), "value": "new"},
		map[string]any{"value": "old"})
	prepared, err := sink.prepareDataRecords([]core.Record{record})
	if err != nil {
		t.Fatalf("prepareDataRecords: %v", err)
	}
	if len(prepared) != 1 || prepared[0].Data["_is_deleted"] != uint8(0) {
		t.Fatalf("unchanged key unexpectedly expanded: %#v", prepared)
	}
}

func TestClickHousePrepareFailsClosedWithoutNumericSourceOrder(t *testing.T) {
	tests := []struct {
		name string
		rec  core.Record
		want string
	}{
		{
			name: "file_has_no_order",
			rec: core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1}, Metadata: core.Metadata{
				SourceType: core.SourceTypeFile, Table: "orders",
			}},
			want: "metadata.source_type and a connector-owned durable position",
		},
		{
			name: "text_batch_cursor_has_no_uint64_version",
			rec: core.Record{Operation: core.OpInsert, Data: map[string]any{"id": "A"}, Metadata: core.Metadata{
				SourceType: core.SourceTypeMySQLBatch, SourcePhase: core.SourcePhaseBatch,
				Cursor: "A", CursorKind: core.SourceCursorOrdered, Table: "orders",
			}},
			want: "metadata.cursor and metadata.cursor_kind=numeric",
		},
		{
			name: "snapshot_handoff_missing",
			rec: core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1}, Metadata: core.Metadata{
				SourceType: core.SourceTypeMySQLSnapshotCDC, SourcePhase: core.SourcePhaseSnapshot,
				Cursor: "1", CursorKind: core.SourceCursorNumeric, Table: "orders",
			}},
			want: "metadata.snapshot_handoff_file and metadata.snapshot_handoff_pos",
		},
		{
			name: "mysql_position_missing",
			rec: core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1}, Metadata: core.Metadata{
				SourceType: core.SourceTypeMySQLCDC, Table: "orders",
			}},
			want: "metadata.binlog_file and metadata.binlog_pos",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sink := mustClickHouseVersionSink(t, map[string]any{"pk_columns": []string{"id"}})
			_, err := sink.prepareDataRecords([]core.Record{test.rec})
			if err == nil || !strings.Contains(err.Error(), test.want) || !strings.Contains(err.Error(), "do not fall back to wall clock") {
				t.Fatalf("error = %v, want actionable missing metadata", err)
			}
		})
	}
}

func TestClickHouseAppendModeIsExplicitlyInsertOnly(t *testing.T) {
	sink := mustClickHouseVersionSink(t, map[string]any{"version_mode": "append"})
	insert := core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1}, Metadata: core.Metadata{SourceType: core.SourceTypeFile, Table: "events"}}
	prepared, err := sink.prepareDataRecords([]core.Record{insert})
	if err != nil {
		t.Fatalf("append insert: %v", err)
	}
	if len(prepared) != 1 {
		t.Fatalf("append records = %d, want 1", len(prepared))
	}
	if _, exists := prepared[0].Data["_version"]; exists {
		t.Fatalf("append mode fabricated source order: %#v", prepared[0].Data)
	}

	update := insert
	update.Operation = core.OpUpdate
	if _, err := sink.prepareDataRecords([]core.Record{update}); err == nil || !strings.Contains(err.Error(), "INSERT-only") {
		t.Fatalf("append UPDATE error = %v", err)
	}
}

func TestValidateClickHouseSourceOrderSchema(t *testing.T) {
	validColumns := []clickhouseColumn{
		{Name: "id", Type: "Int64"},
		{Name: "_version", Type: "UInt64"},
		{Name: "_is_deleted", Type: "UInt8"},
	}
	for _, engineFull := range []string{
		"ReplacingMergeTree(_version, _is_deleted)",
		"ReplacingMergeTree(_version, _is_deleted) ORDER BY (id, tenant_id) SETTINGS index_granularity = 8192",
		"ReplicatedReplacingMergeTree('/clickhouse/{shard}/t', '{replica}', `_version`, `_is_deleted`)",
	} {
		engine := "ReplacingMergeTree"
		if strings.HasPrefix(engineFull, "Replicated") {
			engine = "ReplicatedReplacingMergeTree"
		}
		if err := validateClickHouseSourceOrderSchema(engine, engineFull, validColumns, "_version", "_is_deleted"); err != nil {
			t.Fatalf("valid schema %q: %v", engineFull, err)
		}
	}

	tests := []struct {
		name       string
		engine     string
		engineFull string
		columns    []clickhouseColumn
		want       string
	}{
		{name: "legacy_int64", engine: "ReplacingMergeTree", engineFull: "ReplacingMergeTree(_version)", columns: []clickhouseColumn{{Name: "_version", Type: "Int64"}}, want: "want writable UInt64"},
		{name: "single_argument_rmt", engine: "ReplacingMergeTree", engineFull: "ReplacingMergeTree(_version)", columns: validColumns, want: "final arguments"},
		{name: "plain_merge_tree", engine: "MergeTree", engineFull: "MergeTree", columns: validColumns, want: "not ReplacingMergeTree-compatible"},
		{name: "wrong_delete_type", engine: "ReplacingMergeTree", engineFull: "ReplacingMergeTree(_version, _is_deleted)", columns: []clickhouseColumn{{Name: "_version", Type: "UInt64"}, {Name: "_is_deleted", Type: "Int8"}}, want: "want writable UInt8"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := validateClickHouseSourceOrderSchema(test.engine, test.engineFull, test.columns, "_version", "_is_deleted")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestClickHouseReservedVersionPreservesFullUInt64(t *testing.T) {
	sink := mustClickHouseVersionSink(t, nil)
	rec := core.Record{Data: map[string]any{"_version": uint64(math.MaxUint64), "_is_deleted": uint8(1)}}
	for _, httpValue := range []bool{false, true} {
		got, err := sink.clickHouseColumnValue(rec, clickhouseColumn{Name: "_version", Type: "UInt64"}, httpValue)
		if err != nil {
			t.Fatalf("clickHouseColumnValue(http=%v): %v", httpValue, err)
		}
		if got != uint64(math.MaxUint64) {
			t.Fatalf("version(http=%v) = %#v (%T), want MaxUint64", httpValue, got, got)
		}
	}
	encoded, err := json.Marshal(map[string]any{"_version": uint64(math.MaxUint64)})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), "18446744073709551615") {
		t.Fatalf("JSON version lost precision: %s", encoded)
	}
}

func mustClickHouseVersionSink(t *testing.T, config map[string]any) *ClickHouseSink {
	t.Helper()
	if config == nil {
		config = map[string]any{}
	}
	sink, err := NewClickHouseSink(config)
	if err != nil {
		t.Fatalf("NewClickHouseSink: %v", err)
	}
	return sink
}

func mysqlCDCClickHouseRecord(op core.OpType, file string, pos uint32, data, before map[string]any) core.Record {
	return core.Record{
		Operation: op,
		Data:      data,
		Before:    before,
		Metadata: core.Metadata{
			Source: "orders-reader", SourceType: core.SourceTypeMySQLCDC, SourcePhase: core.SourcePhaseCDC,
			Database: "shop", Table: "orders", BinlogFile: file, BinlogPos: pos,
		},
	}
}
