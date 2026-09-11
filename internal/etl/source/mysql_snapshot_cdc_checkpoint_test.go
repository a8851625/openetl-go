package source

import (
	"context"
	"encoding/json"
	"io"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/a8851625/openetl-go/internal/etl/core"
)

func newSnapshotCheckpointTestReader() *snapshotCDCReader {
	return &snapshotCDCReader{
		source: &MySQLSnapshotCDCSource{
			name:     "mysql_snapshot_cdc",
			table:    "orders",
			database: "shop",
		},
		phase:                "snapshot",
		checkpointPhase:      "snapshot",
		checkpointFile:       "binlog.000001",
		checkpointPos:        4,
		snapshotHandoffFile:  "binlog.000001",
		snapshotHandoffPos:   4,
		snapshotHandoffValid: true,
		tableLastIDs:         map[string]int64{"orders": 1},
		tableLastStr:         map[string]string{"users": "a"},
		snapshotReadIDs:      map[string]int64{"orders": 99},
		snapshotReadStr:      map[string]string{"users": "z"},
		resolvedPKs: map[string]resolvedPK{
			"orders": {column: "id", kind: pkKindNumeric},
			"users":  {column: "user_no", kind: pkKindOrdered},
		},
	}
}

func decodeSnapshotPosition(t *testing.T, cp core.Checkpoint) snapshotCDCPosition {
	t.Helper()
	var pos snapshotCDCPosition
	if err := json.Unmarshal(cp.Position, &pos); err != nil {
		t.Fatalf("decode checkpoint: %v", err)
	}
	return pos
}

func TestSnapshotCheckpointDoesNotUseProducerReadAhead(t *testing.T) {
	r := newSnapshotCheckpointTestReader()
	// Producer completion/runtime phase may run ahead while rows are still
	// buffered. It must not force the durable checkpoint into CDC phase.
	r.phase = "cdc"
	rec := core.Record{
		Data:     map[string]any{"id": int64(2)},
		Metadata: core.Metadata{Table: "orders", Offset: 2},
	}
	cp, err := r.CheckpointForRecords(context.Background(), []core.Record{rec})
	if err != nil {
		t.Fatalf("CheckpointForRecords: %v", err)
	}
	pos := decodeSnapshotPosition(t, cp)
	if pos.LastIDs["orders"] != 2 {
		t.Fatalf("checkpoint cursor = %v, want orders=2", pos.LastIDs)
	}
	if pos.Phase != "snapshot" {
		t.Fatalf("checkpoint phase = %q, want snapshot until a CDC record is acknowledged", pos.Phase)
	}
	if r.tableLastIDs["orders"] != 1 {
		t.Fatalf("durable cursor mutated before AckCheckpoint: %v", r.tableLastIDs)
	}
	if r.snapshotReadIDs["orders"] != 99 {
		t.Fatalf("producer read cursor changed while building checkpoint: %v", r.snapshotReadIDs)
	}
	if err := r.AckCheckpoint(context.Background(), cp); err != nil {
		t.Fatalf("AckCheckpoint: %v", err)
	}
	if r.tableLastIDs["orders"] != 2 {
		t.Fatalf("durable cursor after ack = %v, want orders=2", r.tableLastIDs)
	}
	if r.snapshotReadIDs["orders"] != 99 {
		t.Fatalf("AckCheckpoint rewound producer read cursor: %v", r.snapshotReadIDs)
	}
}

func TestSnapshotCheckpointPersistsStringCursorAfterDurableAck(t *testing.T) {
	r := newSnapshotCheckpointTestReader()
	rec := core.Record{
		Data:     map[string]any{"user_no": "b"},
		Metadata: core.Metadata{Table: "users"},
	}
	cp, err := r.CheckpointForRecords(context.Background(), []core.Record{rec})
	if err != nil {
		t.Fatalf("CheckpointForRecords: %v", err)
	}
	pos := decodeSnapshotPosition(t, cp)
	if pos.LastStrs["users"] != "b" {
		t.Fatalf("string cursor = %q, want b", pos.LastStrs["users"])
	}
	if r.tableLastStr["users"] != "a" {
		t.Fatalf("string durable cursor mutated before ack: %v", r.tableLastStr)
	}
	if err := r.AckCheckpoint(context.Background(), cp); err != nil {
		t.Fatalf("AckCheckpoint: %v", err)
	}
	if r.tableLastStr["users"] != "b" {
		t.Fatalf("string durable cursor after ack = %q, want b", r.tableLastStr["users"])
	}
}

func TestSnapshotCheckpointCoversAllRecordsInMultiTableBatch(t *testing.T) {
	r := newSnapshotCheckpointTestReader()
	records := []core.Record{
		{Data: map[string]any{"id": int64(7)}, Metadata: core.Metadata{Table: "orders", Offset: 7}},
		{Data: map[string]any{"user_no": "u-2"}, Metadata: core.Metadata{Table: "users"}},
	}
	cp, err := r.CheckpointForRecords(context.Background(), records)
	if err != nil {
		t.Fatalf("CheckpointForRecords: %v", err)
	}
	pos := decodeSnapshotPosition(t, cp)
	if pos.LastIDs["orders"] != 7 || pos.LastStrs["users"] != "u-2" {
		t.Fatalf("multi-table checkpoint = ids=%v strs=%v", pos.LastIDs, pos.LastStrs)
	}
}

func TestSnapshotCheckpointRejectsMissingCursorColumn(t *testing.T) {
	r := newSnapshotCheckpointTestReader()
	_, err := r.CheckpointForRecords(context.Background(), []core.Record{{
		Data:     map[string]any{"name": "missing"},
		Metadata: core.Metadata{Table: "orders", Offset: 2},
	}})
	if err == nil || !strings.Contains(err.Error(), "missing cursor column") {
		t.Fatalf("error = %v, want missing cursor column", err)
	}
}

func TestSnapshotCheckpointRejectsInvalidNumericCursor(t *testing.T) {
	r := newSnapshotCheckpointTestReader()
	_, err := r.CheckpointForRecords(context.Background(), []core.Record{{
		Data:     map[string]any{"id": "not-an-integer"},
		Metadata: core.Metadata{Table: "orders"},
	}})
	if err == nil || !strings.Contains(err.Error(), "snapshot cursor orders.id") {
		t.Fatalf("error = %v, want invalid numeric cursor", err)
	}
}

func TestSnapshotCheckpointRejectsNullOrderedCursor(t *testing.T) {
	r := newSnapshotCheckpointTestReader()
	_, err := r.CheckpointForRecords(context.Background(), []core.Record{{
		Data:     map[string]any{"user_no": nil},
		Metadata: core.Metadata{Table: "users"},
	}})
	if err == nil || !strings.Contains(err.Error(), "cursor users.user_no is NULL") {
		t.Fatalf("error = %v, want NULL ordered cursor", err)
	}
}

func TestSnapshotCheckpointTransitionsToCDCFromRecordBoundary(t *testing.T) {
	r := newSnapshotCheckpointTestReader()
	records := []core.Record{
		{Data: map[string]any{"id": int64(2)}, Metadata: core.Metadata{Table: "orders"}},
		{Data: map[string]any{"id": int64(3)}, Metadata: core.Metadata{Table: "orders", BinlogFile: "binlog.000002", BinlogPos: 88}},
	}
	cp, err := r.CheckpointForRecords(context.Background(), records)
	if err != nil {
		t.Fatalf("CheckpointForRecords: %v", err)
	}
	pos := decodeSnapshotPosition(t, cp)
	if pos.Phase != "cdc" || pos.File != "binlog.000002" || pos.Pos != 88 || pos.LastIDs["orders"] != 2 {
		t.Fatalf("CDC checkpoint = %#v, want snapshot cursor 2 and binlog.000002:88", pos)
	}
	if err := r.AckCheckpoint(context.Background(), cp); err != nil {
		t.Fatalf("AckCheckpoint: %v", err)
	}
	file, binlogPos := r.getDurableBinlogPos()
	if file != "binlog.000002" || binlogPos != 88 || r.checkpointPhase != "cdc" {
		t.Fatalf("durable CDC boundary = phase=%s %s:%d", r.checkpointPhase, file, binlogPos)
	}
}

func TestSnapshotCheckpointRejectsMissingHandoff(t *testing.T) {
	r := newSnapshotCheckpointTestReader()
	r.checkpointFile = ""
	r.checkpointPos = 0
	_, err := r.CheckpointForRecords(context.Background(), []core.Record{{
		Data:     map[string]any{"id": int64(2)},
		Metadata: core.Metadata{Table: "orders", Offset: 2},
	}})
	if err == nil || !strings.Contains(err.Error(), "handoff position") {
		t.Fatalf("error = %v, want missing handoff position", err)
	}
	if err := r.AckCheckpoint(context.Background(), core.Checkpoint{Position: []byte(`{"phase":"snapshot"}`)}); err == nil {
		t.Fatal("AckCheckpoint accepted snapshot position without handoff")
	}
}

func TestSnapshotStartPositionReusesRestoredHandoff(t *testing.T) {
	r := newSnapshotCheckpointTestReader()
	r.canal = nil
	got, err := r.snapshotStartPosition()
	if err != nil {
		t.Fatalf("snapshotStartPosition: %v", err)
	}
	if got.Name != "binlog.000001" || got.Pos != 4 {
		t.Fatalf("handoff = %s:%d, want binlog.000001:4", got.Name, got.Pos)
	}
}

func TestResnapshotReplacesOldHandoffAndCheckpointCandidate(t *testing.T) {
	r := newSnapshotCheckpointTestReader()
	r.phase = "cdc"
	r.checkpointPhase = "cdc"
	r.checkpointFile = "mysql-bin.000009"
	r.checkpointPos = 900
	r.snapshotHandoffFile = "mysql-bin.000001"
	r.snapshotHandoffPos = 4
	r.snapshotHandoffValid = true

	r.requestResnapshot()
	if r.phase != "snapshot" || !r.resnapshotRequested {
		t.Fatalf("resnapshot state = phase=%q requested=%v", r.phase, r.resnapshotRequested)
	}
	if _, _, valid := r.getSnapshotHandoffPosition(); valid {
		t.Fatal("resnapshot retained the old source-order handoff")
	}
	// Durable cursors remain available for the documented resume-from-last-key
	// policy; only the new in-memory checkpoint candidate changes here.
	if r.tableLastIDs["orders"] != 1 || r.tableLastStr["users"] != "a" {
		t.Fatalf("resnapshot discarded durable table cursors: ids=%v strs=%v", r.tableLastIDs, r.tableLastStr)
	}

	r.installSnapshotHandoff("mysql-bin.000010", 4)
	file, pos, valid := r.getSnapshotHandoffPosition()
	if !valid || file != "mysql-bin.000010" || pos != 4 {
		t.Fatalf("new handoff = %s:%d valid=%v", file, pos, valid)
	}
	checkpointFile, checkpointPos := r.getDurableBinlogPos()
	if r.checkpointPhase != "snapshot" || checkpointFile != file || checkpointPos != pos {
		t.Fatalf("checkpoint candidate = phase=%q %s:%d", r.checkpointPhase, checkpointFile, checkpointPos)
	}

	order := core.SourceOrder(core.Record{Metadata: core.Metadata{
		SourceType: core.SourceTypeMySQLSnapshotCDC, SourcePhase: core.SourcePhaseSnapshot,
		SnapshotHandoffFile: file, SnapshotHandoffPos: pos,
	}})
	prior := core.SourceOrder(core.Record{Metadata: core.Metadata{
		SourceType: core.SourceTypeMySQLSnapshotCDC, SourcePhase: core.SourcePhaseCDC,
		BinlogFile: "mysql-bin.000009", BinlogPos: 900,
	}})
	if !order.VersionAvailable || order.Version <= prior.Version {
		t.Fatalf("new resnapshot order=%+v did not supersede prior=%+v", order, prior)
	}
}

func TestSnapshotRecordCarriesSourceOrderHandoffForTextCursor(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer db.Close()
	mock.ExpectBegin()
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatalf("BeginTx: %v", err)
	}
	mock.ExpectQuery("SELECT column_name, column_type FROM information_schema.columns").
		WithArgs("shop", "customers").
		WillReturnRows(sqlmock.NewRows([]string{"column_name", "column_type"}).
			AddRow("customer_code", "varchar(64)").AddRow("name", "varchar(64)"))
	query := regexp.QuoteMeta("SELECT * FROM `customers` WHERE `customer_code` > ? ORDER BY `customer_code` LIMIT 2")
	mock.ExpectQuery(query).WithArgs("").
		WillReturnRows(sqlmock.NewRows([]string{"customer_code", "name"}).AddRow("C-1", "Alice"))
	mock.ExpectQuery(query).WithArgs("C-1").
		WillReturnRows(sqlmock.NewRows([]string{"customer_code", "name"}))
	mock.ExpectRollback()

	r := &snapshotCDCReader{
		source:              &MySQLSnapshotCDCSource{name: "snapshot-customers", database: "shop", limit: 2},
		records:             make(chan core.Record, 1),
		snapshotReadStr:     map[string]string{},
		snapshotHandoffFile: "mysql-bin.000010", snapshotHandoffPos: 88, snapshotHandoffValid: true,
	}
	if err := r.snapshotTable(context.Background(), tx, "customers", resolvedPK{column: "customer_code", kind: pkKindOrdered}); err != nil {
		t.Fatalf("snapshotTable: %v", err)
	}
	record := <-r.records
	if record.Metadata.Cursor != "C-1" || record.Metadata.CursorKind != core.SourceCursorOrdered {
		t.Fatalf("snapshot cursor metadata = %+v", record.Metadata)
	}
	if record.Metadata.SnapshotHandoffFile != "mysql-bin.000010" || record.Metadata.SnapshotHandoffPos != 88 {
		t.Fatalf("snapshot handoff metadata = %+v", record.Metadata)
	}
	order := core.SourceOrder(record)
	want := uint64(1)<<63 | uint64(10)<<32 | 88
	if !order.VersionAvailable || order.Version != want || order.Phase != core.SourcePhaseSnapshot {
		t.Fatalf("snapshot SourceOrder = %+v, want version %d", order, want)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("Rollback: %v", err)
	}
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("sql expectations: %v", err)
	}
}

func TestSnapshotCDCReconnectPositionUsesDurableCheckpoint(t *testing.T) {
	r := newSnapshotCheckpointTestReader()
	r.file = "binlog.000009"
	r.pos = 900
	r.checkpointFile = "binlog.000001"
	r.checkpointPos = 120
	file, pos := r.getDurableBinlogPos()
	if file != "binlog.000001" || pos != 120 {
		t.Fatalf("durable reconnect position = %s:%d, want binlog.000001:120", file, pos)
	}
	readFile, readPos := r.getBinlogPos()
	if readFile != "binlog.000009" || readPos != 900 {
		t.Fatalf("runtime read-ahead position = %s:%d, want binlog.000009:900", readFile, readPos)
	}
}

func TestSnapshotCDCReaderClosedChannelsReturnEOF(t *testing.T) {
	r := &snapshotCDCReader{
		records: make(chan core.Record),
		errors:  make(chan error),
	}
	close(r.records)
	close(r.errors)
	_, err := r.Read(context.Background())
	if err != io.EOF {
		t.Fatalf("Read after error channel close = %v, want EOF", err)
	}
}

func TestSnapshotCDCReaderCloseWithoutCanalIsSafe(t *testing.T) {
	r := &snapshotCDCReader{}
	if err := r.Close(); err != nil {
		t.Fatalf("Close without canal: %v", err)
	}
}

func TestSnapshotAckKeepsLegacyLastIDCompatibility(t *testing.T) {
	r := newSnapshotCheckpointTestReader()
	r.tableLastIDs = nil
	position := `{"phase":"snapshot","last_id":8,"file":"binlog.000001","pos":4}`
	if err := r.AckCheckpoint(context.Background(), core.Checkpoint{Position: []byte(position)}); err != nil {
		t.Fatalf("AckCheckpoint: %v", err)
	}
	if got := r.tableLastIDs["orders"]; got != 8 {
		t.Fatalf("legacy LastID cursor = %d, want 8", got)
	}
}

func TestSnapshotCursorStringKeepsLocalWallClock(t *testing.T) {
	loc := time.FixedZone("UTC+8", 8*60*60)
	value := time.Date(2026, 8, 8, 10, 0, 0, 123456000, loc)
	if got := cursorString(value); got != "2026-08-08 10:00:00.123456" {
		t.Fatalf("cursorString(time) = %q, want local wall clock", got)
	}
	zeroFraction := time.Date(2026, 8, 8, 10, 0, 0, 0, loc)
	if got := cursorString(zeroFraction); got != "2026-08-08 10:00:00.000000" {
		t.Fatalf("cursorString(time zero fraction) = %q, want fixed microseconds", got)
	}
}
