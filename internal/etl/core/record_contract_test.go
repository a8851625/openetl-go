package core_test

import (
	"fmt"
	"os/exec"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/core/contracttest"
)

func TestSourceOrderConformanceFixtures(t *testing.T) {
	for _, fixture := range contracttest.SourceOrderFixtures() {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			got := core.SourceOrder(fixture.Record)
			if got.Available != fixture.Available || got.VersionAvailable != fixture.VersionAvailable {
				t.Fatalf("availability = (%v,%v), want (%v,%v): %+v", got.Available, got.VersionAvailable, fixture.Available, fixture.VersionAvailable, got)
			}
			if got.Version != fixture.Version {
				t.Fatalf("version = %d, want %d: %+v", got.Version, fixture.Version, got)
			}
			if got.Reason != fixture.Reason {
				t.Fatalf("reason = %q, want %q", got.Reason, fixture.Reason)
			}
		})
	}
}

func TestSourceOrderBoundariesAreExplicit(t *testing.T) {
	tests := []struct {
		name             string
		metadata         core.Metadata
		available        bool
		versionAvailable bool
		version          uint64
		reason           core.SourceOrderReason
	}{
		{
			name: "binlog_max",
			metadata: core.Metadata{SourceType: core.SourceTypeMySQLCDC,
				BinlogFile: "mysql-bin.2147483647", BinlogPos: ^uint32(0)},
			available: true, versionAvailable: true, version: ^uint64(0),
		},
		{
			name: "binlog_sequence_overflow",
			metadata: core.Metadata{SourceType: core.SourceTypeMySQLCDC,
				BinlogFile: "mysql-bin.2147483648", BinlogPos: 4},
			reason: core.SourceOrderReasonPositionOverflow,
		},
		{
			name: "binlog_bad_suffix",
			metadata: core.Metadata{SourceType: core.SourceTypeMySQLCDC,
				BinlogFile: "mysql-bin.latest", BinlogPos: 4},
			reason: core.SourceOrderReasonPositionInvalid,
		},
		{
			name: "snapshot_handoff_max",
			metadata: core.Metadata{SourceType: core.SourceTypeMySQLSnapshotCDC,
				SourcePhase: core.SourcePhaseSnapshot, CursorKind: core.SourceCursorOrdered,
				Cursor: "customer-z", SnapshotHandoffFile: "mysql-bin.2147483647", SnapshotHandoffPos: ^uint32(0)},
			available: true, versionAvailable: true, version: ^uint64(0),
		},
		{
			name: "snapshot_handoff_overflow",
			metadata: core.Metadata{SourceType: core.SourceTypeMySQLSnapshotCDC,
				SourcePhase: core.SourcePhaseSnapshot, CursorKind: core.SourceCursorNumeric,
				Cursor: "42", SnapshotHandoffFile: "mysql-bin.2147483648", SnapshotHandoffPos: 4},
			reason: core.SourceOrderReasonPositionOverflow,
		},
		{
			name: "snapshot_handoff_missing",
			metadata: core.Metadata{SourceType: core.SourceTypeMySQLSnapshotCDC,
				SourcePhase: core.SourcePhaseSnapshot, CursorKind: core.SourceCursorNumeric, Cursor: "42"},
			reason: core.SourceOrderReasonPositionMissing,
		},
		{
			name: "ordered_cursor_retained_without_fake_number",
			metadata: core.Metadata{SourceType: core.SourceTypeMySQLBatch,
				CursorKind: core.SourceCursorOrdered, Cursor: "customer-z"},
			available: true, reason: core.SourceOrderReasonNumericUnavailable,
		},
		{
			name: "kafka_max",
			metadata: core.Metadata{SourceType: core.SourceTypeKafka,
				Partition: core.MaxKafkaPartition, Offset: core.MaxKafkaOffset},
			available: true, versionAvailable: true,
			version: uint64(core.MaxKafkaPartition)<<48 | uint64(core.MaxKafkaOffset),
		},
		{
			name: "kafka_partition_overflow",
			metadata: core.Metadata{SourceType: core.SourceTypeKafka,
				Partition: core.MaxKafkaPartition + 1, Offset: 1},
			reason: core.SourceOrderReasonPositionOverflow,
		},
		{
			name: "kafka_offset_overflow",
			metadata: core.Metadata{SourceType: core.SourceTypeKafka,
				Partition: 0, Offset: core.MaxKafkaOffset + 1},
			reason: core.SourceOrderReasonPositionOverflow,
		},
		{
			name:      "postgres_full_uint64",
			metadata:  core.Metadata{SourceType: core.SourceTypePostgresCDC, LSN: "FFFFFFFF/FFFFFFFF"},
			available: true, versionAvailable: true, version: ^uint64(0),
		},
		{
			name: "postgres_snapshot_has_no_durable_row_cursor",
			metadata: core.Metadata{SourceType: core.SourceTypePostgresCDC,
				SourcePhase: core.SourcePhaseSnapshot},
			reason: core.SourceOrderReasonPositionMissing,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got := core.SourceOrder(core.Record{Metadata: test.metadata})
			if got.Available != test.available || got.VersionAvailable != test.versionAvailable || got.Version != test.version || got.Reason != test.reason {
				t.Fatalf("SourceOrder() = %+v, want available=%v numeric=%v version=%d reason=%q", got, test.available, test.versionAvailable, test.version, test.reason)
			}
		})
	}
}

func TestSourceOrderSnapshotHandoffSharesCDCOrderDomain(t *testing.T) {
	snapshot := core.SourceOrder(core.Record{Metadata: core.Metadata{
		SourceType: core.SourceTypeMySQLSnapshotCDC, SourcePhase: core.SourcePhaseSnapshot,
		CursorKind: core.SourceCursorOrdered, Cursor: "customer-z",
		SnapshotHandoffFile: "mysql-bin.000012", SnapshotHandoffPos: 88,
	}})
	olderCDC := core.SourceOrder(core.Record{Metadata: core.Metadata{
		SourceType: core.SourceTypeMySQLSnapshotCDC, SourcePhase: core.SourcePhaseCDC,
		BinlogFile: "mysql-bin.000011", BinlogPos: 900,
	}})
	newerCDC := core.SourceOrder(core.Record{Metadata: core.Metadata{
		SourceType: core.SourceTypeMySQLSnapshotCDC, SourcePhase: core.SourcePhaseCDC,
		BinlogFile: "mysql-bin.000012", BinlogPos: 120,
	}})
	if !snapshot.VersionAvailable || !olderCDC.VersionAvailable || !newerCDC.VersionAvailable ||
		olderCDC.Version >= snapshot.Version || snapshot.Version >= newerCDC.Version {
		t.Fatalf("source order must satisfy older CDC < snapshot handoff < newer CDC: old=%+v snapshot=%+v new=%+v", olderCDC, snapshot, newerCDC)
	}
}

func TestSourceOrderResnapshotSupersedesPreviouslyConsumedCDC(t *testing.T) {
	previousCDC := core.SourceOrder(core.Record{Metadata: core.Metadata{
		SourceType: core.SourceTypeMySQLSnapshotCDC, SourcePhase: core.SourcePhaseCDC,
		BinlogFile: "mysql-bin.000020", BinlogPos: 900,
	}})
	resnapshot := core.SourceOrder(core.Record{Metadata: core.Metadata{
		SourceType: core.SourceTypeMySQLSnapshotCDC, SourcePhase: core.SourcePhaseSnapshot,
		CursorKind: core.SourceCursorNumeric, Cursor: "1",
		SnapshotHandoffFile: "mysql-bin.000021", SnapshotHandoffPos: 4,
	}})
	if !previousCDC.VersionAvailable || !resnapshot.VersionAvailable || resnapshot.Version <= previousCDC.Version {
		t.Fatalf("checkpoint-reset snapshot must supersede prior CDC: prior=%+v reset=%+v", previousCDC, resnapshot)
	}
}

func TestSourceOrderIgnoresWallClockAndInstanceName(t *testing.T) {
	record := core.Record{Metadata: core.Metadata{
		Source: "orders-reader-west", SourceType: core.SourceTypeMySQLCDC,
		Database: "shop", Table: "orders", BinlogFile: "mysql-bin.000012", BinlogPos: 88,
		Timestamp: time.Unix(1, 0),
	}}
	first := core.SourceOrder(record)
	for i := 0; i < 100; i++ {
		record.Metadata.Timestamp = time.Unix(int64(10_000+i), int64(i))
		record.Metadata.Source = fmt.Sprintf("restart-%d", i)
		got := core.SourceOrder(record)
		if got.Version != first.Version || got.Scope != first.Scope || !got.Available {
			t.Fatalf("restart derivation changed: first=%+v got=%+v", first, got)
		}
	}
}

func TestSourceOrderConcurrentDerivationDoesNotInvert(t *testing.T) {
	const count = 4096
	versions := make([]uint64, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			got := core.SourceOrder(core.Record{Metadata: core.Metadata{
				SourceType: core.SourceTypeKafka, Table: "orders", Partition: 9, Offset: int64(i),
			}})
			if !got.VersionAvailable {
				t.Errorf("offset %d unavailable: %+v", i, got)
				return
			}
			versions[i] = got.Version
		}()
	}
	wg.Wait()
	for i := 1; i < count; i++ {
		if versions[i] <= versions[i-1] {
			t.Fatalf("version inverted at %d: %d <= %d", i, versions[i], versions[i-1])
		}
	}
}

func TestKafkaSourceOrderIsPartitionScoped(t *testing.T) {
	left := core.SourceOrder(core.Record{Metadata: core.Metadata{
		SourceType: core.SourceTypeKafka, Table: "orders", Partition: 0, Offset: 10,
	}})
	right := core.SourceOrder(core.Record{Metadata: core.Metadata{
		SourceType: core.SourceTypeKafka, Table: "orders", Partition: 1, Offset: 1,
	}})
	if left.Scope == right.Scope || !strings.Contains(left.Scope, "partition=0") || !strings.Contains(right.Scope, "partition=1") {
		t.Fatalf("Kafka scopes do not expose the partition boundary: left=%q right=%q", left.Scope, right.Scope)
	}
}

func TestRecordIdentityConformanceFixtures(t *testing.T) {
	for _, fixture := range contracttest.RecordIdentityFixtures() {
		fixture := fixture
		t.Run(fixture.Name, func(t *testing.T) {
			got := core.RecordIdentity(fixture.Record)
			if got.Complete != fixture.Complete || got.Reason != fixture.Reason {
				t.Fatalf("RecordIdentity() = %+v, want complete=%v reason=%q", got, fixture.Complete, fixture.Reason)
			}
			if strings.Join(got.MissingColumns, ",") != strings.Join(fixture.MissingColumns, ",") {
				t.Fatalf("missing columns = %v, want %v", got.MissingColumns, fixture.MissingColumns)
			}
			for column, want := range fixture.ExpectedKeyValue {
				if value := fmt.Sprint(got.Values[column]); value != want {
					t.Fatalf("value[%s] = %q, want %q", column, value, want)
				}
			}
		})
	}
}

func TestRecordIdentityReasonMatrix(t *testing.T) {
	tests := []struct {
		name   string
		record core.Record
		reason core.RecordIdentityReason
	}{
		{name: "missing_declaration", record: core.Record{Metadata: core.Metadata{Key: `{"id":1}`}}, reason: core.RecordIdentityReasonPKDeclarationMissing},
		{name: "blank_declaration", record: core.Record{Metadata: core.Metadata{Key: `{"id":1}`, PrimaryKeyColumns: []string{""}}}, reason: core.RecordIdentityReasonPKDeclarationInvalid},
		{name: "duplicate_declaration", record: core.Record{Metadata: core.Metadata{Key: `{"id":1}`, PrimaryKeyColumns: []string{"id", "id"}}}, reason: core.RecordIdentityReasonPKDeclarationInvalid},
		{name: "invalid_json", record: core.Record{Metadata: core.Metadata{Key: `{`, PrimaryKeyColumns: []string{"id"}}}, reason: core.RecordIdentityReasonKeyInvalidJSON},
		{name: "scalar_key", record: core.Record{Metadata: core.Metadata{Key: `7`, PrimaryKeyColumns: []string{"id"}}}, reason: core.RecordIdentityReasonKeyNotObject},
		{name: "empty_object", record: core.Record{Metadata: core.Metadata{Key: `{}`, PrimaryKeyColumns: []string{"id"}}}, reason: core.RecordIdentityReasonKeyEmpty},
		{name: "null_component", record: core.Record{Metadata: core.Metadata{Key: `{"id":null}`, PrimaryKeyColumns: []string{"id"}}}, reason: core.RecordIdentityReasonKeyColumnEmpty},
		{name: "empty_component", record: core.Record{Metadata: core.Metadata{Key: `{"id":""}`, PrimaryKeyColumns: []string{"id"}}}, reason: core.RecordIdentityReasonKeyColumnEmpty},
		{name: "undeclared_extra_column", record: core.Record{Metadata: core.Metadata{Key: `{"id":1,"tenant":2}`, PrimaryKeyColumns: []string{"id"}}}, reason: core.RecordIdentityReasonKeyColumnsConflict},
		{name: "update_before_missing", record: core.Record{Operation: core.OpUpdate, Metadata: core.Metadata{Key: `{"id":1}`, PrimaryKeyColumns: []string{"id"}}}, reason: core.RecordIdentityReasonBeforeMissing},
		{name: "update_before_key_missing", record: core.Record{Operation: core.OpUpdate, Before: map[string]any{"name": "old"}, Metadata: core.Metadata{Key: `{"id":1}`, PrimaryKeyColumns: []string{"id"}}}, reason: core.RecordIdentityReasonBeforeKeyMissing},
		{name: "update_before_key_empty", record: core.Record{Operation: core.OpUpdate, Before: map[string]any{"id": ""}, Metadata: core.Metadata{Key: `{"id":"x"}`, PrimaryKeyColumns: []string{"id"}}}, reason: core.RecordIdentityReasonBeforeKeyEmpty},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			got := core.RecordIdentity(test.record)
			if got.Complete || got.Reason != test.reason {
				t.Fatalf("RecordIdentity() = %+v, want incomplete reason=%q", got, test.reason)
			}
		})
	}
}

func TestRecordIdentityReportsKeyChangeAndRejectsLegacyVerifiedChange(t *testing.T) {
	record := core.Record{
		Operation: core.OpUpdate,
		Before:    map[string]any{"tenant_id": "acme", "id": int64(1)},
		Data:      map[string]any{"tenant_id": "acme", "id": int64(2)},
		Metadata: core.Metadata{
			Key: `{"tenant_id":"acme","id":1}`, PrimaryKeyColumns: []string{"tenant_id", "id"},
			FormatContractID: core.FormatContractOpenETLEnvelopeV1,
		},
	}
	identity := core.RecordIdentity(record)
	if !identity.Complete || !identity.KeyChanged {
		t.Fatalf("identity=%+v, want complete key-changing UPDATE", identity)
	}
	record.Metadata.ReplayProvenance = core.DLQReplayProvenanceLegacyVerified
	legacy := core.RecordIdentity(record)
	if legacy.Complete || !legacy.KeyChanged || legacy.Reason != core.RecordIdentityReasonLegacyKeyChangeUnsupported {
		t.Fatalf("legacy identity=%+v, want key-change quarantine", legacy)
	}
}

func TestRecordIdentityPreservesLargeNumbersAndBeforeImage(t *testing.T) {
	record := core.Record{
		Operation: core.OpUpdate,
		Before:    map[string]any{"id": uint64(9007199254740993)},
		Data:      map[string]any{"id": uint64(9007199254740994)},
		Metadata: core.Metadata{
			Key: `{"id":9007199254740993}`, PrimaryKeyColumns: []string{"id"},
		},
	}
	got := core.RecordIdentity(record)
	if !got.Complete || !got.BeforeImage || got.Reason != "" {
		t.Fatalf("large numeric before-image identity = %+v", got)
	}
}

func TestRecordIdentityCanalContractFillsOnlyOmittedOldKeyColumns(t *testing.T) {
	record := core.Record{
		Operation: core.OpUpdate,
		Before:    map[string]any{"id": "old-id", "name": "before"},
		Data:      map[string]any{"tenant_id": "tenant-a", "id": "new-id", "name": "after"},
		Metadata: core.Metadata{
			Key:               `{"tenant_id":"tenant-a","id":"old-id"}`,
			PrimaryKeyColumns: []string{"tenant_id", "id"},
			FormatContractID:  core.FormatContractCanalJSONV1,
			BeforeImageState:  core.BeforeImageStatePartial,
		},
	}
	got := core.RecordIdentity(record)
	if !got.Complete || !got.BeforeImage || got.Reason != "" {
		t.Fatalf("canal identity = %+v, want complete before identity", got)
	}

	record.Metadata.FormatContractID = "unknown/v1"
	got = core.RecordIdentity(record)
	if got.Complete || got.Reason != core.RecordIdentityReasonBeforeKeyMissing {
		t.Fatalf("unknown contract identity = %+v, want before-key missing", got)
	}

	record.Metadata.FormatContractID = core.FormatContractCanalJSONV1
	record.Metadata.BeforeImageState = core.BeforeImageStateAbsent
	got = core.RecordIdentity(record)
	if got.Complete || got.Reason != core.RecordIdentityReasonBeforeStateInvalid {
		t.Fatalf("absent canal old identity = %+v, want invalid before state", got)
	}
}

func TestRecordIdentityRejectsIncompleteOrConflictingDataKey(t *testing.T) {
	tests := []struct {
		name   string
		record core.Record
		reason core.RecordIdentityReason
	}{
		{
			name: "insert data missing",
			record: core.Record{Operation: core.OpInsert, Data: map[string]any{"tenant": "a"}, Metadata: core.Metadata{
				Key: `{"tenant":"a","id":7}`, PrimaryKeyColumns: []string{"tenant", "id"},
			}},
			reason: core.RecordIdentityReasonDataKeyMissing,
		},
		{
			name: "delete data empty",
			record: core.Record{Operation: core.OpDelete, Data: map[string]any{"id": ""}, Metadata: core.Metadata{
				Key: `{"id":"x"}`, PrimaryKeyColumns: []string{"id"},
			}},
			reason: core.RecordIdentityReasonDataKeyEmpty,
		},
		{
			name: "insert key conflicts with data",
			record: core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 8}, Metadata: core.Metadata{
				Key: `{"id":7}`, PrimaryKeyColumns: []string{"id"},
			}},
			reason: core.RecordIdentityReasonDataKeyConflict,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := core.RecordIdentity(test.record)
			if got.Complete || got.Reason != test.reason {
				t.Fatalf("RecordIdentity() = %+v, want reason=%s", got, test.reason)
			}
		})
	}
}

func TestCoreContractHasNoConnectorReverseDependencies(t *testing.T) {
	cmd := exec.Command("go", "list", "-deps", "./...")
	output, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps ./...: %v", err)
	}
	for _, dependency := range strings.Fields(string(output)) {
		if strings.Contains(dependency, "/internal/etl/source") || strings.Contains(dependency, "/internal/etl/sink") {
			t.Fatalf("core contract has forbidden connector dependency %q", dependency)
		}
	}
}
