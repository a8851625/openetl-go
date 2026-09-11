// Package contracttest contains reusable record-contract fixtures for source,
// sink, preflight, and replay conformance tests. It intentionally depends only
// on core so connector packages can consume one shared truth table.
package contracttest

import "github.com/a8851625/openetl-go/internal/etl/core"

type SourceOrderFixture struct {
	Name             string
	Record           core.Record
	Available        bool
	VersionAvailable bool
	Version          uint64
	Reason           core.SourceOrderReason
}

func SourceOrderFixtures() []SourceOrderFixture {
	return []SourceOrderFixture{
		{
			Name: "mysql_cdc",
			Record: core.Record{Metadata: core.Metadata{
				SourceType: core.SourceTypeMySQLCDC, Database: "shop", Table: "orders",
				SourcePhase: core.SourcePhaseCDC, BinlogFile: "mysql-bin.000042", BinlogPos: 99,
			}},
			Available: true, VersionAvailable: true,
			Version: uint64(1)<<63 | uint64(42)<<32 | 99,
		},
		{
			Name: "mysql_snapshot_cdc_snapshot",
			Record: core.Record{Metadata: core.Metadata{
				SourceType: core.SourceTypeMySQLSnapshotCDC, Database: "shop", Table: "orders",
				SourcePhase: core.SourcePhaseSnapshot, CursorKind: core.SourceCursorOrdered, Cursor: "customer-z",
				SnapshotHandoffFile: "mysql-bin.000042", SnapshotHandoffPos: 99,
			}},
			Available: true, VersionAvailable: true,
			Version: uint64(1)<<63 | uint64(42)<<32 | 99,
		},
		{
			Name: "postgres_cdc",
			Record: core.Record{Metadata: core.Metadata{
				SourceType: core.SourceTypePostgresCDC, Database: "shop", Table: "orders",
				SourcePhase: core.SourcePhaseCDC, LSN: "16/B374D848",
			}},
			Available: true, VersionAvailable: true, Version: uint64(0x16)<<32 | 0xB374D848,
		},
		{
			Name: "kafka",
			Record: core.Record{Metadata: core.Metadata{
				SourceType: core.SourceTypeKafka, Table: "orders", Partition: 7, Offset: 42,
			}},
			Available: true, VersionAvailable: true, Version: uint64(7)<<48 | 42,
		},
		{
			Name: "mysql_batch",
			Record: core.Record{Metadata: core.Metadata{
				SourceType: core.SourceTypeMySQLBatch, Database: "shop", Table: "orders",
				SourcePhase: core.SourcePhaseBatch, CursorKind: core.SourceCursorNumeric, Cursor: "42",
			}},
			Available: true, VersionAvailable: true, Version: 42,
		},
		{
			Name:   "file_unavailable",
			Record: core.Record{Metadata: core.Metadata{SourceType: core.SourceTypeFile, Table: "orders"}},
			Reason: core.SourceOrderReasonUnsupportedSource,
		},
		{
			Name:   "http_unavailable",
			Record: core.Record{Metadata: core.Metadata{SourceType: core.SourceTypeHTTP, Table: "orders"}},
			Reason: core.SourceOrderReasonUnsupportedSource,
		},
		{
			Name:   "redis_unavailable",
			Record: core.Record{Metadata: core.Metadata{SourceType: core.SourceTypeRedis, Table: "orders"}},
			Reason: core.SourceOrderReasonUnsupportedSource,
		},
	}
}

type RecordIdentityFixture struct {
	Name             string
	Record           core.Record
	Complete         bool
	Reason           core.RecordIdentityReason
	MissingColumns   []string
	ExpectedKeyValue map[string]string
}

func RecordIdentityFixtures() []RecordIdentityFixture {
	return []RecordIdentityFixture{
		{
			Name: "single_insert",
			Record: core.Record{
				Operation: core.OpInsert,
				Data:      map[string]any{"id": int64(7), "name": "new"},
				Metadata:  core.Metadata{Key: `{"id":7}`, PrimaryKeyColumns: []string{"id"}},
			},
			Complete: true, ExpectedKeyValue: map[string]string{"id": "7"},
		},
		{
			Name: "composite_delete",
			Record: core.Record{
				Operation: core.OpDelete,
				Data:      map[string]any{"tenant_id": int64(3), "id": "a-1"},
				Metadata: core.Metadata{
					Key: `{"tenant_id":3,"id":"a-1"}`, PrimaryKeyColumns: []string{"tenant_id", "id"},
				},
			},
			Complete: true, ExpectedKeyValue: map[string]string{"tenant_id": "3", "id": "a-1"},
		},
		{
			Name: "primary_key_change_update_uses_before",
			Record: core.Record{
				Operation: core.OpUpdate,
				Before:    map[string]any{"tenant_id": int64(3), "id": "old"},
				Data:      map[string]any{"tenant_id": int64(3), "id": "new"},
				Metadata: core.Metadata{
					Key: `{"tenant_id":3,"id":"old"}`, PrimaryKeyColumns: []string{"tenant_id", "id"},
				},
			},
			Complete: true, ExpectedKeyValue: map[string]string{"tenant_id": "3", "id": "old"},
		},
		{
			Name: "partial_composite_key",
			Record: core.Record{
				Operation: core.OpInsert,
				Metadata: core.Metadata{
					Key: `{"tenant_id":3}`, PrimaryKeyColumns: []string{"tenant_id", "id"},
				},
			},
			Reason: core.RecordIdentityReasonKeyColumnMissing, MissingColumns: []string{"id"},
		},
		{
			Name: "empty_key",
			Record: core.Record{
				Operation: core.OpInsert,
				Metadata:  core.Metadata{PrimaryKeyColumns: []string{"id"}},
			},
			Reason: core.RecordIdentityReasonKeyMissing,
		},
		{
			Name: "update_key_conflicts_with_before",
			Record: core.Record{
				Operation: core.OpUpdate,
				Before:    map[string]any{"id": int64(7)},
				Data:      map[string]any{"id": int64(8)},
				Metadata:  core.Metadata{Key: `{"id":8}`, PrimaryKeyColumns: []string{"id"}},
			},
			Reason: core.RecordIdentityReasonBeforeKeyConflict, MissingColumns: []string{"id"},
		},
	}
}

// MetadataPKSinkFixture is the shared conformance matrix consumed by every
// sink that publicly advertises pk_columns_from_metadata. Accepted fixtures
// must produce ExpectedColumns for each source/target table; rejected fixtures
// must return a classified data error containing Reason.
type MetadataPKSinkFixture struct {
	Name                string
	Records             []core.Record
	LegacySafetyColumns []string
	Accepted            bool
	Reason              string
	ExpectedColumns     map[string][]string
	KeyChangeCount      int
}

func MetadataPKSinkFixtures() []MetadataPKSinkFixture {
	return []MetadataPKSinkFixture{
		{
			Name: "single_insert",
			Records: []core.Record{{
				Operation: core.OpInsert,
				Data:      map[string]any{"id": int64(7), "name": "new"},
				Metadata: core.Metadata{
					Database: "shop", Table: "orders", Key: `{"id":7}`,
					PrimaryKeyColumns: []string{"id"}, FormatContractID: core.FormatContractOpenETLEnvelopeV1,
				},
			}},
			Accepted: true, ExpectedColumns: map[string][]string{"orders": {"id"}},
		},
		{
			Name: "composite_insert_update_delete",
			Records: []core.Record{
				{
					Operation: core.OpInsert,
					Data:      map[string]any{"tenant_id": "acme", "id": int64(1), "value": "one"},
					Metadata: core.Metadata{Database: "shop", Table: "orders", Key: `{"tenant_id":"acme","id":1}`,
						PrimaryKeyColumns: []string{"tenant_id", "id"}, FormatContractID: core.FormatContractOpenETLEnvelopeV1},
				},
				{
					Operation: core.OpUpdate,
					Before:    map[string]any{"tenant_id": "acme", "id": int64(2), "value": "old"},
					Data:      map[string]any{"tenant_id": "acme", "id": int64(2), "value": "new"},
					Metadata: core.Metadata{Database: "shop", Table: "orders", Key: `{"tenant_id":"acme","id":2}`,
						PrimaryKeyColumns: []string{"tenant_id", "id"}, FormatContractID: core.FormatContractOpenETLEnvelopeV1},
				},
				{
					Operation: core.OpDelete,
					Data:      map[string]any{"tenant_id": "acme", "id": int64(3), "value": "gone"},
					Metadata: core.Metadata{Database: "shop", Table: "orders", Key: `{"tenant_id":"acme","id":3}`,
						PrimaryKeyColumns: []string{"tenant_id", "id"}, FormatContractID: core.FormatContractOpenETLEnvelopeV1},
				},
			},
			Accepted: true, ExpectedColumns: map[string][]string{"orders": {"id", "tenant_id"}},
		},
		{
			Name: "canal_partial_before_key_change",
			Records: []core.Record{{
				Operation: core.OpUpdate,
				Before:    map[string]any{"id": int64(10)},
				Data:      map[string]any{"tenant_id": "acme", "id": int64(11), "value": "moved"},
				Metadata: core.Metadata{Database: "shop", Table: "orders", Key: `{"tenant_id":"acme","id":10}`,
					PrimaryKeyColumns: []string{"tenant_id", "id"}, FormatContractID: core.FormatContractCanalJSONV1,
					BeforeImageState: core.BeforeImageStatePartial},
			}},
			Accepted: true, ExpectedColumns: map[string][]string{"orders": {"id", "tenant_id"}}, KeyChangeCount: 1,
		},
		{
			Name: "multi_table_fanout",
			Records: []core.Record{
				{Operation: core.OpInsert, Data: map[string]any{"order_id": int64(1)}, Metadata: core.Metadata{
					Database: "shop", Table: "orders", Key: `{"order_id":1}`, PrimaryKeyColumns: []string{"order_id"}, FormatContractID: core.FormatContractOpenETLEnvelopeV1}},
				{Operation: core.OpInsert, Data: map[string]any{"tenant_id": "acme", "user_no": "u-1"}, Metadata: core.Metadata{
					Database: "shop", Table: "users", Key: `{"tenant_id":"acme","user_no":"u-1"}`, PrimaryKeyColumns: []string{"tenant_id", "user_no"}, FormatContractID: core.FormatContractOpenETLEnvelopeV1}},
			},
			Accepted: true, ExpectedColumns: map[string][]string{"orders": {"order_id"}, "users": {"tenant_id", "user_no"}},
		},
		{
			Name: "legacy_verified_exact_static_key",
			Records: []core.Record{{
				Operation: core.OpInsert,
				Data:      map[string]any{"tenant_id": "acme", "id": int64(21)},
				Metadata: core.Metadata{Database: "shop", Table: "orders", Key: `{"tenant_id":"acme","id":21}`,
					PrimaryKeyColumns: []string{"tenant_id", "id"}, ReplayProvenance: core.DLQReplayProvenanceLegacyVerified},
			}},
			LegacySafetyColumns: []string{"tenant_id", "id"}, Accepted: true,
			ExpectedColumns: map[string][]string{"orders": {"id", "tenant_id"}},
		},
		{
			Name: "empty_key",
			Records: []core.Record{{Operation: core.OpInsert, Data: map[string]any{"id": 1}, Metadata: core.Metadata{
				Database: "shop", Table: "orders", PrimaryKeyColumns: []string{"id"}}}},
			Reason: string(core.RecordIdentityReasonKeyMissing),
		},
		{
			Name: "partial_composite_key",
			Records: []core.Record{{Operation: core.OpInsert, Data: map[string]any{"tenant_id": "acme", "id": 1}, Metadata: core.Metadata{
				Database: "shop", Table: "orders", Key: `{"tenant_id":"acme"}`, PrimaryKeyColumns: []string{"tenant_id", "id"}}}},
			Reason: string(core.RecordIdentityReasonKeyColumnMissing),
		},
		{
			Name: "key_data_conflict",
			Records: []core.Record{{Operation: core.OpDelete, Data: map[string]any{"id": int64(2)}, Metadata: core.Metadata{
				Database: "shop", Table: "orders", Key: `{"id":1}`, PrimaryKeyColumns: []string{"id"}}}},
			Reason: string(core.RecordIdentityReasonDataKeyConflict),
		},
		{
			Name: "target_key_set_drift",
			Records: []core.Record{
				{Operation: core.OpInsert, Data: map[string]any{"id": int64(1)}, Metadata: core.Metadata{Database: "shop", Table: "orders", Key: `{"id":1}`, PrimaryKeyColumns: []string{"id"}}},
				{Operation: core.OpInsert, Data: map[string]any{"tenant_id": "acme", "id": int64(2)}, Metadata: core.Metadata{Database: "shop", Table: "orders", Key: `{"tenant_id":"acme","id":2}`, PrimaryKeyColumns: []string{"tenant_id", "id"}}},
			},
			Reason: "target_key_set_changed",
		},
		{
			Name: "legacy_unknown_quarantined",
			Records: []core.Record{{Operation: core.OpInsert, Data: map[string]any{"id": int64(1)}, Metadata: core.Metadata{
				Database: "shop", Table: "orders", Key: `{"id":1}`, PrimaryKeyColumns: []string{"id"}, ReplayProvenance: core.DLQReplayProvenanceLegacyUnknown}}},
			LegacySafetyColumns: []string{"id"}, Reason: string(core.RecordIdentityReasonLegacyProvenanceInvalid),
		},
		{
			Name: "legacy_verified_static_mismatch",
			Records: []core.Record{{Operation: core.OpInsert, Data: map[string]any{"id": int64(1)}, Metadata: core.Metadata{
				Database: "shop", Table: "orders", Key: `{"id":1}`, PrimaryKeyColumns: []string{"id"}, ReplayProvenance: core.DLQReplayProvenanceLegacyVerified}}},
			LegacySafetyColumns: []string{"other_id"}, Reason: string(core.RecordIdentityReasonLegacyProvenanceInvalid),
		},
	}
}
