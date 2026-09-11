package core

import (
	"encoding/base64"
	"reflect"
	"testing"
)

func TestNewDLQIdentityContextFreezesSourcePayloadAndIdentity(t *testing.T) {
	record := Record{
		Operation: OpInsert,
		Data:      map[string]any{"tenant_id": "acme"},
		Metadata: Metadata{
			Database: "shop", Table: "orders",
			Key:               `{"tenant_id":"acme"}`,
			PrimaryKeyColumns: []string{"tenant_id", "id"},
			FormatContractID:  FormatContractCanalJSONV1,
			RawPayload:        []byte(`{"data":[{"tenant_id":"acme"}]}`),
		},
	}

	ctx := NewDLQIdentityContext(record, "identity failed", "analytics", "ods_orders")
	if ctx.RawPayload != string(record.Metadata.RawPayload) || ctx.PayloadEncoding != DLQPayloadEncodingSourceBytes {
		t.Fatalf("raw payload not frozen: %+v", ctx)
	}
	if !reflect.DeepEqual(ctx.PrimaryKeyColumns, []string{"tenant_id", "id"}) || ctx.FailureReason != string(RecordIdentityReasonKeyColumnMissing) || !reflect.DeepEqual(ctx.MissingColumns, []string{"id"}) {
		t.Fatalf("identity context = %+v", ctx)
	}
	if ctx.FormatContractID != FormatContractCanalJSONV1 || ctx.TargetDatabase != "analytics" || ctx.TargetTable != "ods_orders" {
		t.Fatalf("contract/target context = %+v", ctx)
	}
	if ctx.ReplayProvenance != DLQReplayProvenanceNormalFlow || ctx.ReplayState != DLQReplayStatePending {
		t.Fatalf("new replay state = %+v", ctx)
	}

	record.Metadata.RawPayload[0] = '!'
	if ctx.RawPayload[0] != '{' {
		t.Fatal("frozen raw payload aliases mutable source bytes")
	}
}

func TestNormalizePersistedDLQIdentityContextFailsClosedForLegacyRow(t *testing.T) {
	var ctx DLQIdentityContext
	NormalizePersistedDLQIdentityContext(&ctx)
	if ctx.ReplayProvenance != DLQReplayProvenanceLegacyUnknown || ctx.ReplayState != DLQReplayStateRepairRequired {
		t.Fatalf("legacy context = %+v", ctx)
	}
}

func TestNewDLQIdentityContextPreservesNonUTF8SourceBytes(t *testing.T) {
	raw := []byte{0xff, 0x00, 0x81, '{', '}'}
	ctx := NewDLQIdentityContext(Record{
		Operation: OpInsert,
		Data:      map[string]any{"id": 1},
		Metadata:  Metadata{RawPayload: raw},
	}, "parse failure", "", "")
	if ctx.PayloadEncoding != DLQPayloadEncodingSourceBase64 {
		t.Fatalf("payload encoding = %q", ctx.PayloadEncoding)
	}
	decoded, err := base64.StdEncoding.DecodeString(ctx.RawPayload)
	if err != nil || string(decoded) != string(raw) {
		t.Fatalf("raw payload was not preserved: decoded=%v err=%v", decoded, err)
	}
}

func TestReconstructDLQRecordIdentityUsesFrozenCanalContract(t *testing.T) {
	record := Record{
		Operation: OpUpdate,
		Data:      map[string]any{"tenant_id": "acme", "id": "new", "value": 2},
		Before:    map[string]any{"id": "old"},
		Metadata: Metadata{
			Key:               `{"id":"old"}`,
			PrimaryKeyColumns: []string{"tenant_id", "id"},
		},
	}
	ctx := DLQIdentityContext{
		PrimaryKeyColumns: []string{"tenant_id", "id"},
		FormatContractID:  FormatContractCanalJSONV1,
		BeforeImageState:  BeforeImageStatePartial,
		ReplayProvenance:  DLQReplayProvenanceNormalFlow,
		ReplayState:       DLQReplayStatePending,
	}

	rebuilt, result := ReconstructDLQRecordIdentity(record, ctx)
	if !result.Complete || rebuilt.Metadata.Key != `{"id":"old","tenant_id":"acme"}` || rebuilt.Rejection != nil {
		t.Fatalf("rebuilt=%+v identity=%+v", rebuilt, result)
	}
}

func TestReconstructDLQRecordIdentityRejectsUnknownAndLegacyKeyChange(t *testing.T) {
	base := Record{
		Operation: OpUpdate,
		Data:      map[string]any{"id": "new"},
		Before:    map[string]any{"id": "old"},
		Metadata:  Metadata{PrimaryKeyColumns: []string{"id"}},
	}

	_, unknown := ReconstructDLQRecordIdentity(base, DLQIdentityContext{
		PrimaryKeyColumns: []string{"id"}, ReplayProvenance: DLQReplayProvenanceNormalFlow,
	})
	if unknown.Reason != RecordIdentityReasonFormatContractUnknown {
		t.Fatalf("unknown contract result = %+v", unknown)
	}

	_, legacy := ReconstructDLQRecordIdentity(base, DLQIdentityContext{
		PrimaryKeyColumns: []string{"id"}, ReplayProvenance: DLQReplayProvenanceLegacyVerified,
		BeforeImageState: BeforeImageStateFull,
	})
	if legacy.Reason != RecordIdentityReasonLegacyKeyChangeUnsupported {
		t.Fatalf("legacy key-change result = %+v", legacy)
	}
}
