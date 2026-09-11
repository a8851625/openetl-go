package core

import (
	"encoding/base64"
	"encoding/json"
	"strings"
	"time"
	"unicode/utf8"
)

// DLQReplayProvenance freezes why a DLQ record is eligible (or ineligible)
// for replay. Runtime configuration must never be used to guess this value
// for a historical row.
type DLQReplayProvenance string

const (
	DLQReplayProvenanceNormalFlow     DLQReplayProvenance = "normal_flow"
	DLQReplayProvenanceReconstructed  DLQReplayProvenance = "contract_reconstructed"
	DLQReplayProvenanceLegacyUnknown  DLQReplayProvenance = "legacy_unknown"
	DLQReplayProvenanceLegacyVerified DLQReplayProvenance = "legacy_verified"
)

type DLQReplayState string

const (
	DLQReplayStatePending        DLQReplayState = "pending"
	DLQReplayStateRepairRequired DLQReplayState = "repair_required"
	DLQReplayStateQuarantined    DLQReplayState = "quarantined"
	DLQReplayStateSinkAcked      DLQReplayState = "sink_acked"
)

const (
	DLQPayloadEncodingSourceBytes    = "source_bytes"
	DLQPayloadEncodingSourceBase64   = "source_bytes_base64"
	DLQPayloadEncodingNormalizedData = "normalized_data_json"
)

// DLQIdentityContext is the immutable identity/provenance snapshot plus the
// small replay checkpoint stored with every new dead-letter row. The source
// payload and format contract are frozen at failure time, so replay never
// interprets an old event using today's pipeline configuration.
type DLQIdentityContext struct {
	RawPayload         string              `json:"raw_payload,omitempty"`
	PayloadEncoding    string              `json:"payload_encoding,omitempty"`
	PrimaryKeyColumns  []string            `json:"primary_key_columns,omitempty"`
	FailureReason      string              `json:"failure_reason,omitempty"`
	MissingColumns     []string            `json:"missing_columns,omitempty"`
	FormatContractID   string              `json:"format_contract_id,omitempty"`
	BeforeImageState   BeforeImageState    `json:"before_image_state,omitempty"`
	SourceDatabase     string              `json:"source_database,omitempty"`
	SourceTable        string              `json:"source_table,omitempty"`
	TargetDatabase     string              `json:"target_database,omitempty"`
	TargetTable        string              `json:"target_table,omitempty"`
	ReplayProvenance   DLQReplayProvenance `json:"replay_provenance"`
	ReplayState        DLQReplayState      `json:"replay_state"`
	ReplayAttempt      int                 `json:"replay_attempt,omitempty"`
	ReplayError        string              `json:"replay_error,omitempty"`
	SinkAcknowledgedAt *time.Time          `json:"sink_acknowledged_at,omitempty"`
}

// NewDLQIdentityContext captures the record-owned facts available at failure
// time. targetDatabase/targetTable are resolved by the pipeline when it knows
// the sink configuration; direct storage users safely fall back to the record
// source coordinates.
func NewDLQIdentityContext(record Record, failure, targetDatabase, targetTable string) DLQIdentityContext {
	ctx := DLQIdentityContext{
		PrimaryKeyColumns: append([]string(nil), record.Metadata.PrimaryKeyColumns...),
		FormatContractID:  record.Metadata.FormatContractID,
		BeforeImageState:  record.Metadata.BeforeImageState,
		SourceDatabase:    record.Metadata.Database,
		SourceTable:       record.Metadata.Table,
		TargetDatabase:    strings.TrimSpace(targetDatabase),
		TargetTable:       strings.TrimSpace(targetTable),
		ReplayProvenance:  DLQReplayProvenanceNormalFlow,
		ReplayState:       DLQReplayStatePending,
	}
	if ctx.TargetDatabase == "" {
		ctx.TargetDatabase = record.Metadata.Database
	}
	if ctx.TargetTable == "" {
		ctx.TargetTable = record.Metadata.Table
	}
	if len(record.Metadata.RawPayload) > 0 {
		if utf8.Valid(record.Metadata.RawPayload) {
			ctx.RawPayload = string(record.Metadata.RawPayload)
			ctx.PayloadEncoding = DLQPayloadEncodingSourceBytes
		} else {
			ctx.RawPayload = base64.StdEncoding.EncodeToString(record.Metadata.RawPayload)
			ctx.PayloadEncoding = DLQPayloadEncodingSourceBase64
		}
	} else if encoded, err := json.Marshal(record.Data); err == nil {
		ctx.RawPayload = string(encoded)
		ctx.PayloadEncoding = DLQPayloadEncodingNormalizedData
	}

	if record.Rejection != nil && strings.TrimSpace(record.Rejection.Code) != "" {
		ctx.FailureReason = strings.TrimSpace(record.Rejection.Code)
	}
	if len(record.Metadata.PrimaryKeyColumns) > 0 {
		identity := RecordIdentity(record)
		if !identity.Complete {
			ctx.FailureReason = string(identity.Reason)
			ctx.MissingColumns = append([]string(nil), identity.MissingColumns...)
		}
	}
	if ctx.FailureReason == "" {
		ctx.FailureReason = strings.TrimSpace(failure)
	}
	return ctx
}

// NormalizePersistedDLQIdentityContext supplies safe defaults for historical
// rows that predate identity_context_json. Such rows are never silently
// treated as verified legacy input.
func NormalizePersistedDLQIdentityContext(ctx *DLQIdentityContext) {
	if ctx == nil {
		return
	}
	if ctx.ReplayProvenance == "" {
		ctx.ReplayProvenance = DLQReplayProvenanceLegacyUnknown
	}
	if ctx.ReplayState == "" {
		if ctx.ReplayProvenance == DLQReplayProvenanceLegacyUnknown {
			ctx.ReplayState = DLQReplayStateRepairRequired
		} else {
			ctx.ReplayState = DLQReplayStatePending
		}
	}
}
