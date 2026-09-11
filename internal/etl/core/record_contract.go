package core

import (
	"bytes"
	"encoding/json"
	"io"
	"math/big"
	"strconv"
	"strings"
)

// Canonical source types used by the record ordering contract. Source is an
// instance name and may be customized, so contract consumers use SourceType.
const (
	SourceTypeMySQLCDC         = "mysql_cdc"
	SourceTypeMySQLSnapshotCDC = "mysql_snapshot_cdc"
	SourceTypePostgresCDC      = "postgres_cdc"
	SourceTypeKafka            = "kafka"
	SourceTypeMySQLBatch       = "mysql_batch"
	SourceTypeFile             = "file"
	SourceTypeHTTP             = "http"
	SourceTypeREST             = "rest"
	SourceTypeRedis            = "redis"
)

const (
	SourcePhaseSnapshot = "snapshot"
	SourcePhaseCDC      = "cdc"
	SourcePhaseBatch    = "batch"
)

// SourceCursorKind describes how a connector compares its persisted cursor.
// Ordered cursors retain their exact text but deliberately have no numeric
// version: hashing text would be deterministic but would not preserve order.
type SourceCursorKind string

const (
	SourceCursorNumeric SourceCursorKind = "numeric"
	SourceCursorOrdered SourceCursorKind = "ordered"
)

// Published bit limits are part of the SourceOrder contract. Values outside
// these bounds fail explicitly; they are never truncated or wrapped.
const (
	MaxMySQLBinlogFileSequence uint64 = 1<<31 - 1
	MaxKafkaPartition          int32  = 1<<15 - 1
	MaxKafkaOffset             int64  = 1<<48 - 1
)

type SourceOrderKind string

const (
	SourceOrderMySQLBinlog   SourceOrderKind = "mysql_binlog"
	SourceOrderMySQLSnapshot SourceOrderKind = "mysql_snapshot"
	SourceOrderPostgresLSN   SourceOrderKind = "postgres_lsn"
	SourceOrderKafkaOffset   SourceOrderKind = "kafka_offset"
	SourceOrderMySQLBatch    SourceOrderKind = "mysql_batch_cursor"
)

type SourceOrderReason string

const (
	SourceOrderReasonUnknownSource      SourceOrderReason = "source_type_unknown"
	SourceOrderReasonUnsupportedSource  SourceOrderReason = "source_order_unsupported"
	SourceOrderReasonPositionMissing    SourceOrderReason = "source_position_missing"
	SourceOrderReasonPositionInvalid    SourceOrderReason = "source_position_invalid"
	SourceOrderReasonPositionOverflow   SourceOrderReason = "source_position_overflow"
	SourceOrderReasonPhaseInvalid       SourceOrderReason = "source_phase_invalid"
	SourceOrderReasonNumericUnavailable SourceOrderReason = "numeric_version_unavailable"
)

// SourceOrderResult is deliberately a struct rather than a tuple. Consumers
// must inspect both Available and VersionAvailable instead of silently
// discarding an ok flag. Scope states where the order is comparable; Kafka is
// intentionally scoped to one topic partition and is not a cross-partition
// time order.
type SourceOrderResult struct {
	Available        bool              `json:"available"`
	VersionAvailable bool              `json:"version_available"`
	Version          uint64            `json:"version,omitempty"`
	Kind             SourceOrderKind   `json:"kind,omitempty"`
	Scope            string            `json:"scope,omitempty"`
	Phase            string            `json:"phase,omitempty"`
	Major            uint64            `json:"major,omitempty"`
	Minor            uint64            `json:"minor,omitempty"`
	Cursor           string            `json:"cursor,omitempty"`
	Reason           SourceOrderReason `json:"reason,omitempty"`
}

// SourceOrder derives a deterministic position from source-owned metadata.
// It reads no clock and owns no process-local counter or mutable state.
func SourceOrder(record Record) SourceOrderResult {
	m := record.Metadata
	sourceType := canonicalRecordSourceType(m)
	switch sourceType {
	case SourceTypeMySQLCDC:
		return mysqlBinlogOrder(m, SourceOrderMySQLBinlog, SourcePhaseCDC)
	case SourceTypeMySQLSnapshotCDC:
		phase := strings.ToLower(strings.TrimSpace(m.SourcePhase))
		if phase == "" {
			if m.BinlogFile != "" {
				phase = SourcePhaseCDC
			} else {
				phase = SourcePhaseSnapshot
			}
		}
		switch phase {
		case SourcePhaseCDC:
			return mysqlBinlogOrder(m, SourceOrderMySQLBinlog, SourcePhaseCDC)
		case SourcePhaseSnapshot:
			return mysqlSnapshotHandoffOrder(m)
		default:
			return unavailableOrder(SourceOrderMySQLSnapshot, orderScope(sourceType, m), SourceOrderReasonPhaseInvalid)
		}
	case SourceTypePostgresCDC:
		phase := strings.ToLower(strings.TrimSpace(m.SourcePhase))
		if phase == SourcePhaseSnapshot {
			// The current PostgreSQL initial snapshot uses SELECT * without a
			// durable per-row keyset cursor, so claiming an order would be false.
			return unavailableOrder(SourceOrderPostgresLSN, orderScope(sourceType, m), SourceOrderReasonPositionMissing)
		}
		value, ok := parsePostgresLSN(m.LSN)
		if !ok {
			reason := SourceOrderReasonPositionInvalid
			if strings.TrimSpace(m.LSN) == "" {
				reason = SourceOrderReasonPositionMissing
			}
			return unavailableOrder(SourceOrderPostgresLSN, orderScope(sourceType, m), reason)
		}
		return SourceOrderResult{
			Available: true, VersionAvailable: true, Version: value,
			Kind: SourceOrderPostgresLSN, Scope: orderScope(sourceType, m),
			Phase: SourcePhaseCDC, Major: value, Cursor: strings.ToUpper(m.LSN),
		}
	case SourceTypeKafka:
		scope := kafkaOrderScope(m)
		if m.Partition < 0 || m.Offset < 0 {
			return unavailableOrder(SourceOrderKafkaOffset, scope, SourceOrderReasonPositionInvalid)
		}
		if m.Partition > MaxKafkaPartition || m.Offset > MaxKafkaOffset {
			return unavailableOrder(SourceOrderKafkaOffset, scope, SourceOrderReasonPositionOverflow)
		}
		partition := uint64(m.Partition)
		offset := uint64(m.Offset)
		return SourceOrderResult{
			Available: true, VersionAvailable: true,
			Version: partition<<48 | offset,
			Kind:    SourceOrderKafkaOffset, Scope: scope,
			Major: partition, Minor: offset,
			Cursor: strconv.FormatInt(m.Offset, 10),
		}
	case SourceTypeMySQLBatch:
		return cursorOrder(m, SourceOrderMySQLBatch, ^uint64(0), SourcePhaseBatch)
	case SourceTypeFile, SourceTypeHTTP, SourceTypeREST, SourceTypeRedis:
		return unavailableOrder("", orderScope(sourceType, m), SourceOrderReasonUnsupportedSource)
	case "":
		return unavailableOrder("", "", SourceOrderReasonUnknownSource)
	default:
		return unavailableOrder("", orderScope(sourceType, m), SourceOrderReasonUnsupportedSource)
	}
}

func mysqlSnapshotHandoffOrder(m Metadata) SourceOrderResult {
	anchor := m
	anchor.BinlogFile = m.SnapshotHandoffFile
	anchor.BinlogPos = m.SnapshotHandoffPos
	return mysqlBinlogOrder(anchor, SourceOrderMySQLSnapshot, SourcePhaseSnapshot)
}

func mysqlBinlogOrder(m Metadata, kind SourceOrderKind, phase string) SourceOrderResult {
	scope := orderScope(canonicalRecordSourceType(m), m)
	sequence, ok := parseBinlogFileSequence(m.BinlogFile)
	if !ok || m.BinlogPos == 0 {
		reason := SourceOrderReasonPositionInvalid
		if strings.TrimSpace(m.BinlogFile) == "" || m.BinlogPos == 0 {
			reason = SourceOrderReasonPositionMissing
		}
		return unavailableOrder(kind, scope, reason)
	}
	if sequence > MaxMySQLBinlogFileSequence {
		return unavailableOrder(kind, scope, SourceOrderReasonPositionOverflow)
	}
	position := uint64(m.BinlogPos)
	return SourceOrderResult{
		Available: true, VersionAvailable: true,
		// The high bit plus the remaining 31+32 bits retain the complete
		// supported binlog file sequence and uint32 position. Snapshot rows
		// carry their consistent-snapshot handoff boundary, so a re-snapshot
		// compares in the same source-owned domain as CDC events.
		Version: uint64(1)<<63 | sequence<<32 | position,
		Kind:    kind, Scope: scope, Phase: phase,
		Major: sequence, Minor: position,
		Cursor: m.BinlogFile + ":" + strconv.FormatUint(position, 10),
	}
}

func cursorOrder(m Metadata, kind SourceOrderKind, max uint64, phase string) SourceOrderResult {
	scope := orderScope(canonicalRecordSourceType(m), m)
	if m.CursorKind == "" {
		return unavailableOrder(kind, scope, SourceOrderReasonPositionMissing)
	}
	switch m.CursorKind {
	case SourceCursorNumeric:
		value, ok := parseUnsignedDecimal(m.Cursor)
		if !ok {
			return unavailableOrder(kind, scope, SourceOrderReasonPositionInvalid)
		}
		if value > max {
			return unavailableOrder(kind, scope, SourceOrderReasonPositionOverflow)
		}
		return SourceOrderResult{
			Available: true, VersionAvailable: true, Version: value,
			Kind: kind, Scope: scope, Phase: phase, Major: value, Cursor: m.Cursor,
		}
	case SourceCursorOrdered:
		// Empty strings are legitimate first positions for some collations.
		return SourceOrderResult{
			Available: true, VersionAvailable: false,
			Kind: kind, Scope: scope, Phase: phase, Cursor: m.Cursor,
			Reason: SourceOrderReasonNumericUnavailable,
		}
	default:
		return unavailableOrder(kind, scope, SourceOrderReasonPositionInvalid)
	}
}

func unavailableOrder(kind SourceOrderKind, scope string, reason SourceOrderReason) SourceOrderResult {
	return SourceOrderResult{Kind: kind, Scope: scope, Reason: reason}
}

func canonicalRecordSourceType(m Metadata) string {
	if sourceType := strings.ToLower(strings.TrimSpace(m.SourceType)); sourceType != "" {
		return sourceType
	}
	// Legacy compatibility is deliberately conservative. Exact built-in
	// instance names and unambiguous position fields are safe to infer; a
	// custom instance name without SourceType remains unknown.
	source := strings.ToLower(strings.TrimSpace(m.Source))
	switch source {
	case SourceTypeMySQLCDC, SourceTypeMySQLSnapshotCDC, SourceTypePostgresCDC,
		SourceTypeKafka, SourceTypeMySQLBatch, SourceTypeFile, SourceTypeHTTP,
		SourceTypeREST, SourceTypeRedis:
		return source
	}
	if m.LSN != "" {
		return SourceTypePostgresCDC
	}
	if m.SnapshotHandoffFile != "" {
		return SourceTypeMySQLSnapshotCDC
	}
	if m.BinlogFile != "" {
		return SourceTypeMySQLCDC
	}
	return ""
}

func orderScope(sourceType string, m Metadata) string {
	return sourceType + ":" + m.Database + ":" + m.Table
}

func kafkaOrderScope(m Metadata) string {
	return SourceTypeKafka + ":" + m.Table + ":partition=" + strconv.FormatInt(int64(m.Partition), 10)
}

func parseBinlogFileSequence(file string) (uint64, bool) {
	file = strings.TrimSpace(file)
	dot := strings.LastIndexByte(file, '.')
	if dot < 0 || dot == len(file)-1 {
		return 0, false
	}
	return parseUnsignedDecimal(file[dot+1:])
}

func parseUnsignedDecimal(value string) (uint64, bool) {
	if value == "" {
		return 0, false
	}
	for _, r := range value {
		if r < '0' || r > '9' {
			return 0, false
		}
	}
	parsed, err := strconv.ParseUint(value, 10, 64)
	return parsed, err == nil
}

func parsePostgresLSN(value string) (uint64, bool) {
	value = strings.TrimSpace(value)
	parts := strings.Split(value, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
		return 0, false
	}
	high, err := strconv.ParseUint(parts[0], 16, 32)
	if err != nil {
		return 0, false
	}
	low, err := strconv.ParseUint(parts[1], 16, 32)
	if err != nil {
		return 0, false
	}
	return high<<32 | low, true
}

type RecordIdentityReason string

// BeforeImageState describes the original old/before payload shape. It must
// reflect what the source received, not a reconstructed map, so replay can
// apply the exact format contract that was active when the event was read.
type BeforeImageState string

const (
	BeforeImageStateAbsent       BeforeImageState = "absent"
	BeforeImageStateNull         BeforeImageState = "null"
	BeforeImageStateEmptyArray   BeforeImageState = "empty_array"
	BeforeImageStatePresentEmpty BeforeImageState = "present_empty"
	BeforeImageStatePartial      BeforeImageState = "partial"
	BeforeImageStateFull         BeforeImageState = "full"
	BeforeImageStateInvalid      BeforeImageState = "invalid"
)

const (
	// FormatContractCanalJSONV1 records Alibaba Canal flat-message semantics:
	// UPDATE old contains changed columns only, so an omitted key component is
	// unchanged and may be filled from the complete after-image in Data.
	FormatContractCanalJSONV1 = "kafka.canal_json/v1"
	// FormatContractOpenETLEnvelopeV1 identifies the legacy OpenETL envelope.
	// It carries a full before image when UPDATE identity is required.
	FormatContractOpenETLEnvelopeV1 = "openetl.envelope/v1"
	// FormatContractDebeziumEnvelopeV1 identifies a Debezium-style envelope.
	// No missing-before fallback is assumed because connector before-image
	// availability depends on upstream configuration.
	FormatContractDebeziumEnvelopeV1 = "debezium.envelope/v1"
)

const (
	RecordIdentityReasonPKDeclarationMissing       RecordIdentityReason = "primary_key_columns_missing"
	RecordIdentityReasonPKDeclarationInvalid       RecordIdentityReason = "primary_key_columns_invalid"
	RecordIdentityReasonKeyMissing                 RecordIdentityReason = "key_missing"
	RecordIdentityReasonKeyInvalidJSON             RecordIdentityReason = "key_invalid_json"
	RecordIdentityReasonKeyNotObject               RecordIdentityReason = "key_not_object"
	RecordIdentityReasonKeyEmpty                   RecordIdentityReason = "key_empty"
	RecordIdentityReasonKeyColumnMissing           RecordIdentityReason = "key_component_missing"
	RecordIdentityReasonKeyColumnEmpty             RecordIdentityReason = "key_component_empty"
	RecordIdentityReasonKeyColumnsConflict         RecordIdentityReason = "key_columns_conflict"
	RecordIdentityReasonBeforeMissing              RecordIdentityReason = "update_before_image_missing"
	RecordIdentityReasonBeforeKeyMissing           RecordIdentityReason = "update_before_key_missing"
	RecordIdentityReasonBeforeKeyEmpty             RecordIdentityReason = "update_before_key_empty"
	RecordIdentityReasonBeforeKeyConflict          RecordIdentityReason = "update_before_key_conflict"
	RecordIdentityReasonBeforeStateInvalid         RecordIdentityReason = "update_before_state_invalid"
	RecordIdentityReasonDataKeyMissing             RecordIdentityReason = "data_key_component_missing"
	RecordIdentityReasonDataKeyEmpty               RecordIdentityReason = "data_key_component_empty"
	RecordIdentityReasonDataKeyConflict            RecordIdentityReason = "data_key_component_conflict"
	RecordIdentityReasonFormatContractUnknown      RecordIdentityReason = "format_contract_unknown"
	RecordIdentityReasonLegacyProvenanceInvalid    RecordIdentityReason = "legacy_provenance_invalid"
	RecordIdentityReasonLegacyKeyChangeUnsupported RecordIdentityReason = "legacy_key_change_unsupported"
)

// RecordIdentityResult makes incomplete identity a first-class outcome. Values
// contains only the declared primary-key columns, never undeclared JSON keys.
type RecordIdentityResult struct {
	Complete       bool                 `json:"complete"`
	Columns        []string             `json:"columns,omitempty"`
	Values         map[string]any       `json:"values,omitempty"`
	Reason         RecordIdentityReason `json:"reason,omitempty"`
	MissingColumns []string             `json:"missing_columns,omitempty"`
	BeforeImage    bool                 `json:"before_image,omitempty"`
	// KeyChanged is true only for a complete UPDATE whose after-image key is
	// different from the validated before-image key in Values. Metadata-PK
	// sinks use this to materialize the old-key delete before acknowledging the
	// source position; they must not try to rediscover the old key themselves.
	KeyChanged bool `json:"key_changed,omitempty"`
}

// RecordIdentity validates Metadata.Key against the complete declared primary
// key. UPDATE identity is accepted only when the key matches a complete
// before-image, so a primary-key change cannot accidentally target the new row.
func RecordIdentity(record Record) RecordIdentityResult {
	columns, reason := normalizedPKColumns(record.Metadata.PrimaryKeyColumns)
	if reason != "" {
		return RecordIdentityResult{Reason: reason}
	}

	keyText := strings.TrimSpace(record.Metadata.Key)
	if keyText == "" {
		return RecordIdentityResult{Columns: columns, Reason: RecordIdentityReasonKeyMissing}
	}
	decoded, decodeReason := decodeIdentityKey(keyText)
	if decodeReason != "" {
		return RecordIdentityResult{Columns: columns, Reason: decodeReason}
	}
	if len(decoded) == 0 {
		return RecordIdentityResult{Columns: columns, Reason: RecordIdentityReasonKeyEmpty}
	}

	values := make(map[string]any, len(columns))
	missing := make([]string, 0)
	for _, column := range columns {
		value, ok := decoded[column]
		if !ok {
			missing = append(missing, column)
			continue
		}
		if identityValueEmpty(value) {
			return RecordIdentityResult{
				Columns: columns, Values: values,
				Reason:         RecordIdentityReasonKeyColumnEmpty,
				MissingColumns: []string{column},
			}
		}
		values[column] = value
	}
	if len(missing) > 0 {
		return RecordIdentityResult{
			Columns: columns, Values: values,
			Reason: RecordIdentityReasonKeyColumnMissing, MissingColumns: missing,
		}
	}
	if len(decoded) != len(columns) {
		return RecordIdentityResult{Columns: columns, Values: values, Reason: RecordIdentityReasonKeyColumnsConflict}
	}

	// The row image that will reach a metadata-PK sink must also contain every
	// declared key component. For INSERT/DELETE it must agree with Metadata.Key;
	// for UPDATE it is the complete after identity and may legitimately differ
	// from the before-image key when the primary key changes.
	if record.Operation == OpInsert || record.Operation == OpDelete {
		for _, column := range columns {
			dataValue, ok := record.Data[column]
			if !ok {
				return RecordIdentityResult{
					Columns: columns, Values: values,
					Reason: RecordIdentityReasonDataKeyMissing, MissingColumns: []string{column},
				}
			}
			if identityValueEmpty(dataValue) {
				return RecordIdentityResult{
					Columns: columns, Values: values,
					Reason: RecordIdentityReasonDataKeyEmpty, MissingColumns: []string{column},
				}
			}
			if !identityValuesEqual(values[column], dataValue) {
				return RecordIdentityResult{
					Columns: columns, Values: values,
					Reason: RecordIdentityReasonDataKeyConflict, MissingColumns: []string{column},
				}
			}
		}
	}

	if record.Operation == OpUpdate {
		allowCanalFallback := record.Metadata.FormatContractID == FormatContractCanalJSONV1 &&
			canalBeforeStateAllowsDataFallback(record.Metadata.BeforeImageState)
		if record.Metadata.FormatContractID == FormatContractCanalJSONV1 &&
			!allowCanalFallback {
			return RecordIdentityResult{Columns: columns, Values: values, Reason: RecordIdentityReasonBeforeStateInvalid}
		}
		if len(record.Before) == 0 && !allowCanalFallback {
			return RecordIdentityResult{Columns: columns, Values: values, Reason: RecordIdentityReasonBeforeMissing}
		}
		for _, column := range columns {
			before, ok := record.Before[column]
			if !ok && allowCanalFallback {
				before, ok = record.Data[column]
			}
			if !ok {
				return RecordIdentityResult{
					Columns: columns, Values: values,
					Reason: RecordIdentityReasonBeforeKeyMissing, MissingColumns: []string{column},
				}
			}
			if identityValueEmpty(before) {
				return RecordIdentityResult{
					Columns: columns, Values: values,
					Reason: RecordIdentityReasonBeforeKeyEmpty, MissingColumns: []string{column},
				}
			}
			if !identityValuesEqual(values[column], before) {
				return RecordIdentityResult{
					Columns: columns, Values: values,
					Reason: RecordIdentityReasonBeforeKeyConflict, MissingColumns: []string{column},
				}
			}
		}
		keyChanged := false
		for _, column := range columns {
			dataValue, ok := record.Data[column]
			if !ok {
				return RecordIdentityResult{
					Columns: columns, Values: values,
					Reason: RecordIdentityReasonDataKeyMissing, MissingColumns: []string{column},
				}
			}
			if identityValueEmpty(dataValue) {
				return RecordIdentityResult{
					Columns: columns, Values: values,
					Reason: RecordIdentityReasonDataKeyEmpty, MissingColumns: []string{column},
				}
			}
			if !identityValuesEqual(values[column], dataValue) {
				keyChanged = true
			}
		}
		// legacy_verified is a deliberately narrow repair contract. T2.6 can
		// prove an unchanged historical business key, but it cannot prove that
		// an old producer represented a key-changing UPDATE correctly. Keep the
		// event quarantined even if a caller bypasses the replay API and invokes a
		// metadata-PK sink directly.
		if record.Metadata.ReplayProvenance == DLQReplayProvenanceLegacyVerified && keyChanged {
			return RecordIdentityResult{
				Columns: columns, Values: values, BeforeImage: true, KeyChanged: true,
				Reason: RecordIdentityReasonLegacyKeyChangeUnsupported,
			}
		}
		return RecordIdentityResult{Complete: true, Columns: columns, Values: values, BeforeImage: true, KeyChanged: keyChanged}
	}

	return RecordIdentityResult{Complete: true, Columns: columns, Values: values}
}

func canalBeforeStateAllowsDataFallback(state BeforeImageState) bool {
	switch state {
	case BeforeImageStatePresentEmpty, BeforeImageStatePartial, BeforeImageStateFull:
		return true
	default:
		return false
	}
}

// ReconstructDLQRecordIdentity rebuilds Metadata.Key only from facts frozen in
// the DLQ row. It never reads the current source configuration. A caller must
// separately verify that legacy_verified declarations exactly match the
// target's configured static key set before invoking this helper.
func ReconstructDLQRecordIdentity(record Record, ctx DLQIdentityContext) (Record, RecordIdentityResult) {
	rebuilt := record
	rebuilt.Metadata.PrimaryKeyColumns = append([]string(nil), ctx.PrimaryKeyColumns...)
	rebuilt.Metadata.FormatContractID = ctx.FormatContractID
	rebuilt.Metadata.BeforeImageState = ctx.BeforeImageState
	rebuilt.Metadata.ReplayProvenance = ctx.ReplayProvenance

	columns, reason := normalizedPKColumns(rebuilt.Metadata.PrimaryKeyColumns)
	if reason != "" {
		return rebuilt, RecordIdentityResult{Reason: reason}
	}
	knownContract := ctx.FormatContractID == FormatContractCanalJSONV1 ||
		ctx.FormatContractID == FormatContractOpenETLEnvelopeV1 ||
		ctx.FormatContractID == FormatContractDebeziumEnvelopeV1
	legacyVerified := ctx.ReplayProvenance == DLQReplayProvenanceLegacyVerified
	if !knownContract && !legacyVerified {
		return rebuilt, RecordIdentityResult{Columns: columns, Reason: RecordIdentityReasonFormatContractUnknown}
	}

	key := make(map[string]any, len(columns))
	missing := make([]string, 0)
	for _, column := range columns {
		var value any
		var ok bool
		if rebuilt.Operation == OpUpdate {
			value, ok = rebuilt.Before[column]
			if !ok && ctx.FormatContractID == FormatContractCanalJSONV1 && canalBeforeStateAllowsDataFallback(ctx.BeforeImageState) {
				value, ok = rebuilt.Data[column]
			}
		} else {
			value, ok = rebuilt.Data[column]
		}
		if !ok || identityValueEmpty(value) {
			missing = append(missing, column)
			continue
		}
		key[column] = value
	}
	if len(missing) > 0 {
		reason := RecordIdentityReasonDataKeyMissing
		if rebuilt.Operation == OpUpdate {
			reason = RecordIdentityReasonBeforeKeyMissing
		}
		return rebuilt, RecordIdentityResult{Columns: columns, Values: key, Reason: reason, MissingColumns: missing, BeforeImage: rebuilt.Operation == OpUpdate}
	}

	if legacyVerified && rebuilt.Operation == OpUpdate {
		// A legacy static-key fallback cannot prove a primary-key change unless
		// the historical before image is complete, and even then T2.6 permits
		// only unchanged keys. Key-changing UPDATEs require format-aware replay.
		for _, column := range columns {
			after, ok := rebuilt.Data[column]
			if !ok || identityValueEmpty(after) || !identityValuesEqual(key[column], after) {
				return rebuilt, RecordIdentityResult{Columns: columns, Values: key, Reason: RecordIdentityReasonLegacyKeyChangeUnsupported, MissingColumns: []string{column}, BeforeImage: true}
			}
		}
	}

	encoded, err := json.Marshal(key)
	if err != nil {
		return rebuilt, RecordIdentityResult{Columns: columns, Values: key, Reason: RecordIdentityReasonKeyInvalidJSON}
	}
	rebuilt.Metadata.Key = string(encoded)
	rebuilt.Rejection = nil
	identity := RecordIdentity(rebuilt)
	return rebuilt, identity
}

func normalizedPKColumns(input []string) ([]string, RecordIdentityReason) {
	if len(input) == 0 {
		return nil, RecordIdentityReasonPKDeclarationMissing
	}
	columns := make([]string, 0, len(input))
	seen := make(map[string]struct{}, len(input))
	for _, column := range input {
		if strings.TrimSpace(column) == "" {
			return nil, RecordIdentityReasonPKDeclarationInvalid
		}
		if _, exists := seen[column]; exists {
			return nil, RecordIdentityReasonPKDeclarationInvalid
		}
		seen[column] = struct{}{}
		columns = append(columns, column)
	}
	return columns, ""
}

func decodeIdentityKey(input string) (map[string]any, RecordIdentityReason) {
	decoder := json.NewDecoder(strings.NewReader(input))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, RecordIdentityReasonKeyInvalidJSON
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, RecordIdentityReasonKeyInvalidJSON
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil, RecordIdentityReasonKeyNotObject
	}
	return object, ""
}

func identityValueEmpty(value any) bool {
	switch v := value.(type) {
	case nil:
		return true
	case string:
		return v == ""
	case []byte:
		return len(v) == 0
	case []any, map[string]any:
		return true
	default:
		return false
	}
}

func identityValuesEqual(left, right any) bool {
	if leftBytes, ok := left.([]byte); ok {
		left = string(leftBytes)
	}
	if rightBytes, ok := right.([]byte); ok {
		right = string(rightBytes)
	}
	if leftNumber, ok := identityNumber(left); ok {
		if rightNumber, ok := identityNumber(right); ok {
			return leftNumber.Cmp(rightNumber) == 0
		}
	}
	leftJSON, leftErr := json.Marshal(left)
	rightJSON, rightErr := json.Marshal(right)
	return leftErr == nil && rightErr == nil && bytes.Equal(leftJSON, rightJSON)
}

func identityNumber(value any) (*big.Rat, bool) {
	var text string
	switch v := value.(type) {
	case json.Number:
		text = v.String()
	case int:
		text = strconv.FormatInt(int64(v), 10)
	case int8:
		text = strconv.FormatInt(int64(v), 10)
	case int16:
		text = strconv.FormatInt(int64(v), 10)
	case int32:
		text = strconv.FormatInt(int64(v), 10)
	case int64:
		text = strconv.FormatInt(v, 10)
	case uint:
		text = strconv.FormatUint(uint64(v), 10)
	case uint8:
		text = strconv.FormatUint(uint64(v), 10)
	case uint16:
		text = strconv.FormatUint(uint64(v), 10)
	case uint32:
		text = strconv.FormatUint(uint64(v), 10)
	case uint64:
		text = strconv.FormatUint(v, 10)
	case float32:
		text = strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		text = strconv.FormatFloat(v, 'g', -1, 64)
	default:
		return nil, false
	}
	number, ok := new(big.Rat).SetString(text)
	return number, ok
}
