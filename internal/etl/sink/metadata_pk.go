package sink

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// metadataPKTarget is the fully resolved destination owned by a sink. A
// metadata key is validated against this target before DDL, schema drift, or
// row writes are allowed to produce a side effect.
type metadataPKTarget struct {
	Database string
	Table    string
}

type metadataPKTargetResolver func(core.Record) (metadataPKTarget, error)

// metadataPKBatch is the immutable result of validating one sink batch. The
// per-record identities are retained so relational sinks can materialize an
// old-key delete for a primary-key-changing UPDATE without parsing Key again.
type metadataPKBatch struct {
	ColumnsByTable map[string][]string
	Targets        []metadataPKTarget
	Identities     []core.RecordIdentityResult
}

// metadataPKValidationError is intentionally structured so tests, logs, and
// per-target metrics can all identify the same rejected destination and stable
// identity reason. It is wrapped in core.ClassifiedError at the boundary.
type metadataPKValidationError struct {
	Sink           string
	TargetDatabase string
	TargetTable    string
	RecordIndex    int
	Reason         string
	MissingColumns []string
	Detail         string
}

func (e *metadataPKValidationError) Error() string {
	if e == nil {
		return "metadata-PK validation failed"
	}
	target := strings.Trim(strings.TrimSpace(e.TargetDatabase)+"."+strings.TrimSpace(e.TargetTable), ".")
	if target == "" {
		target = "<unresolved>"
	}
	message := fmt.Sprintf("%s sink rejected metadata-PK record %d for target %s: reason=%s", e.Sink, e.RecordIndex, target, e.Reason)
	if len(e.MissingColumns) > 0 {
		message += " missing_columns=" + strings.Join(e.MissingColumns, ",")
	}
	if strings.TrimSpace(e.Detail) != "" {
		message += "; " + strings.TrimSpace(e.Detail)
	}
	return message
}

func newMetadataPKError(class core.ErrorClass, sinkName string, target metadataPKTarget, recordIndex int, reason string, missing []string, detail string) error {
	return core.ClassifiedError{Class: class, Err: &metadataPKValidationError{
		Sink:           sinkName,
		TargetDatabase: target.Database,
		TargetTable:    target.Table,
		RecordIndex:    recordIndex,
		Reason:         reason,
		MissingColumns: append([]string(nil), missing...),
		Detail:         detail,
	}}
}

// validateMetadataPKBatch is the single fail-closed contract used by every
// sink that advertises pk_columns_from_metadata. It trusts neither Key field
// names nor a static/id fallback: the complete authoritative declaration,
// key/data agreement, UPDATE before image, replay provenance, target routing,
// and per-target key set must all agree before any sink side effect.
func validateMetadataPKBatch(sinkName string, records []core.Record, legacySafetyColumns []string, resolve metadataPKTargetResolver) (metadataPKBatch, error) {
	result := metadataPKBatch{
		ColumnsByTable: make(map[string][]string),
		Targets:        make([]metadataPKTarget, len(records)),
		Identities:     make([]core.RecordIdentityResult, len(records)),
	}
	for index, record := range records {
		target := metadataPKTarget{Table: strings.TrimSpace(record.Metadata.Table)}
		if resolve == nil {
			return result, newMetadataPKError(core.ErrorClassConfig, sinkName, target, index,
				"target_resolver_missing", nil, "configure a concrete sink target resolver")
		}
		resolved, err := resolve(record)
		if err != nil {
			return result, newMetadataPKError(core.ErrorClassData, sinkName, target, index,
				"target_metadata_missing", nil, err.Error())
		}
		resolved.Database = strings.TrimSpace(resolved.Database)
		resolved.Table = strings.TrimSpace(resolved.Table)
		result.Targets[index] = resolved
		if resolved.Database == "" {
			return result, newMetadataPKError(core.ErrorClassConfig, sinkName, resolved, index,
				"target_database_missing", nil, "set sink.config.database before enabling pk_columns_from_metadata")
		}
		if resolved.Table == "" {
			return result, newMetadataPKError(core.ErrorClassData, sinkName, resolved, index,
				"target_table_missing", nil, "set sink.config.table or use a source/format that supplies Metadata.Table for every record")
		}

		identity := core.RecordIdentity(record)
		result.Identities[index] = identity
		if !identity.Complete {
			return result, newMetadataPKError(core.ErrorClassData, sinkName, resolved, index,
				string(identity.Reason), identity.MissingColumns,
				"Metadata.PrimaryKeyColumns and JSON-object Metadata.Key must declare the complete business key and agree with the row image")
		}

		switch record.Metadata.ReplayProvenance {
		case "", core.DLQReplayProvenanceNormalFlow, core.DLQReplayProvenanceReconstructed:
			// Empty provenance is normal, non-replay traffic produced before the
			// replay marker was introduced.
		case core.DLQReplayProvenanceLegacyVerified:
			if len(legacySafetyColumns) == 0 || !sameIdentifierSet(legacySafetyColumns, identity.Columns) {
				return result, newMetadataPKError(core.ErrorClassData, sinkName, resolved, index,
					string(core.RecordIdentityReasonLegacyProvenanceInvalid), nil,
					"legacy_verified replay requires sink.config.pk_columns to exactly match the frozen primary-key declaration")
			}
		default:
			return result, newMetadataPKError(core.ErrorClassData, sinkName, resolved, index,
				string(core.RecordIdentityReasonLegacyProvenanceInvalid), nil,
				"repair or reconstruct the frozen DLQ identity before replay; current source configuration is not an identity authority")
		}

		columns := append([]string(nil), identity.Columns...)
		sort.Strings(columns)
		for _, table := range []string{resolved.Table, strings.TrimSpace(record.Metadata.Table)} {
			if table == "" {
				continue
			}
			if existing, ok := result.ColumnsByTable[table]; ok && !sameIdentifierSet(existing, columns) {
				return result, newMetadataPKError(core.ErrorClassData, sinkName, resolved, index,
					"target_key_set_changed", nil,
					fmt.Sprintf("primary-key columns for table %q changed within one batch (%v -> %v)", table, existing, columns))
			}
			result.ColumnsByTable[table] = append([]string(nil), columns...)
		}
	}
	return result, nil
}

// expandMetadataPKKeyChanges turns UPDATE(old PK -> new PK) into an old-key
// DELETE plus the original live UPDATE. Callers still control the target's
// native ordering/atomicity strategy, but no relational sink can silently
// leave the old key behind. The tombstone's Before and Data both contain a
// complete old key so existing delete builders cannot fall back to the new
// after-image by accident.
func expandMetadataPKKeyChanges(records []core.Record, validation metadataPKBatch) ([]core.Record, int) {
	if len(records) == 0 || len(validation.Identities) != len(records) {
		return records, 0
	}
	expanded := make([]core.Record, 0, len(records)+1)
	generatedDeletes := 0
	for index, record := range records {
		identity := validation.Identities[index]
		if record.Operation != core.OpUpdate || !identity.Complete || !identity.KeyChanged {
			expanded = append(expanded, record)
			continue
		}
		oldData := cloneMetadataPKMap(record.Data)
		for column, value := range record.Before {
			oldData[column] = value
		}
		for _, column := range identity.Columns {
			if value, exists := oldData[column]; !exists || value == nil {
				oldData[column] = identity.Values[column]
			}
		}
		tombstone := record
		tombstone.Operation = core.OpDelete
		tombstone.Data = oldData
		tombstone.Before = cloneMetadataPKMap(oldData)
		expanded = append(expanded, tombstone, record)
		generatedDeletes++
	}
	return expanded, generatedDeletes
}

func cloneMetadataPKMap(input map[string]any) map[string]any {
	if input == nil {
		return nil
	}
	result := make(map[string]any, len(input))
	for key, value := range input {
		result[key] = value
	}
	return result
}

// recordMetadataPKFailure exposes a rejected target without counting a row as
// successfully written. The aggregate sink counters are updated by Write's
// normal error path; this helper supplies the per-target dimension.
func recordMetadataPKFailure(metrics *tableMetricsSet, err error, latency time.Duration) {
	if metrics == nil || err == nil {
		return
	}
	var validationErr *metadataPKValidationError
	if errors.As(err, &validationErr) && validationErr.TargetTable != "" {
		metrics.record(validationErr.TargetTable, 0, latency, true)
	}
}

// sameIdentifierSet reports whether two identifier lists name the same set
// (order-insensitive).
func sameIdentifierSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	bm := make(map[string]int, len(b))
	for _, id := range b {
		bm[id]++
	}
	for _, id := range a {
		if bm[id] == 0 {
			return false
		}
		bm[id]--
	}
	return true
}
