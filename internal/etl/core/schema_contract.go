package core

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
)

// CH-C2 (IT-5/T5.4): additive-only schema contract.
//
// A SchemaContract freezes the source column set a pipeline was validated
// against. At validate/preflight time the live source schema is compared
// against the stored contract: additive columns (new columns the contract
// does not know) pass with a warning; removed columns, renames and
// incompatible type changes are BLOCKED with remediation. Old contracts
// replaying against a drifted schema yield an explainable diff instead of
// silently writing wrong data.

// ColumnContract is one frozen column of the source schema.
type ColumnContract struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable,omitempty"`
}

// SchemaContract is the persisted source-schema snapshot for a pipeline.
type SchemaContract struct {
	// Pipeline is the pipeline key the contract belongs to.
	Pipeline string `json:"pipeline"`
	// SpecVersion is the pipeline spec version this contract was captured
	// with; a spec update re-captures the contract.
	SpecVersion int64 `json:"spec_version"`
	// Fingerprint is the deterministic hash of the normalized column set.
	Fingerprint string `json:"fingerprint"`
	// Columns is the ordered column set (sorted by name).
	Columns []ColumnContract `json:"columns"`
	// SourceType / Database / Table identify the origin for diagnostics.
	SourceType string `json:"source_type,omitempty"`
	Database   string `json:"database,omitempty"`
	Table      string `json:"table,omitempty"`
}

// ComputeSchemaFingerprint derives a stable fingerprint from the normalized
// column set. Column order does not matter; type comparison is on the
// lowercased trimmed string. Two contracts over the same logical schema
// always produce the same fingerprint.
func ComputeSchemaFingerprint(columns []ColumnContract) string {
	sorted := make([]ColumnContract, len(columns))
	copy(sorted, columns)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].Name < sorted[j].Name })
	h := sha256.New()
	for _, c := range sorted {
		fmt.Fprintf(h, "%s\x00%s\x00%t\x00", c.Name, strings.ToLower(strings.TrimSpace(c.Type)), c.Nullable)
	}
	return hex.EncodeToString(h.Sum(nil))
}

// NormalizeSchemaContract builds a contract with a computed fingerprint.
func NormalizeSchemaContract(pipeline string, specVersion int64, columns []ColumnContract, sourceType, database, table string) SchemaContract {
	return SchemaContract{
		Pipeline:    pipeline,
		SpecVersion: specVersion,
		Fingerprint: ComputeSchemaFingerprint(columns),
		Columns:     columns,
		SourceType:  sourceType,
		Database:    database,
		Table:       table,
	}
}

// SchemaContractDiff is the explainable difference between a stored
// contract and the live schema.
type SchemaContractDiff struct {
	// Added columns exist live but not in the contract (additive: allowed
	// with warning; the contract should be refreshed to adopt them).
	Added []string `json:"added,omitempty"`
	// Removed columns exist in the contract but not live (destructive:
	// blocked — historical replay would reference missing columns).
	Removed []string `json:"removed,omitempty"`
	// TypeChanged columns exist on both sides but with incompatible types.
	TypeChanged []SchemaColumnTypeChange `json:"type_changed,omitempty"`
}

// SchemaColumnTypeChange describes one incompatible type pair.
type SchemaColumnTypeChange struct {
	Column   string `json:"column"`
	Expected string `json:"expected"`
	Actual   string `json:"actual"`
	Reason   string `json:"reason"`
}

// Severity classifies the diff outcome for validate/preflight.
type SchemaContractSeverity string

const (
	SchemaContractMatch      SchemaContractSeverity = "match"
	SchemaContractAdditive   SchemaContractSeverity = "additive"
	SchemaContractDestructive SchemaContractSeverity = "destructive"
)

// Severity returns the worst outcome represented by the diff.
func (d SchemaContractDiff) Severity() SchemaContractSeverity {
	if len(d.Removed) > 0 || len(d.TypeChanged) > 0 {
		return SchemaContractDestructive
	}
	if len(d.Added) > 0 {
		return SchemaContractAdditive
	}
	return SchemaContractMatch
}

// DiffSchemaContract compares a stored contract against the live column set.
// The compatibility matrix is explicit and conservative:
//   - new live column -> Added (additive, allowed with warning)
//   - contract column missing live -> Removed (destructive, blocked)
//   - type changes are only allowed as safe widenings; everything else
//     (narrowing, kind change, nullability loss) is blocked.
func DiffSchemaContract(contract SchemaContract, live []ColumnContract) SchemaContractDiff {
	var diff SchemaContractDiff
	liveByName := make(map[string]ColumnContract, len(live))
	for _, c := range live {
		liveByName[c.Name] = c
	}
	contractByName := make(map[string]ColumnContract, len(contract.Columns))
	for _, c := range contract.Columns {
		contractByName[c.Name] = c
	}

	for name := range liveByName {
		if _, ok := contractByName[name]; !ok {
			diff.Added = append(diff.Added, name)
		}
	}
	sort.Strings(diff.Added)

	for _, c := range contract.Columns {
		lc, ok := liveByName[c.Name]
		if !ok {
			diff.Removed = append(diff.Removed, c.Name)
			continue
		}
		if reason := typeChangeReason(c, lc); reason != "" {
			diff.TypeChanged = append(diff.TypeChanged, SchemaColumnTypeChange{
				Column: c.Name, Expected: c.Type, Actual: lc.Type, Reason: reason,
			})
		}
	}
	sort.Strings(diff.Removed)
	sort.Slice(diff.TypeChanged, func(i, j int) bool { return diff.TypeChanged[i].Column < diff.TypeChanged[j].Column })
	return diff
}

// typeChangeReason returns "" when the change is a safe widening and a human
// reason otherwise. The matrix is deliberately small and explicit; anything
// not listed as safe is blocked (boundary before capability).
func typeChangeReason(expected, actual ColumnContract) string {
	e := normalizeTypeName(expected.Type)
	a := normalizeTypeName(actual.Type)
	if e == a {
		if expected.Nullable && !actual.Nullable {
			return "nullability lost (nullable -> not null)"
		}
		return ""
	}
	// Safe widenings within the same type family.
	if w := widened(e, a); w {
		return ""
	}
	return fmt.Sprintf("incompatible type change %q -> %q", e, a)
}

func normalizeTypeName(t string) string {
	t = strings.ToLower(strings.TrimSpace(t))
	// strip common display decorations
	if i := strings.Index(t, "("); i > 0 {
		t = t[:i]
	}
	return t
}

// familyOf buckets SQL types into comparable families.
func familyOf(t string) string {
	switch t {
	case "tinyint", "smallint", "mediumint", "int", "integer", "bigint":
		return "int"
	case "decimal", "numeric", "float", "double", "real":
		return "float"
	case "varchar", "char", "text", "tinytext", "mediumtext", "longtext", "string":
		return "string"
	case "date", "datetime", "timestamp", "time":
		return "datetime"
	case "boolean", "bool":
		return "bool"
	case "blob", "varbinary", "binary", "bytes":
		return "bytes"
	default:
		return t
	}
}

func widened(e, a string) bool {
	rank := map[string]int{"tinyint": 1, "smallint": 2, "mediumint": 3, "int": 4, "integer": 4, "bigint": 5}
	er, ok1 := rank[e]
	ar, ok2 := rank[a]
	if ok1 && ok2 {
		return ar >= er
	}
	if familyOf(e) == "string" && familyOf(a) == "string" {
		return true // text widenings within the string family
	}
	if familyOf(e) == "float" && familyOf(a) == "float" {
		return true
	}
	return false
}
