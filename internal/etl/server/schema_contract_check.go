package server

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

// ── CH-C2 (IT-5/T5.4): additive-only schema contract enforcement ──────
//
// Opt-in via sink/source spec: `schema_contract: enforce` on the pipeline
// root. When enforced, preflight compares the live source schema against
// the newest stored contract:
//   - additive columns (live has, contract lacks) -> warning + guidance to
//     re-validate (which captures the new contract);
//   - removed columns / incompatible types -> BLOCKING error with an
//     explainable diff and remediation;
//   - no stored contract yet -> capture one on this validation so later
//     drift has a baseline.
//
// The ddl_guard transform semantics are unchanged: it still rejects DDL
// statements. The contract governs the column set, not DDL statements.

// SchemaContractStore is the optional storage capability for persisted
// schema contracts (implemented by all built-in SQL backends).
type SchemaContractStore interface {
	SaveSchemaContract(ctx context.Context, c core.SchemaContract) error
	LoadLatestSchemaContract(ctx context.Context, pipeline string) (*core.SchemaContract, error)
}

// schemaContractEnforced reads the opt-in flag from the spec root.
func schemaContractEnforced(spec *pipeline.Spec) bool {
	if spec == nil {
		return false
	}
	if sc, ok := spec.Source.Config["schema_contract"]; ok {
		if v, ok := sc.(string); ok && strings.EqualFold(v, "enforce") {
			return true
		}
		if v, ok := sc.(bool); ok && v {
			return true
		}
	}
	if sc, ok := spec.Sink.Config["schema_contract"]; ok {
		if v, ok := sc.(string); ok && strings.EqualFold(v, "enforce") {
			return true
		}
		if v, ok := sc.(bool); ok && v {
			return true
		}
	}
	return false
}

// columnsFromSchemaInfo converts a described source schema into contract
// columns.
func columnsFromSchemaInfo(schema core.SchemaInfo) []core.ColumnContract {
	cols := make([]core.ColumnContract, 0, len(schema.Columns))
	for _, c := range schema.Columns {
		cols = append(cols, core.ColumnContract{
			Name:     c.Name,
			Type:     c.DataType,
			Nullable: c.Nullable,
		})
	}
	return cols
}

// checkSchemaContractEnforcement runs the additive-only comparison when the
// pipeline opted in. capture=true (spec validation path) stores a new
// contract when none exists; preflight re-runs the check read-only so it can
// surface drift without mutating state.
func (s *Server) checkSchemaContractEnforcement(ctx context.Context, spec *pipeline.Spec, schema core.SchemaInfo, result *PreflightResult, capture bool) {
	if !schemaContractEnforced(spec) || len(schema.Columns) == 0 {
		return
	}
	store, ok := s.store.(SchemaContractStore)
	if !ok {
		addPreflightGuidance(result, PreflightGuidance{
			Level:    "warning",
			Category: "schema",
			Code:     "schema-contract-store-unavailable",
			Message:  "schema_contract: enforce requested but the storage backend does not persist contracts",
			Action:   "use a SQL storage backend (sqlite/mysql/postgresql) or remove schema_contract to avoid an unenforced expectation",
		})
		return
	}

	name := spec.Name
	existing, err := store.LoadLatestSchemaContract(ctx, name)
	if err != nil {
		addPreflightGuidance(result, PreflightGuidance{
			Level:    "warning",
			Category: "schema",
			Code:     "schema-contract-load-failed",
			Message:  fmt.Sprintf("schema contract load failed: %v", err),
			Action:   "retry validation; if the error persists, inspect the storage backend",
		})
		return
	}

	live := columnsFromSchemaInfo(schema)

	if existing == nil {
		if !capture {
			addPreflightGuidance(result, PreflightGuidance{
				Level:    "warning",
				Category: "schema",
				Code:     "schema-contract-missing",
				Message:  "schema_contract: enforce has no stored baseline for this pipeline yet",
				Action:   "re-run spec validation (POST /api/v2/specs/validate) to capture the contract baseline",
			})
			return
		}
		version := s.currentSpecVersion(ctx, name)
		contract := core.NormalizeSchemaContract(name, version, live, spec.Source.Type, sourceDatabase(spec), sourceTable(spec))
		if err := store.SaveSchemaContract(ctx, contract); err != nil {
			addPreflightGuidance(result, PreflightGuidance{
				Level:    "warning",
				Category: "schema",
				Code:     "schema-contract-capture-failed",
				Message:  fmt.Sprintf("schema contract capture failed: %v", err),
				Action:   "the pipeline still validates against the live schema; fix storage and re-validate to persist the contract",
			})
			return
		}
		addPreflightGuidance(result, PreflightGuidance{
			Level:    "info",
			Category: "schema",
			Code:     "schema-contract-captured",
			Message:  fmt.Sprintf("schema contract captured (fingerprint %.12s…, %d columns)", contract.Fingerprint, len(contract.Columns)),
			Action:   "later source drift will be compared against this baseline",
		})
		return
	}

	diff := core.DiffSchemaContract(*existing, live)
	switch diff.Severity() {
	case core.SchemaContractMatch:
		addPreflightGuidance(result, PreflightGuidance{
			Level:    "info",
			Category: "schema",
			Code:     "schema-contract-match",
			Message:  fmt.Sprintf("schema contract matches (fingerprint %.12s…)", existing.Fingerprint),
			Action:   "",
		})
	case core.SchemaContractAdditive:
		addPreflightGuidance(result, PreflightGuidance{
			Level:    "warning",
			Category: "schema",
			Code:     "schema-contract-additive-drift",
			Message:  fmt.Sprintf("source schema added column(s): %s (allowed; additive-only)", strings.Join(diff.Added, ", ")),
			Action:   "re-run spec validation to adopt the new columns into the contract baseline",
		})
	default:
		var parts []string
		if len(diff.Removed) > 0 {
			parts = append(parts, fmt.Sprintf("removed column(s): %s", strings.Join(diff.Removed, ", ")))
		}
		for _, tc := range diff.TypeChanged {
			parts = append(parts, fmt.Sprintf("column %q type %s -> %s (%s)", tc.Column, tc.Expected, tc.Actual, tc.Reason))
		}
		result.Issues = append(result.Issues, PreflightIssue{
			Level:  "error",
			Check:  "schema-contract-destructive-drift",
			Message: fmt.Sprintf(
				"source schema drifted destructively against the stored contract (fingerprint %.12s…): %s",
				existing.Fingerprint, strings.Join(parts, "; ")),
			Remediation: "restore the removed columns or migrate the contract deliberately: stop the pipeline, fix the source schema, then re-validate to capture a new contract; historical replay against this schema is not safe",
		})
		result.Passed = false
	}
}

// currentSpecVersion reads the latest version number for the pipeline (0
// when only the current row exists — contract rows still round-trip).
func (s *Server) currentSpecVersion(ctx context.Context, name string) int64 {
	versions, err := s.store.ListPipelineVersions(ctx, name)
	if err != nil || len(versions) == 0 {
		return 0
	}
	return int64(versions[len(versions)-1].Version)
}

// sourceDatabase extracts the origin database label from the source config.
func sourceDatabase(spec *pipeline.Spec) string {
	if spec == nil {
		return ""
	}
	if v, ok := spec.Source.Config["database"].(string); ok {
		return v
	}
	return ""
}

// sourceTable extracts the origin table label from the source config.
func sourceTable(spec *pipeline.Spec) string {
	if spec == nil {
		return ""
	}
	if v, ok := spec.Source.Config["table"].(string); ok {
		return v
	}
	return ""
}

// captureSchemaContract runs the enforced-contract check on the spec
// validation path with capture=true: a pipeline without a baseline gets one;
// drift guidance is appended to warnings. The preflight result may already
// carry contract guidance from the read-only pass; the capture pass only
// adds the baseline when missing.
func (s *Server) captureSchemaContract(ctx context.Context, spec *pipeline.Spec, preflightResult *PreflightResult, warnings *[]string) {
	if !schemaContractEnforced(spec) || s.store == nil {
		return
	}
	schema, ok := s.describeSourceSchema(ctx, spec)
	if !ok || len(schema.Columns) == 0 {
		return
	}
	result := &PreflightResult{Passed: true}
	s.checkSchemaContractEnforcement(ctx, spec, schema, result, true)
	for _, g := range result.Guidance {
		*warnings = append(*warnings, fmt.Sprintf("[schema-contract] %s — %s", g.Message, g.Action))
	}
	for _, issue := range result.Issues {
		*warnings = append(*warnings, fmt.Sprintf("[schema-contract] %s — %s", issue.Message, issue.Remediation))
	}
}

// describeSourceSchema builds the live source schema via the same
// introspection preflight uses (SchemaDescriptor or explicit/inferred
// schema), bounded by a short timeout.
func (s *Server) describeSourceSchema(ctx context.Context, spec *pipeline.Spec) (core.SchemaInfo, bool) {
	source, err := registry.BuildSource(spec.Source.Type, spec.Source.Config)
	if err != nil {
		return core.SchemaInfo{}, false
	}
	defer func() {
		if closer, ok := source.(interface{ Close() error }); ok {
			_ = closer.Close()
		}
	}()
	descriptor, ok := source.(core.SchemaDescriptor)
	if !ok {
		schema, _, inferred, err := inferPreflightSchema(ctx, spec)
		if err != nil || !inferred {
			return core.SchemaInfo{}, false
		}
		return schema, true
	}
	probeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	schema, err := descriptor.Describe(probeCtx)
	if err != nil {
		return core.SchemaInfo{}, false
	}
	return schema, true
}
