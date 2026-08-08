package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

type ConnectorDescriptor struct {
	Version            string             `json:"version"`
	Kind               string             `json:"kind"`
	Type               string             `json:"type"`
	Maturity           string             `json:"maturity"`
	Readiness          ConnectorReadiness `json:"readiness"`
	Required           []string           `json:"required"`
	Capabilities       []string           `json:"capabilities"`
	Fields             []ConfigField      `json:"fields"`
	SecretFields       []string           `json:"secret_fields"`
	Registered         bool               `json:"registered"`
	SupportedSchedules []string           `json:"supported_schedules,omitempty"`
	DefaultSchedule    string             `json:"default_schedule,omitempty"`
}

type ConnectorReadiness struct {
	Status  string                   `json:"status"`
	Summary string                   `json:"summary"`
	Gates   []ConnectorReadinessGate `json:"gates"`
}

type ConnectorReadinessGate struct {
	Code             string                     `json:"code"`
	Label            string                     `json:"label"`
	Status           string                     `json:"status"` // pass, partial, missing, not_applicable
	Evidence         string                     `json:"evidence,omitempty"`
	Remediation      string                     `json:"remediation,omitempty"`
	EvidenceMetadata *ConnectorEvidenceMetadata `json:"evidence_metadata,omitempty"`
}

var connectorMaturityLevels = []string{"production", "beta", "experimental", "dev-only"}

func (s *Server) handleConnectorDescriptors(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{
		"version":         "v1",
		"maturity_levels": connectorMaturityLevels,
		"descriptors":     connectorDescriptors(),
	})
}

func connectorDescriptors() []ConnectorDescriptor {
	schema := configSchema()
	metadata := pluginMetadata()
	var out []ConnectorDescriptor
	out = append(out, descriptorsForKind("source", registry.SourceTypes(), schema["sources"].(map[string][]ConfigField), metadata["sources"].(map[string]any))...)
	out = append(out, descriptorsForKind("sink", registry.SinkTypes(), schema["sinks"].(map[string][]ConfigField), metadata["sinks"].(map[string]any))...)
	out = append(out, descriptorsForKind("transform", registry.TransformTypes(), schema["transforms"].(map[string][]ConfigField), metadata["transforms"].(map[string]any))...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Kind == out[j].Kind {
			return out[i].Type < out[j].Type
		}
		return out[i].Kind < out[j].Kind
	})
	return out
}

func descriptorsForKind(kind string, registered []string, schemas map[string][]ConfigField, metadata map[string]any) []ConnectorDescriptor {
	names := map[string]bool{}
	registeredSet := map[string]bool{}
	for _, name := range registered {
		names[name] = true
		registeredSet[name] = true
	}
	for name := range schemas {
		names[name] = true
	}
	for name := range metadata {
		names[name] = true
	}

	out := make([]ConnectorDescriptor, 0, len(names))
	for name := range names {
		fields := annotateFieldScopes(kind, schemas[name])
		required := requiredFields(fields)
		secretFields := secretFields(fields)
		maturity := "experimental"
		var capabilities []string
		if info, ok := metadata[name].(map[string]any); ok {
			maturity, _ = info["maturity"].(string)
			maturity = normalizeConnectorMaturity(maturity)
			if caps, ok := info["capabilities"].([]string); ok {
				capabilities = append(capabilities, caps...)
			}
		}
		sort.Strings(required)
		sort.Strings(secretFields)
		sort.Strings(capabilities)
		var supportedSchedules []string
		var defaultSchedule string
		if kind == "source" {
			supportedSchedules = pipeline.SupportedSourceSchedules(name)
			defaultSchedule = pipeline.DefaultSourceSchedule(name)
		}
		out = append(out, ConnectorDescriptor{
			Version:            "v1",
			Kind:               kind,
			Type:               name,
			Maturity:           maturity,
			Readiness:          connectorReadiness(kind, name, maturity, capabilities, registeredSet[name], fields, supportedSchedules),
			Required:           required,
			Capabilities:       capabilities,
			Fields:             fields,
			SecretFields:       secretFields,
			Registered:         registeredSet[name],
			SupportedSchedules: supportedSchedules,
			DefaultSchedule:    defaultSchedule,
		})
	}
	return out
}

func normalizeConnectorMaturity(maturity string) string {
	for _, allowed := range connectorMaturityLevels {
		if maturity == allowed {
			return maturity
		}
	}
	return "experimental"
}

func connectorReadiness(kind, typ, maturity string, capabilities []string, registered bool, fields []ConfigField, supportedSchedules []string) ConnectorReadiness {
	capSet := map[string]bool{}
	for _, cap := range capabilities {
		capSet[cap] = true
	}
	gates := []ConnectorReadinessGate{
		{
			Code:        "registered",
			Label:       "Registered implementation",
			Status:      gateStatus(registered, false),
			Evidence:    "connector registry contains this implementation",
			Remediation: "register the connector implementation or remove it from public metadata",
		},
		{
			Code:        "config_schema",
			Label:       "Typed config schema",
			Status:      gateStatus(len(fields) > 0, kind == "transform" && len(fields) == 0),
			Evidence:    "plugin schema exposes fields, defaults, secrets, and required markers",
			Remediation: "add config fields to internal/etl/server/schema.go",
		},
	}
	if kind == "source" {
		gates = append(gates,
			ConnectorReadinessGate{
				Code:        "schedule_policy",
				Label:       "Schedule policy",
				Status:      gateStatus(len(supportedSchedules) > 0, false),
				Evidence:    "source descriptor exposes supported_schedules/default_schedule",
				Remediation: "add source schedule policy in pipeline schedule validation",
			},
			sourceSchemaGate(typ, capSet),
			sourceCheckpointGate(typ, capSet),
			sourceRemotePreflightGate(typ, capSet),
		)
	}
	if kind == "sink" {
		gates = append(gates,
			sinkSchemaGate(typ, capSet),
			sinkReplayGate(typ, capSet),
			sinkRemotePreflightGate(typ, capSet),
		)
	}
	if kind == "transform" {
		gates = append(gates, transformDryRunGate(typ))
	}
	gates = append(gates, evidenceGate(kind, typ, maturity))

	status := readinessStatus(maturity, gates)
	return ConnectorReadiness{
		Status:  status,
		Summary: readinessSummary(kind, typ, maturity, status),
		Gates:   gates,
	}
}

func sourceSchemaGate(typ string, capSet map[string]bool) ConnectorReadinessGate {
	switch typ {
	case "mysql_batch":
		return passGate("schema_introspection", "Schema introspection", "source implements SchemaDescriptor for table/query metadata")
	case "mysql_cdc", "mysql_snapshot_cdc":
		return passGate("schema_introspection", "Schema introspection", "single-table SchemaDescriptor plus per-table multi-table/wildcard preflight contract")
	case "file", "http", "kafka", "rest_source", "salesforce", "github", "hubspot", "stripe", "notion":
		return ConnectorReadinessGate{
			Code:        "schema_introspection",
			Label:       "Schema introspection",
			Status:      "partial",
			Evidence:    "preflight can infer schema from file samples or explicit source.config.schema/sample hints",
			Remediation: "provide source.config.schema/sample for non-database sources when target schema validation matters",
		}
	default:
		if capSet["schema_descriptor"] || capSet["schema_descriptor_single_table"] {
			return passGate("schema_introspection", "Schema introspection", "metadata declares schema descriptor capability")
		}
		return missingGate("schema_introspection", "Schema introspection", "add SchemaDescriptor or explicit sample/schema hint support")
	}
}

func sourceCheckpointGate(typ string, capSet map[string]bool) ConnectorReadinessGate {
	if capSet["checkpoint"] {
		return passGate("checkpoint", "Checkpoint/replay boundary", "source metadata declares checkpoint support")
	}
	return missingGate("checkpoint", "Checkpoint/replay boundary", "implement checkpoint persistence and restart replay tests")
}

func sourceRemotePreflightGate(typ string, capSet map[string]bool) ConnectorReadinessGate {
	switch typ {
	case "mysql_batch":
		return passGate("remote_preflight", "Remote preflight", "preflight opens MySQL, verifies table/query metadata, and checks cursor/column existence")
	case "mysql_cdc", "mysql_snapshot_cdc":
		return passGate("remote_preflight", "Remote preflight", "preflight opens MySQL and checks binlog format, row image, replication grants, server_id, and configured tables")
	case "postgres_cdc":
		return passGate("remote_preflight", "Remote preflight", "preflight opens PostgreSQL and checks wal_level, replication role, publication, configured tables, and slot ownership")
	case "file":
		return ConnectorReadinessGate{Code: "remote_preflight", Label: "Remote preflight", Status: "partial", Evidence: "preflight checks local file readability and parses a sample", Remediation: "run preflight in the same container/host path layout used for deployment"}
	case "http":
		return ConnectorReadinessGate{Code: "remote_preflight", Label: "Remote preflight", Status: "partial", Evidence: "preflight sends a short sample request and validates response JSON/result_key shape", Remediation: "verify production auth headers, rate limits, pagination, and retry policy against the real API"}
	case "rest_source", "salesforce", "github", "hubspot", "stripe", "notion":
		return ConnectorReadinessGate{Code: "remote_preflight", Label: "Remote preflight", Status: "partial", Evidence: "preflight builds the connector and probes the base URL with configured auth", Remediation: "verify production auth, rate limits, pagination, and retry policy against the real API"}
	case "kafka":
		return ConnectorReadinessGate{Code: "remote_preflight", Label: "Remote preflight", Status: "partial", Evidence: "preflight reads broker topic metadata and blocks missing or empty topics when metadata is reachable", Remediation: "verify broker ACLs, consumer group policy, and topic retention in the target environment"}
	default:
		if capSet["remote_preflight"] {
			return passGate("remote_preflight", "Remote preflight", "metadata declares source remote preflight capability")
		}
		return missingGate("remote_preflight", "Remote preflight", "add source reachability, permission, and table/topic/sample checks")
	}
}

func sinkSchemaGate(typ string, capSet map[string]bool) ConnectorReadinessGate {
	switch typ {
	case "file_sink", "s3", "kafka", "redis":
		return ConnectorReadinessGate{Code: "schema_preflight", Label: "Schema preflight", Status: "not_applicable", Evidence: "sink accepts schemaless/append-oriented payloads"}
	}
	if capSet["schema_validator"] {
		return passGate("schema_preflight", "Schema preflight", "sink implements SchemaValidator or equivalent field-level validation")
	}
	return missingGate("schema_preflight", "Schema preflight", "add SchemaValidator, DDL preview, or explicit field-level preflight")
}

func sinkReplayGate(typ string, capSet map[string]bool) ConnectorReadinessGate {
	switch typ {
	case "mysql", "postgres", "postgresql", "doris":
		if capSet["upsert"] {
			return passGate("replay_absorption", "Replay absorption", "upsert mode with stable pk_columns can absorb at-least-once replay")
		}
	case "clickhouse":
		return passGate("replay_absorption", "Replay absorption", "ReplacingMergeTree-style keys and version columns can absorb replay")
	case "kafka":
		return ConnectorReadinessGate{Code: "replay_absorption", Label: "Replay absorption", Status: "partial", Evidence: "idempotent producer reduces duplicates but downstream consumers must handle at-least-once replay", Remediation: "use stable keys and downstream compaction/deduplication where required"}
	case "file_sink", "s3":
		return ConnectorReadinessGate{Code: "replay_absorption", Label: "Replay absorption", Status: "partial", Evidence: "append/object output is replay-visible; content-addressed writes reduce duplicate object creation", Remediation: "use deterministic prefixes/manifests or downstream deduplication for replay-sensitive data"}
	case "elasticsearch", "es":
		return passGate("replay_absorption", "Replay absorption", "deterministic _id/id_column makes replay overwrite the same document")
	case "maxcompute", "odps":
		return ConnectorReadinessGate{Code: "replay_absorption", Label: "Replay absorption", Status: "partial", Evidence: "append mode is at-least-once; partition_overwrite requires a controlled replay plan", Remediation: "use business keys, staging+merge, or controlled partition_overwrite flows"}
	}
	return missingGate("replay_absorption", "Replay absorption", "document and test idempotent/replay behavior")
}

func sinkRemotePreflightGate(typ string, capSet map[string]bool) ConnectorReadinessGate {
	if capSet["remote_preflight"] || capSet["remote_mapping_preflight"] || capSet["partition_preflight"] {
		return passGate("remote_preflight", "Remote preflight", "preflight checks real target metadata or permissions")
	}
	switch typ {
	case "mysql", "postgres", "postgresql", "clickhouse", "doris", "elasticsearch", "es":
		return ConnectorReadinessGate{Code: "remote_preflight", Label: "Remote preflight", Status: "partial", Evidence: "preflight opens the sink and may validate target schema when reachable", Remediation: "extend connection-specific permission/table checks where needed"}
	case "kafka":
		return passGate("remote_preflight", "Remote preflight", "preflight reads broker topic metadata and blocks missing or empty target topics when metadata is reachable")
	case "s3":
		return passGate("remote_preflight", "Remote preflight", "preflight requires endpoint/bucket and opens the S3-compatible target to check bucket reachability")
	case "file_sink":
		return ConnectorReadinessGate{Code: "remote_preflight", Label: "Remote preflight", Status: "partial", Evidence: "connection/open checks are available but target-specific schema checks are limited", Remediation: "use connection test and destination-specific smoke runs before production"}
	default:
		return missingGate("remote_preflight", "Remote preflight", "add target reachability, permission, and schema checks")
	}
}

func transformDryRunGate(typ string) ConnectorReadinessGate {
	switch typ {
	case "ts", "javascript", "js":
		return ConnectorReadinessGate{Code: "dry_run", Label: "Transform dry-run", Status: "partial", Evidence: "dry-run API can execute transforms, but JS/TS depends on CGO build availability", Remediation: "verify build tags and runtime before production use"}
	default:
		return passGate("dry_run", "Transform dry-run", "transform dry-run API supports sample-record validation")
	}
}

func evidenceGate(kind, typ, maturity string) ConnectorReadinessGate {
	manifest, err := LoadConnectorEvidenceManifest()
	return evidenceGateAt(kind, typ, maturity, time.Now(), manifest, err)
}

func evidenceGateAt(kind, typ, maturity string, now time.Time, manifest ConnectorEvidenceManifest, manifestErr error) ConnectorReadinessGate {
	if manifestErr == nil {
		if record, ok := manifest.Record(kind, typ); ok {
			freshness := record.Freshness(now)
			metadata := record.Metadata()
			switch freshness.Status {
			case "pass":
				gate := passGate("e2e_evidence", "E2E evidence", record.EvidenceSummary())
				gate.EvidenceMetadata = &metadata
				return gate
			case "partial":
				return ConnectorReadinessGate{
					Code:             "e2e_evidence",
					Label:            "E2E evidence",
					Status:           "partial",
					Evidence:         fmt.Sprintf("%s; %s", record.EvidenceSummary(), freshness.Explanation),
					Remediation:      "run the listed certification scripts for the current build/image, update the manifest, and rerun the evidence checker",
					EvidenceMetadata: &metadata,
				}
			default:
				return ConnectorReadinessGate{
					Code:             "e2e_evidence",
					Label:            "E2E evidence",
					Status:           "missing",
					Evidence:         freshness.Explanation,
					Remediation:      "repair the manifest and rerun hack/check-connector-evidence.sh before treating this connector as production-ready",
					EvidenceMetadata: &metadata,
				}
			}
		}
	}
	if maturity == "production" {
		message := "connector evidence manifest has no record for this production connector"
		if manifestErr != nil {
			message = fmt.Sprintf("connector evidence manifest unavailable: %v", manifestErr)
		}
		return ConnectorReadinessGate{
			Code:        "e2e_evidence",
			Label:       "E2E evidence",
			Status:      "missing",
			Evidence:    message,
			Remediation: "add a complete production connector record to internal/etl/server/evidence/connector-evidence.json and run hack/check-connector-evidence.sh",
		}
	}
	if evidence := legacyConnectorEvidence(kind, typ); evidence != "" {
		return passGate("e2e_evidence", "E2E evidence", evidence)
	}
	return ConnectorReadinessGate{
		Code:        "e2e_evidence",
		Label:       "E2E evidence",
		Status:      "partial",
		Evidence:    "unit or control-plane coverage exists, but no connector-specific e2e script is recorded in readiness metadata",
		Remediation: "add a connector-specific e2e/smoke script and reference it from readiness metadata",
	}
}

// legacyConnectorEvidence keeps non-production connector descriptions stable
// while the manifest is rolled out to the production certification set.
func legacyConnectorEvidence(kind, typ string) string {
	evidence := map[string]string{
		"source:postgres_cdc": "hack/e2e-postgres-cdc.sh covers PostgreSQL CDC source insert/update/delete and checkpoint restart into MySQL; on_truncate defaults to error (PR-2.3)",
		"sink:elasticsearch":  "hack/e2e-elasticsearch.sh covers OpenSearch bulk indexing and mapping-conflict DLQ/replay",
		"sink:es":             "hack/e2e-elasticsearch.sh covers OpenSearch bulk indexing and mapping-conflict DLQ/replay",
		"sink:maxcompute":     "hack/e2e-maxcompute.sh is env-gated; real MaxCompute evidence is still required",
		"sink:odps":           "hack/e2e-maxcompute.sh is env-gated; real MaxCompute evidence is still required",
	}
	return evidence[kind+":"+typ]
}

func passGate(code, label, evidence string) ConnectorReadinessGate {
	return ConnectorReadinessGate{Code: code, Label: label, Status: "pass", Evidence: evidence}
}

func missingGate(code, label, remediation string) ConnectorReadinessGate {
	return ConnectorReadinessGate{Code: code, Label: label, Status: "missing", Remediation: remediation}
}

func gateStatus(pass, notApplicable bool) string {
	if notApplicable {
		return "not_applicable"
	}
	if pass {
		return "pass"
	}
	return "missing"
}

func readinessStatus(maturity string, gates []ConnectorReadinessGate) string {
	hasMissing := false
	hasPartial := false
	for _, gate := range gates {
		switch gate.Status {
		case "missing":
			hasMissing = true
		case "partial":
			hasPartial = true
		}
	}
	if maturity == "production" && !hasMissing && !hasPartial {
		return "production_ready"
	}
	if maturity == "production" && !hasMissing {
		return "production_with_review"
	}
	if maturity == "beta" && !hasMissing {
		return "beta_ready"
	}
	if hasMissing {
		return "needs_work"
	}
	return maturity + "_with_review"
}

func readinessSummary(kind, typ, maturity, status string) string {
	switch status {
	case "production_ready":
		return "Production maturity with required readiness gates passing."
	case "production_with_review":
		return "Production maturity, but one or more readiness gates require operator review."
	case "beta_ready":
		return "Beta maturity with core readiness gates present; keep production rollout behind validation."
	case "needs_work":
		return "Readiness gaps remain; do not treat this connector as production-ready without additional evidence."
	default:
		return kind + " " + typ + " is " + maturity + "; review readiness gates before production use."
	}
}

func requiredFields(fields []ConfigField) []string {
	var out []string
	for _, field := range fields {
		if field.Required {
			out = append(out, field.Name)
		}
	}
	return out
}

func secretFields(fields []ConfigField) []string {
	var out []string
	for _, field := range fields {
		if field.Secret {
			out = append(out, field.Name)
		}
	}
	return out
}
