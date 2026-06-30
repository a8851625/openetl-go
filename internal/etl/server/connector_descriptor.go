package server

import (
	"encoding/json"
	"net/http"
	"sort"

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
	Code        string `json:"code"`
	Label       string `json:"label"`
	Status      string `json:"status"` // pass, partial, missing, not_applicable
	Evidence    string `json:"evidence,omitempty"`
	Remediation string `json:"remediation,omitempty"`
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
		fields := schemas[name]
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
			if len(required) == 0 {
				if req, ok := info["required"].([]string); ok {
					required = append(required, req...)
				}
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
	gates = append(gates, evidenceGate(kind, typ))

	status := readinessStatus(maturity, gates)
	return ConnectorReadiness{
		Status:  status,
		Summary: readinessSummary(kind, typ, maturity, status),
		Gates:   gates,
	}
}

func sourceSchemaGate(typ string, capSet map[string]bool) ConnectorReadinessGate {
	switch typ {
	case "mysql_batch", "mysql_cdc", "mysql_snapshot_cdc":
		return passGate("schema_introspection", "Schema introspection", "source implements SchemaDescriptor for table/query metadata")
	case "file", "http", "kafka":
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
	case "file_sink", "s3", "kafka":
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

func evidenceGate(kind, typ string) ConnectorReadinessGate {
	if evidence := connectorEvidence(kind, typ); evidence != "" {
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

func connectorEvidence(kind, typ string) string {
	evidence := map[string]string{
		"source:file":               "hack/e2e.sh and hack/e2e-ui.sh cover file source paths",
		"source:http":               "hack/e2e-http-source.sh covers HTTP source pagination/auth headers",
		"source:mysql_batch":        "hack/e2e.sh and hack/e2e-mysql-postgres.sh cover MySQL batch reads",
		"source:mysql_cdc":          "hack/e2e-cdc-mysql.sh and hack/e2e-cdc-postgres.sh cover MySQL CDC",
		"source:mysql_snapshot_cdc": "hack/e2e-snapshot-cdc.sh and snapshot+CDC ClickHouse crash tests cover integrated snapshot+CDC",
		"source:kafka":              "hack/e2e-kafka.sh, hack/e2e-wide-table.sh, and Debezium Kafka e2e cover Kafka source paths",
		"source:postgres_cdc":       "hack/e2e-cdc-postgres.sh covers PostgreSQL CDC",
		"sink:file_sink":            "hack/e2e.sh covers file sink output",
		"sink:s3":                   "hack/e2e-s3-minio.sh covers MinIO-compatible S3 sink replay behavior",
		"sink:mysql":                "hack/e2e.sh, hack/e2e-cdc-mysql.sh, and Debezium MySQL e2e cover MySQL sink upsert/replay",
		"sink:postgres":             "hack/e2e-mysql-postgres.sh and hack/e2e-cdc-postgres.sh cover PostgreSQL sink",
		"sink:postgresql":           "hack/e2e-mysql-postgres.sh and hack/e2e-cdc-postgres.sh cover PostgreSQL sink",
		"sink:clickhouse":           "hack/e2e-clickhouse.sh and snapshot+CDC ClickHouse e2e cover ClickHouse sink",
		"sink:kafka":                "hack/e2e-kafka.sh and hack/e2e-kafka-raw-ods.sh cover Kafka sink",
		"sink:doris":                "hack/e2e-doris.sh covers Doris Stream Load/upsert paths",
		"sink:elasticsearch":        "hack/e2e-elasticsearch.sh covers OpenSearch bulk indexing and mapping-conflict DLQ/replay",
		"sink:es":                   "hack/e2e-elasticsearch.sh covers OpenSearch bulk indexing and mapping-conflict DLQ/replay",
		"sink:maxcompute":           "hack/e2e-maxcompute.sh is env-gated; real MaxCompute evidence is still required",
		"sink:odps":                 "hack/e2e-maxcompute.sh is env-gated; real MaxCompute evidence is still required",
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
