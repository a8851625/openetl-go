package server

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

const aiContextPackVersion = "v1"

type AIContextPack struct {
	Version        string              `json:"version"`
	Product        string              `json:"product"`
	Boundaries     []string            `json:"boundaries"`
	MaturityLevels []string            `json:"maturity_levels"`
	Components     []AIComponent       `json:"components"`
	Examples       []AIExample         `json:"examples"`
	Docs           map[string]string   `json:"docs,omitempty"`
	DAGRules       []string            `json:"dag_rules"`
	CommonErrors   []string            `json:"common_errors"`
	GeneratedFrom  map[string][]string `json:"generated_from"`
}

type AIComponent struct {
	Kind               string        `json:"kind"`
	Type               string        `json:"type"`
	Maturity           string        `json:"maturity"`
	Required           []string      `json:"required"`
	SecretFields       []string      `json:"secret_fields,omitempty"`
	Capabilities       []string      `json:"capabilities,omitempty"`
	SupportedSchedules []string      `json:"supported_schedules,omitempty"`
	DefaultSchedule    string        `json:"default_schedule,omitempty"`
	Fields             []ConfigField `json:"fields,omitempty"`
}

type AIExample struct {
	Name string `json:"name"`
	YAML string `json:"yaml"`
}

type AIGenerationReview struct {
	MissingFields        []AIMissingField `json:"missing_fields,omitempty"`
	RiskFlags            []AIRiskFlag     `json:"risk_flags,omitempty"`
	RequiresConfirmation []AIConfirmation `json:"requires_confirmation,omitempty"`
	RecommendedActions   []string         `json:"recommended_actions,omitempty"`
}

type AIMissingField struct {
	Kind    string `json:"kind"`
	Type    string `json:"type"`
	Field   string `json:"field"`
	Secret  bool   `json:"secret,omitempty"`
	Message string `json:"message"`
}

type AIRiskFlag struct {
	Code        string `json:"code"`
	Level       string `json:"level"`
	Message     string `json:"message"`
	Remediation string `json:"remediation,omitempty"`
}

type AIConfirmation struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func buildAIContextPack() AIContextPack {
	descriptors := connectorDescriptors()
	components := make([]AIComponent, 0, len(descriptors))
	for _, d := range descriptors {
		components = append(components, AIComponent{
			Kind:               d.Kind,
			Type:               d.Type,
			Maturity:           d.Maturity,
			Required:           append([]string(nil), d.Required...),
			SecretFields:       append([]string(nil), d.SecretFields...),
			Capabilities:       append([]string(nil), d.Capabilities...),
			SupportedSchedules: append([]string(nil), d.SupportedSchedules...),
			DefaultSchedule:    d.DefaultSchedule,
			Fields:             append([]ConfigField(nil), d.Fields...),
		})
	}
	sort.Slice(components, func(i, j int) bool {
		if components[i].Kind == components[j].Kind {
			return components[i].Type < components[j].Type
		}
		return components[i].Kind < components[j].Kind
	})
	return AIContextPack{
		Version: "openetl-ai-context/" + aiContextPackVersion,
		Product: "OpenETL-Go is a lightweight, self-hosted CDC/ETL runtime for Source -> Transform -> Sink pipelines. Generate ordinary pipeline or DAG specs only.",
		Boundaries: []string{
			"Default delivery is at-least-once; recommend business keys, upserts, ReplacingMergeTree-style sinks, or explicit deduplication for replay absorption.",
			"Do not claim exactly-once Kafka transactions, cross-sink atomic fanout, Flink SQL compatibility, arbitrary keyed state, processing-time timers, savepoints, sliding/session windows, or a generic SQL planner.",
			"Prefer descriptor/schema-defined connectors and declarative transforms before scripting. Use lua/ts/javascript only when a declarative transform cannot express the requirement.",
			"AI output must be reviewed by the user and pass the same validate/preflight path as YAML or UI-authored specs before start.",
		},
		MaturityLevels: connectorMaturityLevels,
		Components:     components,
		Examples:       defaultAIExamples(),
		Docs:           readComponentDocSummaries("docs/components", 600),
		DAGRules: []string{
			"A linear pipeline uses source, optional transforms, and sink.",
			"DAG must still express ordinary source/transform/sink nodes and edges; do not invent a separate runtime path.",
			"Schedule type must be supported by the selected source descriptor. Streaming sources normally use streaming schedules.",
			"DLQ replay is supported for linear pipelines; DAG DLQ replay is intentionally unsupported until node-level replay is implemented.",
		},
		CommonErrors: []string{
			"Missing required connector fields or secret values.",
			"CDC sources writing to append-only sinks without an explicit deduplication/upsert strategy.",
			"Experimental connectors such as MaxCompute/ODPS being treated as production writer paths.",
			"Unsafe DDL apply policies or generated scripts that should have been declarative transforms.",
		},
		GeneratedFrom: map[string][]string{
			"runtime": {"connector descriptors", "plugin schema", "maturity metadata", "source schedule capabilities"},
			"docs":    {"docs/components/*.md when available", "quickstart/API docs for user-facing boundaries"},
		},
	}
}

func (p AIContextPack) SystemPrompt() string {
	var b strings.Builder
	b.WriteString("You are an OpenETL-Go pipeline configuration assistant.\n")
	b.WriteString(p.Product + "\n\n")
	b.WriteString("Hard boundaries:\n")
	for _, item := range p.Boundaries {
		b.WriteString("- " + item + "\n")
	}
	b.WriteString("\nAvailable components. Use only these kind/type names:\n")
	for _, c := range p.Components {
		b.WriteString(fmt.Sprintf("- %s/%s maturity=%s", c.Kind, c.Type, c.Maturity))
		if len(c.Required) > 0 {
			b.WriteString(" required=" + strings.Join(c.Required, ","))
		}
		if len(c.SecretFields) > 0 {
			b.WriteString(" secrets=" + strings.Join(c.SecretFields, ","))
		}
		if c.DefaultSchedule != "" {
			b.WriteString(" default_schedule=" + c.DefaultSchedule)
		}
		if len(c.SupportedSchedules) > 0 {
			b.WriteString(" supported_schedules=" + strings.Join(c.SupportedSchedules, ","))
		}
		if len(c.Capabilities) > 0 {
			b.WriteString(" capabilities=" + strings.Join(c.Capabilities, ","))
		}
		b.WriteByte('\n')
	}
	if len(p.Docs) > 0 {
		keys := make([]string, 0, len(p.Docs))
		for key := range p.Docs {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		b.WriteString("\nComponent notes:\n")
		for _, key := range keys {
			b.WriteString("## " + key + "\n")
			b.WriteString(p.Docs[key])
			b.WriteString("\n")
		}
	}
	b.WriteString(`
Output only valid YAML, with no markdown fences and no explanation.

Required linear spec shape:
name: "<descriptive-name>"
source:
  type: <source_type>
  config: {}
transforms:
  - type: <transform_type>
    config: {}
sink:
  type: <sink_type>
  config: {}
batch_size: 1000
checkpoint_interval_sec: 30
backpressure_buffer: 100
retry:
  max_attempts: 3
  initial_interval_ms: 1000
  max_interval_ms: 30000
dlq:
  enable: true
`)
	return b.String()
}

func defaultAIExamples() []AIExample {
	return []AIExample{
		{
			Name: "file-json-to-file",
			YAML: `name: file-json-to-file
source:
  type: file
  config:
    path: /app/data/input.jsonl
    format: json
transforms:
  - type: project
    config:
      fields: ["id", "name", "updated_at"]
sink:
  type: file_sink
  config:
    output_dir: /app/data/output
    format: jsonl
batch_size: 100
checkpoint_interval_sec: 5
dlq:
  enable: true`,
		},
		{
			Name: "debezium-kafka-to-mysql-upsert",
			YAML: `name: debezium-kafka-to-mysql-upsert
source:
  type: kafka
  config:
    brokers: ["redpanda:9092"]
    topic: mysql.inventory.customers
    group_id: openetl-ods
    format: json
transforms:
  - type: debezium_cdc
    config:
      table_mapping:
        "{source_db}.{source_table}": "ods_{source_db}__{source_table}"
  - type: cdc_policy
    config:
      skip_tombstone: true
      dangerous_ddl: reject
sink:
  type: mysql
  config:
    host: mysql
    user: root
    database: ods
    table: ods_inventory__customers
    batch_mode: upsert
    pk_columns: ["id"]
batch_size: 500
checkpoint_interval_sec: 3
dlq:
  enable: true`,
		},
	}
}

func readComponentDocSummaries(dir string, maxBytes int) map[string]string {
	if _, err := os.Stat(dir); err != nil {
		for i := 0; i < 5; i++ {
			prefix := strings.Repeat("../", i+1)
			candidate := filepath.Clean(filepath.Join(prefix, dir))
			if _, statErr := os.Stat(candidate); statErr == nil {
				dir = candidate
				break
			}
		}
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	out := map[string]string{}
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".md") {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(dir, entry.Name()))
		if err != nil {
			continue
		}
		text := strings.TrimSpace(string(raw))
		if maxBytes > 0 && len(text) > maxBytes {
			text = text[:maxBytes] + "\n..."
		}
		name := strings.TrimSuffix(entry.Name(), ".md")
		out[name] = text
	}
	return out
}

func reviewGeneratedSpec(ctx context.Context, spec *pipeline.Spec, preflight *PreflightResult) AIGenerationReview {
	_ = ctx
	descriptorByKey := map[string]ConnectorDescriptor{}
	for _, d := range connectorDescriptors() {
		descriptorByKey[d.Kind+"/"+d.Type] = d
	}

	var review AIGenerationReview
	checkComponent := func(kind, typ string, cfg map[string]any, usesConnection bool) {
		d, ok := descriptorByKey[kind+"/"+typ]
		if !ok {
			review.RiskFlags = append(review.RiskFlags, AIRiskFlag{
				Code:        "unknown_component",
				Level:       "error",
				Message:     fmt.Sprintf("%s %q is not in the runtime descriptor list", kind, typ),
				Remediation: "Choose a registered component type from /api/v2/connectors/descriptors.",
			})
			return
		}
		if d.Maturity == "experimental" || d.Maturity == "dev-only" {
			review.RiskFlags = append(review.RiskFlags, AIRiskFlag{
				Code:        "non_production_maturity",
				Level:       "warning",
				Message:     fmt.Sprintf("%s/%s is %s maturity", kind, typ, d.Maturity),
				Remediation: "Keep this as a design-time or explicitly accepted beta path until e2e evidence is available.",
			})
			review.RequiresConfirmation = append(review.RequiresConfirmation, AIConfirmation{
				Code:    "accept_" + d.Maturity,
				Message: fmt.Sprintf("Confirm use of %s/%s with %s maturity.", kind, typ, d.Maturity),
			})
		}
		if usesConnection {
			return
		}
		for _, field := range d.Fields {
			if field.Required && isEmptyConfigValue(cfg[field.Name]) {
				review.MissingFields = append(review.MissingFields, AIMissingField{
					Kind:    kind,
					Type:    typ,
					Field:   field.Name,
					Secret:  field.Secret,
					Message: fmt.Sprintf("%s/%s requires field %q", kind, typ, field.Name),
				})
			}
			if field.Secret {
				if isEmptyConfigValue(cfg[field.Name]) {
					review.RequiresConfirmation = append(review.RequiresConfirmation, AIConfirmation{
						Code:    "provide_secret",
						Message: fmt.Sprintf("Provide %s/%s secret field %q outside the AI prompt when needed.", kind, typ, field.Name),
					})
				} else {
					review.RequiresConfirmation = append(review.RequiresConfirmation, AIConfirmation{
						Code:    "review_secret_storage",
						Message: fmt.Sprintf("Confirm %s/%s secret field %q should be stored in this spec or replaced by a saved connection.", kind, typ, field.Name),
					})
				}
			}
		}
	}

	checkComponent("source", spec.Source.Type, spec.Source.Config, spec.Source.Connection != "" || spec.Source.ConnectionRef != "")
	for _, transform := range spec.Transforms {
		checkComponent("transform", transform.Type, transform.Config, transform.Connection != "" || transform.ConnectionRef != "")
		if transform.Type == "lua" || transform.Type == "ts" || transform.Type == "javascript" || transform.Type == "js" || transform.Type == "flat_map" || transform.Type == "udtf" {
			review.RiskFlags = append(review.RiskFlags, AIRiskFlag{
				Code:        "script_transform",
				Level:       "warning",
				Message:     fmt.Sprintf("Transform %s executes user-provided script code", transform.Type),
				Remediation: "Prefer declarative transforms when possible; review script runtime limits and failure behavior before start.",
			})
		}
	}
	checkComponent("sink", spec.Sink.Type, spec.Sink.Config, spec.Sink.Connection != "" || spec.Sink.ConnectionRef != "")

	if isCDCSource(spec.Source.Type) && isAppendOnlySink(spec.Sink.Type, spec.Sink.Config) {
		review.RiskFlags = append(review.RiskFlags, AIRiskFlag{
			Code:        "cdc_to_append_sink",
			Level:       "warning",
			Message:     fmt.Sprintf("CDC source %s writes to append-oriented sink %s", spec.Source.Type, spec.Sink.Type),
			Remediation: "Use an upsert/idempotent sink or add explicit deduplication/replay absorption guidance.",
		})
		review.RequiresConfirmation = append(review.RequiresConfirmation, AIConfirmation{
			Code:    "accept_replay_duplicates",
			Message: "Confirm downstream consumers can absorb at-least-once replay duplicates.",
		})
	}
	if strings.EqualFold(spec.Sink.Type, "maxcompute") || strings.EqualFold(spec.Sink.Type, "odps") {
		review.RiskFlags = append(review.RiskFlags, AIRiskFlag{
			Code:        "maxcompute_requires_remote_preflight",
			Level:       "warning",
			Message:     "MaxCompute/ODPS writer is SDK-backed but remains experimental until real write evidence is available.",
			Remediation: "Run preflight with real endpoint/project/table/partition permissions and keep replay/idempotency guidance explicit before production use.",
		})
	}
	if policy, ok := stringConfig(spec.Sink.Config, "ddl_policy"); ok && strings.EqualFold(policy, "apply") {
		review.RiskFlags = append(review.RiskFlags, AIRiskFlag{
			Code:        "ddl_apply",
			Level:       "warning",
			Message:     "Sink DDL policy is apply.",
			Remediation: "Review target DDL preview and connector-specific safe DDL limits before starting.",
		})
	}
	if spec.DLQ == nil || !spec.DLQ.Enable {
		review.RiskFlags = append(review.RiskFlags, AIRiskFlag{
			Code:        "dlq_disabled",
			Level:       "warning",
			Message:     "DLQ is disabled or omitted.",
			Remediation: "Enable DLQ for first production-candidate runs so failed records remain visible and replayable.",
		})
	}
	if preflight != nil {
		for _, issue := range preflight.Issues {
			if issue.Level == "error" {
				review.RiskFlags = append(review.RiskFlags, AIRiskFlag{
					Code:        "preflight_" + issue.Check,
					Level:       "error",
					Message:     issue.Message,
					Remediation: issue.Remediation,
				})
			}
		}
		for _, issue := range preflight.FieldIssues {
			if issue.Level == "error" {
				review.MissingFields = append(review.MissingFields, AIMissingField{
					Kind:    "sink",
					Type:    spec.Sink.Type,
					Field:   issue.Field,
					Message: issue.Message,
				})
			}
		}
	}
	review.RecommendedActions = recommendedAIActions(review)
	return review
}

func recommendedAIActions(review AIGenerationReview) []string {
	actions := []string{"Review the YAML diff before applying it to the canvas.", "Run Validate + preflight again after filling secrets or saved connections."}
	if len(review.MissingFields) > 0 {
		actions = append(actions, "Fill all missing required fields or select saved connections from the catalog.")
	}
	for _, risk := range review.RiskFlags {
		if risk.Level == "error" {
			actions = append(actions, "Resolve blocking error-level risks before creating or starting the pipeline.")
			break
		}
	}
	return actions
}

func isEmptyConfigValue(v any) bool {
	if v == nil {
		return true
	}
	switch t := v.(type) {
	case string:
		return strings.TrimSpace(t) == "" || strings.Contains(strings.ToLower(t), "your_") || strings.Contains(strings.ToLower(t), "<")
	case []any:
		return len(t) == 0
	case []string:
		return len(t) == 0
	}
	return false
}

func isCDCSource(sourceType string) bool {
	switch sourceType {
	case "mysql_cdc", "postgres_cdc", "mysql_snapshot_cdc":
		return true
	default:
		return false
	}
}

func isAppendOnlySink(sinkType string, cfg map[string]any) bool {
	switch sinkType {
	case "file", "file_sink", "s3", "kafka":
		return true
	case "mysql", "postgres", "postgresql", "doris":
		mode, _ := stringConfig(cfg, "batch_mode")
		return !strings.EqualFold(mode, "upsert")
	default:
		return false
	}
}

func stringConfig(cfg map[string]any, key string) (string, bool) {
	if cfg == nil {
		return "", false
	}
	v, ok := cfg[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}
