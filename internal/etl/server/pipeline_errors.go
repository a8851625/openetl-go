package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/orchestrator"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

// PipelineIssue is the stable, user-facing error shape shared by validate,
// create, update, preflight and dry-run callers.  The legacy errors/warnings
// arrays remain in responses for compatibility, while UI clients can use the
// path/node metadata to navigate directly to the failing control.
type PipelineIssue struct {
	Code        string `json:"code"`
	Level       string `json:"level,omitempty"`
	Scope       string `json:"scope"`
	Field       string `json:"field,omitempty"`
	Check       string `json:"check,omitempty"`
	NodeID      string `json:"node_id,omitempty"`
	NodeKind    string `json:"node_kind,omitempty"`
	Plugin      string `json:"plugin,omitempty"`
	Message     string `json:"message"`
	Remediation string `json:"remediation,omitempty"`
}

// writePipelineIssues writes an error response before encoding JSON.  Several
// older handlers encoded {error: ...} without WriteHeader, which made a
// failed operation look successful to HTTP clients.
func writePipelineIssues(w http.ResponseWriter, status int, operation, summary string, issues []PipelineIssue, extra map[string]any) {
	if status < http.StatusBadRequest {
		status = http.StatusBadRequest
	}
	if strings.TrimSpace(summary) == "" {
		summary = "pipeline operation failed"
	}
	if len(issues) == 0 {
		issues = []PipelineIssue{{
			Code:        "pipeline_operation_failed",
			Scope:       "pipeline",
			Message:     summary,
			Remediation: "review the pipeline spec and retry after correcting the reported issue",
		}}
	}
	errorsList := make([]string, 0, len(issues))
	for _, issue := range issues {
		if strings.TrimSpace(issue.Message) != "" {
			errorsList = append(errorsList, issue.Message)
		}
	}
	body := map[string]any{
		"error":     summary,
		"message":   summary,
		"code":      "pipeline_" + operation + "_failed",
		"valid":     false,
		"errors":    errorsList,
		"issues":    issues,
		"operation": operation,
	}
	fieldIssues := legacyFieldIssues(issues)
	if len(fieldIssues) > 0 {
		body["field_issues"] = fieldIssues
	}
	for key, value := range extra {
		body[key] = value
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func legacyFieldIssues(issues []PipelineIssue) []map[string]any {
	result := make([]map[string]any, 0, len(issues))
	for _, issue := range issues {
		if strings.TrimSpace(issue.Field) == "" {
			continue
		}
		level := strings.TrimSpace(issue.Level)
		if level == "" {
			level = "error"
		}
		check := strings.TrimSpace(issue.Check)
		if check == "" {
			check = issue.Code
		}
		field := issue.Field
		if issue.NodeID != "" {
			prefix := "dag.nodes." + issue.NodeID
			if strings.HasPrefix(field, prefix) {
				field = "dag.nodes[" + issue.NodeID + "]" + strings.TrimPrefix(field, prefix)
			}
		}
		legacy := map[string]any{
			"level":       level,
			"field":       field,
			"check":       check,
			"message":     issue.Message,
			"remediation": issue.Remediation,
		}
		if issue.NodeID != "" {
			legacy["node"] = issue.NodeID
		}
		result = append(result, legacy)
	}
	return result
}

func pipelineIssueFromError(code, scope, message, remediation string) PipelineIssue {
	return PipelineIssue{
		Code:        code,
		Scope:       scope,
		Message:     strings.TrimSpace(message),
		Remediation: strings.TrimSpace(remediation),
	}
}

func preflightPipelineIssues(result *PreflightResult) []PipelineIssue {
	if result == nil {
		return nil
	}
	issues := make([]PipelineIssue, 0, len(result.Issues)+len(result.FieldIssues))
	for _, issue := range result.Issues {
		level := strings.TrimSpace(issue.Level)
		if level == "" {
			level = "warning"
		}
		issues = append(issues, PipelineIssue{
			Code:        issue.Check,
			Level:       level,
			Scope:       "preflight",
			Check:       issue.Check,
			Message:     issue.Message,
			Remediation: issue.Remediation,
		})
	}
	for _, issue := range result.FieldIssues {
		level := strings.TrimSpace(issue.Level)
		if level == "" {
			level = "error"
		}
		issues = append(issues, PipelineIssue{
			Code:        issue.Check,
			Level:       level,
			Scope:       "field",
			Field:       issue.Field,
			Check:       issue.Check,
			Message:     issue.Message,
			Remediation: issue.Remediation,
		})
	}
	return issues
}

func linearPipelineIssues(spec *pipeline.Spec) []PipelineIssue {
	if spec == nil {
		return []PipelineIssue{pipelineIssueFromError(
			"invalid_spec", "pipeline", "pipeline spec is required", "provide a linear or DAG pipeline spec",
		)}
	}
	err := pipeline.ValidateSpec(spec)
	if err == nil {
		return nil
	}
	message := err.Error()
	// ValidateSpec intentionally keeps a compact legacy string. Split its
	// individual problems so each one can be rendered beside the right field.
	if idx := strings.Index(message, ": "); idx >= 0 {
		message = message[idx+2:]
	}
	parts := strings.Split(message, "; ")
	issues := make([]PipelineIssue, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		field, code, remediation := linearIssueLocation(part)
		scope := "pipeline"
		if strings.HasPrefix(field, "source.") {
			scope = "source"
		} else if strings.HasPrefix(field, "sink.") {
			scope = "sink"
		} else if strings.HasPrefix(field, "transforms[") {
			scope = "transform"
		}
		issues = append(issues, PipelineIssue{
			Code:        code,
			Scope:       scope,
			Field:       field,
			Message:     part,
			Remediation: remediation,
		})
	}
	if len(issues) == 0 {
		issues = append(issues, pipelineIssueFromError(
			"invalid_spec", "pipeline", err.Error(), "review the pipeline spec and correct the reported fields",
		))
	}
	return issues
}

func linearIssueLocation(problem string) (field, code, remediation string) {
	code = "invalid_spec"
	remediation = "review the pipeline spec and correct this field"
	switch {
	case strings.HasPrefix(problem, "name "):
		return "name", "required", "enter a unique pipeline name"
	case strings.HasPrefix(problem, "unknown source.type"):
		return "source.type", "unknown_connector", "choose a registered source from /api/v2/plugins/schema"
	case strings.HasPrefix(problem, "unknown sink.type"):
		return "sink.type", "unknown_connector", "choose a registered sink from /api/v2/plugins/schema"
	case strings.HasPrefix(problem, "unknown transforms["):
		close := strings.Index(problem, "]")
		field := "transforms.type"
		if close >= 0 {
			idx := problem[len("unknown transforms["):close]
			field = "transforms." + idx + ".type"
		}
		return field, "unknown_connector", "choose a registered transform from /api/v2/plugins/schema"
	case strings.HasPrefix(problem, "source.type"):
		field, code = "source.type", "source_connector"
	case strings.HasPrefix(problem, "sink.type"):
		field, code = "sink.type", "sink_connector"
	case strings.HasPrefix(problem, "transforms["):
		close := strings.Index(problem, "]")
		if close >= 0 {
			idx := problem[len("transforms["):close]
			field = "transforms." + idx + ".type"
			code = "transform_connector"
		}
	case strings.HasPrefix(problem, "batch_size"):
		field, code = "batch_size", "positive_integer"
	case strings.HasPrefix(problem, "checkpoint_interval_sec"):
		field, code = "checkpoint_interval_sec", "positive_integer"
	case strings.HasPrefix(problem, "backpressure_buffer"):
		field, code = "backpressure_buffer", "positive_integer"
	case strings.HasPrefix(problem, "retry."):
		field = strings.SplitN(problem, " ", 2)[0]
		code = "positive_integer"
	}
	if field == "" {
		if strings.Contains(problem, "source") {
			field = "source"
		} else if strings.Contains(problem, "sink") {
			field = "sink"
		}
	}
	if strings.HasPrefix(problem, "unknown ") && remediation == "review the pipeline spec and correct this field" {
		remediation = "choose a registered connector from /api/v2/plugins/schema"
	}
	return field, code, remediation
}

func dagPipelineIssues(spec *orchestrator.PipelineSpec) []PipelineIssue {
	if spec == nil {
		return []PipelineIssue{pipelineIssueFromError(
			"invalid_spec", "pipeline", "DAG pipeline spec is required", "provide a DAG pipeline spec",
		)}
	}
	issues := make([]PipelineIssue, 0)
	if strings.TrimSpace(spec.Name) == "" {
		issues = append(issues, PipelineIssue{
			Code: "required", Scope: "pipeline", Field: "name", Message: "name is required",
			Remediation: "enter a unique pipeline name",
		})
	}
	for _, node := range spec.DAG.Nodes {
		if node == nil {
			issues = append(issues, pipelineIssueFromError(
				"invalid_node", "dag_node", "DAG contains a null node", "remove the empty node and validate again",
			))
			continue
		}
		nodeID := strings.TrimSpace(node.ID)
		plugin := strings.TrimSpace(node.Plugin)
		field := "dag.nodes." + nodeID + ".plugin"
		if nodeID == "" {
			field = "dag.nodes.plugin"
		}
		base := PipelineIssue{Scope: "dag_node", Field: field, NodeID: nodeID, NodeKind: string(node.Kind), Plugin: plugin}
		if plugin == "" {
			base.Code = "required"
			base.Message = fmt.Sprintf("DAG node %q plugin is required", nodeID)
			base.Remediation = "choose a connector/plugin for this node"
			issues = append(issues, base)
			continue
		}
		var registered bool
		switch node.Kind {
		case orchestrator.KindSource:
			registered = registry.HasSource(plugin)
		case orchestrator.KindSink:
			registered = registry.HasSink(plugin)
		case orchestrator.KindTransform, orchestrator.KindFanout, orchestrator.KindRouter,
			orchestrator.KindTap, orchestrator.KindRateLimiter, orchestrator.KindEnricher, orchestrator.KindLookup:
			registered = registry.HasTransform(plugin)
		default:
			base.Code = "invalid_node_kind"
			base.Message = fmt.Sprintf("DAG node %q has invalid kind %q", nodeID, node.Kind)
			base.Remediation = "choose source, transform, or sink for this node"
			issues = append(issues, base)
			continue
		}
		if !registered {
			base.Code = "unknown_connector"
			base.Message = fmt.Sprintf("unknown %s connector %q for DAG node %q", node.Kind, plugin, nodeID)
			base.Remediation = "choose a registered connector from /api/v2/plugins/schema"
			issues = append(issues, base)
		}
	}
	if err := spec.DAG.ValidateProduction(spec.AllowUnsafe); err != nil {
		issues = append(issues, PipelineIssue{
			Code: "invalid_dag", Scope: "dag", Message: err.Error(),
			Remediation: "correct the DAG structure or explicitly review allow_unsafe before retrying",
		})
	}
	for _, problem := range validateDAGRuntimeStateRequirements(spec) {
		issues = append(issues, PipelineIssue{
			Code: "runtime_state_requirement", Scope: "dag_node", Message: problem,
			Remediation: "configure the required runtime state backend or remove this transform",
		})
	}
	issues = append(issues, dagNodeConfigPipelineIssues(spec)...)
	return issues
}

func dagNodeConfigPipelineIssues(spec *orchestrator.PipelineSpec) []PipelineIssue {
	rawIssues := validateDAGNodeConfigs(spec)
	if len(rawIssues) == 0 {
		return nil
	}
	nodes := make(map[string]*orchestrator.Node, len(spec.DAG.Nodes))
	for _, node := range spec.DAG.Nodes {
		if node != nil {
			nodes[node.ID] = node
		}
	}
	issues := make([]PipelineIssue, 0, len(rawIssues))
	for _, raw := range rawIssues {
		node := nodes[raw.Node]
		field := "dag.nodes." + raw.Node + ".config"
		if strings.HasSuffix(raw.Field, ".plugin") {
			field = "dag.nodes." + raw.Node + ".plugin"
		}
		issue := PipelineIssue{
			Code:        "dag_node_config",
			Level:       raw.Level,
			Scope:       "dag_node",
			Field:       field,
			Check:       raw.Check,
			NodeID:      raw.Node,
			Message:     raw.Message,
			Remediation: raw.Remediation,
		}
		if node != nil {
			issue.NodeKind = string(node.Kind)
			issue.Plugin = node.Plugin
		}
		issues = append(issues, issue)
	}
	return issues
}

func filterPipelineIssues(issues []PipelineIssue, level string) []PipelineIssue {
	result := make([]PipelineIssue, 0, len(issues))
	for _, issue := range issues {
		if strings.EqualFold(issue.Level, level) {
			result = append(result, issue)
		}
	}
	return result
}

func hasBlockingPipelineIssues(issues []PipelineIssue) bool {
	for _, issue := range issues {
		if !strings.EqualFold(issue.Level, "warning") && !strings.EqualFold(issue.Level, "info") {
			return true
		}
	}
	return false
}

func dagValidationSummary(issues []PipelineIssue) string {
	for _, issue := range issues {
		if issue.Scope == "dag_node" || issue.NodeID != "" {
			return "DAG node validation failed"
		}
	}
	return "DAG validation failed"
}

func issueMessages(issues []PipelineIssue) []string {
	result := make([]string, 0, len(issues))
	for _, issue := range issues {
		if issue.Message != "" {
			result = append(result, issue.Message)
		}
	}
	return result
}

func parseTransformFieldIndex(field string) (int, bool) {
	parts := strings.Split(field, ".")
	if len(parts) < 2 || parts[0] != "transforms" {
		return 0, false
	}
	idx, err := strconv.Atoi(parts[1])
	return idx, err == nil && idx >= 0
}
