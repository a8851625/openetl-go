package server

import (
	"fmt"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/orchestrator"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

// dagNodeValidationIssue keeps connector construction failures attached to the
// node and field that the UI can repair. DAG.ValidateProduction intentionally
// checks graph shape only; this pass mirrors the runtime builders before a DAG
// is persisted or reported as valid.
type dagNodeValidationIssue struct {
	Level       string `json:"level"`
	Node        string `json:"node,omitempty"`
	Field       string `json:"field"`
	Check       string `json:"check"`
	Message     string `json:"message"`
	Remediation string `json:"remediation,omitempty"`
}

func validateDAGNodeConfigs(spec *orchestrator.PipelineSpec) []dagNodeValidationIssue {
	if spec == nil {
		return nil
	}
	var issues []dagNodeValidationIssue
	for _, node := range spec.DAG.Nodes {
		if node == nil {
			continue
		}
		// Registry existence is reported by dagPipelineIssues with a stable
		// unknown_connector code. Avoid emitting a second builder error for the
		// same missing plugin here.
		if !registryBuilderExists(node) {
			continue
		}
		var err error
		config := node.Config
		switch node.Kind {
		case orchestrator.KindSource:
			_, err = registry.BuildSource(node.Plugin, config)
		case orchestrator.KindSink:
			built, buildErr := registry.BuildSink(node.Plugin, config)
			err = buildErr
			if built != nil {
				_ = built.Close()
			}
		case orchestrator.KindTransform:
			built, buildErr := registry.BuildTransform(node.Plugin, pipeline.InjectStateDefaults(spec.Name, node.ID, config))
			err = buildErr
			if built != nil {
				core.TransformChain{built}.CloseChain()
			}
		default:
			// Built-in DAG control nodes are validated by the orchestrator's
			// graph/runtime checks and do not use the connector registry.
			continue
		}
		if err == nil {
			continue
		}
		field := fmt.Sprintf("dag.nodes[%s].config", node.ID)
		remediation := "fix the node configuration or choose a registered connector from the descriptor"
		if !registryBuilderExists(node) {
			field = fmt.Sprintf("dag.nodes[%s].plugin", node.ID)
			remediation = "choose a registered connector from the descriptor or update the plugin name"
		}
		issues = append(issues, dagNodeValidationIssue{
			Level:       "error",
			Node:        node.ID,
			Field:       field,
			Check:       "dag-node-config",
			Message:     err.Error(),
			Remediation: remediation,
		})
	}
	return issues
}

func registryBuilderExists(node *orchestrator.Node) bool {
	if node == nil {
		return false
	}
	switch node.Kind {
	case orchestrator.KindSource:
		return registry.HasSource(node.Plugin)
	case orchestrator.KindSink:
		return registry.HasSink(node.Plugin)
	case orchestrator.KindTransform:
		return registry.HasTransform(node.Plugin)
	default:
		return true
	}
}

func dagValidationErrorStrings(issues []dagNodeValidationIssue) []string {
	result := make([]string, 0, len(issues))
	for _, issue := range issues {
		result = append(result, fmt.Sprintf("%s: %s", issue.Field, issue.Message))
	}
	return result
}
