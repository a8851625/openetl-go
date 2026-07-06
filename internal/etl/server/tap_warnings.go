package server

import (
	"fmt"
	"strings"

	"github.com/a8851625/openetl-go/internal/etl/orchestrator"
	"github.com/a8851625/openetl-go/internal/etl/pipeline"
)

var tapUnimplementedConfigFields = []string{"alert_on", "threshold", "field", "value", "webhook"}

func tapUnimplementedConfigWarningsForPipeline(spec *pipeline.Spec) []string {
	if spec == nil {
		return nil
	}
	var warnings []string
	for i, transform := range spec.Transforms {
		if !strings.EqualFold(strings.TrimSpace(transform.Type), "tap") {
			continue
		}
		warnings = append(warnings, tapUnimplementedConfigWarnings(
			fmt.Sprintf("transforms[%d].config", i),
			transform.Config,
		)...)
	}
	return warnings
}

func tapUnimplementedConfigWarningsForDAG(spec *orchestrator.PipelineSpec) []string {
	if spec == nil {
		return nil
	}
	var warnings []string
	for _, node := range spec.DAG.Nodes {
		if node == nil {
			continue
		}
		if node.Kind != orchestrator.KindTap && !strings.EqualFold(strings.TrimSpace(node.Plugin), "tap") {
			continue
		}
		path := fmt.Sprintf("dag.nodes[%q].config", node.ID)
		warnings = append(warnings, tapUnimplementedConfigWarnings(path, node.Config)...)
	}
	return warnings
}

func tapUnimplementedConfigWarnings(path string, config map[string]any) []string {
	if len(config) == 0 {
		return nil
	}
	var warnings []string
	for _, field := range tapUnimplementedConfigFields {
		if _, ok := config[field]; !ok {
			continue
		}
		warnings = append(warnings, fmt.Sprintf(
			"%s.%s is currently unimplemented for tap and will be ignored; tap currently supports log_every and alert_on_lag_ms log warnings only.",
			path,
			field,
		))
	}
	return warnings
}
