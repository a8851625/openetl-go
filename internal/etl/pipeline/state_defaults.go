package pipeline

import (
	"strconv"
	"strings"
)

// InjectStateDefaults returns a copy of config with pipeline/node namespaces
// filled for stateful transforms that explicitly enable state_backend.
// It never enables state storage by itself and never overrides user-provided
// state_pipeline/state_node values.
func InjectStateDefaults(pipelineName, nodeID string, config map[string]any) map[string]any {
	out := cloneConfig(config)
	if out == nil {
		out = map[string]any{}
	}
	backend, _ := out["state_backend"].(string)
	if strings.TrimSpace(backend) == "" {
		return out
	}
	if current, _ := out["state_pipeline"].(string); strings.TrimSpace(current) == "" && strings.TrimSpace(pipelineName) != "" {
		out["state_pipeline"] = pipelineName
	}
	if current, _ := out["state_node"].(string); strings.TrimSpace(current) == "" && strings.TrimSpace(nodeID) != "" {
		out["state_node"] = nodeID
	}
	return out
}

func TransformStateNodeID(index int, typ string) string {
	typ = strings.TrimSpace(typ)
	if typ == "" {
		typ = "transform"
	}
	return typ + "-" + strconv.Itoa(index)
}
