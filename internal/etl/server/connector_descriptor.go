package server

import (
	"encoding/json"
	"net/http"
	"sort"

	"github.com/a8851625/openetl-go/internal/etl/registry"
)

type ConnectorDescriptor struct {
	Version      string        `json:"version"`
	Kind         string        `json:"kind"`
	Type         string        `json:"type"`
	Maturity     string        `json:"maturity"`
	Required     []string      `json:"required"`
	Capabilities []string      `json:"capabilities"`
	Fields       []ConfigField `json:"fields"`
	SecretFields []string      `json:"secret_fields"`
	Registered   bool          `json:"registered"`
}

func (s *Server) handleConnectorDescriptors(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	if r.Method != http.MethodGet {
		w.WriteHeader(http.StatusMethodNotAllowed)
		json.NewEncoder(w).Encode(map[string]any{"error": "method not allowed"})
		return
	}
	json.NewEncoder(w).Encode(map[string]any{
		"version":     "v1",
		"descriptors": connectorDescriptors(),
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
			if maturity == "" {
				maturity = "experimental"
			}
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
		out = append(out, ConnectorDescriptor{
			Version:      "v1",
			Kind:         kind,
			Type:         name,
			Maturity:     maturity,
			Required:     required,
			Capabilities: capabilities,
			Fields:       fields,
			SecretFields: secretFields,
			Registered:   registeredSet[name],
		})
	}
	return out
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
