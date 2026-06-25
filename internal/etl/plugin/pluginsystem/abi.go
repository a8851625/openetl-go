package pluginsystem

import (
	"fmt"
	"regexp"
	"sort"
)

const (
	// ABIVersionV1 is the first stable plugin ABI contract exposed by OpenETL.
	ABIVersionV1 = "openetl.plugin.abi/v1"

	// MinRuntimeVersionV1 documents the minimum OpenETL runtime generation that
	// understands ABIVersionV1. It is intentionally a contract string rather
	// than a release tag so compatibility can be tested independently.
	MinRuntimeVersionV1 = "openetl-runtime/v1"
)

var pluginManifestNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$`)

// ManifestField describes a plugin config field without depending on the
// server package's UI schema types.
type ManifestField struct {
	Name        string   `json:"name" yaml:"name"`
	Type        string   `json:"type" yaml:"type"`
	Required    bool     `json:"required" yaml:"required"`
	Default     any      `json:"default,omitempty" yaml:"default,omitempty"`
	Description string   `json:"description,omitempty" yaml:"description,omitempty"`
	Secret      bool     `json:"secret,omitempty" yaml:"secret,omitempty"`
	Enum        []string `json:"enum,omitempty" yaml:"enum,omitempty"`
}

// PluginManifest is the package-level contract for third-party plugins.
type PluginManifest struct {
	Name              string          `json:"name" yaml:"name"`
	Kind              PluginKind      `json:"kind" yaml:"kind"`
	Version           string          `json:"version" yaml:"version"`
	ABI               string          `json:"abi" yaml:"abi"`
	MinRuntimeVersion string          `json:"min_runtime_version,omitempty" yaml:"min_runtime_version,omitempty"`
	Entrypoints       []string        `json:"entrypoints" yaml:"entrypoints"`
	Capabilities      []string        `json:"capabilities,omitempty" yaml:"capabilities,omitempty"`
	Config            []ManifestField `json:"config,omitempty" yaml:"config,omitempty"`
}

// ValidateManifest validates the ABI-level metadata required before a plugin
// can enter the connector certification pipeline.
func ValidateManifest(m PluginManifest) error {
	if !pluginManifestNamePattern.MatchString(m.Name) {
		return fmt.Errorf("invalid plugin name")
	}
	switch m.Kind {
	case KindSource, KindSink, KindTransform:
	default:
		return fmt.Errorf("invalid plugin kind %q", m.Kind)
	}
	if m.Version == "" {
		return fmt.Errorf("plugin version is required")
	}
	if m.ABI == "" {
		return fmt.Errorf("plugin ABI is required")
	}
	if m.ABI != ABIVersionV1 {
		return fmt.Errorf("unsupported plugin ABI %q", m.ABI)
	}
	requiredEntrypoint := requiredEntrypointForKind(m.Kind)
	if !containsString(m.Entrypoints, requiredEntrypoint) {
		return fmt.Errorf("%s plugin must export %q", m.Kind, requiredEntrypoint)
	}
	if err := validateManifestFields(m.Config); err != nil {
		return err
	}
	sort.Strings(m.Capabilities)
	return nil
}

func requiredEntrypointForKind(kind PluginKind) string {
	switch kind {
	case KindSource:
		return "read"
	case KindSink:
		return "write"
	default:
		return "transform"
	}
}

func validateManifestFields(fields []ManifestField) error {
	seen := map[string]bool{}
	for _, field := range fields {
		if field.Name == "" {
			return fmt.Errorf("manifest config field name is required")
		}
		if seen[field.Name] {
			return fmt.Errorf("duplicate manifest config field %q", field.Name)
		}
		seen[field.Name] = true
		switch field.Type {
		case "string", "int", "bool", "float", "string_array", "map":
		default:
			return fmt.Errorf("unsupported manifest config field type %q", field.Type)
		}
	}
	return nil
}

func containsString(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}
