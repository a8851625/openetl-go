package pluginsystem

import (
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
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

// ValidatePluginName enforces the portable plugin name grammar used by
// manifests, storage, and filesystem paths.
func ValidatePluginName(name string) error {
	if !pluginManifestNamePattern.MatchString(name) {
		return fmt.Errorf("invalid plugin name %q (allowed: A-Z a-z 0-9 _ - ., must start with alphanumeric, max 128 chars)", name)
	}
	if strings.Contains(name, "..") {
		return fmt.Errorf("plugin name must not contain \"..\"")
	}
	return nil
}

// ValidateKind returns an error when kind is outside the supported plugin
// classes. Keeping this in the plugin package prevents API handlers and the
// runtime manager from drifting.
func ValidateKind(kind PluginKind) error {
	switch kind {
	case KindSource, KindSink, KindTransform:
		return nil
	default:
		return fmt.Errorf("invalid plugin kind %q", kind)
	}
}

// ValidateManifest validates the ABI-level metadata required before a plugin
// can enter the connector certification pipeline.
func ValidateManifest(m PluginManifest) error {
	if err := ValidatePluginName(m.Name); err != nil {
		return err
	}
	if err := ValidateKind(m.Kind); err != nil {
		return err
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
	if m.MinRuntimeVersion == "" {
		return fmt.Errorf("plugin min_runtime_version is required")
	}
	if m.MinRuntimeVersion != MinRuntimeVersionV1 {
		return fmt.Errorf("unsupported plugin min_runtime_version %q", m.MinRuntimeVersion)
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

// DefaultManifest creates a backward-compatible v1 manifest for legacy uploads
// that predate explicit manifest metadata. The returned manifest describes the
// runtime contract but should be reported as not user-validated.
func DefaultManifest(name string, kind PluginKind, version string) PluginManifest {
	if version == "" {
		version = "1.0.0"
	}
	return PluginManifest{
		Name:              name,
		Kind:              kind,
		Version:           version,
		ABI:               ABIVersionV1,
		MinRuntimeVersion: MinRuntimeVersionV1,
		Entrypoints:       []string{requiredEntrypointForKind(kind)},
	}
}

// NormalizeInstallManifest parses and validates an optional manifest supplied
// during plugin install/compile. It returns manifestValidated=false for legacy
// uploads with no explicit manifest.
func NormalizeInstallManifest(name string, kind PluginKind, version string, manifestRaw []byte) (PluginManifest, bool, error) {
	name = strings.TrimSpace(name)
	version = strings.TrimSpace(version)
	if err := ValidatePluginName(name); err != nil {
		return PluginManifest{}, false, err
	}
	if err := ValidateKind(kind); err != nil {
		return PluginManifest{}, false, err
	}

	raw := strings.TrimSpace(string(manifestRaw))
	if raw == "" {
		manifest := DefaultManifest(name, kind, version)
		if err := ValidateManifest(manifest); err != nil {
			return PluginManifest{}, false, err
		}
		return manifest, false, nil
	}

	var manifest PluginManifest
	if err := json.Unmarshal([]byte(raw), &manifest); err != nil {
		return PluginManifest{}, false, fmt.Errorf("parse plugin manifest: %w", err)
	}
	if manifest.Name != name {
		return PluginManifest{}, false, fmt.Errorf("manifest name %q does not match install name %q", manifest.Name, name)
	}
	if manifest.Kind != kind {
		return PluginManifest{}, false, fmt.Errorf("manifest kind %q does not match install kind %q", manifest.Kind, kind)
	}
	if version != "" && manifest.Version != version {
		return PluginManifest{}, false, fmt.Errorf("manifest version %q does not match install version %q", manifest.Version, version)
	}
	if err := ValidateManifest(manifest); err != nil {
		return PluginManifest{}, false, err
	}
	sort.Strings(manifest.Capabilities)
	return manifest, true, nil
}

// MarshalManifestJSON returns a stable JSON representation for storage.
func MarshalManifestJSON(manifest PluginManifest) (string, error) {
	body, err := json.Marshal(manifest)
	if err != nil {
		return "", fmt.Errorf("marshal plugin manifest: %w", err)
	}
	return string(body), nil
}

// ParseManifestJSON parses a manifest previously stored by the plugin manager.
func ParseManifestJSON(raw string) (PluginManifest, error) {
	var manifest PluginManifest
	if err := json.Unmarshal([]byte(raw), &manifest); err != nil {
		return PluginManifest{}, fmt.Errorf("parse stored plugin manifest: %w", err)
	}
	if err := ValidateManifest(manifest); err != nil {
		return PluginManifest{}, err
	}
	return manifest, nil
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
