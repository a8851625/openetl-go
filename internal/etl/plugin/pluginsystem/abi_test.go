package pluginsystem

import (
	"strings"
	"testing"
)

func TestValidateManifestV1(t *testing.T) {
	m := PluginManifest{
		Name:              "vip-order-enricher",
		Kind:              KindTransform,
		Version:           "1.0.0",
		ABI:               ABIVersionV1,
		MinRuntimeVersion: MinRuntimeVersionV1,
		Entrypoints:       []string{"transform"},
		Capabilities:      []string{"dimension_enrichment"},
		Config: []ManifestField{
			{Name: "endpoint", Type: "string", Required: true},
			{Name: "api_token", Type: "string", Secret: true},
		},
	}
	if err := ValidateManifest(m); err != nil {
		t.Fatalf("ValidateManifest: %v", err)
	}
}

func TestValidateManifestRejectsUnsupportedABI(t *testing.T) {
	err := ValidateManifest(PluginManifest{
		Name:        "legacy",
		Kind:        KindTransform,
		Version:     "1.0.0",
		ABI:         "openetl.plugin.abi/v0",
		Entrypoints: []string{"transform"},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported plugin ABI") {
		t.Fatalf("ValidateManifest error = %v", err)
	}
}

func TestValidateManifestRequiresKindEntrypoint(t *testing.T) {
	err := ValidateManifest(PluginManifest{
		Name:        "sink-plugin",
		Kind:        KindSink,
		Version:     "1.0.0",
		ABI:         ABIVersionV1,
		Entrypoints: []string{"transform"},
	})
	if err == nil || !strings.Contains(err.Error(), `must export "write"`) {
		t.Fatalf("ValidateManifest error = %v", err)
	}
}

func TestValidateManifestRejectsBadConfigField(t *testing.T) {
	err := ValidateManifest(PluginManifest{
		Name:        "bad-config",
		Kind:        KindSource,
		Version:     "1.0.0",
		ABI:         ABIVersionV1,
		Entrypoints: []string{"read"},
		Config:      []ManifestField{{Name: "cursor", Type: "object"}},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported manifest config field type") {
		t.Fatalf("ValidateManifest error = %v", err)
	}
}
