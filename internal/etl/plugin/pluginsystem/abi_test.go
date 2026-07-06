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
		Name:              "sink-plugin",
		Kind:              KindSink,
		Version:           "1.0.0",
		ABI:               ABIVersionV1,
		MinRuntimeVersion: MinRuntimeVersionV1,
		Entrypoints:       []string{"transform"},
	})
	if err == nil || !strings.Contains(err.Error(), `must export "write"`) {
		t.Fatalf("ValidateManifest error = %v", err)
	}
}

func TestValidateManifestRejectsBadConfigField(t *testing.T) {
	err := ValidateManifest(PluginManifest{
		Name:              "bad-config",
		Kind:              KindSource,
		Version:           "1.0.0",
		ABI:               ABIVersionV1,
		MinRuntimeVersion: MinRuntimeVersionV1,
		Entrypoints:       []string{"read"},
		Config:            []ManifestField{{Name: "cursor", Type: "object"}},
	})
	if err == nil || !strings.Contains(err.Error(), "unsupported manifest config field type") {
		t.Fatalf("ValidateManifest error = %v", err)
	}
}

func TestNormalizeInstallManifestRequiresExplicitManifestToMatchForm(t *testing.T) {
	manifest := `{
		"name": "vip-order-enricher",
		"kind": "transform",
		"version": "1.2.3",
		"abi": "openetl.plugin.abi/v1",
		"min_runtime_version": "openetl-runtime/v1",
		"entrypoints": ["transform"]
	}`
	_, _, err := NormalizeInstallManifest("vip-order-enricher", KindTransform, "1.2.4", []byte(manifest))
	if err == nil || !strings.Contains(err.Error(), "does not match install version") {
		t.Fatalf("NormalizeInstallManifest error = %v", err)
	}
}

func TestNormalizeInstallManifestCreatesLegacyDefaultManifest(t *testing.T) {
	manifest, validated, err := NormalizeInstallManifest("legacy-transform", KindTransform, "", nil)
	if err != nil {
		t.Fatalf("NormalizeInstallManifest: %v", err)
	}
	if validated {
		t.Fatalf("validated = true, want false for legacy upload")
	}
	if manifest.ABI != ABIVersionV1 || manifest.MinRuntimeVersion != MinRuntimeVersionV1 || manifest.Version != "1.0.0" {
		t.Fatalf("manifest = %#v", manifest)
	}
	if len(manifest.Entrypoints) != 1 || manifest.Entrypoints[0] != "transform" {
		t.Fatalf("entrypoints = %#v", manifest.Entrypoints)
	}
}
