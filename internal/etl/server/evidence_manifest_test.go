package server

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConnectorEvidenceManifestLoadsAndCoversProductionConnectors(t *testing.T) {
	manifest, err := LoadConnectorEvidenceManifest()
	if err != nil {
		t.Fatalf("LoadConnectorEvidenceManifest: %v", err)
	}
	if len(manifest.Records) != 14 {
		t.Fatalf("manifest records = %d, want 14 production source/sink records", len(manifest.Records))
	}
	for _, descriptor := range connectorDescriptors() {
		if descriptor.Maturity != "production" || (descriptor.Kind != "source" && descriptor.Kind != "sink") {
			continue
		}
		record, ok := manifest.Record(descriptor.Kind, descriptor.Type)
		if !ok {
			t.Fatalf("production descriptor %s/%s has no evidence record", descriptor.Kind, descriptor.Type)
		}
		if len(record.Scripts) == 0 || len(record.Cases) == 0 || len(record.Dependencies) == 0 {
			t.Fatalf("incomplete evidence record for %s/%s: %#v", descriptor.Kind, descriptor.Type, record)
		}
		gate, ok := readinessGate(descriptor, "e2e_evidence")
		if !ok || gate.EvidenceMetadata == nil {
			t.Fatalf("production descriptor %s/%s has no evidence gate metadata: %#v", descriptor.Kind, descriptor.Type, gate)
		}
		wantStatus := record.Freshness(time.Now()).Status
		if gate.Status != wantStatus {
			t.Fatalf("production descriptor %s/%s evidence status = %q, want %q from manifest", descriptor.Kind, descriptor.Type, gate.Status, wantStatus)
		}
	}
}

func TestConnectorEvidenceManifestDocsAndCheckerArePresent(t *testing.T) {
	root := filepath.Clean("../../..")
	body, err := os.ReadFile(filepath.Join(root, "docs/connector-certification.md"))
	if err != nil {
		t.Fatalf("read connector certification docs: %v", err)
	}
	text := string(body)
	for _, want := range []string{
		"internal/etl/server/evidence/connector-evidence.json",
		"evidence_metadata",
		"hack/check-connector-evidence.sh",
		"production_with_review",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("connector certification docs missing %q", want)
		}
	}
	if _, err := os.Stat(filepath.Join(root, "hack/check-connector-evidence.sh")); err != nil {
		t.Fatalf("evidence checker missing: %v", err)
	}
}

func TestValidateConnectorEvidenceManifestRejectsStructuralDrift(t *testing.T) {
	base := validEvidenceManifestForTest()
	base.Records = append(base.Records, base.Records[0])
	if err := ValidateConnectorEvidenceManifest(base); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate validation error = %v, want duplicate record error", err)
	}

	base = validEvidenceManifestForTest()
	base.Records[0].ExpiresAt = base.Records[0].FinishedAt
	if err := ValidateConnectorEvidenceManifest(base); err == nil || !strings.Contains(err.Error(), "expires_at") {
		t.Fatalf("window validation error = %v, want expires_at error", err)
	}
}

func TestConnectorEvidenceFreshnessControlsReadinessGate(t *testing.T) {
	manifest := validEvidenceManifestForTest()
	now := time.Date(2026, 8, 8, 2, 0, 0, 0, time.UTC)

	gate := evidenceGateAt("source", "fixture", "production", now, manifest, nil)
	if gate.Status != "partial" || gate.EvidenceMetadata == nil {
		t.Fatalf("unverified gate = %#v, want partial with metadata", gate)
	}

	manifest.Records[0].Verified = true
	gate = evidenceGateAt("source", "fixture", "production", now, manifest, nil)
	if gate.Status != "pass" || gate.EvidenceMetadata == nil {
		t.Fatalf("fresh gate = %#v, want pass with metadata", gate)
	}

	manifest.Records[0].ExpiresAt = "2026-08-08T01:00:00Z"
	gate = evidenceGateAt("source", "fixture", "production", now, manifest, nil)
	if gate.Status != "partial" || !strings.Contains(gate.Evidence, "expired") {
		t.Fatalf("expired gate = %#v, want partial expired evidence", gate)
	}

	gate = evidenceGateAt("source", "missing", "production", now, manifest, nil)
	if gate.Status != "missing" {
		t.Fatalf("missing gate status = %q, want missing", gate.Status)
	}

	gate = evidenceGateAt("source", "fixture", "production", now, ConnectorEvidenceManifest{}, errors.New("broken manifest"))
	if gate.Status != "missing" || !strings.Contains(gate.Evidence, "unavailable") {
		t.Fatalf("broken manifest gate = %#v, want missing/unavailable", gate)
	}
}

func validEvidenceManifestForTest() ConnectorEvidenceManifest {
	return ConnectorEvidenceManifest{
		Version:         "v1",
		Policy:          ConnectorEvidencePolicy{MaxAgeHours: 720},
		CertifiedCommit: "test-commit",
		CertifiedImage:  "sha256:test-image",
		Records: []ConnectorEvidenceRecord{{
			Kind: "source", Type: "fixture", Commit: "test-commit", Image: "sha256:test-image",
			Dependencies: map[string]string{"go": "1.24.13"},
			StartedAt:    "2026-08-08T00:00:00Z", FinishedAt: "2026-08-08T01:00:00Z", ExpiresAt: "2026-09-07T01:00:00Z",
			Scripts: []string{"hack/e2e.sh"}, Cases: []string{"happy"}, Verified: false,
		}},
	}
}
