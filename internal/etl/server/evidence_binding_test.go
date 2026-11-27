package server

import (
	"strings"
	"testing"
)

func TestCandidateEvidenceRequiresActualSourceAndExecutable(t *testing.T) {
	manifest, err := parseConnectorEvidenceManifest(embeddedConnectorEvidence)
	if err != nil {
		t.Fatal(err)
	}
	const source = "0123456789abcdef0123456789abcdef01234567"
	hash := strings.Repeat("a", 64)
	manifest.CertifiedCommit = source
	manifest.CertifiedBinarySHA256 = hash
	for i := range manifest.Records {
		manifest.Records[i].Commit = source
		manifest.Records[i].BinarySHA256 = hash
		manifest.Records[i].Verified = true
	}
	if err := ValidateConnectorEvidenceManifest(manifest); err != nil {
		t.Fatal(err)
	}
	if bound, err := bindConnectorEvidence(manifest, source, hash, true); err != nil || !bound.Records[0].Verified {
		t.Fatal("actual candidate report rejected")
	}
	for _, mismatch := range []struct{ source, hash string }{
		{"different source", hash}, {source, strings.Repeat("b", 64)}, {source, ""},
	} {
		if _, err := bindConnectorEvidence(manifest, mismatch.source, mismatch.hash, true); err == nil {
			t.Fatal("external report silently certified another candidate")
		}
		history, err := bindConnectorEvidence(manifest, mismatch.source, mismatch.hash, false)
		if err != nil || history.Records[0].Verified || !strings.Contains(history.Records[0].Note, "immutable candidate") {
			t.Fatal("embedded history certified this candidate")
		}
	}
	if !manifest.Records[0].Verified {
		t.Fatal("historical downgrade mutated the caller's manifest")
	}
	manifest.Records[0].BinarySHA256 = strings.Repeat("0", 64)
	if err := ValidateConnectorEvidenceManifest(manifest); err == nil {
		t.Fatal("a record from a different binary was accepted")
	}
}
