package harness

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRequiredCandidateCannotBuildASubstitute(t *testing.T) {
	t.Setenv("OPENETL_BIN", "")
	t.Setenv("E2E_CANDIDATE_REQUIRED", "1")
	if _, _, err := suppliedCandidateBinary(); err == nil || !strings.Contains(err.Error(), "substitute") {
		t.Fatalf("required candidate was replaced: %v", err)
	}
}

func TestCandidateEvidenceRejectsMismatchAndMutation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "candidate")
	body := []byte("immutable fixture bytes")
	if err := os.WriteFile(path, body, 0o755); err != nil {
		t.Fatal(err)
	}
	hash := sha256.Sum256(body)
	t.Setenv("OPENETL_BIN", path)
	t.Setenv("E2E_CANDIDATE_REQUIRED", "1")
	t.Setenv("E2E_EXPECTED_BINARY_SHA256", strings.Repeat("0", 64))
	if _, _, err := suppliedCandidateBinary(); err == nil {
		t.Fatal("wrong candidate hash was accepted")
	}
	t.Setenv("E2E_EXPECTED_BINARY_SHA256", hex.EncodeToString(hash[:]))
	t.Setenv("E2E_EVIDENCE_ROOT", t.TempDir())
	f := &fakeLifecycle{}
	recorder := NewEvidenceRecorder(f, "mutation")
	recorder.AddCheck("real_case", "passed", "")
	if err := os.WriteFile(path, []byte("changed after test start"), 0o755); err != nil {
		t.Fatal(err)
	}
	f.runCleanup()
	if recorder.ev.Result != "failed" || recorder.ev.Asset == nil || recorder.ev.Asset.BinarySHA256 != hex.EncodeToString(hash[:]) {
		t.Fatalf("mutation certified: %+v", recorder.ev)
	}
}
