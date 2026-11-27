package harness

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// CandidateAsset distinguishes binary subprocess tests from image tests. A
// binary hash is recorded from the actual executable, never from an env claim.
type CandidateAsset struct {
	Kind         string `json:"kind"`
	BinarySHA256 string `json:"binary_sha256"`
}

func suppliedCandidateBinary() (string, string, error) {
	path := strings.TrimSpace(os.Getenv("OPENETL_BIN"))
	required := os.Getenv("E2E_CANDIDATE_REQUIRED") == "1"
	if path == "" {
		if required {
			return "", "", fmt.Errorf("candidate executable is required; refusing a substitute build")
		}
		return "", "", nil
	}
	path, err := filepath.Abs(path)
	if err != nil {
		return "", "", err
	}
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() {
		return "", "", fmt.Errorf("candidate executable unavailable: %s", path)
	}
	expected := strings.TrimSpace(os.Getenv("E2E_EXPECTED_BINARY_SHA256"))
	if required && expected == "" {
		return "", "", fmt.Errorf("candidate executable SHA256 is required")
	}
	if expected != "" {
		decoded, err := hex.DecodeString(expected)
		if err != nil || len(decoded) != sha256.Size {
			return "", "", fmt.Errorf("candidate executable SHA256 is invalid")
		}
	}
	file, err := os.Open(path)
	if err != nil {
		return "", "", err
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return "", "", err
	}
	actual := hex.EncodeToString(hash.Sum(nil))
	if expected != "" && !strings.EqualFold(expected, actual) {
		return "", "", fmt.Errorf("candidate executable SHA256 mismatch; refusing a substitute build")
	}
	return path, actual, nil
}
