package server

import (
	"crypto/sha256"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/a8851625/openetl-go/internal/consts"
)

// ConnectorEvidenceManifest is the machine-readable source for connector
// certification freshness. It deliberately lives outside connector maturity
// metadata: a connector can remain production maturity while its latest
// evidence requires operator review.
type ConnectorEvidenceManifest struct {
	Version               string                    `json:"version"`
	Policy                ConnectorEvidencePolicy   `json:"policy"`
	CertifiedCommit       string                    `json:"certified_commit"`
	CertifiedImage        string                    `json:"certified_image"`
	CertifiedBinarySHA256 string                    `json:"certified_binary_sha256,omitempty"`
	Records               []ConnectorEvidenceRecord `json:"records"`
}

type ConnectorEvidencePolicy struct {
	MaxAgeHours int `json:"max_age_hours"`
}

type ConnectorEvidenceRecord struct {
	Kind         string            `json:"kind"`
	Type         string            `json:"type"`
	Commit       string            `json:"commit"`
	Image        string            `json:"image"`
	BinarySHA256 string            `json:"binary_sha256,omitempty"`
	Dependencies map[string]string `json:"dependencies"`
	StartedAt    string            `json:"started_at"`
	FinishedAt   string            `json:"finished_at"`
	ExpiresAt    string            `json:"expires_at"`
	Scripts      []string          `json:"scripts"`
	Cases        []string          `json:"cases"`
	Verified     bool              `json:"verified"`
	Note         string            `json:"note,omitempty"`
}

// ConnectorEvidenceMetadata is exposed on readiness gates so API/UI clients
// can show exactly which build, environment and validity window backs a claim.
type ConnectorEvidenceMetadata struct {
	Commit       string            `json:"commit"`
	Image        string            `json:"image"`
	BinarySHA256 string            `json:"binary_sha256,omitempty"`
	Dependencies map[string]string `json:"dependencies"`
	StartedAt    string            `json:"started_at"`
	FinishedAt   string            `json:"finished_at"`
	ExpiresAt    string            `json:"expires_at"`
	Scripts      []string          `json:"scripts"`
	Cases        []string          `json:"cases"`
	Verified     bool              `json:"verified"`
	Note         string            `json:"note,omitempty"`
}

type connectorEvidenceFreshness struct {
	Status      string
	Explanation string
}

//go:embed evidence/connector-evidence.json
var embeddedConnectorEvidence []byte

func LoadConnectorEvidenceManifest() (ConnectorEvidenceManifest, error) {
	external := strings.TrimSpace(os.Getenv("ETL_CONNECTOR_EVIDENCE_FILE"))
	var manifest ConnectorEvidenceManifest
	var err error
	if external != "" {
		manifest, err = LoadConnectorEvidenceManifestFile(external)
	} else {
		manifest, err = parseConnectorEvidenceManifest(embeddedConnectorEvidence)
	}
	if err != nil || (consts.BuildCommit == "" && external == "") {
		return manifest, err
	}
	if external == "" && manifest.CertifiedCommit != consts.BuildCommit {
		return bindConnectorEvidence(manifest, consts.BuildCommit, "", false)
	}
	hash, err := runningExecutableSHA256()
	if err != nil {
		return ConnectorEvidenceManifest{}, err
	}
	return bindConnectorEvidence(manifest, consts.BuildCommit, hash, external != "")
}

var executableEvidenceHash struct {
	sync.Once
	hash string
	err  error
}

func runningExecutableSHA256() (string, error) {
	executableEvidenceHash.Do(func() {
		path, err := os.Executable()
		if err != nil {
			executableEvidenceHash.err = err
			return
		}
		file, err := os.Open(path)
		if err != nil {
			executableEvidenceHash.err = err
			return
		}
		defer file.Close()
		hash := sha256.New()
		if _, err := io.Copy(hash, file); err != nil {
			executableEvidenceHash.err = err
			return
		}
		executableEvidenceHash.hash = hex.EncodeToString(hash.Sum(nil))
	})
	return executableEvidenceHash.hash, executableEvidenceHash.err
}

func bindConnectorEvidence(manifest ConnectorEvidenceManifest, commit, binaryHash string, external bool) (ConnectorEvidenceManifest, error) {
	reason := ""
	if commit != "" && manifest.CertifiedCommit != commit {
		reason = "evidence source does not match this candidate build"
	} else if manifest.CertifiedBinarySHA256 == "" || manifest.CertifiedBinarySHA256 != binaryHash {
		reason = "evidence does not bind the running executable SHA256"
	}
	if reason == "" {
		return manifest, nil
	}
	if external {
		return ConnectorEvidenceManifest{}, fmt.Errorf("candidate evidence unavailable: %s", reason)
	}
	// Embedded history cannot certify a later candidate. A report generated
	// after certification is supplied read-only, without rebuilding the image.
	manifest.Records = append([]ConnectorEvidenceRecord(nil), manifest.Records...)
	for i := range manifest.Records {
		manifest.Records[i].Verified = false
		manifest.Records[i].Note += "; " + reason + "; supply ETL_CONNECTOR_EVIDENCE_FILE for this immutable candidate"
	}
	return manifest, nil
}

func LoadConnectorEvidenceManifestFile(path string) (ConnectorEvidenceManifest, error) {
	body, err := os.ReadFile(path)
	if err != nil {
		return ConnectorEvidenceManifest{}, fmt.Errorf("read connector evidence manifest: %w", err)
	}
	return parseConnectorEvidenceManifest(body)
}

func parseConnectorEvidenceManifest(body []byte) (ConnectorEvidenceManifest, error) {
	var manifest ConnectorEvidenceManifest
	if err := json.Unmarshal(body, &manifest); err != nil {
		return ConnectorEvidenceManifest{}, fmt.Errorf("decode connector evidence manifest: %w", err)
	}
	if err := ValidateConnectorEvidenceManifest(manifest); err != nil {
		return ConnectorEvidenceManifest{}, err
	}
	return manifest, nil
}

// ValidateConnectorEvidenceManifest checks structural invariants without
// applying the current-time freshness rule. This makes malformed manifests a
// hard readiness failure while allowing callers to report expiry separately.
func ValidateConnectorEvidenceManifest(manifest ConnectorEvidenceManifest) error {
	if manifest.CertifiedBinarySHA256 != "" {
		hash, err := hex.DecodeString(manifest.CertifiedBinarySHA256)
		if err != nil || len(hash) != sha256.Size {
			return fmt.Errorf("connector evidence certified_binary_sha256 must be a SHA256")
		}
	}
	if strings.TrimSpace(manifest.Version) != "v1" {
		return fmt.Errorf("connector evidence manifest version %q is unsupported", manifest.Version)
	}
	if manifest.Policy.MaxAgeHours <= 0 {
		return fmt.Errorf("connector evidence manifest policy.max_age_hours must be > 0")
	}
	if strings.TrimSpace(manifest.CertifiedCommit) == "" {
		return fmt.Errorf("connector evidence manifest certified_commit is required")
	}
	if strings.TrimSpace(manifest.CertifiedImage) == "" {
		return fmt.Errorf("connector evidence manifest certified_image is required")
	}
	if len(manifest.Records) == 0 {
		return fmt.Errorf("connector evidence manifest records must not be empty")
	}
	seen := make(map[string]bool, len(manifest.Records))
	for i, record := range manifest.Records {
		key := evidenceRecordKey(record.Kind, record.Type)
		if strings.TrimSpace(record.Kind) == "" || strings.TrimSpace(record.Type) == "" {
			return fmt.Errorf("connector evidence record %d requires kind and type", i)
		}
		if seen[key] {
			return fmt.Errorf("connector evidence manifest contains duplicate record %s", key)
		}
		seen[key] = true
		if strings.TrimSpace(record.Commit) == "" || strings.TrimSpace(record.Image) == "" {
			return fmt.Errorf("connector evidence record %s requires commit and image", key)
		}
		if record.Commit != manifest.CertifiedCommit || record.Image != manifest.CertifiedImage {
			return fmt.Errorf("connector evidence record %s does not match manifest certified commit/image", key)
		}
		if record.BinarySHA256 != manifest.CertifiedBinarySHA256 {
			return fmt.Errorf("connector evidence record %s does not match manifest binary SHA256", key)
		}
		if len(record.Dependencies) == 0 {
			return fmt.Errorf("connector evidence record %s requires dependency versions", key)
		}
		if len(record.Scripts) == 0 {
			return fmt.Errorf("connector evidence record %s requires at least one script", key)
		}
		for _, script := range record.Scripts {
			if !validEvidenceScriptPath(script) {
				return fmt.Errorf("connector evidence record %s has invalid script path %q", key, script)
			}
		}
		if len(record.Cases) == 0 {
			return fmt.Errorf("connector evidence record %s requires at least one case", key)
		}
		started, err := parseEvidenceTime(record.StartedAt, key, "started_at")
		if err != nil {
			return err
		}
		finished, err := parseEvidenceTime(record.FinishedAt, key, "finished_at")
		if err != nil {
			return err
		}
		expires, err := parseEvidenceTime(record.ExpiresAt, key, "expires_at")
		if err != nil {
			return err
		}
		if finished.Before(started) {
			return fmt.Errorf("connector evidence record %s finished_at precedes started_at", key)
		}
		if !expires.After(finished) {
			return fmt.Errorf("connector evidence record %s expires_at must follow finished_at", key)
		}
		if expires.Sub(finished) > time.Duration(manifest.Policy.MaxAgeHours)*time.Hour {
			return fmt.Errorf("connector evidence record %s exceeds policy.max_age_hours", key)
		}
	}
	return nil
}

func (m ConnectorEvidenceManifest) Record(kind, typ string) (ConnectorEvidenceRecord, bool) {
	key := evidenceRecordKey(kind, typ)
	for _, record := range m.Records {
		if evidenceRecordKey(record.Kind, record.Type) == key {
			return record, true
		}
	}
	return ConnectorEvidenceRecord{}, false
}

func (r ConnectorEvidenceRecord) Freshness(now time.Time) connectorEvidenceFreshness {
	expires, err := time.Parse(time.RFC3339, r.ExpiresAt)
	if err != nil {
		return connectorEvidenceFreshness{Status: "missing", Explanation: "expires_at is invalid"}
	}
	if !now.Before(expires) {
		explanation := fmt.Sprintf("evidence expired at %s", expires.UTC().Format(time.RFC3339))
		if !r.Verified {
			explanation += " and is not marked verified by the certification run"
		}
		return connectorEvidenceFreshness{
			Status:      "partial",
			Explanation: explanation,
		}
	}
	if !r.Verified {
		return connectorEvidenceFreshness{
			Status:      "partial",
			Explanation: "record is present but is not marked verified by the certification run",
		}
	}
	return connectorEvidenceFreshness{Status: "pass", Explanation: "evidence is verified and within its freshness window"}
}

func (r ConnectorEvidenceRecord) Metadata() ConnectorEvidenceMetadata {
	return ConnectorEvidenceMetadata{
		Commit: r.Commit, Image: r.Image, BinarySHA256: r.BinarySHA256,
		Dependencies: cloneStringMap(r.Dependencies),
		StartedAt:    r.StartedAt, FinishedAt: r.FinishedAt, ExpiresAt: r.ExpiresAt,
		Scripts: append([]string(nil), r.Scripts...), Cases: append([]string(nil), r.Cases...),
		Verified: r.Verified, Note: r.Note,
	}
}

func (r ConnectorEvidenceRecord) EvidenceSummary() string {
	summary := strings.Join(r.Scripts, ", ")
	if r.Note != "" {
		return summary + "; " + r.Note
	}
	return summary
}

func evidenceRecordKey(kind, typ string) string {
	return strings.ToLower(strings.TrimSpace(kind)) + ":" + strings.ToLower(strings.TrimSpace(typ))
}

func parseEvidenceTime(value, key, field string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(value))
	if err != nil {
		return time.Time{}, fmt.Errorf("connector evidence record %s has invalid %s: %w", key, field, err)
	}
	return parsed, nil
}

func validEvidenceScriptPath(path string) bool {
	path = strings.TrimSpace(path)
	return strings.HasPrefix(path, "hack/") && strings.HasSuffix(path, ".sh") && !strings.Contains(path, "..")
}

func cloneStringMap(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
