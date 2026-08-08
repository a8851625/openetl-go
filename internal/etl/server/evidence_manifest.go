package server

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"
)

// ConnectorEvidenceManifest is the machine-readable source for connector
// certification freshness. It deliberately lives outside connector maturity
// metadata: a connector can remain production maturity while its latest
// evidence requires operator review.
type ConnectorEvidenceManifest struct {
	Version         string                    `json:"version"`
	Policy          ConnectorEvidencePolicy   `json:"policy"`
	CertifiedCommit string                    `json:"certified_commit"`
	CertifiedImage  string                    `json:"certified_image"`
	Records         []ConnectorEvidenceRecord `json:"records"`
}

type ConnectorEvidencePolicy struct {
	MaxAgeHours int `json:"max_age_hours"`
}

type ConnectorEvidenceRecord struct {
	Kind         string            `json:"kind"`
	Type         string            `json:"type"`
	Commit       string            `json:"commit"`
	Image        string            `json:"image"`
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
	return parseConnectorEvidenceManifest(embeddedConnectorEvidence)
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
		Commit: r.Commit, Image: r.Image,
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
