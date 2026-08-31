package harness

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Evidence is the structured, committed artifact produced by each migrated
// path test run (IT-1/T1.5, plan.md §C). It binds a path certification to the
// exact source revision it ran on; the repo-side checker
// (hack/check-connector-evidence.sh) validates its commit binding, freshness
// against related source changes, and internal consistency.
type Evidence struct {
	PathID       string            `json:"path_id"`
	Commit       string            `json:"commit"`
	RunStartedAt string            `json:"run_started_at"`
	Runner       string            `json:"runner"`
	Deps         map[string]string `json:"deps,omitempty"`
	Checks       []Check           `json:"checks"`
	Result       string            `json:"result"` // passed | failed | skipped
	FinishedAt   string            `json:"finished_at,omitempty"`
}

// Check is one assertion block of a path evidence run.
type Check struct {
	Name   string `json:"name"`
	Result string `json:"result"` // passed | failed | skipped
	Reason string `json:"reason,omitempty"`
}

// DeriveResult maps a check list to the overall verdict: any failed check
// fails the run; otherwise any skipped check makes the run "skipped"
// (uncertified); only an all-passed, non-empty list certifies "passed". An
// empty list is "skipped" — a run that certified nothing must never claim
// pass (AGENTS.md: skipped ≠ passed).
func DeriveResult(checks []Check) string {
	if len(checks) == 0 {
		return "skipped"
	}
	anyFailed, anySkipped := false, false
	for _, c := range checks {
		switch c.Result {
		case "failed":
			anyFailed = true
		case "skipped":
			anySkipped = true
		}
	}
	switch {
	case anyFailed:
		return "failed"
	case anySkipped:
		return "skipped"
	default:
		return "passed"
	}
}

// testLifecycle is the subset of *testing.T the recorder depends on, so the
// verdict derivation is unit-testable with a fake.
type testLifecycle interface {
	Failed() bool
	Skipped() bool
	Cleanup(func())
	Errorf(format string, args ...any)
	Logf(format string, args ...any)
}

// Recorder builds Evidence for one path test run and writes
// docs/evidence/<path_id>.json under the repository root on test cleanup.
type Recorder struct {
	mu sync.Mutex
	ev Evidence
	t  testLifecycle
}

// NewEvidenceRecorder binds a recorder to the test lifecycle: the cleanup
// writes the evidence file and folds the test verdict into the result, so a
// fatal failure can never be certified as passed.
func NewEvidenceRecorder(t testLifecycle, pathID string) *Recorder {
	r := &Recorder{
		ev: Evidence{
			PathID:       pathID,
			Commit:       GitCommit(),
			RunStartedAt: time.Now().UTC().Format(time.RFC3339),
			Runner:       RunnerName(),
			Deps:         map[string]string{},
		},
		t: t,
	}
	t.Cleanup(func() {
		r.finish()
		evDir := EvidenceDir()
		if evDir == "" {
			t.Errorf("evidence: repo root: %v", errRepoRoot)
			return
		}
		path := filepath.Join(evDir, pathID+".json")
		if err := WriteEvidence(path, r.ev); err != nil {
			t.Errorf("evidence: write %s: %v", path, err)
			return
		}
		t.Logf("evidence written: %s", path)
	})
	return r
}

// errRepoRoot is the sentinel reported when the evidence directory cannot be
// resolved.
var errRepoRoot = fmt.Errorf("go.mod not found")

// EvidenceDir returns the directory committed path evidence is written to:
// $E2E_EVIDENCE_ROOT when set (tests, hermetic runs), otherwise
// <repo root>/docs/evidence. Empty when the repo root is unresolvable.
func EvidenceDir() string {
	if root := os.Getenv("E2E_EVIDENCE_ROOT"); root != "" {
		return root
	}
	if _, err := os.Stat("go.mod"); err == nil {
		dir, _ := os.Getwd()
		return filepath.Join(dir, "docs", "evidence")
	}
	if root, err := RepoRoot(); err == nil {
		return filepath.Join(root, "docs", "evidence")
	}
	return ""
}

// AddCheck records one assertion block outcome. A skipped check requires a
// reason.
func (r *Recorder) AddCheck(name, result, reason string) {
	switch result {
	case "passed", "failed", "skipped":
	default:
		panic("evidence: invalid check result " + result)
	}
	if result == "skipped" && strings.TrimSpace(reason) == "" {
		panic("evidence: skipped check needs a reason")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ev.Checks = append(r.ev.Checks, Check{Name: name, Result: result, Reason: reason})
}

// AddDep records a dependency version observed on this run (image tag,
// connector version, ...).
func (r *Recorder) AddDep(key, value string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ev.Deps[key] = value
}

func (r *Recorder) finish() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.ev.FinishedAt = time.Now().UTC().Format(time.RFC3339)
	switch {
	case r.t.Skipped():
		r.ev.Result = "skipped"
		r.ev.Checks = append(r.ev.Checks, Check{Name: "test_lifecycle", Result: "skipped", Reason: "test skipped; outcome not certified"})
	case r.t.Failed():
		r.ev.Result = "failed"
		r.ev.Checks = append(r.ev.Checks, Check{Name: "test_lifecycle", Result: "failed", Reason: "go test reported failure"})
	default:
		r.ev.Result = DeriveResult(r.ev.Checks)
	}
}

// GitCommit returns the current HEAD of the repository containing the
// working directory ("unknown" when it cannot be resolved, which the checker
// treats as a binding failure).
func GitCommit() string {
	root, err := RepoRoot()
	if err != nil {
		return "unknown"
	}
	out, err := exec.Command("git", "-C", root, "rev-parse", "HEAD").Output()
	if err != nil {
		return "unknown"
	}
	return strings.TrimSpace(string(out))
}

// RunnerName labels the environment that executed the run.
func RunnerName() string {
	switch {
	case os.Getenv("GITHUB_ACTIONS") == "true":
		return "github-actions"
	case os.Getenv("CI") != "":
		return "ci"
	default:
		return "local"
	}
}

// RepoRoot walks up from the working directory until go.mod is found.
func RepoRoot() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for dir != filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir, nil
		}
		dir = filepath.Dir(dir)
	}
	return "", fmt.Errorf("go.mod not found above %s", dir)
}

// WriteEvidence atomically writes a pretty-printed evidence JSON file.
func WriteEvidence(path string, ev Evidence) error {
	raw, err := json.MarshalIndent(ev, "", "  ")
	if err != nil {
		return err
	}
	raw = append(raw, '\n')
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// LoadEvidence parses a committed path evidence file.
func LoadEvidence(path string) (Evidence, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Evidence{}, err
	}
	var ev Evidence
	if err := json.Unmarshal(raw, &ev); err != nil {
		return Evidence{}, err
	}
	return ev, nil
}