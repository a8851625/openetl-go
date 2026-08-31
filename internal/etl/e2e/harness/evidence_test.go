package harness

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type fakeLifecycle struct {
	failed  bool
	skipped bool
	cleanup []func()
	errs    []string
	logs    []string
}

func (f *fakeLifecycle) Failed() bool                 { return f.failed }
func (f *fakeLifecycle) Skipped() bool                { return f.skipped }
func (f *fakeLifecycle) Cleanup(fn func())            { f.cleanup = append(f.cleanup, fn) }
func (f *fakeLifecycle) Errorf(format string, a ...any) { f.errs = append(f.errs, format) }
func (f *fakeLifecycle) Logf(format string, a ...any)   { f.logs = append(f.logs, format) }
func (f *fakeLifecycle) runCleanup() {
	for i := len(f.cleanup) - 1; i >= 0; i-- {
		f.cleanup[i]()
	}
}

func TestDeriveResult(t *testing.T) {
	cases := []struct {
		name   string
		checks []Check
		want   string
	}{
		{"empty is skipped", nil, "skipped"},
		{"all passed certifies", []Check{{Name: "a", Result: "passed"}, {Name: "b", Result: "passed"}}, "passed"},
		{"one failed fails all", []Check{{Name: "a", Result: "passed"}, {Name: "b", Result: "failed"}}, "failed"},
		{"any skipped uncertified", []Check{{Name: "a", Result: "passed"}, {Name: "b", Result: "skipped"}}, "skipped"},
		{"only skipped", []Check{{Name: "a", Result: "skipped"}}, "skipped"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := DeriveResult(tc.checks); got != tc.want {
				t.Fatalf("DeriveResult = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestRunnerName(t *testing.T) {
	t.Setenv("CI", "")
	t.Setenv("GITHUB_ACTIONS", "")
	if got := RunnerName(); got != "local" {
		t.Fatalf("RunnerName = %q, want local", got)
	}
	t.Setenv("CI", "true")
	if got := RunnerName(); got != "ci" {
		t.Fatalf("RunnerName = %q, want ci", got)
	}
	t.Setenv("GITHUB_ACTIONS", "true")
	if got := RunnerName(); got != "github-actions" {
		t.Fatalf("RunnerName = %q, want github-actions", got)
	}
}

func TestGitCommitResolvesInRepo(t *testing.T) {
	commit := GitCommit()
	if len(commit) != 40 || !isHex(commit) {
		t.Fatalf("GitCommit = %q, want 40-hex sha", commit)
	}
}

func TestWriteAndLoadEvidenceRoundTrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "docs", "evidence", "mysql_cdc__mysql_upsert.json")
	ev := Evidence{
		PathID:       "mysql_cdc__mysql_upsert",
		Commit:       "0123456789abcdef0123456789abcdef01234567",
		RunStartedAt: "2026-08-30T10:00:00Z",
		Runner:       "local",
		Deps:         map[string]string{"mysql": "mysql:8.0"},
		Checks: []Check{
			{Name: "happy_path", Result: "passed"},
		},
		Result:     "passed",
		FinishedAt: "2026-08-30T10:01:00Z",
	}
	if err := WriteEvidence(path, ev); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := LoadEvidence(path)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if got.PathID != ev.PathID || got.Commit != ev.Commit || got.Result != "passed" {
		t.Fatalf("round trip mismatch: %+v", got)
	}
	if len(got.Checks) != 1 || got.Checks[0].Name != "happy_path" {
		t.Fatalf("checks mismatch: %+v", got.Checks)
	}
}

func TestRecorderCleanupWritesAndDerives(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("E2E_EVIDENCE_ROOT", dir)
	f := &fakeLifecycle{}
	r := NewEvidenceRecorder(f, "unit-test-record")
	r.AddCheck("case_a", "passed", "")
	r.AddCheck("case_b", "passed", "")
	f.runCleanup()
	if r.ev.Result != "passed" {
		t.Fatalf("result = %q, want passed", r.ev.Result)
	}
	if len(f.errs) > 0 {
		t.Fatalf("unexpected cleanup errors: %v", f.errs)
	}
	if _, err := os.Stat(filepath.Join(dir, "unit-test-record.json")); err != nil {
		t.Fatalf("evidence file not written: %v", err)
	}
	if r.ev.Runner != "local" || r.ev.Commit == "" {
		t.Fatalf("recorder metadata incomplete: runner=%q commit=%q", r.ev.Runner, r.ev.Commit)
	}

	f2 := &fakeLifecycle{failed: true}
	r2 := NewEvidenceRecorder(f2, "unit-test-record")
	r2.AddCheck("case_a", "passed", "")
	f2.runCleanup()
	if r2.ev.Result != "failed" {
		t.Fatalf("failed lifecycle result = %q, want failed", r2.ev.Result)
	}
	found := false
	for _, c := range r2.ev.Checks {
		if c.Name == "test_lifecycle" && c.Result == "failed" {
			found = true
		}
	}
	if !found {
		t.Fatalf("test_lifecycle check missing: %+v", r2.ev.Checks)
	}

	f3 := &fakeLifecycle{skipped: true}
	r3 := NewEvidenceRecorder(f3, "unit-test-record")
	f3.runCleanup()
	if r3.ev.Result != "skipped" {
		t.Fatalf("skipped lifecycle result = %q, want skipped", r3.ev.Result)
	}

	// Skipped checks need a reason.
	defer func() { _ = recover() }()
	r4 := NewEvidenceRecorder(&fakeLifecycle{}, "unit-test-record")
	r4.AddCheck("x", "skipped", "")
	t.Fatal("AddCheck(skipped) without reason did not panic")
}

func TestEvidenceFileFormatMatchesContract(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "e.json")
	ev := Evidence{
		PathID:       "mysql_snap_cdc__ch_rmt",
		Commit:       "abc",
		RunStartedAt: time.Now().UTC().Format(time.RFC3339),
		Runner:       "local",
		Deps:         map[string]string{"clickhouse": "clickhouse:24.3"},
		Checks:       []Check{{Name: "snapshot", Result: "passed"}},
		Result:       "passed",
	}
	if err := WriteEvidence(path, ev); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{
		`"path_id": "mysql_snap_cdc__ch_rmt"`,
		`"commit": "abc"`,
		`"run_started_at":`,
		`"runner": "local"`,
		`"checks"`,
		`"result": "passed"`,
	} {
		if !strings.Contains(string(raw), want) {
			t.Fatalf("evidence JSON missing %s in:\n%s", want, raw)
		}
	}
}

func isHex(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}