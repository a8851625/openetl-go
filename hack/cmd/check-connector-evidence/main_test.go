package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckCommitBindingAllowsEvidenceOnlyDescendant(t *testing.T) {
	root := initEvidenceGitRepo(t)
	writeEvidenceTestFile(t, root, "internal/etl/server/evidence/connector-evidence.json", "baseline")
	writeEvidenceTestFile(t, root, "docs/connector-certification.md", "baseline")
	commitEvidenceTest(t, root, "baseline")
	certified := mustGitOutput(t, root, "rev-parse", "HEAD")

	writeEvidenceTestFile(t, root, "internal/etl/server/evidence/connector-evidence.json", "refreshed")
	writeEvidenceTestFile(t, root, "docs/ROADMAP.zh.md", "refreshed")
	commitEvidenceTest(t, root, "refresh evidence")
	current := mustGitOutput(t, root, "rev-parse", "HEAD")

	if err := checkCommitBinding(root, certified, current); err != nil {
		t.Fatalf("evidence-only descendant rejected: %v", err)
	}
}

func TestCheckCommitBindingRejectsRuntimeChange(t *testing.T) {
	root := initEvidenceGitRepo(t)
	writeEvidenceTestFile(t, root, "internal/etl/server/evidence/connector-evidence.json", "baseline")
	commitEvidenceTest(t, root, "baseline")
	certified := mustGitOutput(t, root, "rev-parse", "HEAD")

	writeEvidenceTestFile(t, root, "internal/etl/server/runtime.go", "changed")
	commitEvidenceTest(t, root, "runtime change")
	current := mustGitOutput(t, root, "rev-parse", "HEAD")

	err := checkCommitBinding(root, certified, current)
	if err == nil || !strings.Contains(err.Error(), "runtime.go") {
		t.Fatalf("runtime descendant error = %v, want changed runtime path", err)
	}
}

func TestCheckCommitBindingRejectsNonAncestor(t *testing.T) {
	root := initEvidenceGitRepo(t)
	writeEvidenceTestFile(t, root, "internal/etl/server/evidence/connector-evidence.json", "baseline")
	commitEvidenceTest(t, root, "baseline")
	certified := mustGitOutput(t, root, "rev-parse", "HEAD")

	if err := runGit(root, "checkout", "--orphan", "unrelated"); err != nil {
		t.Fatalf("create orphan branch: %v", err)
	}
	if err := runGit(root, "rm", "-rf", "."); err != nil {
		t.Fatalf("clear orphan index: %v", err)
	}
	writeEvidenceTestFile(t, root, "unrelated.txt", "unrelated")
	commitEvidenceTest(t, root, "unrelated")
	current := mustGitOutput(t, root, "rev-parse", "HEAD")

	err := checkCommitBinding(root, certified, current)
	if err == nil || !strings.Contains(err.Error(), "not an ancestor") {
		t.Fatalf("non-ancestor error = %v, want ancestor error", err)
	}
}

func TestCheckPathEvidenceAcceptsFreshPassed(t *testing.T) {
	root := initEvidenceGitRepo(t)
	writeEvidenceTestFile(t, root, "internal/etl/source/mysql_cdc.go", "source")
	commitEvidenceTest(t, root, "source baseline")
	commit := mustGitOutput(t, root, "rev-parse", "HEAD")

	writeEvidenceTestFile(t, root, "docs/evidence/mysql_cdc__mysql_upsert.json", pathEvidenceJSON("mysql_cdc__mysql_upsert", commit, `{"name":"happy_path","result":"passed"}`))
	commitEvidenceTest(t, root, "add path evidence")
	current := mustGitOutput(t, root, "rev-parse", "HEAD")

	if err := checkPathEvidenceFile(root, current, filepath.Join(root, "docs/evidence/mysql_cdc__mysql_upsert.json")); err != nil {
		t.Fatalf("fresh passed evidence rejected: %v", err)
	}
}

func TestCheckPathEvidenceRejectsStaleAfterSourceChange(t *testing.T) {
	root := initEvidenceGitRepo(t)
	writeEvidenceTestFile(t, root, "internal/etl/source/mysql_cdc.go", "v1")
	commitEvidenceTest(t, root, "source v1")
	sourceCommit := mustGitOutput(t, root, "rev-parse", "HEAD")

	writeEvidenceTestFile(t, root, "docs/evidence/mysql_cdc__mysql_upsert.json", pathEvidenceJSON("mysql_cdc__mysql_upsert", sourceCommit, `{"name":"happy_path","result":"passed"}`))
	commitEvidenceTest(t, root, "evidence at source v1")
	current := mustGitOutput(t, root, "rev-parse", "HEAD")

	// Accept while sources are unchanged.
	if err := checkPathEvidenceFile(root, current, filepath.Join(root, "docs/evidence/mysql_cdc__mysql_upsert.json")); err != nil {
		t.Fatalf("evidence fresh before change rejected: %v", err)
	}

	// Source changes after the evidence commit => stale.
	writeEvidenceTestFile(t, root, "internal/etl/sink/mysql.go", "v2")
	commitEvidenceTest(t, root, "sink v2")
	current = mustGitOutput(t, root, "rev-parse", "HEAD")
	err := checkPathEvidenceFile(root, current, filepath.Join(root, "docs/evidence/mysql_cdc__mysql_upsert.json"))
	if err == nil || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("stale error = %v, want stale", err)
	}
}

func TestCheckPathEvidenceRejectsTamper(t *testing.T) {
	cases := []struct {
		name string
		body func(commit string) string
	}{
		{"flipped check", func(c string) string {
			return pathEvidenceJSON("mysql_cdc__mysql_upsert", c, `{"name":"happy_path","result":"failed"}`)
		}},
		{"empty checks", func(c string) string {
			return pathEvidenceJSON("mysql_cdc__mysql_upsert", c, ``)
		}},
		{"bad result", func(c string) string {
			return `{"path_id":"mysql_cdc__mysql_upsert","commit":"` + c + `","checks":[{"name":"happy_path","result":"passed"}],"result":"failed"}`
		}},
		{"wrong id", func(c string) string {
			return `{"path_id":"other","commit":"` + c + `","checks":[{"name":"happy_path","result":"passed"}],"result":"passed"}`
		}},
		{"unbound commit", func(c string) string {
			return `{"path_id":"mysql_cdc__mysql_upsert","commit":"deadbeef00000000000000000000000000000000","checks":[{"name":"happy_path","result":"passed"}],"result":"passed"}`
		}},
		{"skipped check", func(c string) string {
			return pathEvidenceJSON("mysql_cdc__mysql_upsert", c, `{"name":"happy_path","result":"skipped","reason":"no container"}`)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			sub := initEvidenceGitRepo(t)
			writeEvidenceTestFile(t, sub, "internal/etl/source/mysql_cdc.go", "source")
			commitEvidenceTest(t, sub, "baseline")
			current := mustGitOutput(t, sub, "rev-parse", "HEAD")
			writeEvidenceTestFile(t, sub, "docs/evidence/mysql_cdc__mysql_upsert.json", tc.body(current))
			commitEvidenceTest(t, sub, "tamper")
			if err := checkPathEvidenceFile(sub, current, filepath.Join(sub, "docs/evidence/mysql_cdc__mysql_upsert.json")); err == nil {
				t.Fatalf("tampered evidence accepted")
			}
		})
	}
}

func pathEvidenceJSON(pathID, commit, checksJSON string) string {
	return `{"path_id":"` + pathID + `","commit":"` + commit + `","run_started_at":"2026-08-30T10:00:00Z","runner":"local","checks":[` + checksJSON + `],"result":"passed"}`
}

func initEvidenceGitRepo(t *testing.T) string {
	t.Helper()
	root := t.TempDir()
	for _, args := range [][]string{
		{"init", "-q"},
		{"config", "user.email", "evidence-test@example.invalid"},
		{"config", "user.name", "Evidence Test"},
	} {
		if err := runGit(root, args...); err != nil {
			t.Fatalf("git %v: %v", args, err)
		}
	}
	return root
}

func writeEvidenceTestFile(t *testing.T, root, path, body string) {
	t.Helper()
	fullPath := filepath.Join(root, filepath.FromSlash(path))
	if err := os.MkdirAll(filepath.Dir(fullPath), 0o755); err != nil {
		t.Fatalf("mkdir %s: %v", path, err)
	}
	if err := os.WriteFile(fullPath, []byte(body), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}

func commitEvidenceTest(t *testing.T, root, message string) {
	t.Helper()
	if err := runGit(root, "add", "."); err != nil {
		t.Fatalf("git add: %v", err)
	}
	if err := runGit(root, "commit", "-q", "-m", message); err != nil {
		t.Fatalf("git commit %s: %v", message, err)
	}
}

func mustGitOutput(t *testing.T, root string, args ...string) string {
	t.Helper()
	value, err := gitOutput(root, args...)
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return value
}
