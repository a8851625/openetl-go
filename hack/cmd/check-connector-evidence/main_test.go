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
