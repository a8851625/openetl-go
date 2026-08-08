package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/server"
)

func main() {
	manifestPath := flag.String("manifest", "internal/etl/server/evidence/connector-evidence.json", "connector evidence manifest path")
	currentCommit := flag.String("commit", "", "current source revision to bind to certified evidence (optional strict check)")
	currentImage := flag.String("image", "", "expected certification image digest/tag (optional strict check)")
	nowValue := flag.String("now", "", "RFC3339 time used for freshness checks (defaults to current time)")
	strict := flag.Bool("strict", false, "fail on unverified or expired records and missing scripts")
	flag.Parse()

	now := time.Now()
	if strings.TrimSpace(*nowValue) != "" {
		parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(*nowValue))
		if err != nil {
			fail("invalid -now: %v", err)
		}
		now = parsed
	}

	manifest, err := server.LoadConnectorEvidenceManifestFile(*manifestPath)
	if err != nil {
		fail("manifest invalid: %v", err)
	}
	repoRoot := findRepoRoot(*manifestPath)
	if *currentCommit != "" {
		if err := checkCommitBinding(repoRoot, manifest.CertifiedCommit, *currentCommit); err != nil {
			fail("commit binding: %v", err)
		}
	}
	if *currentImage != "" && manifest.CertifiedImage != *currentImage {
		fail("certified_image=%q does not match -image=%q", manifest.CertifiedImage, *currentImage)
	}
	for _, record := range manifest.Records {
		if *currentImage != "" && record.Image != *currentImage {
			fail("record %s/%s image=%q does not match -image=%q", record.Kind, record.Type, record.Image, *currentImage)
		}
	}

	issues := 0
	for _, record := range manifest.Records {
		freshness := record.Freshness(now)
		for _, script := range record.Scripts {
			if _, err := os.Stat(filepath.Join(repoRoot, script)); err != nil {
				fmt.Printf("missing script %s/%s: %s (%v)\n", record.Kind, record.Type, script, err)
				issues++
			}
		}
		if freshness.Status != "pass" {
			fmt.Printf("%s/%s: %s (%s)\n", record.Kind, record.Type, freshness.Status, freshness.Explanation)
			if *strict {
				issues++
			}
		}
	}
	if issues > 0 {
		fail("evidence check found %d issue(s)", issues)
	}
	fmt.Printf("connector evidence manifest OK: %d record(s), certified_commit=%s, certified_image=%s\n", len(manifest.Records), manifest.CertifiedCommit, manifest.CertifiedImage)
}

// checkCommitBinding verifies that the build being gated is the certified
// source revision, or a descendant that only updates the evidence manifest
// and its operator-facing documentation. Updating the manifest after a
// certification run necessarily creates a descendant commit; allowing that
// narrow path avoids a self-referential commit hash while still rejecting any
// runtime, script, workflow, or connector change after certification.
func checkCommitBinding(repoRoot, certifiedCommit, currentCommit string) error {
	certifiedCommit = strings.TrimSpace(certifiedCommit)
	currentCommit = strings.TrimSpace(currentCommit)
	if certifiedCommit == "" || currentCommit == "" {
		return fmt.Errorf("both certified and current commits are required")
	}
	certified, err := resolveCommit(repoRoot, certifiedCommit)
	if err != nil {
		return fmt.Errorf("resolve certified commit %q: %w", certifiedCommit, err)
	}
	current, err := resolveCommit(repoRoot, currentCommit)
	if err != nil {
		return fmt.Errorf("resolve current commit %q: %w", currentCommit, err)
	}
	if certified == current {
		return nil
	}
	if err := runGit(repoRoot, "merge-base", "--is-ancestor", certified, current); err != nil {
		return fmt.Errorf("certified commit %s is not an ancestor of current commit %s", certifiedCommit, currentCommit)
	}
	changed, err := gitOutput(repoRoot, "diff", "--name-only", "--no-renames", certified, current, "--")
	if err != nil {
		return fmt.Errorf("inspect changes since certified commit: %w", err)
	}
	for _, path := range nonEmptyLines(changed) {
		if !allowedEvidenceDescendantPath(path) {
			return fmt.Errorf("path %q changed after certified commit; rerun connector certification", path)
		}
	}
	return nil
}

func resolveCommit(repoRoot, value string) (string, error) {
	return gitOutput(repoRoot, "rev-parse", "--verify", value+"^{commit}")
}

func runGit(repoRoot string, args ...string) error {
	cmd := exec.Command("git", append([]string{"-C", repoRoot}, args...)...)
	if output, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("git %s: %w (%s)", strings.Join(args, " "), err, strings.TrimSpace(string(output)))
	}
	return nil
}

func gitOutput(repoRoot string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", repoRoot}, args...)...)
	output, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("git %s: %w", strings.Join(args, " "), err)
	}
	return strings.TrimSpace(string(output)), nil
}

func nonEmptyLines(value string) []string {
	var lines []string
	for _, line := range strings.Split(value, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

func allowedEvidenceDescendantPath(path string) bool {
	switch filepath.ToSlash(strings.TrimSpace(path)) {
	case "internal/etl/server/evidence/connector-evidence.json",
		"docs/ROADMAP.zh.md",
		"docs/connector-certification.md":
		return true
	default:
		return false
	}
}

func findRepoRoot(manifestPath string) string {
	path := manifestPath
	if !filepath.IsAbs(path) {
		path, _ = filepath.Abs(path)
	}
	// Walk upward until go.mod is found so custom manifest paths still resolve
	// script references relative to the repository root.
	for dir := filepath.Dir(path); dir != filepath.Dir(dir); dir = filepath.Dir(dir) {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
	}
	return "."
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "check-connector-evidence: "+format+"\n", args...)
	os.Exit(1)
}
