package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/e2e/harness"
	"github.com/a8851625/openetl-go/internal/etl/server"
)

func main() {
	manifestPath := flag.String("manifest", "internal/etl/server/evidence/connector-evidence.json", "connector evidence manifest path")
	pathEvidenceDir := flag.String("path-evidence", "docs/evidence", "per-path evidence directory (relative to repo root)")
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

	// Path evidence (IT-1/T1.5): committed per-path run artifacts must be
	// bound to a reachable commit, fresh against related sources, and report
	// a fully-passing, non-empty check list. Tampering with them without
	// re-running the path test either breaks the commit resolution, the
	// ancestor binding, the freshness check, or the check/result consistency.
	current := *currentCommit
	if current == "" {
		if head, err := gitOutput(repoRoot, "rev-parse", "HEAD"); err == nil {
			current = head
		}
	}
	if err := checkPathEvidenceDir(repoRoot, current, *pathEvidenceDir, *strict); err != nil {
		fail("%v", err)
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
		if err := checkRecordProvenance(repoRoot, manifest, record); err != nil {
			fmt.Printf("provenance %s/%s: %v\n", record.Kind, record.Type, err)
			issues++
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

// checkPathEvidenceDir validates every committed path evidence JSON under
// dir (IT-1/T1.5 acceptance 2). current may be empty when no revision is
// bound (structural-only mode); commit binding then uses HEAD.
func checkPathEvidenceDir(repoRoot, current, dir string, required bool) error {
	resolved := filepath.FromSlash(dir)
	if !filepath.IsAbs(resolved) {
		resolved = filepath.Join(repoRoot, resolved)
	}
	pattern := filepath.Join(resolved, "*.json")
	files, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("path evidence glob %s: %w", pattern, err)
	}
	if len(files) == 0 {
		if required {
			return fmt.Errorf("required path evidence: no files under %s", dir)
		}
		fmt.Printf("path evidence: no files under %s\n", dir)
		return nil
	}
	for _, file := range files {
		rel, _ := filepath.Rel(repoRoot, file)
		if err := checkPathEvidenceFile(repoRoot, current, file); err != nil {
			return fmt.Errorf("path evidence %s: %w", rel, err)
		}
	}
	fmt.Printf("path evidence OK: %d file(s) under %s, bound to %s\n", len(files), dir, current)
	return nil
}

// checkPathEvidenceFile validates one path evidence artifact: the commit
// field must resolve and be an ancestor (or equal) of the current revision;
// no related source change may be newer than the evidence commit; the check
// list must be non-empty and fully passed; the total result must be passed.
// Any of these failing means the artifact was tampered with or the path was
// not re-run after the last related change.
func checkPathEvidenceFile(repoRoot, current, path string) error {
	ev, err := harness.LoadEvidence(path)
	if err != nil {
		return fmt.Errorf("parse: %w", err)
	}
	base := filepath.Base(path)
	base = strings.TrimSuffix(base, filepath.Ext(base))
	if ev.PathID != base {
		return fmt.Errorf("path_id %q does not match file name %q", ev.PathID, base)
	}
	if strings.TrimSpace(ev.Commit) == "" || ev.Commit == "unknown" {
		return fmt.Errorf("evidence commit %q not bound to a source revision; rerun the path test", ev.Commit)
	}
	if current == "" {
		return fmt.Errorf("cannot verify commit binding: no current revision (git HEAD unavailable)")
	}
	evidenceCommit, err := resolveCommit(repoRoot, ev.Commit)
	if err != nil {
		return fmt.Errorf("evidence commit %q does not resolve: %w (tampered or rerun needed)", ev.Commit, err)
	}
	if err := runGit(repoRoot, "merge-base", "--is-ancestor", evidenceCommit, current); err != nil {
		return fmt.Errorf("evidence commit %s is not an ancestor of %s; rerun the path test to regenerate", evidenceCommit, current)
	}
	newest, err := newestRelatedSourceCommit(repoRoot, ev.PathID)
	if err != nil {
		return fmt.Errorf("find newest related source commit: %w", err)
	}
	if newest != "" {
		if err := runGit(repoRoot, "merge-base", "--is-ancestor", newest, evidenceCommit); err != nil {
			return fmt.Errorf("stale: related sources changed at %s after evidence commit %s; rerun the path test", newest, evidenceCommit)
		}
	}
	if len(ev.Checks) == 0 {
		return fmt.Errorf("no checks recorded (a run that certified nothing must not claim pass)")
	}
	for _, check := range ev.Checks {
		if check.Result != "passed" {
			return fmt.Errorf("check %q result %s (reason: %s); evidence not certified", check.Name, check.Result, check.Reason)
		}
	}
	if ev.Result != "passed" {
		return fmt.Errorf("total result %q but all checks passed (inconsistent artifact)", ev.Result)
	}
	return nil
}

// newestRelatedSourceCommit returns the newest commit that touched the
// source surface a path depends on, or "" when the repository has no such
// commit yet.
func newestRelatedSourceCommit(repoRoot, pathID string) (string, error) {
	args := []string{"log", "-1", "--format=%H", "--"}
	args = append(args, relatedSourceDirs(pathID)...)
	out, err := gitOutput(repoRoot, args...)
	if err != nil {
		// A pathspec matching nothing (e.g. repo without that dir yet) is
		// treated as "no related history": freshness is vacuously satisfied.
		return "", nil
	}
	return strings.TrimSpace(out), nil
}

// relatedSourceDirs lists the source surface whose changes invalidate path
// evidence: any change there requires re-running the path test.
func relatedSourceDirs(pathID string) []string {
	dirs := []string{
		"internal/etl/source",
		"internal/etl/sink",
		"internal/etl/core",
		"internal/etl/transform",
		"internal/etl/checkpoint",
		"internal/etl/pipeline",
		"internal/etl/server",
		"internal/etl/storage",
		"internal/etl/orchestrator",
		"internal/consts",
		"internal/etl/e2e",
		"internal/logic",
		"manifest/config",
		"docs/etl-config-schema.md",
		"docs/etl-idempotency.md",
	}
	switch pathID {
	case "mysql_snap_cdc__ch_rmt":
		dirs = append(dirs, "internal/etl/ddl")
	}
	// internal/etl/server/evidence holds the certification manifest itself.
	// Rebinding the manifest necessarily touches that path, so counting it as
	// a related source made every rebind immediately invalidate all path
	// evidence, forcing an endless rebind/rerun loop at release time.
	dirs = append(dirs, ":(exclude)internal/etl/server/evidence")
	return dirs
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

// checkRecordProvenance verifies that a record's claimed certification window is
// actually consistent with the revision history it points at.
//
// The 2026-11-09 audit found the manifest claiming "all 16 scripts re-run green
// on the local build of commit fe8692d" for a window ending 2026-11-03, while
// two of those scripts (hack/e2e-doris-source-order.sh, hack/e2e-postgres-cdc.sh)
// only reached their current content on 2026-11-07 and one of them did not exist
// at fe8692d at all. checkCommitBinding cannot catch that: it only asks whether
// the certified commit is an ancestor of HEAD. Provenance needs a second,
// independent question — "could the scripts that this record names have been
// the ones that ran inside the window this record claims?" — and that is what
// this function answers.
func checkRecordProvenance(repoRoot string, manifest server.ConnectorEvidenceManifest, record server.ConnectorEvidenceRecord) error {
	recordCommit, err := resolveCommit(repoRoot, record.Commit)
	if err != nil {
		return fmt.Errorf("record commit %q does not resolve: %w", record.Commit, err)
	}
	// The record's own commit must be reachable from the manifest's certified
	// commit; otherwise the record describes a different revision than the
	// manifest claims to certify.
	certifiedCommit, err := resolveCommit(repoRoot, manifest.CertifiedCommit)
	if err != nil {
		return fmt.Errorf("certified commit %q does not resolve: %w", manifest.CertifiedCommit, err)
	}
	if recordCommit != certifiedCommit {
		if err := runGit(repoRoot, "merge-base", "--is-ancestor", recordCommit, certifiedCommit); err != nil {
			return fmt.Errorf("record commit %s is not an ancestor of certified commit %s", record.Commit, manifest.CertifiedCommit)
		}
	}

	// Every script the record names must have existed at the record's commit.
	// A script that first appeared later proves the record cannot describe a run
	// on that commit, regardless of what its note says.
	for _, script := range record.Scripts {
		if err := runGit(repoRoot, "cat-file", "-e", recordCommit+":"+script); err != nil {
			return fmt.Errorf("script %s did not exist at record commit %s", script, shortSHA(record.Commit))
		}
	}

	// No script may have changed after the window it is claimed to have run in.
	// Comparing against finished_at (not started_at) is deliberate: a script
	// edited during the run would still be a different script than the one that
	// produced the recorded cases.
	finished, err := time.Parse(time.RFC3339, record.FinishedAt)
	if err != nil {
		return fmt.Errorf("finished_at %q is not RFC3339: %w", record.FinishedAt, err)
	}
	for _, script := range record.Scripts {
		committedAt, err := newestCommitTimeForPath(repoRoot, script, recordCommit)
		if err != nil {
			return fmt.Errorf("script %s history: %w", script, err)
		}
		if committedAt.IsZero() {
			continue
		}
		if committedAt.After(finished) {
			return fmt.Errorf("script %s was last changed at %s, after the claimed certification window ended at %s",
				script, committedAt.UTC().Format(time.RFC3339), finished.UTC().Format(time.RFC3339))
		}
	}
	return nil
}

// newestCommitTimeForPath returns the committer time of the newest commit that
// touched path at or before the given revision. Returns the zero time when the
// path has no history reachable from that revision.
func newestCommitTimeForPath(repoRoot, path, revision string) (time.Time, error) {
	out, err := gitOutput(repoRoot, "log", "-1", "--format=%cI", revision, "--", path)
	if err != nil {
		return time.Time{}, err
	}
	out = strings.TrimSpace(out)
	if out == "" {
		return time.Time{}, nil
	}
	parsed, err := time.Parse(time.RFC3339, out)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse commit time %q: %w", out, err)
	}
	return parsed, nil
}

func shortSHA(value string) string {
	value = strings.TrimSpace(value)
	if len(value) > 8 {
		return value[:8]
	}
	return value
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
		"docs/connector-certification.md",
		// Path evidence regeneration (IT-1/T1.5) and release-cut bookkeeping
		// are allowed after a certification run; no runtime, script, workflow,
		// or connector code is allowed through this path.
		"docs/evidence/mysql_cdc__mysql_upsert.json",
		"docs/evidence/mysql_snap_cdc__ch_rmt.json",
		// IT-5 path evidence (CH-C1/C3/C2): same regeneration pattern as the
		// two forced primary paths; committed after a certification run when
		// the manifest rebind lands.
		"docs/evidence/mysql_snap_cdc__ch_rmt_composite_pk.json",
		"docs/evidence/ch_dedup_crash_window.json",
		"docs/evidence/ch_dedup_crash_window_multitable.json",
		"docs/evidence/kafka_envelope_roundtrip.json",
		"docs/evidence/schema_contract_enforcement.json",
		"CHANGELOG.md",
		"CHANGELOG.zh.md",
		"docs/iterations/P1-technical-closure-20261126/ga-evidence.md",
		"docs/iterations/P1-technical-closure-20261126/tasks.md",
		"docs/iterations/P1-technical-closure-20261126/plan.md",
		"docs/ga-assessment-2026-09-16.md":
		return true
	default:
		clean := filepath.ToSlash(path)
		if strings.HasPrefix(clean, "docs/iterations/P1-technical-closure-20261126/evidence/ga-") {
			switch filepath.Ext(clean) {
			case ".json", ".txt", ".png", ".gz":
				return true
			}
		}
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
