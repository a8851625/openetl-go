package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/a8851625/openetl-go/internal/etl/server"
)

func main() {
	manifestPath := flag.String("manifest", "internal/etl/server/evidence/connector-evidence.json", "connector evidence manifest path")
	currentCommit := flag.String("commit", "", "expected certification commit (optional strict check)")
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
	if *currentCommit != "" && manifest.CertifiedCommit != *currentCommit {
		fail("certified_commit=%q does not match -commit=%q", manifest.CertifiedCommit, *currentCommit)
	}
	if *currentImage != "" && manifest.CertifiedImage != *currentImage {
		fail("certified_image=%q does not match -image=%q", manifest.CertifiedImage, *currentImage)
	}
	for _, record := range manifest.Records {
		if *currentCommit != "" && record.Commit != *currentCommit {
			fail("record %s/%s commit=%q does not match -commit=%q", record.Kind, record.Type, record.Commit, *currentCommit)
		}
		if *currentImage != "" && record.Image != *currentImage {
			fail("record %s/%s image=%q does not match -image=%q", record.Kind, record.Type, record.Image, *currentImage)
		}
	}

	repoRoot := findRepoRoot(*manifestPath)
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
