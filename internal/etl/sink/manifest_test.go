package sink

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/checkpoint"
	"github.com/a8851625/openetl-go/internal/etl/core"
)

func manifestTestRecords(values ...int) []core.Record {
	records := make([]core.Record, 0, len(values))
	for _, v := range values {
		records = append(records, core.Record{
			Operation: core.OpInsert,
			Data:      map[string]any{"id": v},
		})
	}
	return records
}

func newFileSinkWithManifest(t *testing.T, manifest bool) (*FileSink, string) {
	t.Helper()
	dir := t.TempDir()
	cfg := map[string]any{"output_dir": dir, "format": "jsonl"}
	if manifest {
		cfg["manifest"] = true
	}
	s, err := NewFileSink(cfg)
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	if err := s.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s, dir
}

func TestFileSinkManifestCommitsBatch(t *testing.T) {
	s, dir := newFileSinkWithManifest(t, true)
	if err := s.Write(context.Background(), manifestTestRecords(1, 2, 3)); err != nil {
		t.Fatalf("Write: %v", err)
	}

	manifests, err := ReadLocalManifests(dir, "")
	if err != nil {
		t.Fatalf("ReadLocalManifests: %v", err)
	}
	if len(manifests) != 1 {
		t.Fatalf("manifests = %d, want 1", len(manifests))
	}
	m := manifests[0]
	if m.Version != manifestVersion || m.Sink != "file_sink" {
		t.Fatalf("manifest header = %+v", m)
	}
	if m.Rows != 3 || len(m.Objects) != 1 {
		t.Fatalf("manifest = %+v", m)
	}
	obj := m.Objects[0]
	if obj.Rows != 3 || obj.Bytes == 0 || obj.SHA256 == "" {
		t.Fatalf("manifest object = %+v", obj)
	}
	// The referenced data object must actually exist under the output dir.
	if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(obj.Key))); err != nil {
		t.Fatalf("data object missing: %v", err)
	}
	// The manifest itself must record its own key when read back.
	if !strings.Contains(m.ManifestKey, defaultManifestPrefix) {
		t.Fatalf("manifest key = %q", m.ManifestKey)
	}

	committed := CommittedObjectKeys(manifests)
	if _, ok := committed[obj.Key]; !ok {
		t.Fatalf("committed set %v missing %s", committed, obj.Key)
	}

	// The checkpoint contract must carry the manifest so the persisted boundary
	// and the readable boundary are the same thing.
	meta, err := checkpoint.BuildSinkCommitMetadata(context.Background(), s, manifestTestRecords(1, 2, 3), "")
	if err != nil {
		t.Fatalf("BuildSinkCommitMetadata: %v", err)
	}
	native, ok := meta["native"].(map[string]any)
	if !ok {
		t.Fatalf("native metadata missing: %+v", meta)
	}
	if native["manifest_key"] != m.ManifestKey {
		t.Fatalf("native manifest_key = %v, want %s", native["manifest_key"], m.ManifestKey)
	}
	if native["commit_semantics"] != "manifest_listed_objects_are_committed" {
		t.Fatalf("native commit semantics = %v", native["commit_semantics"])
	}
}

func TestFileSinkManifestIsIdempotentPerBatch(t *testing.T) {
	s, dir := newFileSinkWithManifest(t, true)
	batch := manifestTestRecords(1, 2, 3)
	for i := 0; i < 2; i++ {
		if err := s.Write(context.Background(), batch); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	manifests, err := ReadLocalManifests(dir, "")
	if err != nil {
		t.Fatalf("ReadLocalManifests: %v", err)
	}
	if len(manifests) != 1 {
		t.Fatalf("replaying the same batch produced %d manifests, want 1 (content-addressed commit)", len(manifests))
	}
	// A different batch must add exactly one more commit.
	if err := s.Write(context.Background(), manifestTestRecords(4, 5)); err != nil {
		t.Fatalf("Write new batch: %v", err)
	}
	manifests, err = ReadLocalManifests(dir, "")
	if err != nil {
		t.Fatalf("ReadLocalManifests: %v", err)
	}
	if len(manifests) != 2 {
		t.Fatalf("manifests = %d, want 2", len(manifests))
	}
	committed := CommittedObjectKeys(manifests)
	if len(committed) != 2 {
		t.Fatalf("committed objects = %d, want 2", len(committed))
	}
}

func TestFileSinkWithoutManifestKeepsLegacyLayout(t *testing.T) {
	s, dir := newFileSinkWithManifest(t, false)
	if err := s.Write(context.Background(), manifestTestRecords(1)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	manifests, err := ReadLocalManifests(dir, "")
	if err != nil {
		t.Fatalf("ReadLocalManifests: %v", err)
	}
	if len(manifests) != 0 {
		t.Fatalf("manifest mode is off but %d manifests appeared", len(manifests))
	}
	if _, err := os.Stat(filepath.Join(dir, "_manifests")); !os.IsNotExist(err) {
		t.Fatalf("manifest dir must not be created when disabled: %v", err)
	}
	meta, err := checkpoint.BuildSinkCommitMetadata(context.Background(), s, manifestTestRecords(1), "")
	if err != nil {
		t.Fatalf("BuildSinkCommitMetadata: %v", err)
	}
	if _, ok := meta["native"]; ok {
		t.Fatalf("manifest-disabled sink must not emit native commit metadata: %+v", meta)
	}
	// The data object still lands in the legacy layout (top-level, flat prefix).
	var found bool
	_ = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() && strings.HasSuffix(path, ".jsonl") {
			found = true
		}
		return nil
	})
	if !found {
		t.Fatal("data object not written")
	}
}

func TestFileSinkManifestMarksCrashWindowObjectUncommitted(t *testing.T) {
	s, dir := newFileSinkWithManifest(t, true)
	if err := s.Write(context.Background(), manifestTestRecords(1)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	// Simulate the crash window: an object written without its manifest.
	orphan := filepath.Join(dir, "2026", "01", "01", "orphan.jsonl")
	if err := os.MkdirAll(filepath.Dir(orphan), 0755); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if err := os.WriteFile(orphan, []byte(`{"id":99}`+"\n"), 0644); err != nil {
		t.Fatalf("write orphan: %v", err)
	}

	manifests, err := ReadLocalManifests(dir, "")
	if err != nil {
		t.Fatalf("ReadLocalManifests: %v", err)
	}
	committed := CommittedObjectKeys(manifests)
	if _, ok := committed["2026/01/01/orphan.jsonl"]; ok {
		t.Fatal("orphan object must not be reported as committed")
	}
	if len(committed) != 1 {
		t.Fatalf("committed objects = %v", committed)
	}
}

func TestFileSinkManifestPrefixOverride(t *testing.T) {
	dir := t.TempDir()
	s, err := NewFileSink(map[string]any{"output_dir": dir, "format": "jsonl", "manifest": true, "manifest_prefix": "commits"})
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	if err := s.Open(context.Background()); err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer s.Close()
	if err := s.Write(context.Background(), manifestTestRecords(7)); err != nil {
		t.Fatalf("Write: %v", err)
	}
	manifests, err := ReadLocalManifests(dir, "commits")
	if err != nil {
		t.Fatalf("ReadLocalManifests: %v", err)
	}
	if len(manifests) != 1 {
		t.Fatalf("manifests = %d, want 1", len(manifests))
	}
	if !strings.HasPrefix(manifests[0].ManifestKey, "commits/") {
		t.Fatalf("manifest key = %q, want commits/ prefix", manifests[0].ManifestKey)
	}
	raw, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(manifests[0].ManifestKey)))
	if err != nil {
		t.Fatalf("read manifest: %v", err)
	}
	var decoded batchManifest
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("manifest is not valid JSON: %v", err)
	}
	if decoded.ContentAddress != manifests[0].ContentAddress {
		t.Fatalf("content address mismatch")
	}
}

func TestParseManifestConfig(t *testing.T) {
	cases := []struct {
		name       string
		config     map[string]any
		wantOn     bool
		wantPrefix string
	}{
		{name: "default off", config: map[string]any{}, wantOn: false, wantPrefix: "_manifests/"},
		{name: "bool on", config: map[string]any{"manifest": true}, wantOn: true, wantPrefix: "_manifests/"},
		{name: "string on", config: map[string]any{"manifest": "TRUE"}, wantOn: true, wantPrefix: "_manifests/"},
		{name: "prefix override", config: map[string]any{"manifest": true, "manifest_prefix": "m/"}, wantOn: true, wantPrefix: "m/"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			on, prefix := parseManifestConfig(tc.config)
			if on != tc.wantOn || prefix != tc.wantPrefix {
				t.Fatalf("parseManifestConfig = (%v,%q), want (%v,%q)", on, prefix, tc.wantOn, tc.wantPrefix)
			}
		})
	}
}
