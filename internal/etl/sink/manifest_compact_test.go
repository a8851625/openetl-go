package sink

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

func newCompactSink(t *testing.T) *FileSink {
	t.Helper()
	s, err := NewFileSink(map[string]any{
		"output_dir": t.TempDir(),
		"format":     "jsonl",
		"prefix":     "out_",
		"manifest":   true,
	})
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	return s
}

func TestFileSinkCompactionMergesCommittedObjects(t *testing.T) {
	s := newCompactSink(t)
	ctx := context.Background()
	// Five batches on the same day produce five small objects and five manifests.
	for i := 0; i < 5; i++ {
		rec := core.Record{Operation: core.OpInsert, Data: map[string]any{"id": i}}
		if err := s.Write(ctx, []core.Record{rec}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	before, err := readLocalManifestsFrom(s.config.OutputDir, manifestPrefixPath(s.config.Prefix, s.config.ManifestPrefix))
	if err != nil {
		t.Fatalf("ReadLocalManifests: %v", err)
	}
	committedBefore := CommittedObjectKeys(before)
	if len(committedBefore) != 5 {
		t.Fatalf("committed objects = %d, want 5", len(committedBefore))
	}

	result, err := s.CompactOutput(ctx, CompactOptions{MinObjects: 4})
	if err != nil {
		t.Fatalf("CompactOutput: %v", err)
	}
	if len(result.Groups) != 1 {
		t.Fatalf("groups = %d, want 1: %+v", len(result.Groups), result)
	}
	group := result.Groups[0]
	if len(group.ReplacedKeys) != 5 {
		t.Fatalf("replaced keys = %d, want 5", len(group.ReplacedKeys))
	}
	if group.Rows != 5 {
		t.Fatalf("rows = %d, want 5", group.Rows)
	}
	if _, err := os.Stat(filepath.Join(s.config.OutputDir, filepath.FromSlash(group.MergedKey))); err != nil {
		t.Fatalf("merged object missing: %v", err)
	}

	after, err := readLocalManifestsFrom(s.config.OutputDir, manifestPrefixPath(s.config.Prefix, s.config.ManifestPrefix))
	if err != nil {
		t.Fatalf("ReadLocalManifests after compaction: %v", err)
	}
	committedAfter := CommittedObjectKeys(after)
	if len(committedAfter) != 1 {
		t.Fatalf("committed objects after compaction = %d, want 1: %v", len(committedAfter), committedAfter)
	}
	if _, ok := committedAfter[group.MergedKey]; !ok {
		t.Fatalf("merged object is not the committed view: %v", committedAfter)
	}
	// The replaced objects still exist on disk (reversible), they are just no
	// longer part of the committed view.
	for _, key := range group.ReplacedKeys {
		if _, err := os.Stat(filepath.Join(s.config.OutputDir, filepath.FromSlash(key))); err != nil {
			t.Fatalf("compaction must not delete %s: %v", key, err)
		}
	}
	// The merged object holds every row.
	payload, err := os.ReadFile(filepath.Join(s.config.OutputDir, filepath.FromSlash(group.MergedKey)))
	if err != nil {
		t.Fatalf("read merged object: %v", err)
	}
	if got := strings.Count(string(payload), "\n"); got != 5 {
		t.Fatalf("merged rows = %d, want 5:\n%s", got, payload)
	}
}

func TestFileSinkCompactionDryRunWritesNothing(t *testing.T) {
	s := newCompactSink(t)
	ctx := context.Background()
	for i := 0; i < 4; i++ {
		if err := s.Write(ctx, []core.Record{core.Record{Operation: core.OpInsert, Data: map[string]any{"id": i}}}); err != nil {
			t.Fatalf("Write %d: %v", i, err)
		}
	}
	before, _ := readLocalManifestsFrom(s.config.OutputDir, manifestPrefixPath(s.config.Prefix, s.config.ManifestPrefix))
	result, err := s.CompactOutput(ctx, CompactOptions{MinObjects: 2, DryRun: true})
	if err != nil {
		t.Fatalf("CompactOutput dry run: %v", err)
	}
	if !result.DryRun || len(result.Groups) != 1 {
		t.Fatalf("dry run result = %+v", result)
	}
	after, _ := readLocalManifestsFrom(s.config.OutputDir, manifestPrefixPath(s.config.Prefix, s.config.ManifestPrefix))
	if len(after) != len(before) {
		t.Fatalf("dry run wrote %d manifests, want %d", len(after), len(before))
	}
	if len(CommittedObjectKeys(after)) != 4 {
		t.Fatalf("dry run changed the committed view")
	}
}

func TestFileSinkCompactionRequiresManifest(t *testing.T) {
	s, err := NewFileSink(map[string]any{"output_dir": t.TempDir(), "format": "jsonl"})
	if err != nil {
		t.Fatalf("NewFileSink: %v", err)
	}
	if _, err := s.CompactOutput(context.Background(), CompactOptions{}); err == nil ||
		!strings.Contains(err.Error(), "manifest: true") {
		t.Fatalf("err = %v, want a manifest requirement", err)
	}
}

func TestFileSinkCrossDayReplaySkipsSecondCommit(t *testing.T) {
	s := newCompactSink(t)
	ctx := context.Background()
	rec := core.Record{Operation: core.OpInsert, Data: map[string]any{"id": 1}}
	if err := s.Write(ctx, []core.Record{rec}); err != nil {
		t.Fatalf("Write: %v", err)
	}
	first, _ := readLocalManifestsFrom(s.config.OutputDir, manifestPrefixPath(s.config.Prefix, s.config.ManifestPrefix))
	if len(first) != 1 {
		t.Fatalf("manifests after first write = %d, want 1", len(first))
	}
	// Simulate the cross-day case: the same batch content is committed again
	// under a different day partition.
	s.mu.Lock()
	dayPrefix := "2030/01/01/"
	s.mu.Unlock()
	payload, err := s.encode([]core.Record{rec})
	if err != nil {
		t.Fatalf("encode: %v", err)
	}
	key := s.config.Prefix + dayPrefix + "deadbeefdeadbeef.jsonl"
	if err := s.writeObject(ctx, key, payload); err != nil {
		t.Fatalf("writeObject: %v", err)
	}
	if err := s.commitBatchManifest(ctx, key, payload, 1); err != nil {
		t.Fatalf("commitBatchManifest: %v", err)
	}
	second, _ := readLocalManifestsFrom(s.config.OutputDir, manifestPrefixPath(s.config.Prefix, s.config.ManifestPrefix))
	if len(second) != 1 {
		t.Fatalf("manifests after cross-day replay = %d, want 1 (no second commit entry)", len(second))
	}
	committed := CommittedObjectKeys(second)
	if _, ok := committed[key]; ok {
		t.Fatalf("cross-day duplicate must stay uncommitted: %v", committed)
	}
	meta, err := s.SinkCommitMetadata(ctx)
	if err != nil {
		t.Fatalf("SinkCommitMetadata: %v", err)
	}
	if skipped, _ := meta["replay_skipped"].(bool); !skipped {
		t.Fatalf("commit metadata must report the replay skip: %v", meta)
	}
	// The checkpoint must still name a real commit record.
	if key, _ := meta["manifest_key"].(string); key == "" {
		t.Fatalf("a skipped replay must reuse the existing manifest key: %v", meta)
	}
}
