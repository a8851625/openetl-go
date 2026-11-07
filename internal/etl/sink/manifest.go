package sink

// Committed manifests for the file / object-store sinks.
//
// Before manifests, s3 and file_sink wrote one content-addressed object per
// batch and stopped there. A crash between the object write and the checkpoint
// save therefore left an object that nobody could classify: it might be a
// committed batch that simply lost its checkpoint, or a half-finished attempt.
// Downstream Hive/Spark jobs that glob the prefix cannot tell the difference and
// will happily read uncommitted data.
//
// With `manifest: true` the sink performs a two-step commit:
//
//  1. write the data object exactly as before (content-addressed, so replaying
//     the same batch overwrites instead of duplicating);
//  2. write a manifest object that lists the data object, its sha256, its row
//     count and the batch sequence.
//
// Readers treat "listed in a manifest" as the commit boundary. An object that
// no manifest references is uncommitted and safe to delete. The manifest name is
// derived from its content, so a retried batch rewrites the same manifest
// instead of adding a second commit marker.
//
// The sink also exposes the manifest key through core.SinkCommitMetadataProvider
// so the checkpoint records which manifest the batch was acknowledged under.

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	minio "github.com/minio/minio-go/v7"
)

const (
	manifestVersion       = 1
	defaultManifestPrefix = "_manifests/"
	// manifestFileExtension keeps manifests distinguishable from data objects:
	// a reader globbing *.json for data must not ingest commit markers.
	manifestFileExtension  = ".manifest.json"
	manifestContentAddress = 16 // hex chars kept from the manifest digest
)

// manifestObject is one data object referenced by a committed manifest.
type manifestObject struct {
	Key    string `json:"key"`
	SHA256 string `json:"sha256"`
	Bytes  int    `json:"bytes"`
	Rows   int    `json:"rows"`
	Format string `json:"format"`
}

// Manifest kinds. A batch manifest commits the objects one Write produced; a
// compaction manifest commits a rewritten object and retires older ones, which
// is how the output layout gets small-file compaction without deleting data.
const (
	manifestKindBatch      = "batch"
	manifestKindCompaction = "compaction"
)

// batchManifest is the commit marker for one sink batch.
type batchManifest struct {
	Version        int              `json:"version"`
	Sink           string           `json:"sink"`
	CreatedAt      time.Time        `json:"created_at"`
	BatchSeq       int64            `json:"batch_seq"`
	Rows           int              `json:"rows"`
	Format         string           `json:"format"`
	Objects        []manifestObject `json:"objects"`
	ContentAddress string           `json:"content_address"`
	// Kind is "batch" (default for older manifests) or "compaction".
	Kind string `json:"kind,omitempty"`
	// BatchID is the full content digest of the batch. It is stable across days
	// and across replays, which is what makes cross-day replay detection
	// possible: a batch whose BatchID is already committed is never committed a
	// second time.
	BatchID string `json:"batch_id,omitempty"`
	// Replaces lists object keys this manifest retires (compaction only). A
	// reader must exclude them from the committed view.
	Replaces []string `json:"replaces,omitempty"`
	// SourceBatchIDs records which committed batches a compaction manifest
	// folded together, so provenance survives the rewrite.
	SourceBatchIDs []string `json:"source_batch_ids,omitempty"`
	// ManifestKey is filled in when the manifest is read back so a consumer can
	// point at the exact commit record it used.
	ManifestKey string `json:"manifest_key,omitempty"`
}

// manifestContentKey derives the deterministic manifest identity from the batch
// content: the same set of objects at the same row counts always produces the
// same manifest, so replay is idempotent.
func manifestContentKey(objects []manifestObject) string {
	keys := make([]string, 0, len(objects))
	for _, obj := range objects {
		keys = append(keys, fmt.Sprintf("%s|%s|%d|%d|%s", obj.Key, obj.SHA256, obj.Bytes, obj.Rows, obj.Format))
	}
	sort.Strings(keys)
	sum := sha256.Sum256([]byte(strings.Join(keys, "\n")))
	return hex.EncodeToString(sum[:])
}

// manifestObjectKey renders the manifest object path for a content address:
// <sink prefix><manifest prefix><date>/<hash>.manifest.json
func manifestObjectKey(prefix, manifestPrefix, datePrefix, contentAddress string) string {
	if manifestPrefix == "" {
		manifestPrefix = defaultManifestPrefix
	}
	if !strings.HasSuffix(manifestPrefix, "/") {
		manifestPrefix += "/"
	}
	short := contentAddress
	if len(short) > manifestContentAddress {
		short = short[:manifestContentAddress]
	}
	return fmt.Sprintf("%s%s%s%s%s", prefix, manifestPrefix, datePrefix, short, manifestFileExtension)
}

// commitManifest writes the commit marker for one batch and records it as the
// last acknowledged commit. Callers must hold s.mu.
func (s *FileSink) commitManifest(ctx context.Context, objects []manifestObject) error {
	if len(objects) == 0 {
		return nil
	}
	contentAddress := manifestContentKey(objects)
	batchID := manifestBatchID(objects)
	// Cross-day replay guard: the same batch content is committed exactly once.
	// A replay on a different day writes its data object under that day's prefix
	// (harmless: no manifest references it, so it stays uncommitted) and skips
	// the second commit entry, which is what used to double-count the batch.
	committed, err := s.committedBatchIDs(ctx)
	if err != nil {
		return err
	}
	if existingKey, alreadyCommitted := committed[batchID]; alreadyCommitted {
		s.replaySkips++
		s.lastCommit = map[string]any{
			// The checkpoint must name a real commit record, so a skipped replay
			// reuses the manifest that already commits this content.
			"manifest_key":     existingKey,
			"batch_id":         batchID,
			"batch_seq":        s.batchSeq,
			"rows":             manifestRows(objects),
			"objects":          len(objects),
			"committed_at":     time.Now().UTC().Format(time.RFC3339Nano),
			"replay_skipped":   true,
			"commit_semantics": "batch_already_committed_content_identical",
		}
		return nil
	}
	s.batchSeq++
	datePrefix := time.Now().UTC().Format("2006/01/02/")
	key := manifestObjectKey(s.config.Prefix, s.config.ManifestPrefix, datePrefix, contentAddress)
	rows := 0
	for _, obj := range objects {
		rows += obj.Rows
	}
	manifest := batchManifest{
		Version:        manifestVersion,
		Sink:           s.name,
		CreatedAt:      time.Now().UTC(),
		BatchSeq:       s.batchSeq,
		Rows:           rows,
		Format:         s.config.Format,
		Objects:        objects,
		ContentAddress: contentAddress,
		Kind:           manifestKindBatch,
		BatchID:        batchID,
	}
	payload, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal sink manifest: %w", err)
	}

	if s.client != nil {
		// The manifest is the commit marker: it must land after the data object.
		if err := s.uploadWithRetry(ctx, key, payload); err != nil {
			return fmt.Errorf("write sink manifest %s: %w", key, err)
		}
	} else {
		filename := filepath.Join(s.config.OutputDir, filepath.FromSlash(key))
		if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
			return fmt.Errorf("create manifest dir: %w", err)
		}
		tmpFilename := filename + ".tmp"
		if err := os.WriteFile(tmpFilename, payload, 0644); err != nil {
			os.Remove(tmpFilename)
			return fmt.Errorf("write sink manifest %s: %w", key, err)
		}
		if err := os.Rename(tmpFilename, filename); err != nil {
			return fmt.Errorf("commit sink manifest %s: %w", key, err)
		}
	}

	s.lastCommit = map[string]any{
		"manifest_key":     key,
		"manifest_version": manifestVersion,
		"content_address":  contentAddress,
		"batch_id":         batchID,
		"batch_seq":        s.batchSeq,
		"rows":             rows,
		"objects":          len(objects),
		"committed_at":     manifest.CreatedAt.Format(time.RFC3339Nano),
		"commit_semantics": "manifest_listed_objects_are_committed",
	}
	s.manifestCount++
	// Keep the replay guard current without re-listing the output location.
	if s.committedIDs == nil {
		s.committedIDs = make(map[string]string, 1)
	}
	s.committedIDs[batchID] = key
	return nil
}

// SinkCommitMetadata implements core.SinkCommitMetadataProvider. It reports the
// manifest the most recent batch was committed under so the checkpoint records
// the same boundary the reader will resolve. With manifests disabled, or before
// the first commit, it returns no native metadata (nil) rather than an error:
// a delete-only interval has nothing to commit, and a failed manifest write
// already fails the Write call that would have advanced the checkpoint.
func (s *FileSink) SinkCommitMetadata(ctx context.Context) (map[string]any, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.config.Manifest || s.lastCommit == nil {
		return nil, nil
	}
	out := make(map[string]any, len(s.lastCommit))
	for k, v := range s.lastCommit {
		out[k] = v
	}
	return out, nil
}

// ReadLocalManifests loads every committed manifest from a file_sink output
// directory. It exists so operators and tests can resolve the commit boundary
// without reimplementing the layout.
func ReadLocalManifests(outputDir, manifestPrefix string) ([]batchManifest, error) {
	return readLocalManifestsFrom(outputDir, manifestPrefixPath("", manifestPrefix))
}

// readLocalManifestsFrom walks a fully-resolved manifest path (sink prefix plus
// manifest prefix) so a sink whose prefix is e.g. "batch_" finds its manifests
// under "batch__manifests/" instead of the bare default.
func readLocalManifestsFrom(outputDir, prefixPath string) ([]batchManifest, error) {
	root := filepath.Join(outputDir, filepath.FromSlash(prefixPath))
	var manifests []batchManifest
	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), manifestFileExtension) {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		var m batchManifest
		if err := json.Unmarshal(raw, &m); err != nil {
			return fmt.Errorf("parse manifest %s: %w", path, err)
		}
		rel, relErr := filepath.Rel(outputDir, path)
		if relErr == nil {
			m.ManifestKey = filepath.ToSlash(rel)
		}
		manifests = append(manifests, m)
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(manifests, func(i, j int) bool { return manifests[i].ManifestKey < manifests[j].ManifestKey })
	return manifests, nil
}

// CommittedObjectKeys returns the set of data object keys a set of manifests
// commits. Anything in the prefix that is not in this set is an uncommitted
// (crash-window) object.
func CommittedObjectKeys(manifests []batchManifest) map[string]manifestObject {
	replaced := replacedObjectKeys(manifests)
	out := make(map[string]manifestObject)
	for _, m := range manifests {
		for _, obj := range m.Objects {
			if replaced[obj.Key] {
				continue
			}
			out[obj.Key] = obj
		}
	}
	return out
}

// replacedObjectKeys returns every key a compaction manifest retired. A key that
// a later batch re-commits wins, so the rule is evaluated over the full manifest
// set with the newest manifest taking precedence.
func replacedObjectKeys(manifests []batchManifest) map[string]bool {
	// Chronological replay of the manifest history: a compaction retires the
	// keys it lists, and a manifest created later that re-commits a key wins.
	ordered := make([]batchManifest, len(manifests))
	copy(ordered, manifests)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].CreatedAt.Before(ordered[j].CreatedAt) })
	active := make(map[string]bool)
	for _, m := range ordered {
		for _, obj := range m.Objects {
			active[obj.Key] = true
		}
		for _, key := range m.Replaces {
			active[key] = false
		}
	}
	out := make(map[string]bool)
	for key, keep := range active {
		if !keep {
			out[key] = true
		}
	}
	return out
}

// CommittedBatchKeys maps every committed batch id to the manifest that commits
// it. A sink consults it before committing a batch again (which is what stops a
// cross-day replay from producing a second commit entry) and reuses the existing
// manifest key so the checkpoint still names a real commit record.
func CommittedBatchKeys(manifests []batchManifest) map[string]string {
	out := make(map[string]string, len(manifests))
	for _, m := range manifests {
		if m.Kind == manifestKindCompaction {
			for _, id := range m.SourceBatchIDs {
				out[id] = m.ManifestKey
			}
			continue
		}
		id := m.BatchID
		if id == "" {
			id = m.ContentAddress
		}
		if id != "" {
			out[id] = m.ManifestKey
		}
	}
	return out
}

// manifestBatchID is the content identity of a batch: it hashes the payload
// digests, row counts and format but deliberately NOT the object keys, because
// the key embeds the date partition. A replay on another day therefore yields
// the same batch id and is recognised instead of being committed twice.
func manifestBatchID(objects []manifestObject) string {
	parts := make([]string, 0, len(objects))
	for _, obj := range objects {
		parts = append(parts, fmt.Sprintf("%s|%d|%s", obj.SHA256, obj.Rows, obj.Format))
	}
	sort.Strings(parts)
	sum := sha256.Sum256([]byte(strings.Join(parts, "\n")))
	return hex.EncodeToString(sum[:])
}

func manifestRows(objects []manifestObject) int {
	rows := 0
	for _, obj := range objects {
		rows += obj.Rows
	}
	return rows
}

// committedBatchIDs returns the batch ids already committed in this output
// location, cached after the first lookup. The cache is invalidated by every
// successful commit, so a long-running sink keeps the guard without re-listing.
func (s *FileSink) committedBatchIDs(ctx context.Context) (map[string]string, error) {
	if s.committedIDs != nil {
		return s.committedIDs, nil
	}
	manifests, err := s.loadManifests(ctx)
	if err != nil {
		return nil, err
	}
	s.committedIDs = CommittedBatchKeys(manifests)
	return s.committedIDs, nil
}

// loadManifests reads every committed manifest from the sink's output location,
// for the local filesystem or the object store.
func (s *FileSink) loadManifests(ctx context.Context) ([]batchManifest, error) {
	if s.client == nil {
		return readLocalManifestsFrom(s.config.OutputDir, manifestPrefixPath(s.config.Prefix, s.config.ManifestPrefix))
	}
	prefix := manifestPrefixPath(s.config.Prefix, s.config.ManifestPrefix)
	objects := s.client.ListObjects(ctx, s.config.Bucket, minio.ListObjectsOptions{Prefix: prefix, Recursive: true})
	var manifests []batchManifest
	for object := range objects {
		if object.Err != nil {
			return nil, object.Err
		}
		if !strings.HasSuffix(object.Key, manifestFileExtension) {
			continue
		}
		raw, err := s.readObject(ctx, object.Key)
		if err != nil {
			return nil, err
		}
		var m batchManifest
		if err := json.Unmarshal(raw, &m); err != nil {
			return nil, fmt.Errorf("parse manifest %s: %w", object.Key, err)
		}
		m.ManifestKey = object.Key
		manifests = append(manifests, m)
	}
	sort.Slice(manifests, func(i, j int) bool { return manifests[i].ManifestKey < manifests[j].ManifestKey })
	return manifests, nil
}

// readObject reads one output object, local or remote.
func (s *FileSink) readObject(ctx context.Context, key string) ([]byte, error) {
	if s.client == nil {
		return os.ReadFile(filepath.Join(s.config.OutputDir, filepath.FromSlash(key)))
	}
	object, err := s.client.GetObject(ctx, s.config.Bucket, key, minio.GetObjectOptions{})
	if err != nil {
		return nil, err
	}
	defer object.Close()
	return io.ReadAll(object)
}

// writeObject writes one output object, local or remote, atomically.
func (s *FileSink) writeObject(ctx context.Context, key string, payload []byte) error {
	if s.client != nil {
		return s.uploadWithRetry(ctx, key, payload)
	}
	filename := filepath.Join(s.config.OutputDir, filepath.FromSlash(key))
	if err := os.MkdirAll(filepath.Dir(filename), 0755); err != nil {
		return fmt.Errorf("create output dir: %w", err)
	}
	tmpFilename := filename + ".tmp"
	if err := os.WriteFile(tmpFilename, payload, 0644); err != nil {
		os.Remove(tmpFilename)
		return err
	}
	return os.Rename(tmpFilename, filename)
}

func manifestPrefixPath(prefix, manifestPrefix string) string {
	if manifestPrefix == "" {
		manifestPrefix = defaultManifestPrefix
	}
	if !strings.HasSuffix(manifestPrefix, "/") {
		manifestPrefix += "/"
	}
	return prefix + manifestPrefix
}

// NewSinkForCompact builds a compaction-capable sink from a pipeline sink
// config. It exists so the server can run a maintenance pass without starting a
// runner (and without duplicating the config parsing the registry already owns).
func NewSinkForCompact(sinkType string, config map[string]any) (*FileSink, error) {
	switch strings.ToLower(strings.TrimSpace(sinkType)) {
	case "s3":
		return NewS3Sink(config)
	case "file_sink":
		return NewFileSink(config)
	default:
		return nil, fmt.Errorf("output compaction supports the s3 and file_sink sinks, got %q", sinkType)
	}
}
