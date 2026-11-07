package sink

// Small-file compaction for manifest-enabled file / object-store output.
//
// Manifests gave the output a commit boundary; they did not stop one object per
// batch, which for a CDC pipeline becomes thousands of tiny objects per day. The
// compactor folds the committed objects of one day into a single object and
// publishes a compaction manifest that
//
//  1. commits the merged object, and
//  2. retires the objects it replaced through `replaces`.
//
// Nothing is deleted: the replaced objects stay on disk, so the operation is
// reversible and a reader that resolves the committed view (CommittedObjectKeys)
// sees exactly one copy of the data. That is the bounded "lakehouse transaction"
// this project promises — a metadata rewrite, not an atomic multi-object commit.

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"
)

// CompactOptions bounds one compaction pass.
type CompactOptions struct {
	// MinObjects is the smallest group worth compacting (default 4).
	MinObjects int
	// MaxObjects caps how many objects are merged into one (default 32).
	MaxObjects int
	// MaxObjectBytes skips groups that already hold large objects (default 1 MiB).
	MaxObjectBytes int64
	// DryRun reports what would happen without writing anything.
	DryRun bool
	// Day restricts the pass to one date partition (yyyy/mm/dd); empty = all.
	Day string
}

// CompactGroup is one merged group in a compaction result.
type CompactGroup struct {
	Day            string   `json:"day"`
	MergedKey      string   `json:"merged_key"`
	ManifestKey    string   `json:"manifest_key"`
	ReplacedKeys   []string `json:"replaced_keys"`
	SourceBatchIDs []string `json:"source_batch_ids,omitempty"`
	Rows           int      `json:"rows"`
	BytesBefore    int64    `json:"bytes_before"`
	BytesAfter     int64    `json:"bytes_after"`
}

// CompactResult summarises a pass.
type CompactResult struct {
	DryRun       bool           `json:"dry_run"`
	Groups       []CompactGroup `json:"groups"`
	Skipped      int            `json:"skipped_groups"`
	ObjectsIn    int            `json:"objects_considered"`
	ObjectsOut   int            `json:"objects_after"`
	BytesBefore  int64          `json:"bytes_before"`
	BytesAfter   int64          `json:"bytes_after"`
	CompactedAt  time.Time      `json:"compacted_at"`
	CommittedVia string         `json:"committed_via"`
}

// CompactOutput merges small committed objects per day and rewrites the commit
// metadata so readers keep seeing a single logical copy of every row.
func (s *FileSink) CompactOutput(ctx context.Context, opts CompactOptions) (CompactResult, error) {
	if !s.config.Manifest {
		return CompactResult{}, fmt.Errorf("%s sink compaction requires manifest: true; without a commit boundary the compactor cannot tell committed objects from crash-window leftovers", s.name)
	}
	if opts.MinObjects <= 0 {
		opts.MinObjects = 4
	}
	if opts.MaxObjects <= 0 {
		opts.MaxObjects = 32
	}
	if opts.MaxObjectBytes <= 0 {
		opts.MaxObjectBytes = 1 << 20
	}
	result := CompactResult{DryRun: opts.DryRun, CompactedAt: time.Now().UTC(), CommittedVia: "compaction_manifest"}

	s.mu.Lock()
	defer s.mu.Unlock()

	manifests, err := s.loadManifests(ctx)
	if err != nil {
		return result, fmt.Errorf("load manifests: %w", err)
	}
	committed := CommittedObjectKeys(manifests)
	rowsByKey := make(map[string]int, len(committed))
	batchIDsByKey := make(map[string][]string, len(committed))
	for _, m := range manifests {
		for _, obj := range m.Objects {
			rowsByKey[obj.Key] = obj.Rows
			if m.BatchID != "" {
				batchIDsByKey[obj.Key] = append(batchIDsByKey[obj.Key], m.BatchID)
			}
		}
	}

	groups := map[string][]manifestObject{}
	for key, obj := range committed {
		day := outputObjectDay(key, s.config.Prefix)
		if day == "" {
			result.Skipped++
			continue
		}
		if opts.Day != "" && day != opts.Day {
			continue
		}
		groups[day] = append(groups[day], obj)
	}

	days := make([]string, 0, len(groups))
	for day := range groups {
		days = append(days, day)
	}
	sort.Strings(days)

	for _, day := range days {
		objs := groups[day]
		result.ObjectsIn += len(objs)
		group := CompactGroup{Day: day}
		tooLarge := false
		for _, obj := range objs {
			group.BytesBefore += int64(obj.Bytes)
			if int64(obj.Bytes) > opts.MaxObjectBytes {
				tooLarge = true
			}
		}
		if len(objs) < opts.MinObjects || tooLarge {
			result.Skipped++
			continue
		}
		if len(objs) > opts.MaxObjects {
			objs = objs[:opts.MaxObjects]
		}
		sort.Slice(objs, func(i, j int) bool { return objs[i].Key < objs[j].Key })

		var merged bytes.Buffer
		rows := 0
		for _, obj := range objs {
			payload, readErr := s.readObject(ctx, obj.Key)
			if readErr != nil {
				return result, fmt.Errorf("read %s for compaction: %w", obj.Key, readErr)
			}
			if merged.Len() > 0 && !bytes.HasSuffix(merged.Bytes(), []byte("\n")) {
				merged.WriteByte('\n')
			}
			merged.Write(payload)
			rows += obj.Rows
			group.ReplacedKeys = append(group.ReplacedKeys, obj.Key)
			group.SourceBatchIDs = append(group.SourceBatchIDs, batchIDsByKey[obj.Key]...)
		}
		payload := merged.Bytes()
		group.Rows = rows
		group.BytesAfter = int64(len(payload))
		sum := sha256.Sum256(payload)
		group.MergedKey = fmt.Sprintf("%s%scompacted-%s.%s", s.config.Prefix, day, hex.EncodeToString(sum[:16]), s.config.Format)
		group.ManifestKey = manifestObjectKey(s.config.Prefix, s.config.ManifestPrefix, day, hex.EncodeToString(sum[:]))

		result.Groups = append(result.Groups, group)
		result.BytesBefore += group.BytesBefore
		result.BytesAfter += group.BytesAfter
	}

	if opts.DryRun || len(result.Groups) == 0 {
		result.ObjectsOut = result.ObjectsIn
		return result, nil
	}

	for _, group := range result.Groups {
		payload, readErr := s.readMergedGroup(ctx, group)
		if readErr != nil {
			return result, readErr
		}
		if err := s.writeObject(ctx, group.MergedKey, payload); err != nil {
			return result, fmt.Errorf("write compacted object %s: %w", group.MergedKey, err)
		}
		manifest := batchManifest{
			Version:        manifestVersion,
			Sink:           s.name,
			CreatedAt:      time.Now().UTC(),
			Kind:           manifestKindCompaction,
			BatchID:        manifestBatchID([]manifestObject{{Key: group.MergedKey, Rows: group.Rows}}),
			Rows:           group.Rows,
			Format:         s.config.Format,
			Objects:        []manifestObject{{Key: group.MergedKey, SHA256: objectSHA(payload), Bytes: len(payload), Rows: group.Rows, Format: s.config.Format}},
			Replaces:       group.ReplacedKeys,
			SourceBatchIDs: group.SourceBatchIDs,
			ContentAddress: manifestContentKey([]manifestObject{{Key: group.MergedKey, Rows: group.Rows}}),
		}
		raw, marshalErr := jsonMarshalIndent(manifest)
		if marshalErr != nil {
			return result, marshalErr
		}
		if err := s.writeObject(ctx, group.ManifestKey, raw); err != nil {
			return result, fmt.Errorf("write compaction manifest %s: %w", group.ManifestKey, err)
		}
		result.ObjectsOut += 1
		s.manifestCount++
	}
	// The committed view changed: force the replay guard to reload.
	s.committedIDs = nil
	return result, nil
}

// readMergedGroup re-reads the group members and concatenates them. It is a
// second pass so the dry-run path never buffers more than the plan needs.
func (s *FileSink) readMergedGroup(ctx context.Context, group CompactGroup) ([]byte, error) {
	var merged bytes.Buffer
	for _, key := range group.ReplacedKeys {
		payload, err := s.readObject(ctx, key)
		if err != nil {
			return nil, fmt.Errorf("read %s for compaction: %w", key, err)
		}
		if merged.Len() > 0 && !bytes.HasSuffix(merged.Bytes(), []byte("\n")) {
			merged.WriteByte('\n')
		}
		merged.Write(payload)
	}
	return merged.Bytes(), nil
}

// outputObjectDay extracts the yyyy/mm/dd partition from a data object key.
func outputObjectDay(key, prefix string) string {
	trimmed := strings.TrimPrefix(key, prefix)
	parts := strings.Split(trimmed, "/")
	if len(parts) < 4 {
		return ""
	}
	return parts[0] + "/" + parts[1] + "/" + parts[2] + "/"
}

func objectSHA(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func jsonMarshalIndent(v any) ([]byte, error) {
	return json.MarshalIndent(v, "", "  ")
}
