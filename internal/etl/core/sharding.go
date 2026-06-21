package core

import (
	"context"
	"fmt"
	"hash/fnv"
	"io"
)

// ShardConfig holds parsed sharding configuration for the ShardedReader decorator.
type ShardConfig struct {
	Index    int    // 0-based shard index
	Total    int    // total number of shards (1 = no sharding)
	Strategy string // "line_modulo" | "hash_modulo" | "noop"
	Key      string // record field to hash for hash_modulo; "@key" for Metadata.Key
}

// ReadShardConfig parses shard_index, shard_total, and shard_strategy from a config map.
// This replaces the per-source readShardConfig utilities with a unified framework function.
func ReadShardConfig(cfg map[string]any) ShardConfig {
	sc := ShardConfig{Total: 1, Strategy: "noop"}
	if v, ok := cfg["shard_index"]; ok {
		switch idx := v.(type) {
		case int:
			sc.Index = idx
		case float64:
			sc.Index = int(idx)
		case int64:
			sc.Index = int(idx)
		}
	}
	if v, ok := cfg["shard_total"]; ok {
		switch t := v.(type) {
		case int:
			sc.Total = t
		case float64:
			sc.Total = int(t)
		case int64:
			sc.Total = int(t)
		}
	}
	if v, ok := cfg["shard_strategy"]; ok {
		if s, ok := v.(string); ok {
			sc.Strategy = s
		}
	}
	// Map high-level strategies to framework strategies
	switch sc.Strategy {
	case "round_robin":
		sc.Strategy = "line_modulo"
	case "id_range", "partition", "table":
		sc.Strategy = "noop" // source handles these natively
	}
	if sc.Strategy == "" {
		sc.Strategy = "line_modulo"
	}
	if sc.Total <= 1 {
		sc.Strategy = "noop"
	}
	if v, ok := cfg["shard_key"]; ok {
		if s, ok := v.(string); ok {
			sc.Key = s
		}
	}
	return sc
}

// RecordFilter returns true if a record belongs to this shard.
type RecordFilter func(rec Record, lineNum int64) bool

// NewRecordFilter creates a RecordFilter from ShardConfig.
func NewRecordFilter(sc ShardConfig) RecordFilter {
	switch sc.Strategy {
	case "line_modulo":
		return func(_ Record, lineNum int64) bool {
			return lineNum%int64(sc.Total) == int64(sc.Index)
		}
	case "hash_modulo":
		return func(rec Record, _ int64) bool {
			val := ""
			if sc.Key == "@key" {
				val = rec.Metadata.Key
			} else if sc.Key != "" {
				if v, ok := rec.Data[sc.Key]; ok {
					val = fmt.Sprint(v)
				}
			}
			return int(hashStr(val))%sc.Total == sc.Index
		}
	default: // noop
		return func(_ Record, _ int64) bool { return true }
	}
}

func hashStr(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// NewShardedReader wraps a RecordReader with shard filtering.
// If sharding is not needed (Total <= 1 or strategy = "noop"), returns the original reader.
func NewShardedReader(inner RecordReader, cfg ShardConfig) RecordReader {
	if cfg.Total <= 1 || cfg.Strategy == "noop" {
		return inner
	}
	return &ShardedReader{
		inner:  inner,
		filter: NewRecordFilter(cfg),
	}
}

// ShardedReader is a decorator that filters records by shard assignment.
// It wraps any RecordReader and only returns records that belong to the configured shard.
type ShardedReader struct {
	inner  RecordReader
	filter RecordFilter
	count  int64
}

func (r *ShardedReader) Read(ctx context.Context) (Record, error) {
	for {
		rec, err := r.inner.Read(ctx)
		if err != nil {
			return rec, err
		}
		r.count++
		if r.filter(rec, r.count) {
			return rec, nil
		}
	}
}

func (r *ShardedReader) ReadBatch(ctx context.Context, n int) ([]Record, error) {
	batch := make([]Record, 0, n)
	for len(batch) < n {
		rec, err := r.Read(ctx)
		if err == io.EOF {
			break
		}
		if err != nil {
			return batch, err
		}
		batch = append(batch, rec)
	}
	return batch, nil
}

func (r *ShardedReader) Snapshot(ctx context.Context) (Checkpoint, error) {
	return r.inner.Snapshot(ctx)
}

func (r *ShardedReader) CheckpointForRecord(ctx context.Context, rec Record) (Checkpoint, error) {
	if cp, ok := r.inner.(RecordCheckpointer); ok {
		return cp.CheckpointForRecord(ctx, rec)
	}
	return r.inner.Snapshot(ctx)
}

func (r *ShardedReader) Close() error {
	return r.inner.Close()
}
