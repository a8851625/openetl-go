package source

import "fmt"

// readShardConfig extracts shard_index and shard_total from a source config map.
// Returns (0, 0) if not present (single-shard mode).
// This is the shared utility used by file, http, redis, and other sources
// that need sharding support.
func readShardConfig(config map[string]any) (shardIndex, shardTotal int) {
	if v, ok := config["shard_index"]; ok {
		switch idx := v.(type) {
		case int:
			shardIndex = idx
		case float64:
			shardIndex = int(idx)
		case int64:
			shardIndex = int(idx)
		}
	}
	if v, ok := config["shard_total"]; ok {
		switch t := v.(type) {
		case int:
			shardTotal = t
		case float64:
			shardTotal = int(t)
		case int64:
			shardTotal = int(t)
		}
	}
	return
}

// readInt reads an integer value from a config map with a default fallback.
func readInt(config map[string]any, key string, defaultVal int) int {
	if v, ok := config[key]; ok {
		switch val := v.(type) {
		case int:
			return val
		case float64:
			return int(val)
		case int64:
			return int(val)
		}
	}
	return defaultVal
}

// readStringSlice accepts the common decoded shapes produced by YAML, JSON,
// tests, and UI-generated specs.
func readStringSlice(config map[string]any, key string) []string {
	raw, ok := config[key]
	if !ok {
		return nil
	}
	switch values := raw.(type) {
	case []string:
		out := make([]string, 0, len(values))
		for _, value := range values {
			if value != "" {
				out = append(out, value)
			}
		}
		return out
	case []any:
		out := make([]string, 0, len(values))
		for _, value := range values {
			if s, ok := value.(string); ok && s != "" {
				out = append(out, s)
			}
		}
		return out
	case string:
		if values != "" {
			return []string{values}
		}
	default:
		if raw != nil {
			return []string{fmt.Sprint(raw)}
		}
	}
	return nil
}
