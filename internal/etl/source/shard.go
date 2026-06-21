package source

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
