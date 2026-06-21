package pipeline

// ShardInfo exposes per-shard status, stats, and logs for parallelism visibility.
type ShardInfo struct {
	Index  int    `json:"index"`
	Status Status `json:"status"`
	Stats  Stats  `json:"stats"`
}
