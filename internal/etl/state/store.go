package state

import (
	"context"
	"time"
)

// Entry is a single persisted state value owned by a pipeline node.
type Entry struct {
	Pipeline  string    `json:"pipeline"`
	Node      string    `json:"node"`
	Key       string    `json:"key"`
	Value     []byte    `json:"value"`
	ExpiresAt time.Time `json:"expires_at,omitempty"`
	UpdatedAt time.Time `json:"updated_at"`
}

// Snapshot is a point-in-time export of a node's state. Checkpoints can refer
// to Snapshot.Version to describe which state version matches a source offset.
type Snapshot struct {
	Pipeline  string    `json:"pipeline"`
	Node      string    `json:"node"`
	Version   string    `json:"version"`
	Entries   []Entry   `json:"entries"`
	CreatedAt time.Time `json:"created_at"`
}

// Stats describes state size and freshness for metrics and preflight output.
type Stats struct {
	Keys      int       `json:"keys"`
	Bytes     int64     `json:"bytes"`
	UpdatedAt time.Time `json:"updated_at,omitempty"`
}

// Store is the state backend contract for stateful transforms such as lookup,
// join, window, and deduplicate.
type Store interface {
	Get(ctx context.Context, pipeline, node, key string) ([]byte, bool, error)
	Set(ctx context.Context, pipeline, node, key string, value []byte, ttl time.Duration) error
	Delete(ctx context.Context, pipeline, node, key string) error
	Snapshot(ctx context.Context, pipeline, node string) (*Snapshot, error)
	Restore(ctx context.Context, snap *Snapshot) error
	Stats(ctx context.Context, pipeline, node string) (Stats, error)
	Close() error
}
