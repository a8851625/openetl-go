package state

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// MemoryStore is a process-local StateStore implementation for tests,
// development, and as a reference implementation for SQL-backed stores.
type MemoryStore struct {
	mu      sync.RWMutex
	entries map[string]Entry
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{entries: make(map[string]Entry)}
}

func (s *MemoryStore) Get(ctx context.Context, pipeline, node, key string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}
	id := stateKey(pipeline, node, key)
	now := time.Now()

	s.mu.RLock()
	entry, ok := s.entries[id]
	s.mu.RUnlock()
	if !ok {
		return nil, false, nil
	}
	if !entry.ExpiresAt.IsZero() && !entry.ExpiresAt.After(now) {
		_ = s.Delete(ctx, pipeline, node, key)
		return nil, false, nil
	}
	value := append([]byte(nil), entry.Value...)
	return value, true, nil
}

func (s *MemoryStore) Set(ctx context.Context, pipeline, node, key string, value []byte, ttl time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	now := time.Now()
	entry := Entry{
		Pipeline:  pipeline,
		Node:      node,
		Key:       key,
		Value:     append([]byte(nil), value...),
		UpdatedAt: now,
	}
	if ttl > 0 {
		entry.ExpiresAt = now.Add(ttl)
	}

	s.mu.Lock()
	s.entries[stateKey(pipeline, node, key)] = entry
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Delete(ctx context.Context, pipeline, node, key string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	delete(s.entries, stateKey(pipeline, node, key))
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Snapshot(ctx context.Context, pipeline, node string) (*Snapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	now := time.Now()
	prefix := stateKeyPrefix(pipeline, node)

	s.mu.RLock()
	entries := make([]Entry, 0)
	for id, entry := range s.entries {
		if len(id) < len(prefix) || id[:len(prefix)] != prefix {
			continue
		}
		if !entry.ExpiresAt.IsZero() && !entry.ExpiresAt.After(now) {
			continue
		}
		entry.Value = append([]byte(nil), entry.Value...)
		entries = append(entries, entry)
	}
	s.mu.RUnlock()

	sort.Slice(entries, func(i, j int) bool { return entries[i].Key < entries[j].Key })
	return &Snapshot{
		Pipeline:  pipeline,
		Node:      node,
		Version:   fmt.Sprintf("%d", now.UnixNano()),
		Entries:   entries,
		CreatedAt: now,
	}, nil
}

func (s *MemoryStore) Restore(ctx context.Context, snap *Snapshot) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if snap == nil {
		return nil
	}
	prefix := stateKeyPrefix(snap.Pipeline, snap.Node)

	s.mu.Lock()
	for id := range s.entries {
		if len(id) >= len(prefix) && id[:len(prefix)] == prefix {
			delete(s.entries, id)
		}
	}
	for _, entry := range snap.Entries {
		entry.Pipeline = snap.Pipeline
		entry.Node = snap.Node
		entry.Value = append([]byte(nil), entry.Value...)
		s.entries[stateKey(entry.Pipeline, entry.Node, entry.Key)] = entry
	}
	s.mu.Unlock()
	return nil
}

func (s *MemoryStore) Stats(ctx context.Context, pipeline, node string) (Stats, error) {
	if err := ctx.Err(); err != nil {
		return Stats{}, err
	}
	now := time.Now()
	prefix := stateKeyPrefix(pipeline, node)

	var stats Stats
	s.mu.RLock()
	for id, entry := range s.entries {
		if len(id) < len(prefix) || id[:len(prefix)] != prefix {
			continue
		}
		if !entry.ExpiresAt.IsZero() && !entry.ExpiresAt.After(now) {
			continue
		}
		stats.Keys++
		stats.Bytes += int64(len(entry.Value))
		if entry.UpdatedAt.After(stats.UpdatedAt) {
			stats.UpdatedAt = entry.UpdatedAt
		}
	}
	s.mu.RUnlock()
	return stats, nil
}

func (s *MemoryStore) Close() error { return nil }

func stateKey(pipeline, node, key string) string {
	return pipeline + "\x00" + node + "\x00" + key
}

func stateKeyPrefix(pipeline, node string) string {
	return pipeline + "\x00" + node + "\x00"
}
