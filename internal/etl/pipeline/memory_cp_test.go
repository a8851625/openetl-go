package pipeline

import (
	"context"
	"sync"

	"github.com/a8851625/openetl-go/internal/etl/core"
)

// memoryCPStore is an in-memory CheckpointStore for testing.
type memoryCPStore struct {
	mu  sync.Mutex
	cps map[string]core.Checkpoint
}

func newMemoryCPStore() *memoryCPStore {
	return &memoryCPStore{cps: map[string]core.Checkpoint{}}
}

func (s *memoryCPStore) Save(_ context.Context, cp core.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cps[cp.JobName] = cp
	return nil
}
func (s *memoryCPStore) Load(_ context.Context, job string) (*core.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cp, ok := s.cps[job]; ok {
		return &cp, nil
	}
	return nil, nil
}
func (s *memoryCPStore) Delete(_ context.Context, job string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cps, job)
	return nil
}
func (s *memoryCPStore) List(_ context.Context) ([]core.Checkpoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]core.Checkpoint, 0, len(s.cps))
	for _, cp := range s.cps {
		out = append(out, cp)
	}
	return out, nil
}
