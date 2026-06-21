package checkpoint

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"openetl-go/internal/etl/core"
)

type FileStore struct {
	dir   string
	mu    sync.RWMutex
	cache map[string]core.Checkpoint
}

func NewFileStore(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("create checkpoint dir: %w", err)
	}
	return &FileStore{dir: dir, cache: map[string]core.Checkpoint{}}, nil
}

func (s *FileStore) path(jobName string) string {
	return filepath.Join(s.dir, jobName+".json")
}

func (s *FileStore) Save(ctx context.Context, cp core.Checkpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cp.Timestamp = time.Now()
	data, err := json.MarshalIndent(cp, "", "  ")
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}
	path := s.path(cp.JobName)
	tmp, err := os.CreateTemp(s.dir, cp.JobName+"-*.tmp")
	if err != nil {
		return fmt.Errorf("create checkpoint temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return fmt.Errorf("write checkpoint temp file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("sync checkpoint temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close checkpoint temp file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename checkpoint file: %w", err)
	}
	if dir, err := os.Open(s.dir); err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	s.cache[cp.JobName] = cp
	return nil
}

func (s *FileStore) Load(ctx context.Context, jobName string) (*core.Checkpoint, error) {
	s.mu.RLock()
	if cp, ok := s.cache[jobName]; ok {
		s.mu.RUnlock()
		return &cp, nil
	}
	s.mu.RUnlock()

	data, err := os.ReadFile(s.path(jobName))
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("read checkpoint: %w", err)
	}
	var cp core.Checkpoint
	if err := json.Unmarshal(data, &cp); err != nil {
		return nil, fmt.Errorf("unmarshal checkpoint: %w", err)
	}

	s.mu.Lock()
	s.cache[jobName] = cp
	s.mu.Unlock()
	return &cp, nil
}

func (s *FileStore) Delete(ctx context.Context, jobName string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.cache, jobName)
	p := s.path(jobName)
	if _, err := os.Stat(p); os.IsNotExist(err) {
		return nil
	}
	return os.Remove(p)
}

func (s *FileStore) List(ctx context.Context) ([]core.Checkpoint, error) {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("read checkpoint dir: %w", err)
	}
	var cps []core.Checkpoint
	for _, e := range entries {
		if e.IsDir() || filepath.Ext(e.Name()) != ".json" {
			continue
		}
		cp, err := s.Load(ctx, e.Name()[:len(e.Name())-5])
		if err != nil || cp == nil {
			continue
		}
		cps = append(cps, *cp)
	}
	return cps, nil
}
