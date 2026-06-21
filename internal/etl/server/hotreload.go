package server

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/fsnotify/fsnotify"
	"github.com/gogf/gf/v2/frame/g"
)

type HotReloader struct {
	server   *Server
	watcher  *fsnotify.Watcher
	dir      string
	mu       sync.Mutex
	debounce time.Duration
}

func NewHotReloader(s *Server, dir string) (*HotReloader, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, 0755); err != nil {
		watcher.Close()
		return nil, err
	}
	if err := watcher.Add(dir); err != nil {
		watcher.Close()
		return nil, err
	}
	return &HotReloader{
		server:   s,
		watcher:  watcher,
		dir:      dir,
		debounce: 2 * time.Second,
	}, nil
}

func (h *HotReloader) Run(ctx context.Context) {
	timer := time.NewTimer(0)
	if !timer.Stop() {
		<-timer.C
	}

	for {
		select {
		case <-ctx.Done():
			h.watcher.Close()
			return
		case event, ok := <-h.watcher.Events:
			if !ok {
				return
			}
			if event.Op&(fsnotify.Create|fsnotify.Write|fsnotify.Rename) == 0 {
				continue
			}
			if filepath.Ext(event.Name) != ".yaml" && filepath.Ext(event.Name) != ".yml" {
				continue
			}
			timer.Reset(h.debounce)
		case _, ok := <-h.watcher.Errors:
			if !ok {
				return
			}
		case <-timer.C:
			h.reload(ctx)
		}
	}
}

func (h *HotReloader) reload(ctx context.Context) {
	h.mu.Lock()
	defer h.mu.Unlock()

	result, err := h.server.ReloadSpecs(ctx)
	if err != nil {
		g.Log().Warningf(ctx, "Hot reload error: %v", err)
		return
	}
	if len(result.Loaded) > 0 {
		for _, name := range result.Loaded {
			h.server.mu.RLock()
			runner, ok := h.server.pipelines[name]
			h.server.mu.RUnlock()
			if ok {
				if err := runner.Start(ctx); err != nil {
					g.Log().Warningf(ctx, "Hot reload: failed to start pipeline %s: %v", name, err)
				} else {
					g.Log().Infof(ctx, "Hot reload: started pipeline %s", name)
				}
			}
		}
	}
	if len(result.Errors) > 0 {
		for file, errMsg := range result.Errors {
			g.Log().Warningf(ctx, "Hot reload: skip %s: %s", file, errMsg)
		}
	}
}

func (h *HotReloader) Close() error {
	return h.watcher.Close()
}
