//go:build nolua

package pipeline

import (
	"fmt"

	"openetl-go/internal/etl/core"
)

// NewLuaHook stub for -tags=nolua builds (P5-22): gopher-lua is excluded, so a
// spec declaring `type: lua` hooks fails loudly instead of silently no-op'ing.
// buildHook logs the error and skips the hook (best-effort, like a misconfigured
// webhook), surfacing the misconfiguration in the startup log.
func NewLuaHook(pipelineName, script string, config map[string]any) (core.LifecycleHook, error) {
	return nil, fmt.Errorf("lua hook not compiled in: rebuild without -tags=nolua to use type:lua hooks (pipeline=%s)", pipelineName)
}
