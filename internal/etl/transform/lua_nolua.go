//go:build nolua

package transform

import (
	"fmt"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

// When the binary is built with -tags=nolua, the gopher-lua runtime is excluded
// (P5-22: 轻量 opt-out for users who don't need Lua transforms/hooks). A spec
// that still declares `type: lua` gets a clear error at registration time
// instead of a silent no-op, so the misconfiguration surfaces immediately.
func init() {
	registry.RegisterTransform("lua", func(config map[string]any) (core.Transform, error) {
		return nil, fmt.Errorf("lua transform not compiled in: rebuild without the -tags=nolua constraint to use type:lua transforms")
	})
	registry.RegisterTransform("flat_map", func(config map[string]any) (core.Transform, error) {
		return nil, fmt.Errorf("flat_map transform not compiled in: rebuild without the -tags=nolua constraint to use type:flat_map transforms")
	})
	registry.RegisterTransform("udtf", func(config map[string]any) (core.Transform, error) {
		return nil, fmt.Errorf("udtf transform not compiled in: rebuild without the -tags=nolua constraint to use type:udtf transforms")
	})
}
