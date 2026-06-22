//go:build !cgo

package transform

import (
	"fmt"

	"github.com/a8851625/openetl-go/internal/etl/core"
	"github.com/a8851625/openetl-go/internal/etl/registry"
)

func init() {
	// Register a stub that explains CGO is required for TS transform.
	registry.RegisterTransform("ts", func(config map[string]any) (core.Transform, error) {
		return nil, fmt.Errorf("ts transform requires CGO_ENABLED=1 (QuickJS runtime); this binary was built without CGO. Use 'lua' transform instead, or build with CGO enabled")
	})
	registry.RegisterTransform("javascript", func(config map[string]any) (core.Transform, error) {
		return nil, fmt.Errorf("javascript transform requires CGO_ENABLED=1; this binary was built without CGO. Use 'lua' transform instead")
	})
	registry.RegisterTransform("js", func(config map[string]any) (core.Transform, error) {
		return nil, fmt.Errorf("js transform requires CGO_ENABLED=1; this binary was built without CGO. Use 'lua' transform instead")
	})
}
