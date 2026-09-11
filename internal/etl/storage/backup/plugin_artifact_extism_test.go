//go:build extism

package backup_test

import (
	"context"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/plugin/pluginsystem"
	"github.com/a8851625/openetl-go/internal/etl/storage/backup"
)

func TestRestoredPluginLoadsWithExtism(t *testing.T) {
	s := openTestStore(t)
	snap := fidelityFixture()
	snap.Plugins[0].ABI = ""
	snap.Plugins[0].ManifestJSON = ""
	dir := t.TempDir()
	ctx := context.Background()
	if err := backup.Restore(ctx, s, snap, backup.Options{ClearBeforeRestore: true, PluginsDir: dir}); err != nil {
		t.Fatal(err)
	}
	manager, err := pluginsystem.NewManager(s, dir)
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close(ctx)
	meta, err := manager.Get("parser")
	if err != nil || meta == nil {
		t.Fatalf("restored artifact could not be loaded by the real runtime: %+v %v", meta, err)
	}
}
