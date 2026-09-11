package backup_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/backup"
	"github.com/a8851625/openetl-go/internal/etl/storage/sqlite"
)

// TestPluginArtifactRoundTrips verifies the WASM artifact is packaged into the
// snapshot and restored with the plugin metadata row (IT-3/T3.4 acceptance 4).
func TestPluginArtifactRoundTrips(t *testing.T) {
	ctx := context.Background()
	raw, err := sqlite.New(filepath.Join(t.TempDir(), "etl.db"))
	if err != nil {
		t.Fatalf("sqlite.New: %v", err)
	}
	t.Cleanup(func() { _ = raw.Close() })

	pluginsDir := t.TempDir()
	wasmBytes := []byte("\x00asm\x01\x00\x00\x00")
	if err := os.WriteFile(filepath.Join(pluginsDir, "mypipe.wasm"), wasmBytes, 0o644); err != nil {
		t.Fatalf("write wasm: %v", err)
	}
	plugin := &storage.PluginEntry{
		Name:     "mypipe",
		Kind:     "transform",
		WASMPath: filepath.Join(pluginsDir, "mypipe.wasm"),
		Version:  "1.0.0",
		ABI:      "abi-v1",
	}
	if err := raw.SavePlugin(ctx, plugin); err != nil {
		t.Fatalf("save plugin: %v", err)
	}

	snap, err := backup.Export(ctx, raw, backup.Options{Backend: "sqlite", PluginsDir: pluginsDir})
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	if len(snap.PluginArtifacts) != 1 || snap.PluginArtifacts["mypipe"] == "" {
		t.Fatalf("plugin artifact missing from snapshot: %+v", snap.PluginArtifacts)
	}

	// Restore into a fresh store with a fresh plugins dir.
	raw2, err := sqlite.New(filepath.Join(t.TempDir(), "restore.db"))
	if err != nil {
		t.Fatalf("sqlite.New restore: %v", err)
	}
	t.Cleanup(func() { _ = raw2.Close() })
	restoreDir := t.TempDir()
	if err := backup.Restore(ctx, raw2, snap, backup.Options{ClearBeforeRestore: true, PluginsDir: restoreDir}); err != nil {
		t.Fatalf("restore: %v", err)
	}
	got, err := raw2.GetPlugin(ctx, "mypipe")
	if err != nil || got == nil {
		t.Fatalf("restored plugin row missing: %v %v", got, err)
	}
	rel, err := filepath.Rel(restoreDir, got.WASMPath)
	if err != nil || filepath.IsAbs(rel) || strings.HasPrefix(rel, "..") || got.WASMPath == plugin.WASMPath {
		t.Fatalf("plugin path was not relocated beneath restore directory: %q", got.WASMPath)
	}
	// Delete the original artifact: the runtime loads exactly WASMPath from SQL.
	if err := os.Remove(plugin.WASMPath); err != nil {
		t.Fatal(err)
	}
	restoredBytes, err := os.ReadFile(got.WASMPath)
	if err != nil {
		t.Fatalf("restored artifact missing: %v", err)
	}
	if !bytes.Equal(restoredBytes, wasmBytes) {
		t.Fatalf("restored artifact mismatch: %x", restoredBytes)
	}
	if got.Version != "1.0.0" {
		t.Fatalf("plugin version not preserved: %+v", got)
	}
}
