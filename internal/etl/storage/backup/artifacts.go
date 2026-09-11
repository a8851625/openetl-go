package backup

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"

	"github.com/a8851625/openetl-go/internal/etl/plugin/pluginsystem"
	"github.com/a8851625/openetl-go/internal/etl/storage"
)

func stageRestoreArtifacts(ctx context.Context, snap *Snapshot, opts Options) ([]*storage.PluginEntry, string, error) {
	if len(snap.Plugins) == 0 {
		if len(snap.PluginArtifacts) != 0 {
			return nil, "", fmt.Errorf("backup: orphaned plugin artifacts without registry entries")
		}
		return nil, "", nil
	}
	if opts.PluginsDir == "" {
		return nil, "", fmt.Errorf("backup: restoring plugins requires PluginsDir; v1 backups also require the original WASM files")
	}
	root, err := filepath.Abs(opts.PluginsDir)
	if err != nil {
		return nil, "", err
	}
	// Decode and validate everything before creating files or changing SQL.
	payloads := make(map[string][]byte, len(snap.Plugins))
	for _, p := range snap.Plugins {
		if p == nil {
			return nil, "", fmt.Errorf("backup: nil plugin registry entry")
		}
		if err := pluginsystem.ValidatePluginName(p.Name); err != nil {
			return nil, "", err
		}
		if _, duplicate := payloads[p.Name]; duplicate {
			return nil, "", fmt.Errorf("backup: duplicate plugin %q", p.Name)
		}
		if encoded, included := snap.PluginArtifacts[p.Name]; included {
			data, err := base64.StdEncoding.DecodeString(encoded)
			if err != nil || len(data) == 0 {
				return nil, "", fmt.Errorf("backup: invalid or empty artifact for plugin %q", p.Name)
			}
			payloads[p.Name] = data
			continue
		}
		if snap.FormatVersion == FormatVersion {
			return nil, "", fmt.Errorf("backup: v%d snapshot is missing artifact for plugin %q", FormatVersion, p.Name)
		}
		// v1 contained paths only. The operator must restore the old files to
		// PluginsDir first, or still have the original recorded path available.
		data, err := os.ReadFile(filepath.Join(root, p.Name+".wasm"))
		if err != nil && p.WASMPath != "" {
			data, err = os.ReadFile(p.WASMPath)
		}
		if err != nil || len(data) == 0 {
			return nil, "", fmt.Errorf("backup: legacy plugin %q requires its original WASM file in PluginsDir", p.Name)
		}
		payloads[p.Name] = data
	}
	for name := range snap.PluginArtifacts {
		if _, exists := payloads[name]; !exists {
			return nil, "", fmt.Errorf("backup: artifact %q has no registry entry", name)
		}
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, "", err
	}
	dir, err := os.MkdirTemp(root, ".restore-")
	if err != nil {
		return nil, "", err
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.RemoveAll(dir) // fresh private generation, never a live path
		}
	}()
	entries := make([]*storage.PluginEntry, 0, len(snap.Plugins))
	for _, plugin := range snap.Plugins {
		if err := ctx.Err(); err != nil {
			return nil, "", err
		}
		p := *plugin
		p.WASMPath = filepath.Join(dir, p.Name+".wasm")
		f, err := os.OpenFile(p.WASMPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return nil, "", err
		}
		_, writeErr := f.Write(payloads[p.Name])
		if writeErr == nil {
			writeErr = f.Sync()
		}
		closeErr := f.Close()
		if writeErr != nil {
			return nil, "", writeErr
		}
		if closeErr != nil {
			return nil, "", closeErr
		}
		entries = append(entries, &p)
	}
	for _, path := range []string{dir, root} {
		if err := syncDirectory(path); err != nil {
			return nil, "", err
		}
	}
	complete = true
	return entries, dir, nil
}

func syncDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
