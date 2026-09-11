package backup_test

import (
	"context"
	"testing"

	"github.com/a8851625/openetl-go/internal/etl/storage"
	"github.com/a8851625/openetl-go/internal/etl/storage/backup"
)

func TestInvalidArtifactsLeaveStateUntouched(t *testing.T) {
	for name, corrupt := range map[string]func(*backup.Snapshot, *backup.Options){
		"bad_base64":    func(s *backup.Snapshot, _ *backup.Options) { s.PluginArtifacts["parser"] = "bad!!base64" },
		"missing_bytes": func(s *backup.Snapshot, _ *backup.Options) { delete(s.PluginArtifacts, "parser") },
		"empty_bytes":   func(s *backup.Snapshot, _ *backup.Options) { s.PluginArtifacts["parser"] = "" },
		"orphan_artifact": func(s *backup.Snapshot, _ *backup.Options) {
			s.PluginArtifacts["unknown"] = s.PluginArtifacts["parser"]
		},
		"path_traversal":      func(s *backup.Snapshot, _ *backup.Options) { s.Plugins[0].Name = "../parser" },
		"duplicate_plugin":    func(s *backup.Snapshot, _ *backup.Options) { s.Plugins = append(s.Plugins, s.Plugins[0]) },
		"nil_plugin":          func(s *backup.Snapshot, _ *backup.Options) { s.Plugins = append(s.Plugins, nil) },
		"missing_destination": func(_ *backup.Snapshot, o *backup.Options) { o.PluginsDir = "" },
	} {
		t.Run(name, func(t *testing.T) {
			s := openTestStore(t)
			ctx := context.Background()
			opts := backup.Options{ClearBeforeRestore: true, PluginsDir: t.TempDir()}
			if err := backup.Restore(ctx, s, fidelityFixture(), opts); err != nil {
				t.Fatal(err)
			}
			before := exportForRestoreTest(t, s)
			snap := fidelityFixture()
			corrupt(snap, &opts)
			if err := backup.Restore(ctx, s, snap, opts); err == nil {
				t.Fatal("invalid artifact input accepted")
			}
			after := exportForRestoreTest(t, s)
			assertSnapshotPayload(t, before, after)
			if before.Plugins[0].WASMPath != after.Plugins[0].WASMPath {
				t.Fatal("invalid artifact input changed live path")
			}
		})
	}
}

type storageWithoutTransaction struct{ storage.Storage }

func TestRestoreRefusesNonAtomicBackendBeforeMutation(t *testing.T) {
	s := openTestStore(t)
	ctx := context.Background()
	if err := s.SavePipeline(ctx, &storage.PipelineRow{ID: "keep", Name: "keep", Status: "stopped"}); err != nil {
		t.Fatal(err)
	}
	before := exportForRestoreTest(t, s)
	err := backup.Restore(ctx, storageWithoutTransaction{s}, &backup.Snapshot{FormatVersion: 2}, backup.Options{ClearBeforeRestore: true})
	if err == nil {
		t.Fatal("restore without an atomic backend was accepted")
	}
	assertSnapshotPayload(t, before, exportForRestoreTest(t, s))
}
