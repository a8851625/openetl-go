package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/gogf/gf/v2/frame/g"

	"github.com/a8851625/openetl-go/internal/etl/server"
	"github.com/a8851625/openetl-go/internal/etl/storage/backup"
	"github.com/a8851625/openetl-go/internal/etl/storage/factory"
)

// Called before runtime initialization: no HTTP listener, scheduler, pipeline,
// worker heartbeat, janitor or legacy-file import runs during maintenance.
func runBackupMaintenance(ctx context.Context, flags *runtimeFlags, out io.Writer) (err error) {
	var snap *backup.Snapshot
	if flags.restoreFile != "" {
		// Parse first, so an unreadable/invalid input never opens the destination.
		snap, err = backup.ReadFile(flags.restoreFile)
		if err != nil {
			return fmt.Errorf("read restore file: %w", err)
		}
	}
	backend := g.Cfg().MustGet(ctx, "etl.storage.type", "sqlite").String()
	checkpointDir := g.Cfg().MustGet(ctx, "etl.checkpointDir", "./data/checkpoint").String()
	if backend == "sqlite" || backend == "" {
		path := g.Cfg().MustGet(ctx, "etl.storage.sqlite.path", "").String()
		if path == "" {
			path = filepath.Join(filepath.Dir(checkpointDir), "etl.db")
		}
		for _, file := range []string{flags.backupFile, flags.restoreFile} {
			if file == "" {
				continue
			}
			dbPath, err := filepath.Abs(path)
			if err != nil {
				return err
			}
			filePath, err := filepath.Abs(file)
			if err != nil {
				return err
			}
			dbInfo, dbErr := os.Stat(dbPath)
			fileInfo, fileErr := os.Stat(filePath)
			if dbPath == filePath || (dbErr == nil && fileErr == nil && os.SameFile(dbInfo, fileInfo)) {
				return fmt.Errorf("portable backup path must differ from the SQLite database path")
			}
		}
		if flags.backupFile != "" || flags.checkSecrets || flags.remediateSecrets {
			if _, err := os.Stat(path); err != nil {
				return fmt.Errorf("maintenance source SQLite database: %w", err)
			}
		}
	}
	store, err := factory.NewMaintenanceStore(ctx, backend, checkpointDir)
	if err != nil {
		return fmt.Errorf("open maintenance storage: %w", err)
	}
	defer func() { err = errors.Join(err, store.Close()) }()
	if flags.checkSecrets || flags.remediateSecrets {
		return runSecretMaintenance(ctx, store, flags.remediateSecrets, out)
	}
	opts := backup.Options{
		Backend: backend, ClearBeforeRestore: true,
		PluginsDir:          g.Cfg().MustGet(ctx, "etl.pluginsDir", "./data/plugins").String(),
		SecretFieldResolver: server.NewDescriptorSecretFieldResolver(),
	}
	if flags.backupFile != "" {
		counts, err := backup.ExportFile(ctx, store, flags.backupFile, opts)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(out, "backup complete: %s\ncounts: %+v\n", flags.backupFile, counts)
		return err
	}

	if err := backup.Restore(ctx, store, snap, opts); err != nil {
		return err
	}
	report, ok, err := backup.Reconcile(ctx, store, snap)
	if err != nil {
		return fmt.Errorf("restore committed; reconciliation failed: %w", err)
	}
	if !ok {
		return fmt.Errorf("restore committed; reconciliation mismatch:\n%s", report)
	}
	_, err = fmt.Fprintf(out, "restore complete: %s\n%s", flags.restoreFile, report)
	return err
}
