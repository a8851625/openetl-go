package backup

import (
	"context"
	"fmt"

	"github.com/a8851625/openetl-go/internal/etl/storage"
)

type restoreStore interface {
	txRunner
	fidelityWriter
	SetSetting(context.Context, string, string) error
}

// Restore is an offline maintenance operation. All SQL changes share one
// transaction, including when the caller supplies the runtime secret wrapper.
// Plugin bytes are durably staged at fresh paths before those paths can become
// visible in the database. Existing plugin files are never overwritten.
func Restore(ctx context.Context, store storage.Storage, snap *Snapshot, opts Options) error {
	if store == nil || snap == nil {
		return fmt.Errorf("backup: store and snapshot are required")
	}
	if snap.FormatVersion != 0 && snap.FormatVersion != 1 && snap.FormatVersion != FormatVersion {
		return fmt.Errorf("backup: unsupported format_version %d", snap.FormatVersion)
	}
	raw := storage.UnwrapStorage(store)
	writer, ok := raw.(restoreStore)
	if !ok {
		return fmt.Errorf("backup: %T does not support atomic, fidelity-preserving restore", raw)
	}
	plugins, stagedDir, err := stageRestoreArtifacts(ctx, snap, opts)
	if err != nil {
		return err
	}
	err = writer.WithTx(ctx, func(txCtx context.Context) error {
		if opts.ClearBeforeRestore {
			if err := writer.WipeControlPlane(txCtx); err != nil {
				return fmt.Errorf("backup: clear before restore: %w", err)
			}
		}
		return restoreRows(txCtx, writer, snap, plugins)
	})
	if err != nil && stagedDir != "" {
		// A transport failure during COMMIT can have an unknown outcome. Never
		// delete files that the database might now reference. Unreferenced
		// generations are harmless and can be removed after reconciliation.
		return fmt.Errorf("%w (plugin generation retained at %s; reconcile database references before cleanup)", err, stagedDir)
	}
	return err
}

func restoreRows(ctx context.Context, w restoreStore, snap *Snapshot, plugins []*storage.PluginEntry) error {
	steps := []func() error{
		func() error { return restoreTable(ctx, "pipelines", snap.Pipelines, w.WritePipelineFidelity) },
		func() error {
			return restoreTable(ctx, "pipeline_versions", snap.Versions, w.WritePipelineVersionFidelity)
		},
		func() error { return restoreTable(ctx, "checkpoints", snap.Checkpoints, w.WriteCheckpointFidelity) },
		func() error { return restoreTable(ctx, "dead_letters", snap.DeadLetters, w.WriteDeadLetterFidelity) },
		func() error { return restoreTable(ctx, "audit_logs", snap.AuditLogs, w.WriteAuditFidelity) },
		func() error { return restoreTable(ctx, "run_history", snap.RunHistory, w.WriteRunRecordFidelity) },
		func() error { return restoreTable(ctx, "workers", snap.Workers, w.WriteWorkerFidelity) },
		func() error { return restoreTable(ctx, "task_assignments", snap.Tasks, w.WriteTaskFidelity) },
		func() error { return restoreTable(ctx, "plugins", plugins, w.WritePluginFidelity) },
		func() error { return restoreTable(ctx, "connections", snap.Connections, w.WriteConnectionFidelity) },
	}
	for _, step := range steps {
		if err := step(); err != nil {
			return err
		}
	}
	for key, value := range snap.Settings {
		if err := w.SetSetting(ctx, key, value); err != nil {
			return fmt.Errorf("backup: restore setting %q: %w", key, err)
		}
	}
	return w.AdvanceBackupSequences(ctx)
}

func restoreTable[T any](ctx context.Context, table string, rows []*T, write func(context.Context, *T) error) error {
	for i, row := range rows {
		if row == nil {
			return fmt.Errorf("backup: nil row in %s at index %d", table, i)
		}
		if err := write(ctx, row); err != nil {
			return fmt.Errorf("backup: restore %s row %d: %w", table, i, err)
		}
	}
	return nil
}
