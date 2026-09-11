package storage

import "context"

// BackupRow is one retained row, including history without a current pipeline.
// Key is its primary key encoded as text; Value is the corresponding storage
// record pointer, or *BackupSetting for settings. It never contains decrypted
// credentials. Keys and values remain owned by the caller after a page returns.
type BackupRow struct {
	Key   string
	Value any
}

type BackupSetting struct {
	Key   string
	Value string
}

// BackupReader is the complete, bounded maintenance view of the control plane.
// ReadBackupPage orders by the primary key, strictly after afterKey (nil starts
// at the first row), and returns up to limit rows. Runtime list filters/limits
// must not affect this view. Callers must stop all writers for a consistent
// multi-table backup; this interface does not promise an online snapshot.
type BackupReader interface {
	ReadBackupPage(ctx context.Context, table string, afterKey *string, limit int) ([]BackupRow, error)
	CountObjects(context.Context) (ObjectCounts, error)
	SchemaVersions(context.Context) ([]SchemaVersionRow, error)
}
