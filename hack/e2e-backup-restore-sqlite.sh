#!/bin/sh
# PR-1.3: SQLite backup → restore → reconcile smoke (always runnable, hermetic).
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "==> PR-1.3 backup/restore (SQLite)"
go test ./internal/etl/storage/backup/ ./internal/etl/storage/ \
  -count=1 -run 'TestBackupRestoreRoundTripSQLite|TestBackupRestoreClearsPreviousState|TestBackupRestoreUpgradePath/sqlite|TestRetentionPurgeAuditRunTasks|TestSchemaVersionsPresent' -v
echo "==> SQLite backup/restore: PASS"
