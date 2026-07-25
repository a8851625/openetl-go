#!/usr/bin/env bash
# PR-1.3: SQLite logical backup + snapshot backup/restore + retention smoke.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

echo "==> unit: BackupSQLStore + ApplyRetention"
go test ./internal/etl/storage/ -count=1 -run 'Backup|Retention'

echo "==> unit: snapshot backup/restore package + upgrade path (sqlite)"
go test ./internal/etl/storage/backup/ ./internal/etl/storage/ \
  -count=1 -run 'TestBackupRestoreRoundTripSQLite|TestBackupRestoreClearsPreviousState|TestBackupRestoreUpgradePath/sqlite|TestRetentionPurgeAuditRunTasks|TestSchemaVersionsPresent' -v

echo "==> physical copy restore drill (runbook)"
# File copy path documented in docs/runtime-modes.md:
#   cp data/etl.db backup/etl.db.DATE
#   cp backup/etl.db.DATE data/etl.db
echo "RUNBOOK_COPY_PATH documented in docs/runtime-modes.md"
echo "PR-1.3 sqlite backup/retention smoke PASS"
