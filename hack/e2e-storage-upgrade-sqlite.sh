#!/bin/sh
# PR-1.3: SQLite forward upgrade from legacy schema + failure blocks startup.
set -eu

ROOT_DIR="$(CDPATH= cd -- "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

echo "==> PR-1.3 storage upgrade (SQLite legacy → current)"
go test ./internal/etl/storage/ -count=1 -v \
  -run 'TestSQLiteForwardUpgradeFromLegacySchema|TestSQLiteUpgradeFailureBlocksStartup'
echo "==> SQLite storage upgrade: PASS"
