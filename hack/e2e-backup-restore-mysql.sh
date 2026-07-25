#!/usr/bin/env bash
# PR-1.3: MySQL logical backup + snapshot backup/restore against throwaway container.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

. "$ROOT/hack/container-cli.sh"
detect_container_cli

CONTAINER="etl-backup-test-mysql"
DB="openetl_backup"
ROOT_PASS="root123456"
HOST_PORT="13401"

cleanup() {
  "$CONTAINER_CLI" rm -f "$CONTAINER" >/dev/null 2>&1 || true
}
trap cleanup EXIT INT TERM

echo "==> Start throwaway MySQL 8 for backup e2e (port $HOST_PORT)"
cleanup
"$CONTAINER_CLI" run -d --name "$CONTAINER" \
  --add-host host.docker.internal:host-gateway \
  -e MYSQL_ROOT_PASSWORD="$ROOT_PASS" \
  -e MYSQL_DATABASE="$DB" \
  -p "$HOST_PORT:3306" \
  docker.io/library/mysql:8.0 >/dev/null

echo "==> Wait for MySQL"
i=0
while [ "$i" -lt 60 ]; do
  if "$CONTAINER_CLI" exec "$CONTAINER" mysql -h localhost -u root -p"$ROOT_PASS" -e "SELECT 1" >/dev/null 2>&1; then
    break
  fi
  i=$((i + 1)); sleep 2
done
if [ "$i" -ge 60 ]; then
  echo "!! MySQL not ready"; exit 1
fi

export MYSQL_DSN="root:${ROOT_PASS}@tcp(127.0.0.1:${HOST_PORT})/${DB}?parseTime=true&multiStatements=true"

echo "==> unit: BackupSQLStore + ApplyRetention"
go test ./internal/etl/storage/ -count=1 -run 'Backup|Retention'

OUT="$(mktemp -d)"
echo "==> MySQL BackupSQLStore smoke → $OUT"
go run ./hack/cmd/mysql-backup-smoke "$OUT"

echo "==> Snapshot backup/restore + upgrade path against MySQL"
if "$CONTAINER_CLI" ps --format '{{.Names}}' 2>/dev/null | grep -q '^etl-go-dev$'; then
  DEV_DSN="root:${ROOT_PASS}@tcp(host.docker.internal:${HOST_PORT})/${DB}?parseTime=true&multiStatements=true"
  "$CONTAINER_CLI" exec -e MYSQL_DSN="$DEV_DSN" -w /workspace etl-go-dev \
    go test -count=1 -v -run 'TestBackupRestoreUpgradePath/mysql' ./internal/etl/storage/
else
  echo "   (host Go toolchain)"
  go test -count=1 -v -run 'TestBackupRestoreUpgradePath/mysql' ./internal/etl/storage/
fi

echo "PR-1.3 MySQL backup e2e PASS"
