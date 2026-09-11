#!/usr/bin/env bash
# Shared PR-1.3 / IT-3 restore drill. Called by e2e-backup-restore-{backend}.sh.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
backend="${1:?backend is required}"
case "$backend" in sqlite|mysql|postgres) ;; *) echo "unknown backend: $backend" >&2; exit 2 ;; esac
. "$ROOT/hack/container-cli.sh"

work_dir="$(mktemp -d "${TMPDIR:-/tmp}/openetl-backup-${backend}.XXXXXX")"
created_container=""
created_network=""
network_args=()
cleanup() {
  if [ -n "$created_container" ]; then
    "$CONTAINER_CLI" rm -f "$created_container" >/dev/null
  fi
  if [ -n "$created_network" ]; then
    "$CONTAINER_CLI" network rm "$created_network" >/dev/null
  fi
  rm -rf "$work_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

use_host_go=false
backup_host="host.docker.internal"
if command -v go >/dev/null 2>&1; then
  use_host_go=true
  backup_host="127.0.0.1"
else
  detect_container_cli
fi
run_go() {
  if "$use_host_go"; then
    go "$@"
  else
    "$CONTAINER_CLI" run --rm ${network_args[@]+"${network_args[@]}"} \
      -v "$ROOT:/workspace" -v "$work_dir:$work_dir" \
      -v openetl-go_go-cache:/go -v openetl-go_go-build-cache:/root/.cache/go-build \
      -w /workspace -e CGO_ENABLED -e MYSQL_DSN -e POSTGRES_DSN \
      -e BACKUP_TEST_MYSQL_DSN -e BACKUP_TEST_POSTGRES_DSN -e BACKUP_TEST_REQUIRED \
      -e BACKUP_CLI_BINARY -e BACKUP_CLI_BACKEND -e BACKUP_CLI_DSN \
      "${GO_TEST_IMAGE:-etl-go-dev:latest}" go "$@"
  fi
}

# Never borrow an ambient test/production DSN. The optional backend tests below
# can only reach the service created by this invocation.
unset MYSQL_DSN POSTGRES_DSN BACKUP_TEST_MYSQL_DSN BACKUP_TEST_POSTGRES_DSN BACKUP_CLI_DSN
if [ "$backend" != "sqlite" ]; then
  detect_container_cli
  name="openetl-backup-${backend}-$(date +%s)-$$"
  if ! "$use_host_go"; then
    created_network="$("$CONTAINER_CLI" network create "$name-net")"
    network_args=(--network "$created_network")
    backup_host="$name"
  fi
  if [ "$backend" = "mysql" ]; then
    created_container="$("$CONTAINER_CLI" run -d --name "$name" ${network_args[@]+"${network_args[@]}"} \
      -e MYSQL_ROOT_PASSWORD=backup-drill-only -e MYSQL_DATABASE=openetl_backup \
      -p 127.0.0.1::3306 docker.io/library/mysql:8.0)"
    port="$("$CONTAINER_CLI" port "$created_container" 3306 | head -n 1 | sed 's/.*://')"
    if ! "$use_host_go"; then port=3306; fi
    ready() { "$CONTAINER_CLI" exec "$created_container" mysql --protocol=tcp -h127.0.0.1 -uroot -pbackup-drill-only -e 'SELECT 1' >/dev/null 2>&1; }
    export MYSQL_DSN="root:backup-drill-only@tcp(${backup_host}:${port})/openetl_backup?parseTime=true&multiStatements=true"
    export BACKUP_TEST_MYSQL_DSN="$MYSQL_DSN" BACKUP_CLI_DSN="$MYSQL_DSN"
  else
    created_container="$("$CONTAINER_CLI" run -d --name "$name" ${network_args[@]+"${network_args[@]}"} \
      -e POSTGRES_PASSWORD=backup-drill-only -e POSTGRES_DB=openetl_backup \
      -p 127.0.0.1::5432 docker.io/library/postgres:16-alpine)"
    port="$("$CONTAINER_CLI" port "$created_container" 5432 | head -n 1 | sed 's/.*://')"
    if ! "$use_host_go"; then port=5432; fi
    ready() { "$CONTAINER_CLI" exec "$created_container" pg_isready -h127.0.0.1 -U postgres -d openetl_backup >/dev/null 2>&1; }
    export POSTGRES_DSN="postgres://postgres:backup-drill-only@${backup_host}:${port}/openetl_backup?sslmode=disable"
    export BACKUP_TEST_POSTGRES_DSN="$POSTGRES_DSN" BACKUP_CLI_DSN="$POSTGRES_DSN"
  fi
  for ((attempt=0; attempt<60; attempt++)); do
    if ready; then break; fi
    sleep 1
  done
  if ! ready; then echo "$backend did not become ready" >&2; exit 1; fi
fi

export BACKUP_TEST_REQUIRED="$backend"
echo "==> $backend: exact row fidelity, v1/v2, SQL failure rollback, artifact relocation, next IDs"
run_go test -race -count=1 -timeout=180s -v -run "^TestBackupRestoreConformance$/${backend}$" ./internal/etl/storage/backup

echo "==> $backend: fresh executable offline backup/restore and failed restore recovery"
export BACKUP_CLI_BINARY="$work_dir/openetl-go" BACKUP_CLI_BACKEND="$backend"
if [ "$backend" = "postgres" ]; then export BACKUP_CLI_BACKEND=postgresql; fi
CGO_ENABLED=0 run_go build -o "$BACKUP_CLI_BINARY" .
run_go test -count=1 -timeout=180s -v -run '^TestBackupMaintenanceCLI$' ./internal/cmd

echo "==> $backend: existing backup/restore and retention regression"
run_go test -count=1 -timeout=120s -v -run "^TestBackupRestoreUpgradePath$/${backend}$" ./internal/etl/storage
run_go test -count=1 -timeout=120s -run 'BackupSQLStore|ApplyRetention' ./internal/etl/storage
if [ "$backend" != "sqlite" ]; then
  run_go run "./hack/cmd/${backend}-backup-smoke" "$work_dir/logical"
fi
echo "PR-1.3 $backend backup/restore e2e PASS"
