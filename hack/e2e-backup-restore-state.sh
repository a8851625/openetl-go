#!/usr/bin/env bash
# Offline, coordinated metadata + Redis RDB restore drill. No live writer.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
. "$ROOT/hack/container-cli.sh"
detect_container_cli
command -v go >/dev/null || { echo "Go is required; run this drill in the documented Go development environment" >&2; exit 2; }
work_dir="$(mktemp -d "${TMPDIR:-/tmp}/openetl-state-backup.XXXXXX")"
source_id=""
restored_id=""
cleanup() {
  if [ -n "$source_id" ]; then "$CONTAINER_CLI" rm -fv "$source_id" >/dev/null; fi
  if [ -n "$restored_id" ]; then "$CONTAINER_CLI" rm -fv "$restored_id" >/dev/null; fi
  rm -rf "$work_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
ready() {
  local id="$1"
  for ((i=0; i<30; i++)); do
    if "$CONTAINER_CLI" exec "$id" redis-cli ping 2>/dev/null | rg -q '^PONG$'; then return; fi
    sleep 1
  done
  echo "Redis failed to become ready" >&2; exit 1
}
name="openetl-state-backup-$(date +%s)-$$"
source_id="$("$CONTAINER_CLI" run -d --name "$name-source" -p 127.0.0.1::6379 docker.io/library/redis:7-alpine redis-server --appendonly no)"
ready "$source_id"
source_port="$("$CONTAINER_CLI" port "$source_id" 6379 | head -n 1 | sed 's/.*://')"
go run ./hack/cmd/redis-backup-drill seed "127.0.0.1:$source_port" "$work_dir"
# seed exits and closes every writer before either artifact is finalized.
"$CONTAINER_CLI" exec "$source_id" redis-cli --rdb /tmp/state.rdb
"$CONTAINER_CLI" cp "$source_id:/tmp/state.rdb" "$work_dir/dump.rdb"
"$CONTAINER_CLI" stop "$source_id" >/dev/null

restored_id="$("$CONTAINER_CLI" create --name "$name-restored" -p 127.0.0.1::6379 docker.io/library/redis:7-alpine redis-server --appendonly no)"
"$CONTAINER_CLI" cp "$work_dir/dump.rdb" "$restored_id:/data/dump.rdb"
"$CONTAINER_CLI" start "$restored_id" >/dev/null
ready "$restored_id"
restored_port="$("$CONTAINER_CLI" port "$restored_id" 6379 | head -n 1 | sed 's/.*://')"
go run ./hack/cmd/redis-backup-drill verify "127.0.0.1:$restored_port" "$work_dir"
echo "PR-1.3 offline SQL + Redis RDB backup/restore drill PASS"
