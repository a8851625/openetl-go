#!/usr/bin/env bash
# RA-6: real CLI streaming export, >100k EACH DLQ/audit/run, full content + RSS.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
backend="${1:-sqlite}"
case "$backend" in sqlite|mysql|postgres) ;; *) echo 'backend must be sqlite, mysql or postgres' >&2; exit 2 ;; esac
command -v go >/dev/null || { echo 'Go is required for the volume fixture' >&2; exit 2; }
command -v python3 >/dev/null || { echo 'python3 is required for process RSS measurement' >&2; exit 2; }
. "$ROOT/hack/container-cli.sh"
volume_dir="$(mktemp -d "${TMPDIR:-/tmp}/openetl-volume-${backend}.XXXXXX")"
volume_container=""
cleanup() {
  if [ -n "$volume_container" ]; then "$CONTAINER_CLI" rm -fv "$volume_container" >/dev/null; fi
  rm -rf "$volume_dir"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
unset ETL_STORAGE_DSN ETL_SPEC_ENCRYPTION_KEY ETL_SPEC_ENCRYPTION_KEY_ID ETL_SPEC_ENCRYPTION_PREVIOUS_KEYS
export BACKUP_VOLUME_SQLITE_PATH="$volume_dir/metadata.db" BACKUP_VOLUME_DSN=""
cli_backend="$backend"
if [ "$backend" != sqlite ]; then
  detect_container_cli
  name="openetl-volume-${backend}-$(date +%s)-$$"
  if [ "$backend" = mysql ]; then
    volume_container="$("$CONTAINER_CLI" run -d --name "$name" -e MYSQL_ROOT_PASSWORD=volume-drill-only -e MYSQL_DATABASE=openetl_volume -p 127.0.0.1::3306 docker.io/library/mysql:8.0)"
    volume_port="$("$CONTAINER_CLI" port "$volume_container" 3306 | head -n 1 | sed 's/.*://')"
    export BACKUP_VOLUME_DSN="root:volume-drill-only@tcp(127.0.0.1:${volume_port})/openetl_volume?parseTime=true&multiStatements=true"
    ready() { "$CONTAINER_CLI" exec "$volume_container" mysql --protocol=tcp -h127.0.0.1 -uroot -pvolume-drill-only -e 'SELECT 1' >/dev/null 2>&1; }
  else
    cli_backend=postgresql
    volume_container="$("$CONTAINER_CLI" run -d --name "$name" -e POSTGRES_PASSWORD=volume-drill-only -e POSTGRES_DB=openetl_volume -p 127.0.0.1::5432 docker.io/library/postgres:16-alpine)"
    volume_port="$("$CONTAINER_CLI" port "$volume_container" 5432 | head -n 1 | sed 's/.*://')"
    export BACKUP_VOLUME_DSN="postgres://postgres:volume-drill-only@127.0.0.1:${volume_port}/openetl_volume?sslmode=disable"
    ready() { "$CONTAINER_CLI" exec "$volume_container" pg_isready -h127.0.0.1 -U postgres -d openetl_volume >/dev/null 2>&1; }
  fi
  for ((attempt=0; attempt<60; attempt++)); do if ready; then break; fi; sleep 1; done
  if ! ready; then echo "$backend did not become ready" >&2; exit 1; fi
fi
printf 'etl:\n  profile: development\n' > "$volume_dir/config.yaml"
CGO_ENABLED=0 go build -o "$volume_dir/openetl-go" .
CGO_ENABLED=0 go build -o "$volume_dir/fixture" ./hack/cmd/backup-volume-fixture
fixture() { "$volume_dir/fixture" "$1" "$backend" "$2" "${@:3}"; }
cli_args=(--config "$volume_dir/config.yaml" --storage "$cli_backend" --data-dir "$volume_dir/data" --sqlite-path "$BACKUP_VOLUME_SQLITE_PATH")
measure() {
  local label="$1" flag="$2" artifact="$3"
  ETL_STORAGE_DSN="$BACKUP_VOLUME_DSN" python3 - "$volume_dir/$label.json" "$volume_dir/openetl-go" "${cli_args[@]}" "$flag" "$artifact" <<'PY'
import json, pathlib, resource, subprocess, sys, time
report = pathlib.Path(sys.argv[1])
start = time.monotonic()
result = subprocess.run(sys.argv[2:], check=False)
usage = resource.getrusage(resource.RUSAGE_CHILDREN)
peak_bytes = usage.ru_maxrss if sys.platform == 'darwin' else usage.ru_maxrss * 1024
record = {'operation':sys.argv[-2], 'elapsed_seconds':round(time.monotonic()-start,6), 'peak_rss_bytes':peak_bytes, 'measurement':'child process getrusage ru_maxrss (not Go heap)', 'exit_code':result.returncode}
report.write_text(json.dumps(record,indent=2)+'\n')
print(json.dumps(record),flush=True)
if result.returncode != 0:
    raise SystemExit(result.returncode)
PY
}

small=10003
large=100037
echo "==> $backend: small streaming export ($small rows per history table)"
fixture seed "$small"
measure export-small --backup-file "$volume_dir/small.json"
fixture verify-file "$small" "$volume_dir/small.json"
rm "$volume_dir/small.json"

echo "==> $backend: full-content acceptance ($large rows EACH DLQ/audit/run)"
fixture extend "$large"
fixture fingerprint "$large" > "$volume_dir/before.json"
measure export-large --backup-file "$volume_dir/large.json"
fixture verify-file "$large" "$volume_dir/large.json"
measure restore-large --restore-file "$volume_dir/large.json"
fixture fingerprint "$large" > "$volume_dir/after.json"
python3 - "$volume_dir" "$backend" <<'PY'
import hashlib, json, pathlib, sys
root = pathlib.Path(sys.argv[1])
before = json.loads((root/'before.json').read_text())
after = json.loads((root/'after.json').read_text())
if before != after:
    changed = [key for key in before if before[key] != after.get(key)]
    raise SystemExit('restored raw SQL content differs: '+', '.join(changed))
small = json.loads((root/'export-small.json').read_text())
large = json.loads((root/'export-large.json').read_text())
# A generous shape check catches full-table accumulation. These bounds do not
# claim a production capacity limit and do not tune runtime defaults.
if large['peak_rss_bytes'] > 2 * small['peak_rss_bytes'] + 64 * 1024 * 1024:
    raise SystemExit('export RSS grew beyond the bounded-page tolerance')
report = {'backend':sys.argv[2], 'result':'passed', 'rows_per_history_table':100037, 'small_rows_per_history_table':10003, 'export_small':small, 'export_large':large, 'restore_large':json.loads((root/'restore-large.json').read_text()), 'backup_bytes':(root/'large.json').stat().st_size, 'content_verification':'every DLQ/audit/run field versus generated input; independent raw SQL hashes before/after restore; 2003 terminal/orphan tasks', 'sql_inventory':before}
with (root/'large.json').open('rb') as f:
    report['backup_sha256'] = hashlib.file_digest(f, 'sha256').hexdigest()
(root/'result.json').write_text(json.dumps(report,indent=2)+'\n')
print(json.dumps(report),flush=True)
PY
if [ -n "${BACKUP_VOLUME_OUTPUT:-}" ]; then
  archive="$BACKUP_VOLUME_OUTPUT/$backend"
  mkdir -p "$archive"
  for file in before.json after.json export-small.json export-large.json restore-large.json result.json; do cp "$volume_dir/$file" "$archive/$file"; done
  go version > "$archive/go-version.txt"
  git rev-parse HEAD > "$archive/head.txt"
fi
echo "RA-6 $backend volume backup/restore e2e PASS"
