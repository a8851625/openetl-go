#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
source "$ROOT/hack/container-cli.sh"
detect_container_cli
if [[ "$(uname -s)" == "Darwin" ]] && command -v caffeinate >/dev/null 2>&1; then
  # Keep the measuring VM active for this command only; do not change settings.
  exec caffeinate -i python3 "$ROOT/hack/bench_capacity.py" --root "$ROOT" --container-cli "$CONTAINER_CLI" "$@"
fi
exec python3 "$ROOT/hack/bench_capacity.py" --root "$ROOT" --container-cli "$CONTAINER_CLI" "$@"
