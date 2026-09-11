#!/usr/bin/env bash
# RA-8 packaging/startup/restore baseline; excludes pending CH specialty work.
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli
export PYTHONDONTWRITEBYTECODE=1
exec python3 "$ROOT_DIR/hack/bench_baseline.py" --root "$ROOT_DIR" --container-cli "$CONTAINER_CLI" "$@"
