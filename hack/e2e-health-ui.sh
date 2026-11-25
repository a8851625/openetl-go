#!/usr/bin/env bash
set -euo pipefail
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli
IMAGE="${E2E_IMAGE:-openetl-go-etl:ui-e2e}"
if [ "${E2E_SKIP_BUILD:-0}" != "1" ]; then
  "$CONTAINER_CLI" build -t "$IMAGE" "$ROOT_DIR"
fi
python3 "$ROOT_DIR/hack/health_ui_e2e.py" --cli "$CONTAINER_CLI" --image "$IMAGE" \
  --out "${E2E_OUTPUT_DIR:-}"
