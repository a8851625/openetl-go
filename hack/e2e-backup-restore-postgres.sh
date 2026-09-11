#!/usr/bin/env bash
# PR-1.3 / IT-3: isolated, executable backup/restore acceptance drill.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
exec bash "$ROOT/hack/backup-restore-e2e.sh" postgres
