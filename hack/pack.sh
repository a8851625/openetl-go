#!/usr/bin/env bash
# Pack resource/ into internal/packed/packed.go so the binary embeds the UI
# via gres and can run standalone (no sibling resource/ directory required).
#
# Used by:
#   - goreleaser before.hooks (.goreleaser.yml) so release artifacts embed UI
#   - `make build` indirectly via `gf build -ew`
#
# This script also (re)builds the frontend from web/ into resource/public so
# releases always ship current UI source — without this, goreleaser would
# pack whatever stale resource/public happens to be committed.
#
# Idempotent: safe to re-run. Exits non-zero on failure so CI fails fast.
set -euo pipefail

cd "$(dirname "$0")/.."

SRC="${SRC:-resource}"
DST="${DST:-internal/packed/packed.go}"

# ── 1. Build frontend (web/ → resource/public) ─────────────────────────
# Skip only when SKIP_UI=1 (e.g. local `make build` where you know the UI
# is already built). goreleaser should leave this unset so releases always
# rebuild from source.
if [[ "${SKIP_UI:-0}" != "1" ]]; then
  if [[ -d web && -f web/package.json ]]; then
    echo "Building frontend (web/ → resource/public)..."
    # Prefer local npm; fall back to the node:20-alpine container so this
    # works on CI runners that have neither node nor Docker.
    if command -v npm >/dev/null 2>&1; then
      (cd web && npm install --no-audit --no-fund && npm run build)
    elif command -v docker >/dev/null 2>&1; then
      echo "npm not found locally; building via docker node:20-alpine"
      docker run --rm -v "$PWD:/workspace" -w /workspace/web docker.io/library/node:20-alpine \
        sh -c 'npm install --no-audit --no-fund && npm run build'
    else
      echo "WARNING: neither npm nor a container runtime is available;" \
           "packing the existing resource/public as-is." >&2
    fi
  fi
fi

# ── 2. gf pack resource/ → internal/packed/packed.go ───────────────────
# Use project-local gf if installed by `make cli`, else download a binary
# matching the current OS/arch (GoFrame ships prebuilt releases).
if command -v gf >/dev/null 2>&1; then
  GF_BIN="$(command -v gf)"
else
  case "$(uname -s)" in
    Linux*)  GOOS=linux   ;;
    Darwin*) GOOS=darwin  ;;
    *)       GOOS="$(uname -s | tr '[:upper:]' '[:lower:]')" ;;
  esac
  case "$(uname -m)" in
    x86_64|amd64) GOARCH=amd64 ;;
    aarch64|arm64) GOARCH=arm64 ;;
    *)             GOARCH="$(uname -m)" ;;
  esac
  GF_BIN="/tmp/gf-$$"
  URL="https://github.com/gogf/gf/releases/latest/download/gf_${GOOS}_${GOARCH}"
  echo "Downloading gf CLI: $URL"
  curl -fsSL "$URL" -o "$GF_BIN"
  chmod +x "$GF_BIN"
  trap 'rm -f "$GF_BIN"' EXIT
fi

# gf pack refuses to overwrite a non-empty dst without confirmation.
# packed.go is committed (so plain `go build` works), so pipe "y" through.
echo "Packing $SRC -> $DST via ${GF_BIN}"
printf 'y\n' | "$GF_BIN" pack "$SRC" "$DST" --keepPath=true

if ! grep -q 'gres.Add' "$DST"; then
  echo "ERROR: $DST does not contain gres.Add — pack failed" >&2
  exit 1
fi
echo "OK: $DST ($(wc -c < "$DST") bytes, gres.Add present)"
