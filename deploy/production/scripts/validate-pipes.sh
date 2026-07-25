#!/usr/bin/env bash
# 对生产 pipes 做 validate/preflight（需 OpenETL 已启动且 .env 已注入）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT/.env}"
BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"
PIPES_DIR="${PIPES_DIR:-$ROOT/pipes}"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1090
  source "$ENV_FILE"
  set +a
fi

if [[ -z "${ETL_API_TOKEN:-}" ]]; then
  echo "ETL_API_TOKEN is empty" >&2
  exit 1
fi

auth=(-H "X-API-Token: ${ETL_API_TOKEN}" -H "Content-Type: application/yaml")

fail=0
for f in "$PIPES_DIR"/*.yaml; do
  [[ -f "$f" ]] || continue
  name="$(basename "$f")"
  echo "==> validate $name"
  # envsubst-like: expand ${VAR} and ${VAR:-default} for local validate body
  body="$(python3 - "$f" <<'PY'
import os, re, sys
path = sys.argv[1]
text = open(path, encoding="utf-8").read()
pat = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^}]*))?\}")

def repl(m):
    name, default = m.group(1), m.group(2)
    if name in os.environ:
        return os.environ[name]
    if default is not None:
        return default
    raise SystemExit(f"unresolved env: {name}")

print(pat.sub(repl, text), end="")
PY
)"
  code="$(curl -sS -o /tmp/openetl-validate.json -w "%{http_code}" \
    "${auth[@]}" \
    -d "$body" \
    "${BASE_URL}/api/v2/specs/validate")"
  echo "   http=$code"
  cat /tmp/openetl-validate.json
  echo
  if [[ "$code" != "200" ]]; then
    fail=1
  fi
done

exit "$fail"
