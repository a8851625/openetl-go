#!/usr/bin/env bash
# 生产部署冒烟：health + 管道列表 + 可选 metrics。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${ENV_FILE:-$ROOT/.env}"
BASE_URL="${BASE_URL:-http://127.0.0.1:8000}"

if [[ -f "$ENV_FILE" ]]; then
  # shellcheck disable=SC1090
  set -a
  # only export simple KEY=VAL lines
  # shellcheck disable=SC1091
  source "$ENV_FILE"
  set +a
fi

if [[ -z "${ETL_API_TOKEN:-}" ]]; then
  echo "ETL_API_TOKEN is empty; set it in $ENV_FILE or environment" >&2
  exit 1
fi

auth=(-H "X-API-Token: ${ETL_API_TOKEN}")

echo "==> health"
curl -fsS "${auth[@]}" "${BASE_URL}/api/v2/health" | tee /tmp/openetl-health.json
echo

echo "==> pipelines"
curl -fsS "${auth[@]}" "${BASE_URL}/api/v2/pipelines" | tee /tmp/openetl-pipelines.json
echo

echo "==> metrics (first 20 lines)"
curl -fsS "${BASE_URL}/metrics" | head -n 20 || true
echo

echo "smoke ok"
