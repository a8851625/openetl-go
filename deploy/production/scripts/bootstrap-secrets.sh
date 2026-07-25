#!/usr/bin/env bash
# 生成生产 .env 中的密钥字段（不覆盖已有非空值）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
ENV_FILE="${1:-$ROOT/.env}"
EXAMPLE="$ROOT/.env.example"

if [[ ! -f "$ENV_FILE" ]]; then
  if [[ -f "$EXAMPLE" ]]; then
    cp "$EXAMPLE" "$ENV_FILE"
    echo "created $ENV_FILE from .env.example"
  else
    echo "missing $EXAMPLE" >&2
    exit 1
  fi
fi

set_if_empty() {
  local key="$1"
  local value="$2"
  if grep -qE "^${key}=$" "$ENV_FILE" || grep -qE "^${key}=\s*$" "$ENV_FILE"; then
    # portable in-place replace for empty assignment
    if [[ "$(uname -s)" == "Darwin" ]]; then
      sed -i '' "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
    else
      sed -i "s|^${key}=.*|${key}=${value}|" "$ENV_FILE"
    fi
    echo "set $key"
  else
    echo "skip $key (already set)"
  fi
}

set_if_empty ETL_API_TOKEN "$(openssl rand -hex 32)"
set_if_empty ETL_SPEC_ENCRYPTION_KEY "$(openssl rand -base64 32)"
set_if_empty MYSQL_ROOT_PASSWORD "$(openssl rand -hex 16)"
set_if_empty MYSQL_PASSWORD "$(openssl rand -hex 16)"
set_if_empty REDIS_PASSWORD "$(openssl rand -hex 16)"

# 业务 Redis 默认同 state Redis 密码（同实例 db1）
if grep -qE "^REDIS_PASSWORD=.+$" "$ENV_FILE"; then
  redis_pw="$(grep -E "^REDIS_PASSWORD=" "$ENV_FILE" | head -1 | cut -d= -f2-)"
  set_if_empty CACHE_REDIS_PASSWORD "$redis_pw"
fi

echo
echo "Review and fill business endpoints in: $ENV_FILE"
echo "  KAFKA_BROKER_*, ODS_MYSQL_*, CACHE_REDIS_* (if different from state redis)"
