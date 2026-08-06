#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# gen-doris-specs-grouped.sh
# 按「真实主键」分组生成 mysql_snapshot_cdc → doris spec（spec 数最少的方案）
#
# 原理：mysql_snapshot_cdc 的 tables 支持数组（多表聚合），但 pk_column 是
#       全局的。所以把「主键列完全相同」的表聚到一个 spec，主键唯一的表
#       独立成 spec。对主键高度碎片化的库，spec 数从「表数」降到「主键种类数」。
#
# 多表模式下用 table_mapping 把每个源表路由到 Doris 的 ods_<源表名>。
#
# 用法：
#   MYSQL_CLI="docker exec <容器> mysql" \
#   MYSQL_HOST=... MYSQL_PORT=33067 MYSQL_USER=root MYSQL_PASS=xxx MYSQL_DB=cct_test \
#   DORIS_HOST=doris-fe DORIS_DB=ods_cct SERVER_ID_BASE=2500 CRON_EXPR="0 2 * * *" \
#   bash gen-doris-specs-grouped.sh ./pipes-dwh/cct_test
# ─────────────────────────────────────────────────────────────────────
set -euo pipefail

OUT_DIR="${1:?output dir required}"
MYSQL_CLI="${MYSQL_CLI:-mysql}"
MYSQL_HOST="${MYSQL_HOST:?MYSQL_HOST required}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:?MYSQL_USER required}"
MYSQL_PASS="${MYSQL_PASS:?MYSQL_PASS required}"
MYSQL_DB="${MYSQL_DB:?MYSQL_DB required}"
DORIS_HOST="${DORIS_HOST:-doris-fe}"
DORIS_DB="${DORIS_DB:-ods}"
SERVER_ID_BASE="${SERVER_ID_BASE:-2500}"
CRON_EXPR="${CRON_EXPR:-0 2 * * *}"
SPREAD_WINDOW_MIN="${SPREAD_WINDOW_MIN:-120}"

run_mysql() { $MYSQL_CLI -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASS" -N -B "$@" 2>&1 | grep -v "Warning"; }

mkdir -p "$OUT_DIR"
WORK_DIR="$(mktemp -d)"; trap 'rm -rf "$WORK_DIR"' EXIT

# 每表：表名 \t 主键（多列逗号拼；无主键 __nopk__）
ROWS=$(run_mysql -e "
SELECT t.TABLE_NAME,
       COALESCE((SELECT GROUP_CONCAT(k.COLUMN_NAME ORDER BY k.ORDINAL_POSITION)
                 FROM information_schema.KEY_COLUMN_USAGE k
                 WHERE k.TABLE_SCHEMA=t.TABLE_SCHEMA AND k.TABLE_NAME=t.TABLE_NAME AND k.CONSTRAINT_NAME='PRIMARY'),
                '__nopk__') AS pk_cols
FROM information_schema.TABLES t
WHERE t.TABLE_SCHEMA='${MYSQL_DB}' AND t.TABLE_TYPE='BASE TABLE'
ORDER BY t.TABLE_NAME;")

[ -z "$ROWS" ] && { echo "⚠️  库 ${MYSQL_DB} 无基础表"; exit 0; }

# 按主键分组（临时文件当关联数组，兼容 bash 3.2）
GROUP_COUNT=0
TABLE_COUNT=0
safe() { echo "$1" | tr ',/' '__' | tr -dc 'A-Za-z0-9_'; }
while IFS=$'\t' read -r tbl pk; do
  key="$(safe "$pk")"
  f="$WORK_DIR/$key.list"
  [ ! -f "$f" ] && { : > "$f"; printf '%s\n' "$pk" > "$WORK_DIR/$key.pk"; GROUP_COUNT=$((GROUP_COUNT+1)); }
  printf '%s\n' "$tbl" >> "$f"
  TABLE_COUNT=$((TABLE_COUNT+1))
done <<< "$ROWS"

echo "==> ${MYSQL_DB}: ${TABLE_COUNT} 张表，主键 ${GROUP_COUNT} 种 → 生成 ${GROUP_COUNT} 个聚合 spec"

BASE_MIN=$(echo "$CRON_EXPR" | awk '{print $1}')
BASE_HR=$(echo "$CRON_EXPR" | awk '{print $2}')
DOM=$(echo "$CRON_EXPR" | awk '{print $3}'); MON=$(echo "$CRON_EXPR" | awk '{print $4}'); DOW=$(echo "$CRON_EXPR" | awk '{print $5}')

i=0
for list_file in "$WORK_DIR"/*.list; do
  [ -f "$list_file" ] || continue
  i=$((i + 1))
  pk="$(cat "${list_file%.list}.pk")"
  tables=$(cat "$list_file")           # 每行一个表名
  tbl_count=$(echo "$tables" | wc -l | tr -d ' ')

  # cron 错峰（窗口内取模）
  offset=$(( (i - 1) % SPREAD_WINDOW_MIN ))
  total_min=$((BASE_MIN + offset))
  this_hr=$(( (BASE_HR + total_min / 60) % 24 ))
  this_min=$((total_min % 60))
  this_cron="${this_min} ${this_hr} ${DOM} ${MON} ${DOW}"
  server_id=$((SERVER_ID_BASE + i))

  # YAML tables 列表
  yaml_tables=$(echo "$tables" | sed 's/^/      - "/; s/$/"/')

  # 主键处理
  if [ "$pk" = "__nopk__" ]; then
    pk_cursor="id"
    pk_cols_yaml="[]"
    note="# ⚠️ 本组表无主键，需手工指定 pk_column / pk_columns"
  else
    pk_cursor=$(echo "$pk" | cut -d',' -f1)
    pk_cols_yaml=$(echo "$pk" | sed 's/,/", "/g; s/.*/[ "&" ]/')
    note=""
  fi

  group_name="$(safe "$pk")"
  out="${OUT_DIR}/group-${group_name}.yaml"
  cat > "$out" <<EOF
${note}
name: "${MYSQL_DB}-group-${group_name}"
tags: ["dwh", "ods", "doris", "auto", "${MYSQL_DB}", "grouped"]

schedule:
  type: cron
  cron: "${this_cron}"     # 本组 ${tbl_count} 张表一次性抽数

source:
  type: mysql_snapshot_cdc
  config:
    host: "${MYSQL_HOST}"
    port: ${MYSQL_PORT}
    user: "${MYSQL_USER}"
    password: "${MYSQL_PASS}"
    database: "${MYSQL_DB}"
    server_id: ${server_id}
    tables:
${yaml_tables}
    pk_column: "${pk_cursor}"     # 本组所有表共用此主键列
    limit: 1000

# 多表路由：每个源表 → Doris 的 ods_<源表名>
table_mapping:
  template: "ods_{source_table}"

sink:
  type: doris
  config:
    host: "${DORIS_HOST}"
    port: 9030
    http_port: 8030
    user: "root"
    password: ""
    database: "${DORIS_DB}"
    # table 省略：按 table_mapping 后的表名逐表路由，sink 自动建表
    write_mode: "stream_load"
    stream_load_format: "json"
    pk_columns: ${pk_cols_yaml}   # 本组所有表共用此 UNIQUE KEY
    auto_create: true
    schema_drift: "add_columns"

batch_size: 1000
checkpoint_interval_sec: 10
backpressure_buffer: 1000
retry: { max_attempts: 3, initial_interval_ms: 100, max_interval_ms: 1000 }
dlq: { enable: true }
EOF
  printf '  ✓ group-%-30s %2d 张表  pk=[%s]\n' "$group_name" "$tbl_count" "$pk"
done

echo ""
echo "==> 完成！${GROUP_COUNT} 个聚合 spec 已生成到 ${OUT_DIR}/"
echo "    （相比一表一 spec 的 ${TABLE_COUNT} 个，减少了 $((TABLE_COUNT - GROUP_COUNT)) 个）"
echo ""
echo "⚠️ 复核："
echo "   1. 无主键组 (group-nopk.yaml)：需手工指定 pk_column"
echo "   2. 复合主键组：pk_column 取第一列做游标"
echo "   3. 多库时 server_id 用不同区间（本库 ${SERVER_ID_BASE}+）"
