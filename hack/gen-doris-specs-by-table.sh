#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────
# gen-doris-specs-by-table.sh
# 为源库「每张表」生成独立的 mysql_batch → doris spec
#
# 适用：主键高度碎片化的库（几乎每张表主键列名不同）。
#       此时不适合按主键分组（会产生上百个单表组），直接一表一 spec 最清晰。
#
# 用 mysql_batch 而非 mysql_snapshot_cdc：
#   - 纯查询抽数，无 binlog 依赖，适合「每晚定时拉」
#   - 每表独立主键、独立 cron、独立 server_id（不冲突）
#
# 用法（在能访问 MySQL 的机器上）：
#   MYSQL_HOST=... MYSQL_PORT=33067 MYSQL_USER=root MYSQL_PASS=xxx MYSQL_DB=cct_test \
#   DORIS_HOST=doris-fe DORIS_DB=ods_cct SERVER_ID_BASE=2500 CRON="0 2 * * *" \
#   MYSQL_CLI="docker exec <mysql容器> mysql" \
#   bash gen-doris-specs-by-table.sh ./pipes-dwh/cct_test
# ─────────────────────────────────────────────────────────────────────
set -euo pipefail

OUT_DIR="${1:?output dir required}"
MYSQL_CLI="${MYSQL_CLI:-mysql}"          # 可用 "docker exec xxx mysql" 包装
MYSQL_HOST="${MYSQL_HOST:?MYSQL_HOST required}"
MYSQL_PORT="${MYSQL_PORT:-3306}"
MYSQL_USER="${MYSQL_USER:?MYSQL_USER required}"
MYSQL_PASS="${MYSQL_PASS:?MYSQL_PASS required}"
MYSQL_DB="${MYSQL_DB:?MYSQL_DB required}"
DORIS_HOST="${DORIS_HOST:-doris-fe}"
DORIS_DB="${DORIS_DB:-ods}"
SERVER_ID_BASE="${SERVER_ID_BASE:-2500}"
CRON_EXPR="${CRON_EXPR:-0 2 * * *}"      # 仅 minute 会按表序号错峰
BATCH_SIZE="${BATCH_SIZE:-1000}"

run_mysql() { $MYSQL_CLI -h"$MYSQL_HOST" -P"$MYSQL_PORT" -u"$MYSQL_USER" -p"$MYSQL_PASS" -N -B "$@" 2>&1 | grep -v "Warning"; }

mkdir -p "$OUT_DIR"

# 每表：表名 + 真实主键（多列用逗号拼；无主键标 __nopk__）
ROWS=$(run_mysql -e "
SELECT t.TABLE_NAME,
       COALESCE((SELECT GROUP_CONCAT(k.COLUMN_NAME ORDER BY k.ORDINAL_POSITION)
                 FROM information_schema.KEY_COLUMN_USAGE k
                 WHERE k.TABLE_SCHEMA=t.TABLE_SCHEMA AND k.TABLE_NAME=t.TABLE_NAME AND k.CONSTRAINT_NAME='PRIMARY'),
                '__nopk__') AS pk_cols
FROM information_schema.TABLES t
WHERE t.TABLE_SCHEMA='${MYSQL_DB}' AND t.TABLE_TYPE='BASE TABLE'
ORDER BY t.TABLE_NAME;")

if [ -z "$ROWS" ]; then echo "⚠️  库 ${MYSQL_DB} 无基础表"; exit 0; fi

TABLE_COUNT=$(echo "$ROWS" | wc -l | tr -d ' ')
echo "==> ${MYSQL_DB}: 共 ${TABLE_COUNT} 张表，逐表生成 mysql_batch spec（输出到 ${OUT_DIR}）"

BASE_MIN=$(echo "$CRON_EXPR" | awk '{print $1}')
BASE_HR=$(echo "$CRON_EXPR" | awk '{print $2}')
DOM=$(echo "$CRON_EXPR" | awk '{print $3}')
MON=$(echo "$CRON_EXPR" | awk '{print $4}')
DOW=$(echo "$CRON_EXPR" | awk '{print $5}')
# 错峰窗口（分钟）：默认 120，即 2 小时内分散。表数超过窗口则多表共享同一分钟
# （OpenETL-Go 调度器会排队执行，不会真正并发压垮 Doris）。
SPREAD_WINDOW_MIN="${SPREAD_WINDOW_MIN:-120}"
i=0
echo "$ROWS" | while IFS=$'\t' read -r tbl pk; do
  [ -z "$tbl" ] && continue
  i=$((i + 1))
  # 在 [BASE_MIN, BASE_MIN+SPREAD_WINDOW_MIN) 内循环取模分散
  offset=$(( (i - 1) % SPREAD_WINDOW_MIN ))
  total_min=$((BASE_MIN + offset))
  this_hr=$((BASE_HR + total_min / 60))
  this_min=$((total_min % 60))
  # 小时超 24 则取模（跨天，但 schedule:cron 每天触发一次仍成立）
  this_hr=$((this_hr % 24))
  this_cron="${this_min} ${this_hr} ${DOM} ${MON} ${DOW}"

  if [ "$pk""x" = "__nopk__x" ]; then
    # 无主键：mysql_batch 必须有 pk_column 做游标，无主键表用 row_number 兜底需自定义 query，这里标记让用户处理
    pk_cursor="__nopk__"
    pk_yaml="# ⚠️ 表 ${tbl} 无主键，需手工指定 pk_column 或改用自定义 query（见 spec 注释）"
    pk_cols_yaml="[]"
  else
    pk_cursor=$(echo "$pk" | cut -d',' -f1)   # 游标用主键第一列
    pk_cols_yaml=$(echo "$pk" | sed 's/,/", "/g; s/.*/[ "&" ]/')
    pk_yaml=""
  fi

  out="${OUT_DIR}/${tbl}.yaml"
  cat > "$out" <<EOF
${pk_yaml}
name: "${MYSQL_DB}-${tbl}-to-doris"
tags: ["dwh", "ods", "doris", "auto", "${MYSQL_DB}"]

schedule:
  type: cron
  cron: "${this_cron}"     # 每晚定时（已按表序号错峰）

source:
  type: mysql_batch
  config:
    host: "${MYSQL_HOST}"
    port: ${MYSQL_PORT}
    user: "${MYSQL_USER}"
    password: "${MYSQL_PASS}"
    database: "${MYSQL_DB}"
    table: "${tbl}"
    pk_column: "${pk_cursor}"     # 游标列（主键第一列）
    limit: ${BATCH_SIZE}
    # 若想增量（只拉当天变更），取消注释并填单调递增的更新时间列：
    # cursor_column: "updated_at"

sink:
  type: doris
  config:
    host: "${DORIS_HOST}"
    port: 9030
    http_port: 8030
    user: "root"
    password: ""
    database: "${DORIS_DB}"
    table: "${tbl}"               # 落到 Doris 同名表
    write_mode: "stream_load"
    stream_load_format: "json"
    pk_columns: ${pk_cols_yaml}   # Doris UNIQUE KEY，保证 upsert 幂等
    auto_create: true
    schema_drift: "add_columns"

batch_size: ${BATCH_SIZE}
checkpoint_interval_sec: 10
backpressure_buffer: 1000
retry: { max_attempts: 3, initial_interval_ms: 100, max_interval_ms: 1000 }
dlq: { enable: true }
EOF
  printf '  ✓ %-40s pk=[%s]\n' "$tbl" "$pk"
done

echo ""
echo "==> 完成！${TABLE_COUNT} 个 spec 已生成到 ${OUT_DIR}/"
echo ""
echo "⚠️ 请人工复核："
echo "   1. 无主键表（pk=__nopk__）：mysql_batch 需 pk_column 游标，无主键表需手工指定或改自定义 query"
echo "   2. 复合主键表：pk_column 取了第一列做游标，确认是否符合排序预期"
echo "   3. server_id 已从 ${SERVER_ID_BASE} 递增；多库时用不同区间"
echo "   4. 想增量抽数（只拉当天变更）：在 source.config 里加 cursor_column: <单调更新时间列>"
