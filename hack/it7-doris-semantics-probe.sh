#!/bin/sh
# IT-7 / T7.1 语义探针（可重跑复现）：在真实 Doris FE/BE 上验证
# sequence column / delete 序保护 / delete-sign 可写性 / 序列列类型 / 存量表属性。
# 结论与逐条观察见 docs/iterations/IT-7-doris-source-order/findings.md。
#
# 用法：CONTAINER_CLI=podman sh hack/it7-doris-semantics-probe.sh
# 说明：FE 对 _stream_load 返回 307 到 BE:8040，且 curl 跨主机重定向会丢 Authorization
#      （Go 的 http.Client 会用 Location 里的 userinfo，因此 sink 路径正常），
#      所以探针直接打 BE:8040 —— 即 307 的落点，语义等价。
set -u

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"
. "$ROOT_DIR/hack/container-cli.sh"

DORIS_VERSION="${DORIS_VERSION:-2.1.11}"
DORIS_FE_IMAGE="${DORIS_FE_IMAGE:-docker.io/apache/doris:fe-$DORIS_VERSION}"
DORIS_BE_IMAGE="${DORIS_BE_IMAGE:-docker.io/apache/doris:be-$DORIS_VERSION}"
FE_CONTAINER="${IT7_FE_CONTAINER:-it7-doris-fe}"
BE_CONTAINER="${IT7_BE_CONTAINER:-it7-doris-be}"
NETWORK="${IT7_NETWORK:-it7-doris-net}"
DB="it7"
BE_HTTP="http://172.31.97.3:8040"
LOG_DIR="$ROOT_DIR/data-it7"
OUT="$LOG_DIR/semantics-probe.log"
mkdir -p "$LOG_DIR"
: >"$OUT"

say() { echo "$@" | tee -a "$OUT"; }
section() { say ""; say "### $* ###"; }
doris_sql() { "$CONTAINER_CLI" exec "$FE_CONTAINER" mysql -h127.0.0.1 -P9030 -uroot "$@"; }

sql() {
  desc="$1"; shift
  say "--- SQL: $desc"
  out="$(doris_sql "$@" 2>&1)"; rc=$?
  say "    [exit=$rc]"
  echo "$out" | tee -a "$OUT"
  return 0
}

query() {
  desc="$1"; q="$2"
  out="$(doris_sql -N "$DB" -e "$q" 2>&1)"; rc=$?
  say "--- QUERY: $desc"
  say "    rows: [$(echo "$out" | tr '\n' '|')]  [exit=$rc]"
  return 0
}

# load <table> <json-line|json-lines-file> [extra curl args...]
load() {
  tbl="$1"; body="$2"; shift 2
  label="it7_$(date +%s)_$tbl"
  say "--- LOAD -> $tbl (label=$label)"
  say "    body: $body"
  [ "$#" -gt 0 ] && say "    extra: $*"
  printf '%s\n' "$body" >"$LOG_DIR/in.jsonl"
  "$CONTAINER_CLI" exec -i "$FE_CONTAINER" sh -c 'cat > /tmp/it7.jsonl' <"$LOG_DIR/in.jsonl"
  resp="$("$CONTAINER_CLI" exec "$FE_CONTAINER" curl -sS -X PUT "$BE_HTTP/api/$DB/$tbl/_stream_load" \
    -u root: \
    -H "format: json" -H "read_json_by_line: true" -H "Expect: 100-continue" \
    -H "label: $label" "$@" --data-binary @/tmp/it7.jsonl 2>&1)"
  echo "$resp" | tee -a "$OUT"
  status="$(echo "$resp" | sed -n 's/.*"Status"[[:space:]]*:[[:space:]]*"\([A-Za-z]*\)".*/\1/p' | head -1)"
  say "    [Status=${status:-<none>}]"
  return 0
}

# fresh <table> [mow] [seq_type]
fresh() {
  tbl="$1"; mow="${2:-true}"; seqtype="${3:-LARGEINT}"
  sql "重建 $tbl (mow=$mow, seq=$seqtype)" -e "DROP TABLE IF EXISTS $DB.$tbl;"
  sql "建表 $tbl" -e "
CREATE TABLE $DB.$tbl (
  id INT NOT NULL, name VARCHAR(64) NULL, _version $seqtype NOT NULL
) ENGINE=OLAP UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (
  \"replication_allocation\" = \"tag.location.default: 1\",
  \"enable_unique_key_merge_on_write\" = \"$mow\",
  \"function_column.sequence_col\" = \"_version\"
);"
}

########################################################################
section "0. 环境"
say "container-cli=$CONTAINER_CLI doris=$DORIS_VERSION"

"$CONTAINER_CLI" rm -f "$FE_CONTAINER" "$BE_CONTAINER" >/dev/null 2>&1 || true
"$CONTAINER_CLI" network rm "$NETWORK" >/dev/null 2>&1 || true
"$CONTAINER_CLI" network create --subnet=172.31.97.0/24 "$NETWORK" >/dev/null 2>&1 || true
for img in "$DORIS_FE_IMAGE" "$DORIS_BE_IMAGE"; do
  "$CONTAINER_CLI" image inspect "$img" >/dev/null 2>&1 || "$CONTAINER_CLI" pull "$img"
done
"$CONTAINER_CLI" run -d --name "$FE_CONTAINER" --network "$NETWORK" --ip 172.31.97.2 \
  -e FE_SERVERS="fe1:172.31.97.2:9010" -e FE_ID=1 -p 8030:8030 -p 9030:9030 \
  "$DORIS_FE_IMAGE" >/dev/null || { say "FE start FAILED"; exit 1; }
"$CONTAINER_CLI" run -d --name "$BE_CONTAINER" --network "$NETWORK" --ip 172.31.97.3 \
  -e FE_SERVERS="fe1:172.31.97.2:9010" -e BE_ADDR="172.31.97.3:9050" \
  "$DORIS_BE_IMAGE" >/dev/null || { say "BE start FAILED"; exit 1; }

i=0
while [ "$i" -lt 120 ]; do doris_sql -e "SELECT 1" >/dev/null 2>&1 && break; i=$((i + 1)); sleep 2; done
i=0
while [ "$i" -lt 150 ]; do
  alive="$(doris_sql -N -e "SHOW BACKENDS;" 2>/dev/null | grep -c 'true' || true)"
  [ "${alive:-0}" -ge 1 ] && break
  i=$((i + 1)); sleep 2
done
sql "版本" -e "SELECT VERSION();"
sql "建库" -e "CREATE DATABASE IF NOT EXISTS $DB;"

########################################################################
section "事实 1：sequence 阻止旧写覆盖新写"
fresh t_seq
load t_seq '{"id":1,"name":"v2","_version":2}'
sleep 1
load t_seq '{"id":1,"name":"v1","_version":1}'
sleep 2
query "预期 v2/2（旧写被忽略）" "SELECT id,name,_version FROM $DB.t_seq WHERE id=1;"
sql "回显属性" -e "SHOW CREATE TABLE $DB.t_seq;"

########################################################################
section "事实 2：删除的序保护（严格更旧被挡住，更新事件可复活）"
fresh t_del
load t_del '{"id":10,"name":"v100","_version":100}'
sleep 1
sql "DELETE（行 seq=100）" -e "DELETE FROM $DB.t_del WHERE id=10;"
sleep 1
query "删除后不可见" "SELECT id,name,_version FROM $DB.t_del WHERE id=10;"
load t_del '{"id":10,"name":"s50","_version":50}'
sleep 2
query "预期空（seq=50 严格更旧，不复活）" "SELECT id,name,_version FROM $DB.t_del WHERE id=10;"
load t_del '{"id":10,"name":"s101","_version":101}'
sleep 2
query "预期 s101（更新事件正确复活）" "SELECT id,name,_version FROM $DB.t_del WHERE id=10;"

section "残窗 1：删除从未存在的行 → 无墓碑，旧写插入幽灵行"
fresh t_ghost
sql "DELETE 不存在的行" -e "DELETE FROM $DB.t_ghost WHERE id=99;"
load t_ghost '{"id":99,"name":"old-ghost","_version":1}'
sleep 2
query "预期出现 99/old-ghost（残窗，已知边界）" "SELECT id,name,_version FROM $DB.t_ghost WHERE id=99;"

section "残窗 2：等序（equal sequence）后写者胜"
fresh t_tie
load t_tie '{"id":1,"name":"v1","_version":1}'
sleep 1
sql "DELETE（墓碑继承 seq=1）" -e "DELETE FROM $DB.t_tie WHERE id=1;"
load t_tie '{"id":1,"name":"stale","_version":1}'
sleep 2
query "预期出现 1/stale（等序复活，已知边界）" "SELECT id,name,_version FROM $DB.t_tie WHERE id=1;"

########################################################################
section "事实 3：__DORIS_DELETE_SIGN 不可通过 Stream Load 写入"
fresh t_sign
load t_sign '{"id":1,"name":"v1","_version":1}'
sleep 1
load t_sign '{"id":1,"_version":2,"__DORIS_DELETE_SIGN":1}' -H "columns: id,name,_version,__DORIS_DELETE_SIGN"
sleep 2
query "预期仍可见（sign 被忽略）" "SELECT id,name,_version FROM $DB.t_sign WHERE id=1;"
load t_sign '{"id":1,"_version":3,"__DORIS_DELETE_SIGN":1}' -H "hidden_columns: __DORIS_DELETE_SIGN" -H "partial_columns: true"
sleep 2
query "预期仍可见（hidden_columns 也无效）" "SELECT id,name,_version FROM $DB.t_sign WHERE id=1;"
query "隐藏列不可查询" "SELECT __DORIS_DELETE_SIGN FROM $DB.t_sign LIMIT 1;"

########################################################################
section "事实 4：存量表无法后设 sequence 属性"
sql "建无属性表" -e "
CREATE TABLE IF NOT EXISTS $DB.t_alt (
  id INT NOT NULL, name VARCHAR(64) NULL
) ENGINE=OLAP UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (\"replication_allocation\" = \"tag.location.default: 1\",
            \"enable_unique_key_merge_on_write\" = \"true\");"
sql "ADD COLUMN ... DEFAULT 0（预期语法错）" -e "ALTER TABLE $DB.t_alt ADD COLUMN _version BIGINT NOT NULL DEFAULT 0;"
sql "SET function_column.sequence_col（预期 Unknown table property）" -e "ALTER TABLE $DB.t_alt SET (\"function_column.sequence_col\" = \"_version\");"

########################################################################
section "事实 5：序列列类型"
sql "DECIMAL 作序列列（预期被拒）" -e "
CREATE TABLE IF NOT EXISTS $DB.t_dec (
  id INT NOT NULL, _version DECIMAL(20,0) NOT NULL
) ENGINE=OLAP UNIQUE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1
PROPERTIES (\"replication_allocation\" = \"tag.location.default: 1\",
            \"enable_unique_key_merge_on_write\" = \"true\",
            \"function_column.sequence_col\" = \"_version\");"
fresh t_big true BIGINT
sql "BIGINT 写 2^63（预期静默回绕）" -e "INSERT INTO $DB.t_big VALUES (1,'x',9223372036854775808);"
sleep 2
query "预期为 -9223372036854775808" "SELECT id,_version FROM $DB.t_big WHERE id=1;"
fresh t_large true LARGEINT
load t_large '{"id":1,"name":"big","_version":18446744073709551614}'
sleep 1
load t_large '{"id":1,"name":"small","_version":1}'
sleep 2
query "预期 big/18446744073709551614（uint64 精确、旧写不覆盖）" "SELECT id,name,_version FROM $DB.t_large WHERE id=1;"

########################################################################
section "事实 6：mow=false 时删除保护失效"
fresh t_mow false LARGEINT
load t_mow '{"id":20,"name":"v100","_version":100}'
sleep 1
sql "DELETE" -e "DELETE FROM $DB.t_mow WHERE id=20;"
sleep 1
load t_mow '{"id":20,"name":"s50","_version":50}'
sleep 2
query "预期出现 20/s50（mow=false 下旧写复活，故必须强制 mow=true）" "SELECT id,name,_version FROM $DB.t_mow WHERE id=20;"

say ""
say "=== PROBE DONE: $(date -u +%Y-%m-%dT%H:%M:%SZ) ==="
say "log: $OUT"
