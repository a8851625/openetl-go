#!/usr/bin/env bash
# ============================================================
#  MySQL JOIN → PostgreSQL ETL 完整复现脚本
#
#  用法:  bash hack/demo-mysql-to-postgres.sh
#
#  前提:  docker 或 podman 已安装，镜像 openetl-go-etl:ui-e2e 已构建
#
#  本脚本会:
#    1. 启动 MySQL 8（源库）+ PostgreSQL 16（目标库）
#    2. 在 MySQL 中建 users/orders 表并插入关联数据
#    3. 在 PostgreSQL 中建 user_order 大表
#    4. 启动 ETL 服务
#    5. 通过 API 创建并运行 pipeline（JOIN 查询 → PG）
#    6. 验证结果并打印对比表
#    7. 浏览器打开 http://localhost:7000 供你操作
#
#  清理:  脚本会自动检测容器运行时；也可通过 CONTAINER_CLI 覆盖后手动 rm 容器
# ============================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

. "$ROOT_DIR/hack/container-cli.sh"
detect_container_cli

MYSQL_C="etl-mysql-src"
PG_C="etl-pg-dst"
APP_C="etl-demo"
DATA_DIR="$ROOT_DIR/data-demo"

# ── 颜色 ──
G='\033[0;32m'; Y='\033[1;33m'; C='\033[0;36m'; R='\033[0;31m'; N='\033[0m'
info()  { echo -e "${C}▶ $1${N}"; }
ok()    { echo -e "${G}✓ $1${N}"; }
warn()  { echo -e "${Y}⚠ $1${N}"; }

# ── 0. 清理旧容器 ──
info "清理旧容器..."
"$CONTAINER_CLI" rm -f $MYSQL_C $PG_C $APP_C 2>/dev/null || true
rm -rf "$DATA_DIR"
mkdir -p "$DATA_DIR"/{output,checkpoint,dlq}
chmod -R a+rwX "$DATA_DIR"

# ── 1. 启动 MySQL ──
info "启动 MySQL 8 源库 (端口 3316)..."
"$CONTAINER_CLI" run -d --name $MYSQL_C \
  --add-host host.docker.internal:host-gateway \
  -e MYSQL_ROOT_PASSWORD=root123 \
  -e MYSQL_DATABASE=shop \
  -e MYSQL_USER=etl \
  -e MYSQL_PASSWORD=etl123 \
  -p 3316:3306 \
  docker.io/library/mysql:8.0 >/dev/null

info "等待 MySQL 就绪..."
for i in $(seq 1 60); do
  "$CONTAINER_CLI" exec $MYSQL_C mysql -uetl -petl123 -e "SELECT 1" shop >/dev/null 2>&1 && break
  sleep 2
done
ok "MySQL 就绪"

# ── 2. 启动 PostgreSQL ──
info "启动 PostgreSQL 16 目标库 (端口 5433)..."
"$CONTAINER_CLI" run -d --name $PG_C \
  --add-host host.docker.internal:host-gateway \
  -e POSTGRES_PASSWORD=pg123 \
  -e POSTGRES_DB=analytics \
  -e POSTGRES_USER=etl \
  -p 5433:5432 \
  docker.io/library/postgres:16 >/dev/null

info "等待 PostgreSQL 就绪..."
for i in $(seq 1 40); do
  "$CONTAINER_CLI" exec $PG_C psql -U etl -d analytics -c "SELECT 1" >/dev/null 2>&1 && break
  sleep 2
done
ok "PostgreSQL 就绪"

# ── 3. 建表 + 插入数据 ──
info "创建 MySQL 表结构..."
"$CONTAINER_CLI" exec $MYSQL_C mysql -uetl -petl123 shop -e "
CREATE TABLE users (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    email VARCHAR(100) NOT NULL,
    city VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE orders (
    id INT AUTO_INCREMENT PRIMARY KEY,
    user_id INT NOT NULL,
    product VARCHAR(100) NOT NULL,
    amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
" 2>/dev/null

info "插入测试数据..."
"$CONTAINER_CLI" exec $MYSQL_C mysql -uetl -petl123 shop -e "
INSERT INTO users (name,email,city) VALUES
('Alice Chen','alice@example.com','Shanghai'),
('Bob Wang','bob@example.com','Beijing'),
('Carol Li','carol@example.com','Shenzhen'),
('David Zhang','david@example.com','Hangzhou'),
('Eve Wu','eve@example.com','Chengdu');
INSERT INTO orders (user_id,product,amount,status) VALUES
(1,'MacBook Pro',18999.00,'completed'),
(1,'iPhone 15',7999.00,'completed'),
(2,'AirPods Pro',1999.00,'shipped'),
(2,'iPad Air',4799.00,'pending'),
(3,'Apple Watch',2999.00,'completed'),
(3,'Magic Keyboard',699.00,'completed'),
(4,'Mac Mini',4499.00,'shipped'),
(5,'HomePod',2299.00,'pending'),
(5,'Apple TV',1499.00,'completed');
" 2>/dev/null

echo ""
echo "┌─────────────────────────────────┐"
echo "│  MySQL 源数据                    │"
echo "├─────────────────────────────────┤"
"$CONTAINER_CLI" exec $MYSQL_C mysql -uetl -petl123 shop -e "
SELECT CONCAT('users: ', COUNT(*), ' 行') AS info FROM users
UNION ALL
SELECT CONCAT('orders: ', COUNT(*), ' 行') FROM orders;
" 2>/dev/null
echo "└─────────────────────────────────┘"

# ── 4. 创建 PG 目标表 ──
info "创建 PostgreSQL 目标表 user_order..."
"$CONTAINER_CLI" exec $PG_C psql -U etl -d analytics -c "
CREATE TABLE IF NOT EXISTS user_order (
    order_id INT PRIMARY KEY,
    user_id INT NOT NULL,
    user_name VARCHAR(100),
    user_email VARCHAR(100),
    user_city VARCHAR(50),
    product VARCHAR(100),
    amount DECIMAL(10,2),
    status VARCHAR(20),
    created_at TIMESTAMP
);
TRUNCATE user_order;
" >/dev/null 2>&1
ok "目标表就绪"

# ── 5. 启动 ETL 服务 ──
info "启动 ETL 平台..."
"$CONTAINER_CLI" run -d --name $APP_C \
  --add-host host.docker.internal:host-gateway \
  -p 7000:8000 -p 7001:8001 \
  -v "$DATA_DIR:/app/data" \
  openetl-go-etl:ui-e2e >/dev/null

info "等待 ETL 服务就绪..."
for i in $(seq 1 30); do
  curl -fsS http://127.0.0.1:7000/api/v2/health >/dev/null 2>&1 && break
  sleep 1
done
ok "ETL 服务就绪"

# ── 6. 创建并运行 pipeline ──
info "通过 API 创建 pipeline (mysql JOIN → postgres)..."
curl -sS -X POST http://127.0.0.1:7000/api/v2/pipelines \
  -H 'Content-Type: application/json' \
  -d '{
    "spec": {
      "name": "mysql-to-postgres",
      "source": {
        "type": "mysql_batch",
        "config": {
          "host": "host.docker.internal",
          "port": 3316,
          "user": "etl",
          "password": "etl123",
          "database": "shop",
          "query": "SELECT o.id AS order_id, u.id AS user_id, u.name AS user_name, u.email AS user_email, u.city AS user_city, o.product, o.amount, o.status, o.created_at FROM orders o JOIN users u ON o.user_id = u.id",
          "limit": 100
        }
      },
      "transforms": [{"type": "identity", "config": {}}],
      "sink": {
        "type": "postgres",
        "config": {
          "host": "host.docker.internal",
          "port": 5433,
          "user": "etl",
          "password": "pg123",
          "database": "analytics",
          "table": "user_order",
          "batch_mode": "upsert",
          "pk_columns": ["order_id"]
        }
      },
      "batch_size": 100,
      "checkpoint_interval_sec": 5,
      "backpressure_buffer": 100
    }
  }' 2>/dev/null | python3 -m json.tool 2>/dev/null
echo ""

info "启动 pipeline..."
curl -sS -X POST http://127.0.0.1:7000/api/v2/pipelines/mysql-to-postgres/start 2>/dev/null
echo ""

info "等待 pipeline 执行..."
sleep 5

# ── 7. 验证结果 ──
echo ""
echo "============================================"
echo "  Pipeline 运行状态"
echo "============================================"
curl -fsS http://127.0.0.1:7000/api/v2/pipelines/mysql-to-postgres 2>/dev/null | python3 -c "
import sys, json
d = json.load(sys.stdin)
s = d['stats']
print(f'  状态:       {d[\"status\"]}')
print(f'  读取记录:   {s[\"records_read\"]}')
print(f'  写入记录:   {s[\"records_written\"]}')
print(f'  失败记录:   {s[\"records_failed\"]}')
print(f'  死信队列:   {s[\"records_dlq\"]}')
" 2>/dev/null

echo ""
echo "============================================"
echo "  PostgreSQL user_order 表数据"
echo "============================================"
"$CONTAINER_CLI" exec $PG_C psql -U etl -d analytics -c "
SELECT order_id, user_name, user_city, product, amount, status
FROM user_order ORDER BY order_id;
" 2>/dev/null

echo "============================================"
echo "  汇总统计"
echo "============================================"
"$CONTAINER_CLI" exec $PG_C psql -U etl -d analytics -c "
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT user_id) AS unique_users,
       SUM(amount) AS total_amount
FROM user_order;
" 2>/dev/null

echo ""
echo "═══════════════════════════════════════════════"
ok "ETL 完成! 9 条 JOIN 记录已从 MySQL 同步到 PostgreSQL"
echo ""
echo "  浏览器体验:  http://localhost:7000"
echo "  清理环境:    $CONTAINER_CLI rm -f $MYSQL_C $PG_C $APP_C"
echo "═══════════════════════════════════════════════"
