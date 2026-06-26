#!/bin/sh

set -eu

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

IMAGE="openetl-go-etl:dev"
MYSQL_CONTAINER="etl-mysql-source"
PG_CONTAINER="etl-postgres-target"
APP_CONTAINER="etl-openetl-go-mysql-postgres"
PG_PORT="15433"
APP_PORT="8021"
PIPELINE="mysql-batch-join-to-postgres"

cleanup() {
  docker rm -f "$APP_CONTAINER" "$PG_CONTAINER" >/dev/null 2>&1 || true
}

wait_http() {
  url="$1"
  i=0
  while [ "$i" -lt 60 ]; do
    if curl -fsS "$url" >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  return 1
}

wait_pg() {
  i=0
  while [ "$i" -lt 60 ]; do
    if docker exec "$PG_CONTAINER" pg_isready -U etl -d analytics >/dev/null 2>&1; then
      return 0
    fi
    i=$((i + 1))
    sleep 2
  done
  return 1
}

if [ "${E2E_SKIP_BUILD:-0}" = "1" ]; then
  echo "==> Skip image build (E2E_SKIP_BUILD=1, using $IMAGE)"
else
  echo "==> Build image"
  docker build -t "$IMAGE" -f Dockerfile .
fi

echo "==> Start MySQL source"
docker compose -f docker-compose.dev.yml up -d mysql-source

echo "==> Wait MySQL healthy"
i=0
while [ "$i" -lt 60 ]; do
  status="$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER" 2>/dev/null || true)"
  if [ "$status" = "healthy" ]; then
    break
  fi
  i=$((i + 1))
  sleep 2
done
[ "$(docker inspect -f '{{.State.Health.Status}}' "$MYSQL_CONTAINER")" = "healthy" ]

echo "==> Start PostgreSQL target"
cleanup
docker run -d --name "$PG_CONTAINER" \
  --add-host host.docker.internal:host-gateway \
  -e POSTGRES_DB=analytics \
  -e POSTGRES_USER=etl \
  -e POSTGRES_PASSWORD=etl123 \
  -p "$PG_PORT:5432" \
  docker.io/library/postgres:16-alpine >/dev/null
wait_pg

echo "==> Prepare MySQL source tables"
docker exec "$MYSQL_CONTAINER" mysql -uroot -proot123456 -e "
CREATE DATABASE IF NOT EXISTS dzh3136_go;
DROP TABLE IF EXISTS dzh3136_go.pg_e2e_orders;
DROP TABLE IF EXISTS dzh3136_go.pg_e2e_users;
CREATE TABLE dzh3136_go.pg_e2e_users (
  id INT PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  email VARCHAR(100) NOT NULL,
  city VARCHAR(50),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE dzh3136_go.pg_e2e_orders (
  id INT PRIMARY KEY,
  user_id INT NOT NULL,
  product VARCHAR(100) NOT NULL,
  amount DECIMAL(10,2) NOT NULL,
  status VARCHAR(20) DEFAULT 'pending',
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
INSERT INTO dzh3136_go.pg_e2e_users (id, name, email, city) VALUES
  (1, 'Alice Chen', 'alice@example.com', 'Shanghai'),
  (2, 'Bob Wang', 'bob@example.com', 'Beijing'),
  (3, 'Carol Li', 'carol@example.com', 'Shenzhen'),
  (4, 'David Zhang', 'david@example.com', 'Hangzhou'),
  (5, 'Eve Wu', 'eve@example.com', 'Chengdu');
INSERT INTO dzh3136_go.pg_e2e_orders (id, user_id, product, amount, status) VALUES
  (1, 1, 'MacBook Pro', 18999.00, 'completed'),
  (2, 1, 'iPhone 15', 7999.00, 'completed'),
  (3, 2, 'AirPods Pro', 1999.00, 'shipped'),
  (4, 2, 'iPad Air', 4799.00, 'pending'),
  (5, 3, 'Apple Watch', 2999.00, 'completed'),
  (6, 3, 'Magic Keyboard', 699.00, 'completed'),
  (7, 4, 'Mac Mini', 4499.00, 'shipped'),
  (8, 5, 'HomePod', 2299.00, 'pending'),
  (9, 5, 'Apple TV', 1499.00, 'completed');
GRANT SELECT ON dzh3136_go.pg_e2e_users TO 'sync_user'@'%';
GRANT SELECT ON dzh3136_go.pg_e2e_orders TO 'sync_user'@'%';
FLUSH PRIVILEGES;
"

echo "==> Prepare PostgreSQL target table"
docker exec "$PG_CONTAINER" psql -U etl -d analytics -v ON_ERROR_STOP=1 -c "
DROP TABLE IF EXISTS user_order;
CREATE TABLE user_order (
  order_id INT PRIMARY KEY,
  user_id INT NOT NULL,
  user_name VARCHAR(100),
  user_email VARCHAR(100),
  user_city VARCHAR(50),
  product VARCHAR(100),
  amount NUMERIC(10,2),
  status VARCHAR(20),
  created_at TIMESTAMP
);
"

echo "==> Reset ETL data"
rm -rf data-mysql-postgres
mkdir -p data-mysql-postgres/output data-mysql-postgres/checkpoint data-mysql-postgres/dlq logs
chmod -R a+rwX data-mysql-postgres
chmod a+rwX logs

echo "==> Run MySQL batch JOIN -> PostgreSQL pipeline"
docker run -d \
  --add-host host.docker.internal:host-gateway \
  --name "$APP_CONTAINER" \
  -p "$APP_PORT:8001" \
  -v "$ROOT_DIR/testdata/pipes-mysql-postgres:/app/pipes:ro" \
  -v "$ROOT_DIR/testdata:/app/testdata:ro" \
  -v "$ROOT_DIR/data-mysql-postgres:/app/data" \
  -v "$ROOT_DIR/logs:/app/logs" \
  "$IMAGE" >/dev/null

wait_http "http://127.0.0.1:$APP_PORT/api/v2/health"

echo "==> Wait pipeline complete"
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
  echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"status":"completed"' >/dev/null 2>&1 && break
  i=$((i + 1))
  sleep 1
done
body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
echo "$body"
echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"status":"completed"'
echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"records_written":9'

echo "==> Verify PostgreSQL rows"
rows="$(docker exec "$PG_CONTAINER" psql -U etl -d analytics -At -c "SELECT COUNT(*) FROM user_order;" | tr -d '[:space:]')"
test "$rows" = "9"
summary="$(docker exec "$PG_CONTAINER" psql -U etl -d analytics -At -c "SELECT COUNT(DISTINCT user_id) || '|' || SUM(amount)::text FROM user_order;" | tr -d '[:space:]')"
test "$summary" = "5|45791.00"
alice="$(docker exec "$PG_CONTAINER" psql -U etl -d analytics -At -c "SELECT user_name || '|' || product || '|' || amount::text FROM user_order WHERE order_id = 1;" | tr -d '[:space:]')"
test "$alice" = "AliceChen|MacBookPro|18999.00"

echo "==> Verify schema preflight blocks incompatible PostgreSQL target"
validate_body="$(cat <<'JSON' | curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/specs/validate" -H 'Content-Type: application/json' -d @-
{
  "spec": {
    "name": "mysql-batch-join-to-postgres-schema-mismatch",
    "source": {
      "type": "mysql_batch",
      "config": {
        "host": "host.docker.internal",
        "port": 13306,
        "user": "sync_user",
        "password": "sync_password_123",
        "database": "dzh3136_go",
        "table": "user_order",
        "pk_column": "order_id",
        "cursor_column": "order_id",
        "query": "SELECT o.id AS order_id, u.id AS user_id, u.name AS user_name, u.email AS user_email, u.city AS user_city, o.product, o.amount, o.status, o.created_at, 'x' AS unexpected_field FROM pg_e2e_orders o JOIN pg_e2e_users u ON o.user_id = u.id",
        "limit": 100
      }
    },
    "transforms": [{"type": "identity", "config": {}}],
    "sink": {
      "type": "postgres",
      "config": {
        "host": "host.docker.internal",
        "port": 15433,
        "user": "etl",
        "password": "etl123",
        "database": "analytics",
        "table": "user_order",
        "batch_mode": "upsert",
        "pk_columns": ["order_id"],
        "schema_drift": "ignore"
      }
    }
  }
}
JSON
)"
echo "$validate_body"
echo "$validate_body" | grep '"valid":false'
echo "$validate_body" | grep 'schema-compatibility'
echo "$validate_body" | grep 'unexpected_field'

echo "==> Replay from beginning and verify PostgreSQL upsert absorbs duplicates"
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/checkpoint/reset" >/dev/null
curl -fsS -X POST "http://127.0.0.1:$APP_PORT/api/v2/pipelines/$PIPELINE/start" >/dev/null
i=0
while [ "$i" -lt 60 ]; do
  body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
  echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"status":"completed"' >/dev/null 2>&1 && break
  i=$((i + 1))
  sleep 1
done
body="$(curl -fsS "http://127.0.0.1:$APP_PORT/api/v2/pipelines")"
echo "$body"
echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"status":"completed"'
echo "$body" | grep "\"name\":\"$PIPELINE\"" | grep '"records_written":9'
rows_after_replay="$(docker exec "$PG_CONTAINER" psql -U etl -d analytics -At -c "SELECT COUNT(*) FROM user_order;" | tr -d '[:space:]')"
test "$rows_after_replay" = "9"

echo "MySQL batch JOIN -> PostgreSQL E2E passed"
