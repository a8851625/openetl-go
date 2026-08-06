# 轻量数仓：OpenETL-Go + Doris

最简形态的两组件数仓：业务库 → OpenETL-Go（cron + CDC/批量）→ Doris。
**无 Kafka、无 MinIO、无 Airflow、无独立 BI**。

## 架构

```
        ┌──────────────┐
        │  MySQL / PG  │  业务库（宿主机或同网络）
        └──────┬───────┘
               │ binlog + 批量（OpenETL-Go 直连，无中间件）
               ▼
        ┌──────────────┐
        │  OpenETL-Go  │  cron 调度 + 抽数 + checkpoint/DLQ + Web UI
        └──────┬───────┘    :8000
               │ Stream Load（HTTP 直写）
               ▼
        ┌──────────────┐
        │    Doris     │  OLAP + 自带 FE Web 查询  :8030 / :9030
        └──────────────┘
```

资源占用：OpenETL-Go ~150MB + Doris FE ~1-2GB + Doris BE ~2-4GB。
最低 8GB 内存可验证；生产建议 16GB+（主要留给 BE）。

## 1. 启动

```bash
# 在项目根目录
docker compose -f docker-compose.lightweight-dwh.yml up -d
```

等待约 1-2 分钟（Doris BE 加入集群需要时间）。检查：

```bash
# Doris SQL 可用 + BE 已 alive
docker exec dwh-doris-fe mysql -h127.0.0.1 -P9030 -uroot -e "SHOW BACKENDS\G"
# 看到 Alive: true 即就绪

# OpenETL-Go Web UI
open http://localhost:8000   # token: change-me（在 compose 里改）
```

## 2. 在 Doris 建目标库

```bash
docker exec dwh-doris-fe mysql -h127.0.0.1 -P9030 -uroot \
  -e "CREATE DATABASE IF NOT EXISTS ods;"
```

> Doris sink 的 `auto_create: true` 会自动建**表**，但**库**需要先建一次。

## 3. 配置抽数 spec

本项目为源库「每张表」生成独立的 `mysql_batch → doris` spec（**一表一 spec**）。

**为什么是一表一 spec 而非聚合？** 这个库（OpenCart 电商系统）主键高度碎片化（190 张表、161 种主键），`mysql_snapshot_cdc` 的多表聚合能合并的表很少，而 CDC 模式带来 binlog 依赖/全局锁/server_id 冲突等运维负担。对「每晚定时批量抽数」这种场景，`mysql_batch` 纯查询、无锁、无 binlog 依赖、表间故障完全隔离，是更合适的选型。

| 特性 | `mysql_batch`（本项目采用） | `mysql_snapshot_cdc`（聚合版） |
| --- | --- | --- |
| binlog 依赖 | ❌ 不需要 | ✅ 必须 |
| 全局锁 | ❌ 无 | ✅ 首次快照短暂持锁 |
| server_id 冲突 | ❌ 无 | ✅ 需全局唯一 |
| 故障隔离 | ✅ 表间完全独立 | ❌ 同组绑定 |
| 调度粒度 | ✅ 每表独立 cron/频率 | ❌ 同组同一 cron |
| CDC 准实时增量 | ❌ 仅批量 | ✅ 支持 |

### spec 位置

```
pipes-dwh/
├── cct_test/<表名>.yaml      # 190 个 spec
└── wallet_test/<表名>.yaml   # 9 个 spec
```

每个 spec：`mysql_batch` 源（按真实主键分页）→ Doris sink（Stream Load + UNIQUE KEY upsert），每晚 cron 定时。spec 里 `host: 172.22.22.1` `port: 33067` 是源 MySQL 的内网地址，按实际部署调整。

### 部署

```bash
docker compose -f docker-compose.lightweight-dwh.yml up -d
# compose 里挂载 ./pipes-dwh → /app/manifest/pipes
# OpenETL-Go 启动时自动加载并注册 cron
```

### 重新生成 spec（源库 schema 变了时）

```bash
# 一表一 spec（主用）
MYSQL_CLI="docker exec <mysql容器> mysql" \
MYSQL_HOST=172.22.22.1 MYSQL_PORT=33067 MYSQL_USER=root MYSQL_PASS=xxx MYSQL_DB=cct_test \
DORIS_HOST=doris-fe DORIS_DB=ods_cct SERVER_ID_BASE=2500 \
bash hack/gen-doris-specs-by-table.sh ./pipes-dwh/cct_test

# 若个别表需要 CDC 增量，备选聚合脚本
bash hack/gen-doris-specs-grouped.sh ./pipes-dwh-grouped/cct_test
```

spec 目录挂载后，OpenETL-Go 启动时自动加载并注册 cron。改完文件热重载：

```bash
curl -X POST http://localhost:8000/api/v2/specs/reload -H "X-API-Token: change-me"
```

## 4. 验证链路

到 Web UI（Pipelines 页）手动触发一次 `ods-whole-db-to-doris`，观察：
- 状态变为 running → running/succeeded
- Metrics：`batch_count`、`last_batch_size`、`sink_write_latency_ms`
- DLQ 页：应有 0 条（若有失败行可在这里回放）

然后到 Doris 查：

```bash
docker exec dwh-doris-fe mysql -h127.0.0.1 -P9030 -uroot ods \
  -e "SHOW TABLES; SELECT COUNT(*) FROM ods_<你的表名>;"
```

## 5. 常见调整

### 源库不在宿主机（容器/远程）
把 spec 里 `host` 改成源库实际地址。若源库在另一台宿主机，确保 Doris 网络（`dwh-net`）能路由到，或给 openetl 容器加 `extra_hosts` / 用宿主机 IP。

### 多个源库
复制 spec，每个库一份，`name` 和 `server_id` 各不相同，cron 错开（如 02:00 / 02:15 / 02:30）避免同时压 Doris。

### 源库所有表主键不统一（最常见情况）

本项目采用「一表一 spec」方案：每张表用独立的 `mysql_batch` spec，各自使用真实主键（`pk_column` + Doris `pk_columns`）。用脚本自动扫描源库 `information_schema` 生成：

```bash
MYSQL_CLI="docker exec <mysql容器> mysql" \
MYSQL_HOST=172.22.22.1 MYSQL_PORT=33067 MYSQL_USER=root MYSQL_PASS=xxx MYSQL_DB=cct_test \
DORIS_HOST=doris-fe DORIS_DB=ods_cct SERVER_ID_BASE=2500 \
bash hack/gen-doris-specs-by-table.sh ./pipes-dwh/cct_test
```

脚本会查每张表的真实主键（含复合主键、无主键表），逐表生成 spec。

> 备选：`hack/gen-doris-specs-grouped.sh` 按主键分组生成聚合 spec（`mysql_snapshot_cdc` 多表 + CDC），适合需要准实时增量的少量表。

### 只想每晚纯批量、不要 CDC
把 source 换成 `mysql_batch`（单表）或每表一份 `mysql_batch` spec。`mysql_batch` 不依赖 binlog，纯查询抽数。

### Doris BE 内存吃紧
BE 默认 `mem_limit=80%`。机器内存小可在容器内调：

```bash
docker exec dwh-doris-be bash -c 'echo "mem_limit=50%" >> /opt/apache-doris/be/conf/be.conf && exit'
docker restart dwh-doris-be
```

### 要看图表/Dashboard
起步用 Doris FE Web（`http://localhost:8030`）写 SQL。需要正式 dashboard 时再加 Grafana（最轻，~150MB）或 Superset（图表最强，~500MB-1GB）。**不建议 Metabase（JVM，2-4GB）**。

### 经 Kafka 中转（可选）
最简形态不引入 Kafka（业务库 → OpenETL-Go → Doris 直连）。若确实需要用 Kafka 解耦（多消费端、削峰、跨网段），可以用 OpenETL 的 `kafka` sink + `kafka` source 走「中转链路」，并保留 CDC 语义：

- **产出端**：sink 配 `topic_template: cdc-{db}-{table}`，按源表路由到各自 topic（不再混写一个 topic）。
- **消费端**：source 配 `format: envelope`，解析 OpenETL 自家 envelope（`{event_id,op,table,key,data,timestamp}`），还原 INSERT/UPDATE/DELETE 语义，让 Doris 的 upsert+delete 表现与直连 CDC 一致。

注意两点语义边界：

1. **envelope 只携带 `Data`（后镜像），不携带 `Before`（前镜像）**。因此 MySQL CDC 的 update/delete 在中转后只有 after-image；依赖 `Before` 的下游 transform/sink（如用前镜像回填的 compact）在中转链路不可用。Doris/MySQL 的 upsert+delete 靠 `Data` 里的主键行，不受影响。
2. **`topic_template` 模式跳过 sink 的 topic 校验/自动创建**。生产环境若 broker 未开 `auto.create.topics.enable=true`，必须预先创建所有目标 topic（`cdc-<db>-<table>`），否则首批写入会以 `UNKNOWN_TOPIC_OR_PARTITION` 失败。模板引用 `{db}`/`{table}` 但上游 record 无对应元数据时（例如未配 database 的 source），写入会直接报错而非静默落到默认 topic。

## 文件清单

| 文件 | 作用 |
| --- | --- |
| `docker-compose.lightweight-dwh.yml` | 三容器编排：openetl + doris-fe + doris-be |
| `pipes-dwh/ods-whole-db-to-doris.yaml.example` | 全库→Doris 抽数 spec 模板 |
| `docs/lightweight-dwh.md` | 本文档 |
