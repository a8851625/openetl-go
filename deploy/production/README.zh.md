# OpenETL-Go 生产部署包

面向 **standalone 单机生产**，并预置你前面确认的两条业务链路：

| 管道 | 作用 | 生产建议 |
| --- | --- | --- |
| `pipes/kafka-orders-to-mysql.yaml` | Kafka（12 分区）→ MySQL 主键 upsert | **可生产** |
| `pipes/kafka-orders-to-redis.yaml` | 同 topic → Redis `prefix+pk` 缓存 | **可用，Redis sink 为 experimental** |

两条管道使用 **不同 consumer group**，独立 checkpoint / DLQ；**不保证** MySQL 与 Redis 跨 sink 原子一致。

---

## 1. 目录结构

```text
deploy/production/
  .env.example              # 密钥与业务地址模板（复制为 .env）
  config.yaml               # 运行配置（可挂载覆盖）
  docker-compose.yml        # 元数据 MySQL + state Redis + OpenETL
  pipes/                    # 业务管道（挂载到 /app/pipes）
  sql/init-ods-orders.sql   # 业务 ODS 表 DDL（在 ODS MySQL 上执行）
  scripts/
    bootstrap-secrets.sh    # 生成 token/密码
    smoke.sh                # health 冒烟
    validate-pipes.sh       # preflight 校验
  README.zh.md              # 本文
```

**两类 MySQL / 两类 Redis 请分清：**

| 角色 | 默认 | 用途 |
| --- | --- | --- |
| 元数据 MySQL | compose 服务 `mysql` | pipeline / checkpoint / DLQ / audit |
| 业务 ODS MySQL | `.env` 的 `ODS_MYSQL_*` | Kafka upsert 目标表 |
| State Redis | compose `redis` db0 | lookup/window 等运行时状态 |
| 业务 Cache Redis | 默认同实例 **db1** | 订单缓存 sink |

---

## 2. 上线步骤（推荐）

### 2.1 准备密钥与业务地址

```bash
cd deploy/production
cp .env.example .env
bash scripts/bootstrap-secrets.sh
# 编辑 .env：
#   KAFKA_BROKER_1/2/3、KAFKA_TOPIC_ORDERS
#   ODS_MYSQL_*、CACHE_REDIS_*（若与 state Redis 不同）
#   OPENETL_IMAGE 钉死版本
```

### 2.2 初始化业务 ODS 表

在 **业务 MySQL**（不是 compose 元数据库）执行：

```bash
mysql -h "$ODS_MYSQL_HOST" -u root -p < sql/init-ods-orders.sql
# 或按 sql 内注释创建最小权限 sync 账号
```

### 2.3 确认 Kafka topic 分区

```bash
# 分区数必须 ≥ pipes 中 logical_shards（默认 12）
kafka-topics.sh --bootstrap-server <broker> --describe --topic orders
```

若只有 6 分区，请把两条 YAML 里的 `logical_shards` / `max_active_shards` 改为 `6`。

### 2.4 启动

```bash
# 在 deploy/production 目录
docker compose --env-file .env up -d
# 或从仓库根：
# docker compose --env-file deploy/production/.env -f deploy/production/docker-compose.yml up -d
```

### 2.5 冒烟与校验

```bash
export $(grep -v '^#' .env | xargs)   # 或手动 export ETL_API_TOKEN
bash scripts/smoke.sh
bash scripts/validate-pipes.sh
```

### 2.6 启动管道

热加载会自动加载 `pipes/*.yaml`。若需 API 显式启动：

```bash
TOKEN="$ETL_API_TOKEN"
curl -fsS -X POST -H "X-API-Token: $TOKEN" \
  http://127.0.0.1:8000/api/v2/pipelines/kafka-orders-to-mysql/start
curl -fsS -X POST -H "X-API-Token: $TOKEN" \
  http://127.0.0.1:8000/api/v2/pipelines/kafka-orders-to-redis/start
```

UI：`http://<host>:8000`（Header 或登录方式按部署的 token 策略）。

---

## 3. 生产检查清单

- [ ] `ETL_API_TOKEN` 已设置且足够长  
- [ ] `ETL_SPEC_ENCRYPTION_KEY` 已设置（spec 落盘加密）  
- [ ] `OPENETL_IMAGE` 使用固定版本 tag，不用裸 `latest`  
- [ ] 元数据 MySQL / Redis 密码非默认  
- [ ] ODS 表主键与 `pk_columns: [id]` 一致  
- [ ] Kafka JSON 字段与 `project.fields` / 表结构对齐  
- [ ] topic 分区 ≥ `logical_shards`  
- [ ] MySQL / Redis 使用不同 `group_id`（模板已分开）  
- [ ] Redis sink 使用 `hash`（模板已设置），不要用 `list`  
- [ ] 配置告警 webhook（钉钉/飞书/Slack）  
- [ ] 监控：`checkpoint_age_seconds`、`dlq_*`、sink latency、lag  
- [ ] 备份：元数据 MySQL + `pipes/` + 卷 `openetl-data`  

可选加固：

- [ ] 反向代理 TLS 终结，或启用 `ETL_TLS_*`  
- [ ] 仅内网暴露 8000，不暴露 8001 / 元数据端口  
- [ ] 资源 limit（compose 已默认 2 CPU / 2G，可按吞吐调）  

---

## 4. 运维速查

### 健康与指标

```bash
curl -H "X-API-Token: $ETL_API_TOKEN" http://127.0.0.1:8000/api/v2/health
curl http://127.0.0.1:8000/metrics
```

重点指标：`source_read_latency_ms`、`sink_write_latency_ms`、`checkpoint_age_seconds`、`dlq_file_count`、`cdc_lag_ms`（若适用）。

### DLQ

```bash
# 列表
curl -H "X-API-Token: $ETL_API_TOKEN" \
  "http://127.0.0.1:8000/api/v2/dlq/kafka-orders-to-mysql?limit=50"

# 按 id 重放
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  "http://127.0.0.1:8000/api/v2/dlq/kafka-orders-to-mysql/<id>/replay"
```

### 元数据备份（compose MySQL）

```bash
docker exec openetl-prod-mysql \
  mysqldump -u root -p"$MYSQL_ROOT_PASSWORD" --single-transaction openetl \
  > backup-openetl-$(date +%Y%m%d).sql
```

### 升级

1. 备份元数据 MySQL + 记录当前 `OPENETL_IMAGE`  
2. 改 `.env` 中 `OPENETL_IMAGE` 为新版本  
3. `docker compose --env-file .env pull && docker compose --env-file .env up -d`  
4. `bash scripts/smoke.sh` + 抽查两条管道 lag/DLQ  

回滚：改回旧镜像 tag 并 `up -d`；若 schema 迁移失败再 restore dump。

### 暂停 / 恢复（不丢 checkpoint）

```bash
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  http://127.0.0.1:8000/api/v2/pipelines/kafka-orders-to-mysql/pause
curl -X POST -H "X-API-Token: $ETL_API_TOKEN" \
  http://127.0.0.1:8000/api/v2/pipelines/kafka-orders-to-mysql/resume
```

---

## 5. 水平扩展（可选）

本包默认 **standalone**。分片多、单机 CPU 不够时：

1. 使用仓库根目录 `docker-compose.distributed.yml`（master + worker）  
2. 共享同一 MySQL 元数据 + Redis state  
3. **仅线性 pipeline** 走分布式分发；保持两条链路仍为独立线性 spec  

详见 `docs/runtime-modes.md`、`docs/parallelism-and-batching.md`。

---

## 6. 语义边界（上线必读）

| 项 | 说明 |
| --- | --- |
| 投递语义 | **at-least-once**；crash 可能重放最后一批 |
| MySQL | `upsert` + 主键吸收重复 |
| Redis | `hash`/`string` 同 key 覆盖；**experimental** |
| 双写 | 两 group 独立推进，**非事务**；允许短暂不一致 |
| Kafka brokers | 必须列表形式，禁止逗号拼接单字符串 |

消息字段若与模板不一致，只改 `transforms.project.fields` 与 `sql/init-ods-orders.sql`，不必改框架。

---

## 7. 与仓库其它入口的关系

| 文件 | 关系 |
| --- | --- |
| 根 `docker-compose.yml` | 通用生产 standalone；本包是 **带业务 pipes + .env 契约** 的专用包 |
| `manifest/examples/config.production.yaml` | 配置说明同源；本包 `config.yaml` 可直接挂载 |
| `docs/runtime-modes.md` | 通用 runbook |
| `docs/reliability-certification.md` | Kafka/MySQL 可靠性证据边界 |

---

## 8. 故障排查

| 现象 | 排查 |
| --- | --- |
| preflight 失败 | `scripts/validate-pipes.sh` 看 field_issues；检查 ODS 连通与表结构 |
| 管道 running 无数据 | topic 名/分区、`initial_offset`、group 是否已消费到最新 |
| MySQL 重复键报错 | 确认 `batch_mode: upsert` 与表 PRIMARY KEY |
| Redis key 不对 | 检查 `key_prefix` / `key_field` / `CACHE_REDIS_DB` |
| 部分 shard 空闲 | `logical_shards` > topic 分区数 |
| unresolved env | 容器环境缺少 `KAFKA_BROKER_1` 等；检查 compose `environment` 与 `.env` |
