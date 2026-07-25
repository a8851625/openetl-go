# Path Contract（PR-2）

> 版本锚：与 [reliability-certification.md](./reliability-certification.md)、[etl-idempotency.md](./etl-idempotency.md) 配套。  
> 默认语义：**checkpointed at-least-once**（非跨 sink exactly-once）。  
> 机器可读契约：`internal/etl/server/path_contract.go` → `GET /api/v2/paths/contracts`。

## 1. 合同字段

每条公开 production path 必须可追溯：

| 字段 | 含义 |
| --- | --- |
| `path_id` | 稳定 ID，如 `mysql_cdc__mysql_upsert` |
| `source` | connector + mode（batch/cdc/snapshot+cdc） |
| `transforms` | 有序 transform 类型列表（可空） |
| `sink` | connector + `write_mode` |
| `business_key` | 业务主键 / 文档 ID / 对象 key 策略 |
| `version_col` | 版本列或 ReplacingMergeTree 策略（可空） |
| `storage` | metadata backend（sqlite/mysql/postgres） |
| `runtime` | standalone / master-worker |
| `delivery` | 固定 `at_least_once` |
| `replay_absorption` | 如何吸收重放（upsert / RMT / content-addressed / 显式重复可见） |
| `evidence` | 当前版本 e2e / 单测路径 |
| `last_certified` | commit 或发布 tag（认证时填写） |
| `residuals` | 已知边界 |
| `rpo` / `rto` | 恢复点/恢复时间目标声明 |

## 2. 首批强制认证路径（PR-2.2）

| path_id | source → sink | write_mode / key | 故障证据入口 | residuals |
| --- | --- | --- | --- | --- |
| `mysql_cdc__mysql_upsert` | MySQL CDC → MySQL | `batch_mode: upsert` + 稳定 PK | `hack/e2e-path-mysql-cdc-mysql.sh`（含 crash / checkpoint reset / sink outage / DLQ replay）· `hack/e2e-cdc-mysql.sh` · `hack/e2e-cdc-crash-recovery.sh` | 源 binlog 与 sink 非分布式事务；允许至少一次重复，由 upsert 吸收 |
| `mysql_snap_cdc__ch_rmt` | MySQL snapshot+CDC → ClickHouse | ReplacingMergeTree + `pk_columns` + `_version` | `hack/e2e-snapshot-cdc-clickhouse.sh` · `hack/e2e-snapshot-cdc-crash.sh` | binlog 与 sink 非原子；查询当前态需 `FINAL` 或物化 |

扩展候选（非 PR-2 强制但已有证据）：

| path_id | 证据 | maturity |
| --- | --- | --- |
| `debezium_kafka__mysql` | `hack/e2e-debezium-mysql.sh` | production_with_review（Debezium 生命周期外部） |
| `kafka__file_unsafe` | `hack/e2e-kafka.sh` + `allow_unsafe: true` | 默认阻断，需显式 opt-in |
| `mysql_snap_cdc__doris_uk` | `hack/e2e-doris.sh` | production_with_review |
| `file_batch__s3_content_key` | `hack/e2e-s3-minio.sh` | production_with_review（无 first-class manifest） |

完整矩阵见 [reliability-certification.md](./reliability-certification.md)。

## 3. 故障认证最低集合

对强制路径，至少记录并自动对账：

1. **Happy path** 业务键行数/值对账  
2. **Crash / SIGKILL** 后从 durable checkpoint 恢复  
3. **Checkpoint reset** 重放被 sink 幂等吸收或显式重复可见  
4. **依赖 outage**（sink/DB）→ DLQ → 恢复后 replay  
5. **Schema drift**（若 sink 支持 `schema_drift`/`auto_create`）  
6. **DLQ persistence failure** 不得推进不安全 checkpoint（单测门闩）

## 4. 自动对账格式

```json
{
  "path_id": "mysql_cdc__mysql_upsert",
  "commit": "<git sha>",
  "started_at": "RFC3339",
  "finished_at": "RFC3339",
  "cases": [
    {"name": "happy", "ok": true, "source_count": 3, "sink_count": 3, "silent_loss": 0},
    {"name": "crash_restart", "ok": true, "replay_duplicates_absorbed": true, "silent_loss": 0},
    {"name": "checkpoint_reset", "ok": true, "replay_duplicates_absorbed": true, "silent_loss": 0},
    {"name": "sink_outage_dlq_replay", "ok": true, "silent_loss": 0}
  ],
  "rpo": "last durable checkpoint; in-flight batch may replay",
  "rto": "process restart + source reconnect + one checkpoint interval",
  "residuals": ["no cross-sink atomicity", "not exactly-once"]
}
```

脚本：

- `hack/e2e-path-contract-smoke.sh` — 文档交叉引用 + 单元门闩 + descriptor API 契约  
- `hack/e2e-path-mysql-cdc-mysql.sh` — 强制 path 1 故障矩阵  
- `hack/e2e-snapshot-cdc-clickhouse.sh` — 强制 path 2 故障矩阵  

## 5. 边界语义（PR-2.3）

| 边界 | 策略 | 入口 |
| --- | --- | --- |
| PostgreSQL CDC `TRUNCATE` | 默认 `on_truncate: error` 阻断启动；`skip` 仅在显式配置且 `allow_unsafe` 后放行 | `source.config.on_truncate` + preflight |
| CDC → file/S3 append | 默认 `ValidateSpec` 拒绝；需 `allow_unsafe: true` | `pipeline.ValidateSpec` |
| 跨 sink fanout（DAG 多 sink） | 默认拒绝；需 `allow_unsafe: true`，且文档声明非原子 | `DAG.ValidateProduction` / preflight |
| Kafka/File/S3 append 生产声明 | 保持 `production_with_review` 或更低，直至 first-class manifest/transaction | connector maturity + path contract |

## 6. RPO / RTO（发布声明）

| 指标 | 声明 |
| --- | --- |
| **RPO** | 最近一次 **durable checkpoint**。sink ack 成功但 checkpoint 落盘失败时，整批重放；不会静默跳过。 |
| **RTO** | 进程重启 + 源/汇重连 + 至多一个 `checkpoint_interval_sec` 的恢复窗口（standalone）。 |
| **重复上界** | 未 checkpoint 的最后若干 batch；由 upsert / ReplacingMergeTree / 内容寻址 key 吸收，或显式可见。 |
| **不宣称** | 跨 sink 原子 fanout、Kafka transactional EOS、source offset / Redis state / sink 三方原子提交。 |

## 7. 非宣称

- 不宣称 Kafka transactional exactly-once  
- 不宣称 source offset / Redis state / sink 三方原子提交  
- 不宣称 fanout 多 sink 原子性  
- 不宣称 MaxCompute 真环境 production（仍 `blocked_external`）  
