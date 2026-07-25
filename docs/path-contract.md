# Path Contract（PR-2.1）

> 版本锚：与 `docs/reliability-certification.md` 配套。  
> 默认语义：**checkpointed at-least-once**（非跨 sink exactly-once）。

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

## 2. 首批强制认证路径

| path_id | source → sink | write_mode / key | 故障证据入口 | residuals |
| --- | --- | --- | --- | --- |
| `mysql_cdc__mysql_upsert` | MySQL CDC → MySQL | upsert + 稳定 PK | `hack/e2e-cdc-crash-recovery.sh` · `hack/e2e-cdc-mysql.sh` | 非分布式事务 |
| `mysql_snap_cdc__ch_rmt` | MySQL snapshot+CDC → ClickHouse | ReplacingMergeTree / 业务键版本 | `hack/e2e-snapshot-cdc-clickhouse.sh` · `hack/e2e-snapshot-cdc-crash.sh` | binlog 与 sink 非原子 |
| `debezium_kafka__mysql` | Debezium Kafka → MySQL | upsert | `hack/e2e-debezium-mysql.sh` | Debezium 生命周期外部 |
| `kafka__file_unsafe` | Kafka → file | content-addressed + `allow_unsafe` | `hack/e2e-kafka.sh` | 默认生产阻断，需显式 opt-in |

完整矩阵见 [reliability-certification.md](./reliability-certification.md)。

## 3. 故障认证最低集合

对强制路径，至少记录：

1. **Happy path** 行数/校验和对账  
2. **Crash / SIGKILL** 后从 checkpoint 恢复  
3. **Checkpoint reset** 重放被 sink 幂等吸收或显式重复可见  
4. **依赖 outage**（sink/DB）→ DLQ → 恢复后 replay  
5. **DLQ persistence failure** 不得推进不安全 checkpoint（单测门闩）

## 4. 自动对账格式（建议 JSON）

```json
{
  "path_id": "mysql_cdc__mysql_upsert",
  "commit": "<git sha>",
  "started_at": "RFC3339",
  "finished_at": "RFC3339",
  "cases": [
    {"name": "happy", "ok": true, "source_count": 100, "sink_count": 100},
    {"name": "crash_restart", "ok": true, "replay_duplicates_absorbed": true},
    {"name": "dlq_replay", "ok": true}
  ],
  "residuals": ["no cross-sink atomicity"]
}
```

`hack/e2e-path-contract-smoke.sh` 校验本文件与 reliability 矩阵交叉引用，并在环境允许时调度已有 crash e2e。

## 5. 非宣称

- 不宣称 Kafka transactional EOS  
- 不宣称 source offset / Redis state / sink 三方原子提交  
- 不宣称 fanout 多 sink 原子性  
