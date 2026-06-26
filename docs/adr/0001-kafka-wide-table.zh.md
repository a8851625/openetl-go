# ADR-0001：Kafka 驱动的聚合宽表 production candidate

## 状态

Accepted

## 背景

OpenETL 已具备 Kafka source、lookup/window transform、ClickHouse sink、checkpoint、DLQ 和 Docker e2e 基础。下一阶段首要产品方向是把这些能力收敛成一条可验证的实时宽表链路，而不是继续横向扩展 connector 数量。

## 决策

第一条 production candidate 链路定义为：

```text
Kafka JSON/Debezium 订单事件
  -> normalize_envelope
  -> MySQL/PostgreSQL stream-table lookup 维表补全
  -> ClickHouse 明细宽表
  -> tumbling event-time window 聚合
  -> ClickHouse 聚合宽表
```

默认交付语义：

- 默认承诺 at-least-once，不承诺 exactly-once。
- Kafka offset 必须在 sink 写入成功后推进。
- 宽表 sink 必须使用业务主键、版本列或确定性 document/object key 吸收重复写入。
- 记录级失败必须进入 DLQ 或返回错误触发 retry；不允许静默跳过。
- stream-table lookup 是第一批生产候选 join 模型；stream-stream join 在状态持久化前保持 beta/experimental。
- window 第一阶段只承诺 tumbling window；sliding/session 不进入生产配置路径。

## 不承诺边界

- 不承诺跨 Kafka offset、状态快照、多个 sink commit 的强事务。
- 不承诺 Kafka transactions。
- 不承诺 stream-stream join 在重启/rebalance 后的连续语义，直到 `StateStore` 接入 join 状态。
- 不承诺 sliding/session window，直到对应实现、watermark 和 late data side-output 完成。

## 恢复策略

- Kafka source 使用 checkpoint 保存 topic/partition offset。
- Stateful transform 后续通过 `StateStore` 保存 snapshot version，checkpoint 可引用该 state version。
- ClickHouse 宽表优先使用 ReplacingMergeTree + version column，重复写入由主键和版本消除。
- DLQ replay 对 linear pipeline 使用 DLQ ID 精确删除；DAG/stateful replay 在实现 node-level replay 前必须明确拒绝并给出恢复 runbook。

## 验收测试

首条链路必须新增 Docker e2e，至少覆盖：

- Redpanda + MySQL/PostgreSQL 维表 + ClickHouse 正常写入。
- Kafka consumer crash/restart。
- consumer group rebalance。
- 重复消息重放。
- join miss 进入 DLQ 或 left join 显式置空。
- 维表刷新失败。
- ClickHouse 写入失败与恢复。

## 影响

- `pluginMetadata()`、配置 schema、UI 插件矩阵必须使用 `production|beta|experimental|dev-only` 成熟度。
- `window_type` 生产 schema 只暴露已实现的 `tumbling`。
- 服务端插件编译不能默认走请求时 `npx` 网络拉取；生产环境应预装编译器或在 CI/CLI 离线构建。
