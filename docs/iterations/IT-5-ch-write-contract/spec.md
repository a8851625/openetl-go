# IT-5：CH 第一批 —— 写入确认/去重契约 + Kafka metadata envelope + additive-only schema contract

> 状态：`queued` | 依赖：IT-2（RA-1）、GAP-7、P3.1、schema evolution 决议（2026-09-05）、RA-8/T3.6（均已 delivered） | 归属 roadmap 条目：CH-C1、CH-C3、CH-C2

## 问题陈述

1. **写入确认与去重缺生产契约**：`core.SinkCommitMetadataProvider`（internal/etl/core/core.go:195）与 checkpoint 集成（internal/etl/checkpoint/sink_commit.go:41）已存在，但没有任何 sink 实现它。ClickHouse native 与 HTTP 两条写入路径（internal/etl/sink/clickhouse.go）在"目标已确认但 checkpoint 未提交"的窗口内崩溃后，重启 replay 无法区分"已写入/未写入"，重复吸收依赖 ReplacingMergeTree 的最终合并而非显式 dedup token；错误分类、确认等待语义在两协议下未证明等价。
2. **Kafka metadata 不全链路**：Kafka source（internal/etl/source/kafka.go:357）只透传 key/timestamp/partition/offset，headers 未进入统一 `core.Metadata`（internal/etl/core/core.go:21 无 Headers 字段）；Kafka sink producer（internal/etl/sink/kafka.go:362）Timestamp 取 `time.Now()` 而非源 timestamp，headers 不透传。重放与审计无法用同一事件身份。
3. **schema 无持久化 contract**：auto-create、DDL preview、`ColumnTypes` 已有，但 schema contract/fingerprint 不随 pipeline spec/version 持久化；源端新增列静默放行、删除/重命名/类型冲突无 validate/preflight 阻断，旧 spec replay 无可解释差异报告。2026-09-05 用户决议采 (b) additive-only 立项，尚未实施。

## 可观察结果

1. ClickHouse sink（native 与 HTTP）在"目标确认后、checkpoint 提交前"崩溃 → 重启/replay 后，运维能在 checkpoint envelope 中看到 sink-native 提交元数据（dedup token、批次边界），重复吸收结果在两协议下一致。
2. Kafka → transform → sink → DLQ → replay 全链路中，key、源 timestamp、partition、offset、headers 逐字段不丢失；Kafka 目标消息的事件时间等于源事件时间。
3. 源 schema 新增列时管道按兼容矩阵放行并在 spec 中留下 contract/fingerprint 记录；删除列、重命名、不兼容类型变更在 validate/preflight 被阻断并给出 remediation；旧 contract replay 报告可解释差异。

## 验收标准

| # | 验收标准 | 判定方式 |
| --- | --- | --- |
| 1 | CH-C1：`SinkCommitMetadataProvider` 有 ClickHouse 生产实现；native 与 HTTP 各完成一次"目标确认后、checkpoint 提交前崩溃 → 重启/replay"测试；稳定 dedup token、错误分类（确认/未确认/未知）、确认等待和重复吸收结果在两协议一致 | `internal/etl/e2e/path_mysql_snapcdc_clickhouse_test.go` 扩展 + 新 crash-injection e2e；checkpoint envelope 含 `native` 提交元数据断言 |
| 2 | CH-C1：tombstone 与物理 mutation 的边界文档化；失败仍进 DLQ 不丢记录 | docs/etl-idempotency.md 更新 + DLQ replay e2e 断言 |
| 3 | CH-C3：headers 进入 `core.Metadata`；key/源 timestamp/partition/offset/headers 在 source→transform→sink→DLQ→replay 逐字段 round-trip | 新 e2e（kafka→kafka 全链路 round-trip + DLQ envelope 字段断言） |
| 4 | CH-C3：Kafka producer 使用源 timestamp、透传 headers；无 Schema Registry 时 capability/错误边界可查询 | sink 单测 + e2e 消息属性断言；capability 查询端点/descriptor 字段 |
| 5 | CH-C2：schema contract/fingerprint 随 pipeline spec/version 与运行证据持久化 | storage schema + spec validate/preflight 单测 |
| 6 | CH-C2：新增列按明确兼容矩阵放行；删除列、重命名、不兼容类型在 validate/preflight 阻断；旧 contract replay 有可解释结果 | e2e：源加列→放行；删列/改类型→阻断含 remediation；旧 spec replay 差异报告 |
| 7 | 共同锚点：故障注入不把 at-least-once 变 at-most-once（ack 后 commit 前崩溃、outage、reset、replay 全覆盖）；native/HTTP、单表/多表共用同一份 conformance | e2e 矩阵全绿；conformance 测试文件 |
| 8 | 共同锚点：量化指标（重复吸收率、ack→checkpoint 延迟、schema 阻断/放行数）与 commit、镜像 digest、依赖版本绑定 | 证据 manifest + docs/evidence/ 归档 |

## 非目标

- 跨 sink exactly-once、两阶段提交、跨 pipeline 原子性。
- 通用 keyed state、timer、流计算语义（沿用"明确暂缓或不做"）。
- 删除列/重命名/不兼容类型的自动传播或迁移（只阻断，不修复）。
- Schema Registry 服务端实现或新外部依赖（只做 capability/preflight 探测）。
- CH-C4..CH-C8（S3 manifest、资源预算、诊断事件流、drain/rollout、插件 ABI）——保持候选池。
- Doris/ES/关系型 sink 的 commit metadata（本迭代只交付 ClickHouse 首个生产实现；其他 sink 是有界后续）。

## 交付约束

- 任何重试/replay 改动不得把可能重复变成可能丢失（at-least-once 底线）。
- `SinkCommitMetadata` 返回 error 必须阻断 checkpoint 推进（现有语义保持：宁可 replay 不可存不可验证边界）。
- Metadata 新字段必须向后兼容旧 checkpoint/DLQ 记录反序列化。
- schema contract 存储需走三 backend（sqlite/mysql/postgres）migration 路径，兼容既有 upgrade drill。
- 现有 163 条 UI e2e 与全部既有 e2e 不得回归。

## 依赖与前置

| 类型 | 内容 | 状态 |
| --- | --- | --- |
| 迭代依赖 | IT-2（RA-1 版本/删除语义）、GAP-7 身份契约 | delivered |
| 迭代依赖 | P3.1 descriptor 契约、schema evolution 决议（2026-09-05 采 b） | delivered/已决议 |
| 迭代依赖 | RA-8/T3.6 容量基线（提供性能回归参照） | delivered |
| 外部输入 | 无（全部依赖本地容器可测：ClickHouse/Redpanda/MySQL 镜像已有） | — |

## 完成定义（DoD）

1. `tasks.md` 全部 task `done` 且有实际执行证据记录。
2. 上方验收标准全部 `passed`；skip/block 必须转有界后续并在 ROADMAP 留痕。
3. ROADMAP CH-C1/C3/C2 候选条目标注晋级交付，IT-5 状态更新。
4. docs/etl-idempotency.md、etl-config-schema.md、resource-baseline.md（若性能数字变化）、connector-certification.md 同一交付内更新。
5. `git diff --check` 通过；无 secret/生成产物/调试开关进入错误位面。
