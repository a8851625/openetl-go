# IT-5 任务分解

> 状态取值：`todo` / `active` / `done` / `blocked`。一次只允许一个 `active`。

## Round 分组

| Round | 包含 task | 目标 |
| --- | --- | --- |
| 1/5 | T5.1 | CH-C1：ClickHouse `SinkCommitMetadataProvider` 生产实现（token + provider + 错误分类 + 单测） |
| 2/5 | T5.2 | CH-C1：native/HTTP crash-window e2e + 协议等价 conformance + idempotency 文档 |
| 3/5 | T5.3 | CH-C3：Metadata.Headers + kafka source/sink 透传 + round-trip e2e |
| 4/5 | T5.4 | CH-C2：schema contract 类型 + storage 表 + validate/preflight 阻断 |
| 5/5 | T5.5 | CH-C2：e2e（加列放行/删列阻断/replay 差异）+ 三 backend migration drill + 迭代收口 |

## 任务表

| ID | 任务 | 依赖 | Round | 状态 | 证据落点 |
| --- | --- | --- | --- | --- | --- |
| T5.1 | CH-C1：dedup token + provider 实现与错误分类 | — | Round 1 | `done` | commit da3f80c |
| T5.2 | CH-C1：crash-window/协议等价 e2e + 文档 | T5.1 | Round 2 | `todo` | e2e + docs/etl-idempotency.md |
| T5.3 | CH-C3：Kafka metadata envelope 全链路 | — | Round 3 | `todo` | core/source/sink + e2e |
| T5.4 | CH-C2：schema contract 存储与校验 | — | Round 4 | `todo` | storage + server validate/preflight |
| T5.5 | CH-C2：e2e + migration drill + 迭代收口 | T5.4 | Round 5 | `todo` | e2e + upgrade drill + ROADMAP 回填 |

## 任务明细

### T5.1 CH-C1：dedup token + provider 实现与错误分类

**验收**：

1. `ClickHouseSink` 实现 `SinkCommitMetadataProvider`：成功 Write 后返回 `{dedup_token, protocol, batch_seq, record_count, (native) server insert 确认信息}`；token 由 pipeline key + source position + batch 序号确定性派生，同输入同 token。
2. 错误分类三态（acked/not-acked/unknown）进 `ClassifiedError` 体系；native 与 HTTP 共用同一分类函数。
3. `insert_dedup_token` setting 按 server version 探测启用（旧版本降级 warn，token 仍记录）。
4. 单测：token 确定性/唯一性、version 探测降级、错误分类映射。

**证据落点**：

- `internal/etl/sink/clickhouse_dedup.go`（新）、`clickhouse.go`、`clickhouse_dedup_test.go`（新）、`internal/etl/core/core.go`（PipelineKeySetter）、`internal/etl/pipeline/pipeline.go`（注入）
- commit `da3f80c`；`go test ./internal/etl/sink/ ./internal/etl/core/`、`go test -race -run 'ClickHouse|Dedup'` 全绿

**领取记录**：

```text
Round: 1/5
Roadmap item: CH-C1 (IT-5/T5.1)
Profile/path: standalone + clickhouse connector path (native/http)
Objective: ClickHouse sink 提供 SinkCommitMetadataProvider 首个生产实现——每逻辑 Write 批次确定性 dedup token + 三态 ack 错误分类。
Scope: internal/etl/sink/clickhouse{,_dedup}.go、internal/etl/core/core.go、internal/etl/pipeline/pipeline.go
Non-goals: crash-window e2e（T5.2）；Kafka envelope（T5.3）；schema contract（T5.4/5.5）；跨 sink exactly-once
Acceptance: T5.1 验收 1-4（provider 返回 token/protocol/seq/rows/tables；token 确定性；三态分类 native/HTTP 共用；探测降级 record_only）
Evidence: go test ./internal/etl/sink/ ./internal/etl/core/ 全绿；-race 通过；单测覆盖确定性/唯一性/NUL 别名/位置边界/分类三态/provider 契约/探测回退
Result: delivered
Residual/follow-up: T5.2 crash-window e2e + 协议等价 conformance + idempotency 文档
```

### T5.2 CH-C1：crash-window/协议等价 e2e + 文档

**验收**：

1. native 路径："sink ack 后、checkpoint Save 前"进程崩溃 → 重启 replay：dedup token 相同、ReplacingMergeTree 终态一致、无 at-most-once 丢失。
2. HTTP 路径：同一场景等价结果。
3. 单表与多表（table_template）至少各一 case。
4. tombstone 与物理 mutation 边界写入 docs/etl-idempotency.md；失败仍 DLQ 断言。

**证据落点**：

- `internal/etl/e2e/path_mysql_snapcdc_clickhouse_test.go`（扩展）+ 新 HTTP case 文件
- `go test -tags=e2e -e2e.strict ./internal/etl/e2e/ -run 'ClickHouse' -count=1`
- `docs/etl-idempotency.md`

### T5.3 CH-C3：Kafka metadata envelope 全链路

**验收**：

1. `core.Metadata.Headers map[string][]byte` 向后兼容序列化。
2. Kafka source 透传 headers；sink producer 用源 timestamp（零值回退）、透传 headers（`__` 前缀过滤、`max_header_bytes` 保护超限 DLQ）。
3. kafka→transform→kafka→DLQ→replay headers/timestamp/key/partition/offset 逐字段 round-trip e2e。
4. Schema Registry capability 字段：descriptor 可查询，preflight 对 registry 配置给明确 warning（不假装支持）。

**证据落点**：

- `internal/etl/core/core.go`、`internal/etl/source/kafka.go`、`internal/etl/sink/kafka.go`
- `internal/etl/e2e/kafka_envelope_test.go`（新）
- `hack/e2e-kafka.sh` 回归通过

### T5.4 CH-C2：schema contract 存储与校验

**验收**：

1. `SchemaContract` 类型 + fingerprint 确定性构造。
2. storage `pipeline_schema_contracts` 表三 backend migration + backup/restore 对账覆盖。
3. `schema_contract: enforce` opt-in：validate 时捕获 contract 随 spec version 保存。
4. 兼容矩阵：新增列放行+warn；删除/重命名/不兼容类型阻断含 remediation；live vs contract diff 报告。
5. 旧 contract replay：差异可解释（added/removed/changed 列表），不静默错乱。

**证据落点**：

- `internal/etl/core/`（或 server/schema_contract.go）、`internal/etl/storage/` migration
- `internal/etl/server/schema_test.go` 扩展
- `hack/e2e-storage-upgrade-*.sh` 通过

### T5.5 CH-C2：e2e + migration drill + 迭代收口

**验收**：

1. `hack/e2e-schema-contract.sh`：加列放行/删列阻断/类型冲突阻断/旧 replay 差异报告四场景。
2. 三 backend upgrade drill 重跑通过（新表不破坏既有 drill）。
3. 既有 e2e 与 UI e2e（163 条）无回归。
4. ROADMAP CH-C1/C3/C2 候选标注晋级交付；connector-certification.md / etl-config-schema.md / resource-baseline.md（若数字变化）更新；证据归档 docs/evidence/。

**证据落点**：

- `hack/e2e-schema-contract.sh`（新）
- `hack/e2e-storage-upgrade-{sqlite,mysql,postgres}.sh`
- `hack/e2e-ui.sh`（回归）
- ROADMAP/文档 diff + evidence manifest

**领取记录模板**（复制到 PR / 工作日志）：

```text
Round: <n>/5
Roadmap item: CH-C1/CH-C3/CH-C2 (IT-5/T5.x)
Profile/path: standalone + connector path (clickhouse native/http, kafka, schema contract)
Objective: <one observable outcome>
Scope: <files/components allowed>
Non-goals: cross-sink exactly-once; keyed state; registry client; CH-C4..C8
Acceptance: <numbered checks>
Evidence: <commands, e2e, docs, release record>
Result: <delivered|active|blocked_external>
Residual/follow-up: <bounded next item or none>
```
