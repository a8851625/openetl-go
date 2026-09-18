# IT-5 技术方案

> 本文是实现输入，不是验收标准。验收以 [spec.md](./spec.md) 为准。

## 方案总览

三个子项共用一个原则：把"隐式约定"变成"显式契约"。CH-C1 给 ClickHouse sink 实现首个 `SinkCommitMetadataProvider`，用 pipeline+source-position+batch 序号派生稳定 dedup token，native/HTTP 共用同一 token 构造与错误分类；CH-C3 给 `core.Metadata` 加 `Headers` 字段并让 Kafka source/sink 全链路透传（producer 用源 timestamp）；CH-C2 在 spec 校验层引入 additive-only schema contract/fingerprint，持久化到 storage 并在 validate/preflight 阻断破坏性变更。不引入新外部服务、不动 scheduler/runner 主循环语义。

## 分项设计

### CH-C1 ClickHouse 写入确认与去重契约

**现状**：`ClickHouseSink.Write`（internal/etl/sink/clickhouse.go:537）native（go-clickhouse driver）与 HTTP 两条路径写入后即返回；`SinkCommitMetadataProvider`（internal/etl/core/core.go:200）无实现；checkpoint 的 `native` 元数据槽位（internal/etl/checkpoint/sink_commit.go:41-50）空置；RA-1 已交付 `_version` 源事件序（ReplacingMergeTree 吸收重复）。

**目标**：
1. `ClickHouseSink` 实现 `SinkCommitMetadata(ctx) (map[string]any, error)`：返回本批次 dedup token、协议（native/http）、确认的 block/batch 边界、（native 路径）server-generated insert 信息。
2. Dedup token 构造：`pipelineKey + source position（binlog file:pos / kafka partition:offset / snapshot cursor）+ batch sequence + record count`，确定性哈希（与现有 `FormatContractID` 风格一致的字符串拼接，不做时间戳成分）。同一批次重试/重放得到同一 token。
3. 写入路径改造：`Write` 记录 token → 成功确认后由 `SinkCommitMetadata` 暴露；native 路径开启 `insert_dedup_token` setting（ClickHouse ≥23.x 支持，按 server version 探测，旧版本降级为 token 仅记录不启用去重并 warn）；HTTP 路径把 token 放入 `INSERT` 的 query param/settings。
4. 错误分类三态：`ErrAcked`（目标已确认）/`ErrNotAcked`（明确未写入，可安全重试）/`ErrUnknown`（超时/连接中断，重放更安全）。分类进 `core.ClassifiedError` 既有体系；DLQ 与 retry 决策沿用现有分类消费。
5. 崩溃窗口测试：复用 `internal/etl/e2e/harness` 的 fault injection，在 sink ack 成功返回后、checkpoint `Save` 前注入崩溃（进程 exit / pause），重启后 replay——断言 dedup token 相同、ReplacingMergeTree 吸收后终态一致、native 与 HTTP 结果等价。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| internal/etl/core/core.go | 不改接口；可选：ClassifiedError 三态辅助构造 |
| internal/etl/sink/clickhouse.go | 实现 provider；token 构造；settings；错误分类 |
| internal/etl/sink/clickhouse_version_test.go | version 探测单测 |
| internal/etl/e2e/path_mysql_snapcdc_clickhouse_test.go | crash-window case（native） |
| internal/etl/e2e/（新）ch_http_dedup_test.go | HTTP 协议等价 case |
| docs/etl-idempotency.md | dedup token/tombstone/mutation 边界文档 |

**数据语义**：token 只影响"目标端去重提示"，不改变 at-least-once：`SinkCommitMetadata` 出错仍阻断 checkpoint（宁可 replay）；ack 后崩溃窗口重放时相同 token 让 ClickHouse 幂等拒绝重复 block（若 server 支持），否则由 ReplacingMergeTree `_version` 吸收——两种路径终态一致，这正是 e2e 要证明的等价性。tombstone（DELETE）记录不参与 dedup token 去重提示（物理 mutation 边界），文档显式声明。

**设计权衡**：否决"两阶段 INSERT/分布式事务"（违背项目 no-cross-sink-exactly-once 边界）；否决"用 INSERT ID 随机值"（重试后变化，无法幂等）。确定性 token + server dedup hint 是最小可靠方案。

### CH-C3 Kafka metadata envelope

**现状**：`core.Metadata`（internal/etl/core/core.go:21-70）无 Headers 字段；Kafka source（internal/etl/source/kafka.go:357）填 Source/SourceType/SourcePhase/Table/Timestamp/Partition/Offset，headers 丢弃；Kafka sink（internal/etl/sink/kafka.go:362）`Timestamp: time.Now()`、不写 headers、key 不透传（除非 topic template）。

**目标**：
1. `core.Metadata` 增加 `Headers map[string][]byte \`json:"headers,omitempty"\``（向后兼容：旧 JSON 无此字段反序列化为 nil）。
2. Kafka source：`msg.Headers`（sarama RecordHeader）拷贝进 `rec.Metadata.Headers`；保留 RawPayload 既有行为。
3. Kafka sink：producer `Timestamp` 优先 `rec.Metadata.Timestamp`（零值回退 `time.Now()`）；`msg.Headers` 从 `rec.Metadata.Headers` 透传（过滤 `__` 前缀内部键）；message key 透传 `rec.Metadata.Key`（可配置 `pass_key`，默认开，DLQ replay 同样生效）。
4. DLQ envelope：DLQ 记录序列化已含 Metadata JSON——确认 headers 随之持久化并在 replay 时还原（round-trip 断言点）。
5. Schema Registry capability：descriptor 增加 `schema_registry: capability` 探测字段（只报告"格式可解析性"，avro/schema-registry 客户端不在本期），preflight 对配置了 registry 的 spec 给 warning 而非放行假象。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| internal/etl/core/core.go | Metadata.Headers 字段 |
| internal/etl/source/kafka.go | headers 透传 |
| internal/etl/sink/kafka.go | 源 timestamp/headers/key 透传 |
| internal/etl/dlq/（序列化路径） | envelope 字段确认/补齐 |
| internal/etl/e2e/（新）kafka_envelope_test.go | 全链路 round-trip |
| internal/etl/server/connector_descriptor.go | registry capability 字段 |

**数据语义**：headers 是事件身份的一部分，进 checkpoint envelope 与 DLQ 记录；重放后 headers 逐字节一致。源 timestamp 零值（batch source 无事件时间）回退写入时钟并在文档声明。

**设计权衡**：否决"headers 放进 Record.Data 字段"（污染业务数据、transform 可见性错误）；Metadata 一等字段是唯一干净路径。否决"内置 avro 反序列化"（新依赖+范围蔓延）。

### CH-C2 additive-only schema contract

**现状**：spec validate/preflight 用实时 `SchemaInfo` 对比；`ddl_guard` transform 拒绝所有 DDL；无 contract 持久化，源 schema 漂移后旧 spec replay 无差异报告。

**目标**：
1. contract 定义：`SchemaContract{ Fingerprint string, Columns []ColumnContract{Name,Type,Nullable}, Version int, CreatedAt, SourceSnapshot… }`；fingerprint = 规范化列集确定性哈希。
2. 持久化：storage `pipeline_schema_contracts` 表（pipeline_key、version、fingerprint、columns JSON、绑定 spec version）；三 backend migration；backup/restore 纳入既有对账。
3. 时机：spec validate（创建/更新）时若开启 `schema_contract: enforce`（默认 off，显式 opt-in），从源 introspection 捕获 contract 并随 spec version 保存。
4. 演进判定：运行/preflight 时对比 live schema vs contract——新增列（contract 没有的）→ 放行 + warn + 提示"更新 spec 采纳新 contract"；列删除/重命名（contract 有 live 没有）→ **阻断**；类型不兼容矩阵（ widening int→bigint 放行、string→int 阻断等显式矩阵）→ 阻断含 remediation。`ddl_guard` 语义不变（仍拒绝 DDL 语句本身），contract 管列集。
5. 旧 contract replay：checkpoint 携带 contract fingerprint；replay 时 contract 与 live 不一致 → 不静默错乱，报可解释差异（added/removed/changed 列表）。
6. UI/API：spec validate 响应与 preflight `field_issues` 暴露 contract 结果（复用 UI-B.4 introspection 事实）。

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| internal/etl/core/core.go | SchemaContract 类型（或独立 schema_contract.go） |
| internal/etl/server/schema*.go | 对比矩阵、diff 报告 |
| internal/etl/storage/*.go + migrations | contract 表 |
| internal/etl/server/server.go | validate/preflight 接入 |
| hack/e2e-schema-contract.sh | 加列放行/删列阻断/replay 差异 e2e |
| docs/etl-config-schema.md | schema_contract 配置文档 |

**数据语义**：contract 只影响 validate/preflight 与 replay 诊断，不改变写入路径；阻断发生在管道启动前，不存在部分写入窗口。contract 不进 checkpoint 数据本身（fingerprint 引用即可），避免 envelope 膨胀。

**设计权衡**：否决"自动 ALTER sink 加列"（迁移越权，用户决议只做 additive 放行）；否决"contract 强制默认开"（破坏现有管道兼容，opt-in 迁移友好）。

## 架构约束

- 不新增外部服务依赖（无 Schema Registry 客户端、无新基础设施）。
- 不改 `Runner`/`ParallelRunner` 主循环与 scheduler 语义。
- 不引入跨 sink 事务或 exactly-once 声明。
- Metadata/DLQ/envelope 变更必须向后兼容（旧数据可读）。

## 风险与回滚

| 风险 | 影响 | 缓解 | 回滚路径 |
| --- | --- | --- | --- |
| dedup token 与 server 版本不兼容（旧 CH 拒绝 setting） | native 写入失败 | version 探测降级 + warn（token 仅记录） | settings 开关 `insert_dedup: auto/off` |
| headers 大（消息头膨胀）影响吞吐 | kafka 路径性能 | 默认透传 + `max_header_bytes` 保护（超限 DLQ） | 配置开关 `pass_headers: off` |
| contract 表 migration 失败 | 启动阻断 | 走既有 fail-closed migration 路径 + upgrade drill 覆盖 | migration 回滚脚本（ additive 表，drop 即回滚） |
| crash-window e2e 不稳定（时序敏感） | CI 抖动 | harness 既有 pause/exit 注入原语 + 重试预算内稳定断言（终态一致而非时序） | — |

## 测试策略

| 层级 | 范围 | 命令 |
| --- | --- | --- |
| 1 静态与单测 | token 构造、兼容矩阵、Metadata 序列化 | `go test ./internal/etl/core/... ./internal/etl/sink/... ./internal/etl/server/...` |
| 2 包级 / -race | sink 并发写、contract storage | `go test -race ./internal/etl/...` |
| 3 后端矩阵 | contract 表三 backend migration | `hack/e2e-storage-upgrade-{sqlite,mysql,postgres}.sh` |
| 4 容器 e2e / 故障注入 | crash-window（native+HTTP）、kafka envelope round-trip、schema contract 演进 | `go test -tags=e2e -e2e.strict ./internal/etl/e2e/...` + `hack/e2e-kafka.sh` + `hack/e2e-schema-contract.sh` |
| 5 回归 | 既有全量 e2e 不回归 | `hack/e2e-all.sh`（或按门禁子集）+ UI e2e 163 条 |
