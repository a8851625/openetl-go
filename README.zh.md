# OpenETL-Go

轻量、自托管、开源的 CDC/ETL 数据同步、清洗、汇聚运行时。

[English README](./README.md)

OpenETL-Go 运行 `Source -> Transform -> Sink` 管道，用 YAML、API 和 Web UI 操作同一份 spec。简单场景可以是一条文件或数据库同步任务；复杂场景可以升级为 DAG 编排、并行分片和 master-worker 分布式执行，用多源、多分支、lookup/join、tumbling window 和多 sink 表达完整链路。

项目不再把宽表汇聚做成单独功能线。明细宽表、实时聚合表都通过普通 pipeline 或 DAG spec 组合 source、transform、state 和 sink 来实现。

OpenETL-Go 的目标不是替代 Flink/Spark 的复杂有状态流计算、Airflow/Dagster 的通用工作流调度、Airbyte 的 SaaS ELT 连接器生态，或 Debezium/Kafka Connect 的 CDC 基础设施。它更适合替代脚本化同步任务、轻量 DataX/Canal/Kafka consumer 程序，以及不想引入重型平台的自托管 CDC/ETL 链路。详细边界见[产品定位](./docs/positioning.zh.md)。

## 它解决什么

| 领域 | 能力 |
| --- | --- |
| 管道编排 | 线性 pipeline、DAG 节点/边、条件路由、fanout、并行分片、定时/流式执行 |
| 数据搬运 | CDC、批处理、流、文件、HTTP、Redis、对象存储、数仓、搜索/索引 sink |
| 数据加工 | filter、validate、类型转换、字段重命名/删除/新增、envelope 标准化、lookup/enrichment、join、tumbling window、去重、Lua/JS/TS/WASM 扩展 |
| 运维入口 | Web UI、REST API、连接目录、spec 校验、连接测试、transform dry-run、Prometheus 指标、审计日志 |
| 可靠性 | 默认 at-least-once、checkpoint、retry/backoff、DLQ list/replay/delete、可配置的幂等 sink 写入模式 |
| 运行形态 | SQLite 单机模式、MySQL/PostgreSQL 共享存储、master-worker 分布式调度 |

连接器覆盖面比较广，但不同连接器和边界场景的成熟度并不完全一致。默认交付语义应按 at-least-once 理解，再通过业务主键、版本列、upsert 或 sink 侧幂等策略消除重复影响。当前生产就绪边界见[幂等语义文档](./docs/etl-idempotency.zh.md)和[路线图](./docs/ROADMAP.zh.md)。

## 什么时候使用

适合：

- 数据库/Kafka/文件/HTTP/对象存储到 OLAP、搜索、数据库或对象存储的同步与清洗。
- MySQL/PostgreSQL batch、CDC、snapshot+CDC 到 ClickHouse/MySQL/PostgreSQL/Doris/ES/S3/Kafka。
- Kafka JSON/Debezium 事件补维、去重、tumbling window 聚合后落明细表或聚合表。
- 需要 checkpoint、DLQ 可视化 replay、幂等策略提示、preflight 和轻量 UI/API 的自托管管道。

不适合：

- 任意 keyed state、processing-time timer、多流状态机、复杂告警生命周期等 Flink 风格实时业务计算。
- 需要 exactly-once savepoint、SQL planner、sliding/session window、late side-output 或 retraction 的流处理任务。
- 主要价值来自海量 SaaS 连接器目录的 ELT 平台。

## 快速开始

启动内置的 MySQL CDC 到 ClickHouse 示例：

```bash
docker compose -f docker-compose.quickstart.yml up -d
# podman 用户: podman compose -f docker-compose.quickstart.yml up -d
```

然后打开：

- Web UI 和代理后的 API：<http://localhost:8000>
- ETL API 直连端口：<http://localhost:8001>

示例 spec 从 [`pipes-quickstart/`](./pipes-quickstart) 加载。完整步骤见
[docs/quickstart.zh.md](./docs/quickstart.zh.md)。

## 最小 Pipeline Spec

Pipeline spec 是 `pipes/` 或配置项 `etl.specsDir` 下的 YAML 文件。

```yaml
name: file-to-file
source:
  type: file
  config:
    path: /app/data/input/orders.jsonl
    format: json
sink:
  type: file_sink
  config:
    output_dir: /app/data/output
```

完整连接器字段见
[docs/etl-config-schema.zh.md](./docs/etl-config-schema.zh.md)。

## 复用已保存连接

通过 UI 或 `POST /api/v2/connections` 创建的连接，可以被线性 pipeline 和 DAG 节点引用。已保存连接提供 `kind`、`type` 和共享配置；内联 `config` 覆盖每条任务自己的 table、topic、query 或输出路径等字段。

```yaml
source:
  connection: orders-mysql
  config:
    table: orders
sink:
  connection_ref: warehouse-clickhouse
  config:
    table: orders_wide
```

DAG 设计器可以直接选择已保存连接，也可以在保存或启动 spec 前做连接测试。

## 用编排实现宽表汇聚

宽表和实时聚合不是专用 API 或专用页面，而是普通 pipeline 能力的组合：

```text
Kafka/MySQL CDC 事实流
  -> normalize_envelope / filter / type_convert
  -> lookup 或 join 维度数据
  -> 可选 deduplicate 和 tumbling window 聚合
  -> ClickHouse / MySQL / PostgreSQL / Doris / S3 / Kafka sink
```

当前示例：

- [`testdata/pipes-wide-table/kafka-orders-detail-clickhouse.yaml`](./testdata/pipes-wide-table/kafka-orders-detail-clickhouse.yaml)：Kafka 订单事件补 MySQL 维度后写入 ClickHouse 明细宽表。
- [`testdata/pipes-wide-table/kafka-orders-aggregate-clickhouse.yaml`](./testdata/pipes-wide-table/kafka-orders-aggregate-clickhouse.yaml)：Kafka 订单事件去重、补维、窗口聚合后写入 ClickHouse。
- [`docs/adr/0001-kafka-wide-table.zh.md`](./docs/adr/0001-kafka-wide-table.zh.md)：编排式实现的设计说明。

也就是说，当前管道已经具备表达“事实流 + 维表 lookup/join + tumbling window + ClickHouse 落地”这类宽表汇聚链路的基础能力。更复杂的流流 join、sliding/session window、CDC 维表增量更新、late data 处理、DAG/状态型 replay 还需要继续做生产级认证，这些差距放在 roadmap 中推进，而不是拆出新的专用功能线。

## 连接器与算子

| 阶段 | 内置能力面 |
| --- | --- |
| Sources | `mysql_cdc`、`mysql_snapshot_cdc`、`postgres_cdc`、`mysql_batch`、`kafka`、`file`、`http`、`redis` |
| Transforms | `normalize_envelope`、`debezium_cdc`、`cdc_policy`、`ddl_guard`、`filter`、`validate`、`project`、`select_fields`、`flat_map`、`udtf`、`type_convert`、`rename`、`drop_field`、`add_field`、`deduplicate`、`lookup`、`enricher`、`join`、`window`、`router`、`fanout`、`tap`、`rate_limiter`、`lua`、`javascript`、`typescript`、WASM 插件 |
| Sinks | `clickhouse`、`mysql`、`postgres`/`postgresql`、`doris`、`elasticsearch`/`es`、`kafka`、`redis`、`s3`、`file_sink`、`jdbc` |

字段、默认值、密钥标记和示例以插件 schema API（`GET /api/v2/plugins/schema`）和
[docs/etl-config-schema.zh.md](./docs/etl-config-schema.zh.md)为准。

## 运行与构建

可以从 [Releases](../../releases) 下载压缩包，也可以直接运行容器镜像：

```bash
docker run -d --name openetl-go -p 8000:8000 -p 8001:8001 \
  -v "$PWD/pipes:/app/pipes" \
  ghcr.io/a8851625/openetl-go:latest
# podman 用户: 把 docker 换成 podman
```

源码构建：

```bash
make build
```

常用开发命令：

```bash
make test          # 带 -race 的单元测试
make test-quick    # 更快的内部 ETL 测试循环

cd web
npm install
npm run build      # 重新生成 resource/public
```

可选构建形态：

| 构建选项 | 效果 |
| --- | --- |
| 默认 | 纯 Go 核心、内置连接器和 Lua |
| `-tags=extism` | 启用 WASM 插件运行时 |
| `-tags=nolua` | 移除 Lua 运行时，减小二进制 |
| `CGO_ENABLED=1` | 通过 QuickJS 启用 JavaScript/TypeScript transform |

## 运行模型

- 配置文件：[`manifest/config/config.yaml`](./manifest/config/config.yaml)。
- Spec：`pipes/` 或 `etl.specsDir` 下的 YAML 文件，通过文件监听热加载。
- 存储：默认 SQLite；多节点共享状态使用 MySQL/PostgreSQL。
- API 认证：设置 `ETL_API_TOKEN` 后，通过 `X-API-Token` 或 `Authorization: Bearer <token>` 访问。
- 指标：Prometheus endpoint 为 `/metrics`。
- UI/API：GoFrame 在 `:8000` 提供 Web UI，并把 `/api/v2/*` 代理到 `:8001` 的 ETL API server。

## 文档

- [快速开始](./docs/quickstart.zh.md) / [English](./docs/quickstart.md)
- [产品定位](./docs/positioning.zh.md) / [English](./docs/positioning.md)
- [REST API](./docs/etl-api.zh.md) / [English](./docs/etl-api.md)
- [YAML 配置参考](./docs/etl-config-schema.zh.md) / [English](./docs/etl-config-schema.md)
- [幂等与投递语义](./docs/etl-idempotency.zh.md) / [English](./docs/etl-idempotency.md)
- [并行与批处理](./docs/parallelism-and-batching.zh.md) / [English](./docs/parallelism-and-batching.md)
- [Roadmap 与成熟度说明](./docs/ROADMAP.zh.md)
- [架构与生产就绪标准](./SPEC.md)
- [贡献指南](./CONTRIBUTING.zh.md) / [English](./CONTRIBUTING.md)

## License

MIT，详见 [LICENSE](./LICENSE)。
