# 产品定位

OpenETL-Go 的核心定位是：

> 轻量、自托管、开源的 CDC/ETL 数据同步、清洗、汇聚运行时。

它面向数据库、Kafka、文件、HTTP、对象存储、OLAP 和搜索系统之间的常见数据管道，用一套 `Source -> Transform -> Sink` 模型提供 YAML、API 和 Web UI 三种入口，并内置 checkpoint、重试、DLQ、指标、审计、连接目录、schema/preflight 校验和可扩展 transform。

## 适合的场景

- MySQL/PostgreSQL 全量、增量、snapshot+CDC 同步到 ClickHouse、MySQL、PostgreSQL、Doris、Elasticsearch、S3 或 Kafka。
- Kafka JSON/Debezium 事件清洗、补维、去重、tumbling window 聚合后写入明细表或聚合表。
- 文件、HTTP、Redis、对象存储等轻量数据源的批量抽取、转换和落地。
- 替代脚本化同步任务、轻量 DataX/Canal/Kafka consumer 程序，以及不想引入 Flink/Spark/Airbyte 全套平台的中小型自托管链路。
- 作为 Airflow、Dagster、Prefect、Kestra 等调度系统里的一个可运行数据管道任务。

## 不适合的场景

- 复杂有状态实时业务计算，例如任意 keyed state、processing-time timer、CoProcessFunction、多流状态机、复杂告警生命周期。
- 需要 Flink/Spark 级 savepoint、exactly-once 状态快照、SQL planner、复杂 sliding/session window、late side-output、retraction 的任务。
- 以 SaaS API 连接器数量为核心价值的 ELT 平台替代品。
- 大规模 Kafka Connect/Debezium CDC 基础设施替代品，尤其是已经依赖标准 Debezium envelope、Kafka Connect offset/connector 运维生态的团队。

## 和常见项目的关系

| 项目 | 更擅长 | OpenETL-Go 的差异 |
| --- | --- | --- |
| Airbyte / Meltano / dlt | SaaS/数据库到数仓的 ELT 连接器生态 | 更轻，偏 CDC/实时同步、运行治理、DLQ replay 和自托管小闭环 |
| Airflow / Dagster / Prefect | 工作流调度和任务依赖编排 | 内置数据读写、checkpoint、DLQ、幂等建议；也可以被这些调度器调用 |
| Apache SeaTunnel / ChunJun | 更重的批流数据集成、大规模分布式执行 | 更小、更易部署，以单机/小集群和 Go 单二进制为默认体验 |
| Kafka Connect / Debezium | 标准 CDC connector runtime 和 Kafka 生态 | 更一体化，覆盖 transform、sink、DLQ、UI/API；CDC 协议成熟度不应夸大 |
| DataX | 稳定离线批同步 | 更实时，带 CDC、checkpoint、DLQ、UI/API 和轻量聚合 |
| Flink / Spark Streaming | 复杂有状态流计算 | 不追求替代，OpenETL-Go 只覆盖轻量清洗、补维、去重和 tumbling window 汇聚 |

## 产品原则

- 可靠优先：失败记录必须可见，进入 DLQ、返回错误触发 retry，或由显式允许的 drop 策略计数审计。
- 轻量优先：默认路径应能单二进制或单容器启动，SQLite 单机可用，MySQL/PostgreSQL 存储用于共享状态和 master-worker。
- 易用优先：常见任务应通过 UI/API/YAML 快速完成，preflight 应在启动前解释风险、字段问题和幂等策略。
- 可扩展优先：连接器、transform 和插件共享 descriptor、schema、preflight、metrics、DLQ、测试认证合约。
- 诚实优先：默认投递语义是 at-least-once；生产依赖业务主键、版本列、upsert、ReplacingMergeTree 或 deduplicate 消除重放影响。

