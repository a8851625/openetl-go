# IT-3 技术方案

> 本文是实现输入，不是验收标准。验收以 [spec.md](./spec.md) 为准。

## 方案总览

三条独立技术线，无相互依赖，可按 round 顺序推进：

1. **secret 真值来源合并**：把 descriptor 的 `ConfigField.Secret` 提升为唯一真值，
   字段名模式匹配降级为兜底。这是消除「标了也不加密」这类反直觉行为的唯一办法 ——
   补 `dsn` 到 patterns 只能解决今天这一个字段，不能解决两套真值的结构问题。
2. **backup 完整性**：去掉硬截断，补齐 restore 原子性与保真度。
3. **容量基线**：把估算值替换为可复现的实测值，并把回归阈值接入 IT-1 的门禁。

## 分项设计

### A. RA-5：secret 单一真值来源

**现状**：两条互不相通的判定路径（见 [spec.md 问题陈述](./spec.md#问题陈述)）。

**目标**：

```text
descriptor ConfigField.Secret  ──(唯一真值)──▶ 保存 / 加密 / mask / 导出 / 轮换
                                     │
IsSecretFieldKey 字段名匹配 ──(兜底)──┘  仅在无 descriptor 时生效，且记录 WARN
```

**关键接口与文件**：

| 文件 | 改动性质 |
| --- | --- |
| `internal/etl/storage/secret_fields.go` | `EncryptConfigSecrets` 等接受 descriptor 提供的 secret 字段集合；`IsSecretFieldKey` 降级为兜底 |
| `internal/etl/storage/adapters.go:705` | 同上，改为消费传入的字段集合 |
| `internal/etl/server/server.go:5186/5220` | 传入 descriptor 派生的字段集合 |
| `internal/etl/server/schema.go` | 补 `:568` enricher、`:582` lookup 的 `dsn` 为 `Secret: true`；全量扫描 `dsn`/`url`/`uri`/`conn`/`endpoint` 类字段 |
| `internal/etl/server/connector_descriptor.go` | `secretFields()` 成为共享入口 |
| 新增迁移 | 启动时检测未加密 secret + 一次性重新加密 |

**设计权衡**：

| 备选 | 结论 | 理由 |
| --- | --- | --- |
| 只把 `dsn` 加进 patterns | 否决 | 解决单个字段，不解决两套真值；下一个新字段会重蹈覆辙 |
| 移除 descriptor `Secret`，全用字段名 | 否决 | 字段名猜测无法表达「这个 string 承载凭据」的语义 |
| **descriptor 为唯一真值 + 字段名兜底** | 采纳 | 语义准确、单点维护、兜底保证无 descriptor 路径不裸奔 |

**存量迁移语义**：检测 → 报告 → 重新加密三段分离。检测阶段只读不写并在 health 可见；
重新加密幂等（已是 envelope 的跳过，明文的加密），中断后重跑安全。

**新增守护**：`hack/check-plaintext-secrets.sh` 扫描 backup 产物与 SQL dump，接入 IT-1 的
`_gate.yml`，防止回归。

### B. RA-6 + PR-1.3 残留：backup 完整性

**现状**：`backup.go:114/131/146` 硬截断；`:299` 附近 restore 先 clear 再逐行写入无全局事务。

**目标**：

| 缺口 | 方案 |
| --- | --- |
| 硬截断 | 改游标分页全量导出；或保留上限但触达时**失败**并在 `Snapshot` 元数据记录真实总数与截断事实 |
| 导出内存 | 流式写出（逐页 append 到输出流），不在内存聚合全量 |
| restore 非原子 | 单事务包裹（backend 支持时）；或写入影子表后原子切换；失败留下可重试的一致状态 |
| version/ID 保真 | 导出与恢复保留原始 ID 与 version 序号，不重新分配 |
| run history 重建 | 直接写入历史行，不通过 `RecordStart/End` 重放 |
| plugin artifact | WASM 二进制随 backup 一并导出与恢复 |
| Redis state | 纳入统一 backup/restore drill；无法纳入时显式声明边界并在 runbook 记录手工步骤 |

**设计权衡**：全量分页 vs 保留上限 —— 优先全量分页；若某 backend 的实现代价过高，
则回退为「保留上限但失败」，**不允许**维持现状的「静默截断 + 报成功」（交付约束 3）。

**数据语义**：backup/restore 不触碰运行中的 checkpoint 语义。restore 后 pipeline 的
desired state 与 checkpoint 必须与快照时刻一致 —— 与 IT-2 的 desired/observed 拆分有交互，
若 IT-2 先行则 backup 需覆盖新列。

### C. RA-8：容量基线

**现状**：`resource-baseline.md` 多为估算或目标值。

**目标**：可复现的实测基线 + CI 回归阈值。

**测定项**：

| 指标 | 分组维度 |
| --- | --- |
| 单 pipeline 吞吐 | 按 source/sink 类型（CDC / batch / Kafka；关系型 / OLAP / 对象存储） |
| 并发 pipeline 数上限 | 按 storage backend（SQLite / MySQL / PostgreSQL） |
| 稳态与峰值内存 | 按并发档位 |
| 启动耗时 | 冷启动 / 含 restore |
| checkpoint 延迟分布 | p50 / p95 / p99 |
| 镜像与二进制大小 | 按 build tag 组合 |
| **sqlite checkpoint 排队拐点** | 并发 streaming pipeline 数递增下的劣化曲线（对应 BUG-3 边界） |

**方法约束**：固定硬件画像；数据集构造方式记录在脚本内；每次测定记录 commit、镜像 digest、
依赖版本、硬件规格。缺任一项则数字不得入库（交付约束 4）。

**CI 接入**：`hack/bench-baseline.sh` 产出结构化结果；回归阈值先以 warning 模式运行一个
版本周期，确认噪声水平后再转为门禁失败。

### D. 两项待决策的落地形态

| 决策 | 选 (a) 时的动作 | 选 (b) 时的动作 |
| --- | --- | --- |
| schema evolution 立场 | 写入 ROADMAP「明确暂缓或不做」；`positioning` / `README` 声明为有意边界，说明 `ddl_guard` 拒绝语义 | 立项 additive-only 列变更（破坏性变更仍拒绝），单独排期，不在本迭代实施 |
| ClickHouse 吞吐 | 不立项，RA-8 只测通用指标 | 与 RA-8 合并测定，共用 `hack/bench-baseline.sh` 与同一硬件画像，增加 batch 大小 / flush 间隔 / 并发画像与 `async_insert` 对比 |

## 架构约束

- 不引入外部 KMS / Vault、对象存储或新的基础设施依赖。
- 不改变加密 envelope 格式版本语义（只改**判定谁该加密**，不改**怎么加密**）。
- 不改变默认 storage backend。
- 基准脚本不得进入主二进制依赖链。

## 风险与回滚

| 风险 | 影响 | 缓解 | 回滚路径 |
| --- | --- | --- | --- |
| descriptor 覆盖不全，某字段既无 `Secret` 也不命中兜底 | 仍有明文落盘 | 全量 descriptor 遍历测试（每个 `FieldString` 逐一判定是否承载凭据）；`check-plaintext-secrets.sh` 兜底 | 兜底模式可临时放宽为更激进的匹配 |
| 存量重新加密中断导致混合状态 | 部分密文部分明文 | 幂等设计 + 检测报告先行 | 检测阶段只读，未写入前可随时中止 |
| 全量分页导出使 backup 耗时显著增长 | 运维窗口不足 | 流式写出 + 实测耗时记录；必要时提供选择性导出 | 回退为「上限 + 失败」形态 |
| restore 原子化在某 backend 不可行 | 三 backend 行为不一致 | 影子表切换作为备选；不可行则显式声明该 backend 边界 | 保留逐行写入 + 明确的失败可重试语义 |
| 基线在错误实现上建立 | IT-2 修复后基线失效 | 建议 IT-3 的 RA-8 部分排在 IT-2 之后 | 重新测定 |
| 基准噪声导致 CI flaky | 门禁失去信任 | warning 模式跑满一个版本周期，量化噪声后再定阈值 | 阈值可调，随时退回 warning |

## 测试策略

| 层级 | 范围 | 命令 |
| --- | --- | --- |
| 1 静态与单测 | secret 判定、descriptor 遍历、分页导出 | `go test ./internal/etl/storage/... ./internal/etl/server/...` |
| 2 包级 / `-race` | 迁移幂等、并发导出 | `go test -race ./internal/etl/storage/...` |
| 3 后端矩阵 | 三 backend 的 backup/restore conformance | `hack/e2e-backup-restore-{sqlite,mysql,postgres}.sh` |
| 4 容器 e2e / 安全扫描 | 大数据集导出恢复、明文扫描、容量基线 | `hack/check-plaintext-secrets.sh`、`hack/bench-baseline.sh` |
| 5 外部环境认证 | 不适用 | — |
