# IT-3 任务分解

> 状态取值：`todo` / `active` / `done` / `blocked`。一次只允许一个 `active`。
>
> 三条技术线互不依赖，round 顺序可调；但 T3.6（基线测定）建议排在 IT-2 之后，
> 避免在待修复的实现上建立基线。

## Round 分组

| Round | 包含 task | 目标 | 可独立发版 |
| --- | --- | --- | --- |
| 1/5 | T3.1、T3.2 | secret 单一真值 + 存量明文迁移 | 是 |
| 2/5 | T3.3 | backup 无静默截断，导出内存可控 | 是 |
| 3/5 | T3.4 | restore 原子性与保真度 | 是 |
| 4/5 | T3.5、T3.6 | 两项待决策结论 + 实测容量基线 | 是 |
| 5/5 | T3.7 | 回归阈值接入 CI + 迭代收口 | 是 |

## 任务表

| ID | 任务 | 依赖 | 状态 | 证据落点 |
| --- | --- | --- | --- | --- |
| T3.1 | RA-5：descriptor 成为 secret 唯一真值 | — | `todo` | descriptor 遍历 conformance 测试 |
| T3.2 | RA-5：存量明文检测与幂等重新加密 | T3.1 | `todo` | 迁移测试、`check-plaintext-secrets.sh` |
| T3.3 | RA-6：去除硬截断 + 流式导出 | — | `todo` | `backup_large_test.go`、内存占用记录 |
| T3.4 | PR-1.3 残留：原子 restore + 保真度 + artifact/state | T3.3 | `todo` | 三 backend conformance |
| T3.5 | 两项待决策结论与文档落地 | — | `todo` | ROADMAP 决策记录、positioning/README |
| T3.6 | RA-8：实测容量基线 | IT-2、T3.5 | `todo` | `bench-baseline.sh` 原始输出 |
| T3.7 | 回归阈值接入 CI + 迭代收口 | T3.6、IT-1 | `todo` | CI 配置、README 看板 |

## 任务明细

### T3.1 RA-5：descriptor 成为 secret 唯一真值

**验收**（spec 验收 1、2、5）：

1. 标 `Secret: true` 的字段落盘为密文；未标但命中兜底模式的也加密并产生 WARN。
2. jdbc（`schema.go:417`）、dbt（`:650`）、enricher mode=sql（`:568`）、lookup（`:582`）
   四条 DSN 路径保存后，**直接查询 DB 表**得到密文。
3. 全量 descriptor 遍历测试：每个 `FieldString` 逐一判定是否承载凭据，遗漏即失败。
4. key rotation 后旧密文仍可读，新写入使用新 key（复用 PR-0.1 语义）。
5. `IsSecretFieldKey` 降级为兜底路径，仅在无 descriptor 时生效。

**证据落点**：`internal/etl/storage/secret_fields_test.go` 扩展、
`internal/etl/server/schema_secret_conformance_test.go`（新增）。

### T3.2 RA-5：存量明文检测与幂等重新加密

**验收**（spec 验收 3、4）：

1. 启动时检测未加密的 secret 字段，结果在日志与 health 中可见（**检测阶段只读不写**）。
2. 一次性重新加密路径可重复执行且幂等：已是 envelope 的跳过，明文的加密；中断后重跑
   不产生双重加密或损坏密文。
3. `hack/check-plaintext-secrets.sh` 扫描 portable backup 产物与 SQL dump，
   断言不含已知明文口令；该脚本接入 IT-1 的 `_gate.yml`。

**证据落点**：迁移测试、`hack/check-plaintext-secrets.sh`、CI 门禁配置。

### T3.3 RA-6：去除硬截断 + 流式导出

**验收**（spec 验收 6、7）：

1. 构造 >100000 行的 DLQ / audit / run_history 数据集，导出 → 恢复后行数与内容完全一致。
2. 若最终保留导出上限，则触达上限时 backup **失败**或在产物与 CLI 输出中显著标注截断，
   并在 `Snapshot` 元数据记录真实总数 —— 不允许维持「静默截断 + 报成功」。
3. 导出后校验 `Counts` 与实际导出行数一致，不一致则 backup 失败。
4. 大数据集导出的内存占用有实测记录，流式写出不因全量聚合而 OOM。

**证据落点**：`internal/etl/storage/backup/backup_large_test.go`（新增）、内存占用记录、
`docs/ops-runbook.md` 的 backup 容量说明。

### T3.4 PR-1.3 残留：原子 restore + 保真度

**验收**（spec 验收 8-11）：

1. restore 中途失败不留下半清空状态；可重试至一致结果（单事务或影子表切换）。
2. pipeline version 与 ID 原样保留，不重新分配。
3. run history 直接写入历史行，不通过 `RecordStart/End` 重放；时间、ID、状态不变。
4. plugin WASM artifact 随 backup 导出与恢复。
5. Redis transform state 纳入统一 backup/restore drill；无法纳入时**显式声明边界**
   并在 runbook 记录手工步骤。
6. 三 backend conformance 通过；`hack/e2e-backup-restore-{sqlite,mysql,postgres}.sh` 回归通过。
7. 若 IT-2 已交付 desired/observed 拆列，backup 覆盖新列。

**证据落点**：三 backend conformance 用例、`docs/ops-runbook.md`。

### T3.5 两项待决策结论与文档落地

**验收**（spec 验收 16）：

1. **schema evolution 立场**得到明确结论：
   - 选 (a)：写入 ROADMAP「明确暂缓或不做」；`positioning.zh.md` / `README.zh.md` 声明为
     有意边界，说明 `ddl_guard` 的拒绝语义与用户应如何应对源端 DDL。
   - 选 (b)：立项 additive-only 列变更，单独排期，**不在本迭代实施**。
2. **ClickHouse 吞吐是否立项**得到明确结论；若立项则并入 T3.6 的测定范围，
   共用脚本与硬件画像。
3. 两项结论均记录决策日期与理由。

**证据落点**：ROADMAP「待用户决策」条目更新；相关文档 diff。

### T3.6 RA-8：实测容量基线

**验收**（spec 验收 12-14）：

1. 每项指标有可重复的测定脚本与原始输出，非估算值：单 pipeline 吞吐（按 source/sink 分组）、
   并发 pipeline 数上限（按 backend）、稳态/峰值内存、启动耗时、checkpoint 延迟 p50/p95/p99、
   镜像与二进制大小（按 build tag）。
2. **sqlite checkpoint 排队拐点**有实测曲线，「多 streaming pipeline 推荐
   MySQL/PostgreSQL」由数据支撑（对应 BUG-3 的边界声明）。
3. 基线与具体 commit / 镜像 digest 绑定，跨版本可比较；硬件规格、数据集构造方式、
   并发参数全部记录 —— 缺任一项则该数字不得写入 `resource-baseline.md`。
4. 若 T3.5 决定立项 ClickHouse 吞吐，则包含 batch 大小 / flush 间隔 / 并发画像与
   `async_insert` 对比。

**注意**：本任务只**测定**并**声明**边界。发现的优化点记入有界后续，不在本迭代实施。

**证据落点**：`hack/bench-baseline.sh`（新增）、原始输出归档、
`docs/resource-baseline.md` 重写。

### T3.7 回归阈值接入 CI + 迭代收口

**验收**（spec 验收 15 + DoD）：

1. 回归阈值接入 IT-1 的 `_gate.yml`，先以 **warning 模式**运行一个版本周期，
   量化噪声水平后再转为门禁失败。
2. `spec.md` 的 16 项验收标准逐条核对为 `passed`。
3. ROADMAP 中 RA-5、RA-6、RA-8 置 `delivered`；PR-1.3 残留标注闭合；
   两项待决策记录结论与日期。
4. `ops-runbook.md`、`resource-baseline.md` 已重写。
5. `docs/iterations/README.md` 状态看板更新。

**证据落点**：CI 配置 diff；ROADMAP diff；README 看板。

## 领取记录模板

```text
Round: <n>/5
Roadmap item: <RA-5 | RA-6 | RA-8 | PR-1.3 residual> (IT-3/T3.<n>)
Profile/path: <standalone | storage backend>
Objective: <one observable outcome>
Scope: <files/components allowed>
Non-goals: <explicit exclusions>
Acceptance: <numbered checks，直接引用本文件对应 task 的验收>
Evidence: <commands, run URL, e2e, docs>
Result: <delivered|active|blocked_external>
Residual/follow-up: <bounded next item or none>
```
