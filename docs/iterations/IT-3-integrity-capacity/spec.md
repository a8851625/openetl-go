# IT-3：完整性与容量

> 状态：`queued` | 依赖：IT-1 | 归属 roadmap 条目：RA-5、RA-6、RA-8、PR-1.3 残留、2 项待决策

## 问题陈述

本迭代的缺口有共同形态：**声明与事实脱节，且只在故障或扩容当天才暴露**。

**密钥保护声明与落盘事实不一致**：加密与 API masking 使用两套互不相通的真值来源。

- 落盘加密走字段名子串匹配：`internal/etl/storage/secret_fields.go:10-12`
  `{"password","passwd","secret","token","api_key","apikey","credential","private_key"}`，
  消费点在 `secret_fields.go:65/114/185`、`adapters.go:705`、`server.go:5186/5220`。
  **该列表中没有 `dsn`。**
- API masking 与 AI context 走 descriptor 元数据：`connector_descriptor.go:452` 的
  `secretFields()` 读 `field.Secret`；`ai_context.go:329/333` 同理。

后果是 `internal/etl/server/schema.go:417`（jdbc）与 `:650`（dbt）的 `dsn` 虽标了
`Secret: true`，**在 API 被 mask，落盘时却因字段名不匹配而不加密**；`:568`（enricher mode=sql）
与 `:582`（lookup 维表）的 `dsn` 连 `Secret: true` 都没有，两侧都不保护。含用户名密码的 DSN
以明文写入 storage row，并随之进入 SQL dump、portable backup 产物与数据库备份 —— 而 UI 上
显示为已掩码，进一步掩盖了风险。

**备份声称成功但不完整**：`internal/etl/storage/backup/backup.go:114`（DLQ）、`:131`（audit）、
`:146`（run history）各自硬编码 `Limit: 100000`，超出部分静默丢弃，backup 仍报成功并写出
`Counts`。BUG-3 已证明 audit / run_history 在无 TTL 时会膨胀到数十万行量级（用户环境
`etl.db` 达 688MB）。此外 PR-1.3 仍有残留：restore 先 clear 再逐表逐行写入无全局事务、
pipeline version/ID 未原样保留、run history 通过重新 `RecordStart/End` 重建导致时间与 ID 变化、
plugin backup 只留 metadata/path 不复制 WASM artifact、Redis transform state 不在统一
backup/restore drill 内。

**容量边界靠经验判断而非实测**：`resource-baseline.md` 按审计「多为估算或目标值，不是当前
release 的实测记录」。选择「轻量自托管」的用户通常没有 SRE 兜底，最需要的恰恰是「单实例能扛
多少」的确定答案，而当前无法回答：单进程支持多少并发 pipeline、每类 storage backend 的 TPS
拐点、稳态内存与 checkpoint 延迟分布。BUG-3 已证明 sqlite 存在硬容量边界，但该边界目前只有
文字描述，无量化拐点。

## 可观察结果

1. 任何标记 `Secret: true` 的字段在 storage row 中为密文；backup 产物与 SQL dump 扫描
   不含已知明文口令。
2. backup → restore 全量保真：>100k 行的 DLQ/audit/run_history、pipeline version 与 ID、
   plugin WASM artifact、Redis transform state 均可恢复，且 restore 具备原子性。
3. `resource-baseline.md` 中每项指标为实测值，标注测定日期、commit、镜像 digest 与硬件规格。
4. sqlite 容量拐点有实测曲线，「多 pipeline 推荐 MySQL/PostgreSQL」由数据支撑而非经验判断。

## 验收标准

| # | 验收标准 | 判定方式 |
| --- | --- | --- |
| 1 | descriptor 成为 secret 唯一真值来源 | 标 `Secret: true` 的字段落盘为密文；未标但命中兜底模式的也加密并产生 WARN |
| 2 | 四条 DSN 路径落盘为密文 | jdbc、dbt、enricher(mode=sql)、lookup 保存后直接查询 DB 表得到密文 |
| 3 | 存量明文可迁移 | 启动时检测未加密 secret，一次性重新加密路径可重复执行且幂等 |
| 4 | 产物无明文 secret | 扫描 portable backup 与 SQL dump，断言不含已知明文口令（新增安全测试） |
| 5 | key rotation 不回退 | 旧密文仍可读，新写入使用新 key（复用 PR-0.1 语义） |
| 6 | backup 无静默截断 | >100000 行的 DLQ/audit/run_history 导出→恢复后行数与内容完全一致；若保留上限则触达时**失败或显著标注**，不得报成功 |
| 7 | 导出内存可控 | 大数据集导出的内存占用有实测记录，不因全量加载而 OOM |
| 8 | restore 原子性 | restore 中途失败不留下半清空状态；可重试至一致结果 |
| 9 | 保真度 | pipeline version 与 ID 原样保留；run history 时间/ID/状态不因恢复而改变 |
| 10 | artifact 与 state 纳入 | plugin WASM artifact 被复制；Redis transform state 在统一 backup/restore drill 内 |
| 11 | 三 backend conformance | SQLite / MySQL / PostgreSQL 均通过；既有 `hack/e2e-backup-restore-*.sh` 回归通过 |
| 12 | 基线为实测 | 每项指标有可重复测定脚本与原始输出，非估算 |
| 13 | sqlite 拐点量化 | checkpoint 排队开始劣化的并发 pipeline 数有实测曲线 |
| 14 | 基线可比较 | 与具体 commit / 镜像 digest 绑定，跨版本可比 |
| 15 | 回归阈值接入 CI | 显著劣化可触发失败（允许先以 warning 模式运行一个版本周期） |
| 16 | 两项待决策已结论 | schema evolution 立场与 ClickHouse 吞吐立项均有明确结论并落文档 |

## 非目标

- 不引入外部 KMS / Vault 依赖。
- 不改变现有加密 envelope 的格式版本语义。
- 不在本迭代实现字段级审计。
- 不做与 Flink / SeaTunnel 的横向性能对比。
- 不改变默认 storage backend（sqlite 保持开箱默认，只声明边界）。
- 不为提升基线数字而调整默认配置。

## 交付约束

1. **不得为通过测试而降低保护强度**。若某字段因兼容原因暂时不能加密，必须显式登记豁免
   并在 health / 启动日志中可见，不得从 descriptor 移除 `Secret` 标记。
2. **迁移必须幂等且可重复**。存量明文重新加密在中断后重跑不得产生双重加密或损坏密文。
3. **backup 语义只能变严不能变松**。若最终保留导出上限，触达上限必须是失败或显著标注，
   不允许维持「静默截断 + 报成功」。
4. **基线测定条件必须可复现**。硬件规格、镜像 digest、数据集构造方式、并发参数全部记录；
   缺任一项则该数字不得写入 `resource-baseline.md`。
5. **性能优化不属于本迭代**。本迭代只**测定**并**声明**边界；发现的优化点记入有界后续，
   不在本迭代实施。
6. **两项待决策先于相关 task 结论**。schema evolution 立场决定文档写法，
   ClickHouse 吞吐是否立项决定 RA-8 的测定范围。

## 依赖与前置

| 类型 | 内容 | 状态 |
| --- | --- | --- |
| 迭代依赖 | IT-1（回归阈值需接入 CI 门禁） | `queued` |
| 迭代依赖 | IT-2（建议在正确性稳定后再测性能，避免为错误实现建立基线） | `queued` |
| 外部输入 | 固定硬件画像的测定环境 | 需确认 |
| 待决策 | schema evolution 立场：(a) 声明为边界 / (b) 立项做 additive-only | 未决 |
| 待决策 | ClickHouse 写入吞吐是否立项；若立项则与 RA-8 合并测定，共用脚本与硬件画像 | 未决 |

## 完成定义（DoD）

1. `tasks.md` 中全部 task 状态为 `done`，每项有实际执行的证据记录。
2. 上方 16 项验收标准全部 `passed`。
3. ROADMAP 中 RA-5、RA-6、RA-8 置 `delivered`；PR-1.3 残留条目标注闭合；
   两项待决策记录结论与决策日期。
4. `ops-runbook.md`、`resource-baseline.md` 已重写；若 schema evolution 选择 (a)，
   则 `positioning.zh.md` / `README.zh.md` / ROADMAP「明确暂缓或不做」同步更新。
5. `git diff --check` 通过；测定产生的原始输出已归档且不含真实凭据。
