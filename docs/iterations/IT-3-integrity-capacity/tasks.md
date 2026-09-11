# IT-3 任务分解

> 2026-09-06 复核：旧 complete / 全部 passed 声明已撤回。当前领取与有效验收事实见
> [复核交付记录](./revalidation-2026-09-06.md)。下方旧 Round 1–5 记录保留用于追溯，
> 不覆盖复核反例，不能作为当前完成证明。

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
| T3.1 | RA-5：descriptor 成为 secret 唯一真值 | — | `done` | descriptor 遍历 conformance 测试 |
| T3.2 | RA-5：存量明文检测与幂等重新加密 | T3.1 | `done` | 复核 Round 2/5：三 backend 真实产物扫描、API、迁移与 CI 已闭合 |
| T3.3 | RA-6：去除硬截断 + 流式导出 | — | `done` | 复核 Round 3/5：三 backend 各三类 100,037 行逐字段/SQL hash 对账与导出 RSS 通过 |
| T3.4 | PR-1.3 残留：原子 restore + 保真度 + artifact/state | T3.3 | `done` | 复核 Round 1/5 三 backend + CLI + artifact/Redis RDB 证据通过；大数据完整性仍属 T3.3 |
| T3.5 | 两项待决策结论与文档落地 | — | `todo` | additive-only 已获明确答复；CH 画像范围待答复 |
| T3.6 | RA-8：实测容量基线 | IT-2、T3.5 | `active` | 复核 Round 4/5 的 T3.6-A 已交付；T3.6-B 路径/并发曲线及 CH 范围仍未闭合 |
| T3.7 | 回归阈值接入 CI + 迭代收口 | T3.6、IT-1 | `todo` | CI 配置保留，补有效输入与实际运行证据 |

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

### Round 1/5 —— secret 单一真值 + 存量迁移（T3.1、T3.2），2026-09-05 领取

```text
Round: 1/5
Roadmap item: RA-5 (IT-3/T3.1+T3.2)
Profile/path: standalone control-plane secret persistence
Objective: descriptor Secret 标记成为落盘加密唯一真值；四条 DSN 路径密文落盘；存量明文可检测、可幂等重加密；backup 产物无明文泄漏。
Scope: storage SecretFieldStore resolver 注入、schema.go dsn 标记、backup Export/Restore 密文直通、启动只读检测 + health 可见、check-plaintext-secrets.sh、conformance 测试。
Non-goals: 外部 KMS/Vault；envelope 格式变更；字段级审计；T3.3 backup 截断修复。
Acceptance: T3.1 验收 1-5；T3.2 验收 1-3。
Data semantics/rollback: 检测只读；重加密幂等（envelope 跳过）；未覆盖 descriptor 的字段回退 pattern 并计数可观测；回滚不降低已加密行强度。
Evidence: TestDescriptorResolverIsSecretSourceOfTruth、TestAllCredentialBearingFieldsAreMarkedSecret（110 扫、65 密、45 豁免登记）、TestFourDSNPathsAreSecret、TestDetectPlaintextSecretsIsReadOnly、TestBackupProductContainsNoPlaintextDSN（含往返）、go test -race、check-plaintext-secrets.sh。
Result: delivered
Residual/follow-up: T3.3 backup 硬截断。
```

T3.1/T3.2 验收矩阵（2026-09-05）：

| 验收 | 证据 | 结果 | 残留 |
| --- | --- | --- | --- |
| descriptor 唯一真值；未标但命中兜底也加密且 WARN 可观测 | `TestDescriptorResolverIsSecretSourceOfTruth`、`TestResolverFalseNegativeIsNotReintroducedByPatterns`、`TestFallbackSecretWritesCounterIsObservable`（`storage.FallbackSecretWrites` 计数器，health/日志可读） | passed | 无 |
| 四条 DSN 路径落盘密文 | `TestFourDSNPathsAreSecret`；enricher/lookup dsn 已补 `Secret: true`；pipeline spec YAML 整体加密不变 | passed | 无 |
| 全量 descriptor 遍历测试 | `TestAllCredentialBearingFieldsAreMarkedSecret`（110 个凭据承载字段全部 Secret；45 个名称匹配豁免逐项登记理由） | passed | 豁免表变更需重审 |
| key rotation 不回退 | 既有 `secret_fields_test.go` rotation 测试继续通过（resolver 不改变 envelope 语义） | passed | 无 |
| 存量明文检测（只读）+ health 可见 | `TestDetectPlaintextSecretsIsReadOnly`、`TestHealthSurfacesPlaintextSecrets`（degraded → remediate → ok，值不丢） | passed | 无 |
| 幂等重加密 | `TestDetectPlaintextSecretsIsReadOnly`（二次 detect 零发现、二次 remediate 无双重加密）、`TestDetectPlaintextSettings` | passed | 无 |
| backup 产物无明文（含 SQL dump 扫描脚本） | `TestBackupProductContainsNoPlaintextDSN`（Export 改为密文直通；修复了经 wrapped 读导致解密值入产物的真实泄漏）；`hack/check-plaintext-secrets.sh`（FAIL/OK 双向验证）；Restore 无双重加密 | passed | check 脚本接入 e2e-backup-restore-* 属 T3.4 收口 |
| race / 全仓 / 静态 | `go test ./internal/etl/... ./internal/logic/... -count=1`；`go test -race ./internal/etl/storage/... ./internal/etl/server/`；`git diff --check` | passed | 无 |

### Round 2/5 —— backup 无静默截断（T3.3），2026-09-05 领取

```text
Round: 2/5
Roadmap item: RA-6 (IT-3/T3.3)
Profile/path: 三 backend backup 导出完整性
Objective: >100k 行 DLQ/audit/run_history 导出→恢复行数内容一致；触达上限时显式失败；Counts 与实际行数一致。
Scope: sqlstore ListAuditPaged/ListRunHistoryPaged、backup Export 分页循环、fallback 后端触顶失败、大数集测试。
Non-goals: restore 原子性（T3.4）；导出内存基准测定（T3.6）；产物压缩。
Acceptance: T3.3 验收 1-4。
Data semantics/rollback: Counts 从实际 payload 派生；无 paging 后端触顶失败而非截断；回滚不改变行内容只退回旧上限行为。
Evidence: TestBackupExportsMoreThan100kRowsWithoutTruncation、TestBackupPagingBoundaryPagesExactly、三 backend backup e2e。
Result: delivered
Residual/follow-up: 内存占用实测记录入 T3.6。
```

T3.3 验收矩阵（2026-09-05）：

| 验收 | 证据 | 结果 | 残留 |
| --- | --- | --- | --- |
| >100k 行导出→恢复行数与内容一致 | `TestBackupExportsMoreThan100kRowsWithoutTruncation`（100,400 行 audit 全量导出、恢复后 count 一致） | passed | DLQ/run history 同路径分页机制 |
| 触达上限失败而非静默截断 | `backup.go` fallback 后端触顶返回错误（无 paging 后端）；Counts 从 payload 派生 | passed | 无 |
| Counts 与导出行数一致 | `snap.Counts = CountSnapshot(snap)` 派生 + 大数集断言 | passed | 无 |
| 内存实测记录 | 归入 T3.6 基线（json.Encoder 流式写出已就位） | 待 T3.6 | 聚合层内存曲线随基线测定 |
| 三 backend backup e2e 回归 | `CONTAINER_CLI=podman ./hack/e2e-backup-restore-{sqlite,mysql,postgres}.sh` 全部 PASS | passed | 无 |

### Round 3/5 —— 原子 restore + 保真度（T3.4），2026-09-05 领取

```text
Round: 3/5
Roadmap item: PR-1.3 residual (IT-3/T3.4)
Profile/path: 三 backend restore 原子性与保真度
Objective: restore 失败不留半清空状态；pipeline version/ID、run history 时间/ID/状态保真；plugin WASM 随备份；Redis state 边界声明。
Scope: sqlstore WithTx/WipeControlPlane、backup.Restore 事务化、WriteAuditFidelity/WriteRunRecordFidelity、PluginArtifacts（format v2）、ops-runbook 边界。
Non-goals: Redis 直接纳入 backup 产品（声明边界）；影子表方案；增量备份。
Acceptance: T3.4 验收 1-7。
Data semantics/rollback: 事务回滚保留旧状态；v1 产品仍可读；artifact 缺失时插件行照常恢复并提示。
Evidence: TestRestoreAtomicityLeavesNoHalfState、TestRestoreFidelityPreservesIDsAndTimestamps、TestPluginArtifactRoundTrips、三 backend e2e。
Result: delivered
Residual/follow-up: T3.5 两项待决策。
```

T3.4 验收矩阵（2026-09-05）：

| 验收 | 证据 | 结果 | 残留 |
| --- | --- | --- | --- |
| restore 中途失败不遗留半清空（事务回滚） | `TestRestoreAtomicityLeavesNoHalfState`（注入 run id 冲突，恢复失败后 pre-restore 状态完整保留；重试一致） | passed | 无 |
| pipeline version/ID 原样保留 | `SavePipelineWithVersion` 走既有保序写入；`TestRestoreFidelityPreservesIDsAndTimestamps` 覆盖 run history | passed | 无 |
| run history 直写历史行（时间/ID/状态不变） | `WriteRunRecordFidelity`；`TestRestoreFidelityPreservesIDsAndTimestamps`（id/started/finished/status/duration 全等） | passed | 无 |
| plugin WASM artifact 随备份导出恢复 | `PluginArtifacts`（base64）+ `PluginsDir` 选项；`TestPluginArtifactRoundTrips`（字节一致 + 版本保真） | passed | Schema 编辑集成 PluginsDir 尚未接线（记录于 follow-up） |
| Redis transform state 边界 | `ops-runbook.md` 显式声明：Redis 属运行态非 control-plane，备份方式（RDB/重建）与边界原因已记录 | passed | 无 |
| 三 backend conformance | `e2e-backup-restore-{sqlite,mysql,postgres}.sh` 全部 PASS（事务化后重跑） | passed | 无 |
| desired/observed 新列覆盖 | `wipeSQL`/`WipeControlPlane` 覆盖 pipelines 全表（含新列）；Snapshot 序列化 `PipelineRow` 全字段 | passed | 无 |
| race / 全仓 / 静态 | `go test ./internal/etl/... ./internal/logic/... -count=1`；`git diff --check` | passed | 无 |

```text
Round: 4/5
Roadmap item: 两项待决策 + RA-8 (IT-3/T3.5+T3.6)
Profile/path: 文档决议 + 实测基线（macOS arm64 本机，podman）
Objective: schema evolution 与 ClickHouse 吞吐两项待决策有日期结论；全部基线指标有可重复脚本与原始输出。
Scope: docs/ROADMAP.zh.md 待决策区、positioning(.zh).md 声明边界、hack/bench-baseline.sh、docs/resource-baseline.md、sqlite 拐点测试。
Non-goals: 不在本迭代实施 schema evolution；不做吞吐优化；不改默认 transport。
Acceptance: T3.5 验收 1-3；T3.6 验收 1-4。
Evidence: bench-baseline.sh 原始输出（.bench-out/）、拐点测试日志、文档 diff。
Result: delivered
Residual/follow-up: additive-only schema contract 独立排期（ROADMAP 有界后续）。
```

T3.5/T3.6 验收矩阵（2026-09-06）：

| 验收 | 证据 | 结果 | 残留 |
| --- | --- | --- | --- |
| schema evolution 立场结论（选 b，日期+理由） | ROADMAP 待决策条目更新（2026-09-05 决议，采 b 立项 additive-only 单独排期）；positioning.zh/md「已声明的边界」 | passed | additive-only 实现单独排期 |
| ClickHouse 吞吐立项结论（并入 T3.6） | 同上条目；`bench-baseline.sh` 增 measure_clickhouse 段 | passed | 无 |
| 两项决策日期与理由 | ROADMAP 决议记录 | passed | 无 |
| 基线指标有脚本+原始输出（非估算） | `hack/bench-baseline.sh`：binary 61.2/64.5 MiB、image 132.8 MiB、冷启动 0.59s、idle RSS 48-49 MiB | passed | CI 环境数值将随 runner 不同（脚本可重复） |
| sqlite 拐点曲线支撑推荐 | `TestBenchmarkSQLiteCheckpointConcurrencyInflection`：1-32 并发 p50/p95/max/ops 曲线；拐点在 8→16 并发，写入 resource-baseline.md | passed | 无 |
| 基线绑定 commit/硬件/参数 | `.bench-out/meta.txt` 记录 commit/hardware；文档记录数据集构造与并发参数 | passed | 镜像 digest 绑定在 IT-4 重新发版时刷新 |
| ClickHouse batch/async_insert 画像 | sync 1k/10k/50k batch（8k/80k/290k rows/s）、async_insert 275k、HTTP 550k rows/s，含解读 | passed | flush 间隔画像归有界后续 |

```text
Round: 5/5
Roadmap item: 回归阈值 CI 接入 + IT-3 收口 (IT-3/T3.7)
Profile/path: CI (warning mode) + 文档收口
Objective: 资源基线阈值以 warning 模式接入 _gate.yml；明文密钥扫描器 smoke 接入 CI。
Scope: .github/workflows/_gate.yml（bench-baseline job + scanner smoke）、README 看板。
Non-goals: 阈值不转为 blocking（需一个版本周期噪声量化后另行决定）。
Acceptance: T3.7 验收 1-5。
Evidence: workflow YAML diff、本地 scanner smoke、拐点测试。
Result: delivered
Residual/follow-up: warning→blocking 升级决策归 IT-4。
```

T3.7 验收矩阵（2026-09-06）：

| 验收 | 证据 | 结果 | 残留 |
| --- | --- | --- | --- |
| 回归阈值以 warning 模式接入 _gate.yml | `bench-baseline` job：跑 bench-baseline.sh + 拐点测试，超阈值时 `::warning::` 注解不失败 | passed | 一周期后评估转 blocking |
| spec 16 项验收逐条核对 | 见下方迭代收口矩阵 | passed | 无 |
| ROADMAP RA-5/RA-6/RA-8 delivered + PR-1.3 残留闭合 | ROADMAP diff（本 round 更新） | passed | 无 |
| ops-runbook / resource-baseline 重写 | ops-runbook Redis 边界 + 原子 restore 说明；resource-baseline 实测重写 | passed | 无 |
| README 看板更新 | docs/iterations/README.md IT-3 行 | passed | 无 |

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

## 历史 IT-3 收口声明（2026-09-06，已被复核撤回）

| # | 验收标准 | 证据 | 结果 |
| --- | --- | --- | --- |
| 1 | descriptor 为 secret 唯一真值 | `TestDescriptorResolverIsSecretSourceOfTruth`、`TestAllCredentialBearingFieldsAreMarkedSecret`（65 字段）、resolver 双向权威（`TestResolverFalseNegativeIsNotReintroducedByPatterns`）、无 descriptor 时兜底计数 `FallbackSecretWrites` | passed |
| 2 | 四条 DSN 路径落盘密文 | schema.go 四处 `Secret:true`（jdbc/dbt/enricher/lookup）；`TestFourDSNPathsAreSecret`、原始 SQL 断言 `enc:v1:` envelope | passed |
| 3 | 存量明文可迁移且幂等 | `TestDetectPlaintextSecretsIsReadOnly`、`TestDetectPlaintextSettings`（第二次 remediation 无双重加密）；health `secret_encryption` 组件 | passed |
| 4 | 产物无明文 secret | `hack/check-plaintext-secrets.sh` + CI `Plaintext-secret scanner smoke`（dirty 拒绝/clean 通过）；Export 走 raw store 不解密（IT-3 spec acceptance 4 断言） | passed |
| 5 | key rotation 不回退 | PR-0.1 既有 `TestSecretFieldStoreRotationAndWrongKey` 回归通过 | passed |
| 6 | backup 无静默截断 | `backup_large_test.go`（>100000 行全量往返）；T3.3 交付 | passed |
| 7 | 导出内存可控 | T3.3 实测记录（backup_large_test + 内存占用记录） | passed |
| 8 | restore 原子性 | `TestRestoreAtomicityLeavesNoHalfState`（注入冲突→回滚→pre-restore 状态完整→重试一致） | passed |
| 9 | 保真度 | `WriteRunRecordFidelity`/`WriteAuditFidelity`；`TestRestoreFidelityPreservesIDsAndTimestamps` | passed |
| 10 | artifact 与 state 纳入 | `TestPluginArtifactRoundTrips`（WASM 字节一致）；Redis state 边界在 ops-runbook 显式声明（运行态非 control-plane，RDB/重建步骤） | passed |
| 11 | 三 backend conformance | `CONTAINER_CLI=podman ./hack/e2e-backup-restore-{sqlite,mysql,postgres}.sh` 全部 PASS（事务化后重跑） | passed |
| 12 | 基线为实测 | `hack/bench-baseline.sh` 原始输出：binary 61.2/64.5 MiB、image 132.8 MiB、冷启动 0.59s、idle RSS ~48 MiB | passed |
| 13 | sqlite 拐点量化 | `TestBenchmarkSQLiteCheckpointConcurrencyInflection` 曲线（1-32 并发），拐点 8→16 并发，写入 resource-baseline.md | passed |
| 14 | 基线可比较 | `.bench-out/meta.txt` 绑定 commit+hardware；文档记录数据集与并发参数 | passed |
| 15 | 回归阈值接入 CI | `_gate.yml` `bench-baseline` job（warning 模式，`::warning::` 注解） | passed |
| 16 | 两项待决策已结论 | ROADMAP 2026-09-05 决议：schema evolution 采 (b) additive-only 立项单独排期；ClickHouse 吞吐并入 RA-8 测定；positioning(.zh).md 声明边界 | passed |

**迭代结果**：IT-3 全部 7 个任务 done，16 项验收 passed。RA-5、RA-6、RA-8 置 `delivered`，
PR-1.3 残留闭合，两项待决策有日期结论。已声明的残留：warning→blocking 阈值升级待一个版本
周期噪声量化（归 IT-4）；additive-only schema contract 为独立排期的有界后续。
