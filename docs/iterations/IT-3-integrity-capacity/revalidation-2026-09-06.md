# IT-3 复核修复与后续交付（2026-09-06）

本记录承接用户对交付声明的审查及“继续完成后续的迭代”的授权。旧 IT-3
`complete` 声明被实际反例推翻，先修复既有验收缺口，再进入 IT-4；不扩大产品范围。
P0 MaxCompute 继续 `blocked_external`，所缺真实环境凭据与解除条件不变。

## Round 1/5：恢复修复（已交付）

```text
Round: 1/5
Roadmap item: PR-1.3 residual / IT-3 T3.4
Profile/path: standalone control-plane; SQLite/MySQL/PostgreSQL backup/restore
Objective: v1/v2 备份可恢复；控制面恢复失败不丢原状态；版本、运行历史、DLQ 与插件文件恢复后可用且保真。
Eligibility: 用户在审查确认恢复缺陷后授权继续交付；这些是既有 PR-1.3 验收缺口，不是新增 roadmap 项。
Scope: internal/etl/storage/{backup,sqlstore,adapters}、必要的备份命令入口、三 backend conformance/e2e、ops runbook 与本迭代证据。
Non-goals: 新 connector、Redis 自动备份产品、吞吐优化、schema evolution 实现、发布 maturity 升级。
Dependencies: IT-2 lifecycle/DLQ 新列保持不变；真实 MySQL/PostgreSQL 测试实例；本机 Go 可用，容器认证不能用本机单测替代。
Acceptance: 1) v1/v2；2) ID-keyed version 导出及原始 ID/序号/时间恢复；3) 原始与加密包装 store 的事务失败回滚；4) WASM 迁移后路径有效、失败不覆盖旧 artifact；5) 三后端对账及后续自增写入；6) 可执行运维入口、runbook、race/e2e。
Data semantics/rollback: 维护窗口离线恢复；checkpoint/source position 与 desired state 原样保存；不回放源或调用 sink；DLQ 原文/身份上下文不变；密文直通且不双重加密。插件先写入独立不可变目录，SQL 原子提交新路径；失败不修改原 artifact，孤立暂存文件可安全清理。
Evidence: 七个审查反例、针对性/共享 conformance、hack/e2e-backup-restore-{sqlite,mysql,postgres}.sh、docs/ops-runbook.md。
Result: delivered
Residual/follow-up: RA-5 安全产物扫描；RA-6 流式导出/大数集；RA-8 有效基线/CI；IT-4 按既有顺序收口。
```

## 有界增量

| 增量 | 可观察结果 | 允许接口/文件 | 验收及故障数据 | 回滚与证据 |
| --- | --- | --- | --- | --- |
| T3.4-R1a | 带多个历史版本的备份可完整恢复，失败保留旧库 | backup、sqlstore、secret wrapper、共享 conformance | v1/v2、ID 与 name 不同、版本间隙、重复版本/故障注入、三 backend、后续序列分配 | 单事务回滚；密文/位点不变；本记录回填 |
| T3.4-R1b | 插件随备份迁移到新主机后仍可加载 | backup artifact staging、运维命令、plugin fixture、runbook | 缺 artifact、非法名称/路径、坏 base64、后续 SQL 失败、重试、旧 v1 边界 | 不覆盖原文件；提交后路径指向已持久化文件 |
| T3.4-R1c | 维护者通过同一入口完成三个 backend 恢复演练 | backup CLI/hack、三 backend e2e、runbook | 加密包装、版本/ID/时间/位点/身份逐字段对账、失败回滚、恢复后继续写入 | 隔离测试资源；记录命令与 pass/fail/skip，不外推认证 |

后续领取仍一次一个主项：RA-5/RA-6 补齐产物扫描、流式导出与大数据内存证据；
RA-8 完成合法写入的基线与 warning CI；随后按 IT-4 T4.1→T4.2→T4.3→T4.4→T4.5 执行。
本轮计数仅在完成 claim-to-close 时增加，不把测试重试计作新 round。

## 当前验收事实

| 原声明 | 复核证据 | 结果 | 必须补齐 |
| --- | --- | --- | --- |
| v1 可恢复 | ReadJSON 接受 1，Restore 拒绝 1 | failed | 恢复入口兼容及往返测试 |
| version/ID 保真 | ID-keyed 版本导出 1→0；恢复 42/7→1/1；两个版本唯一键冲突 | failed | 原始历史行导入，不重新分配 |
| 加密包装下原子恢复 | 注入 checkpoint 保存错误后旧 pipeline 丢失 | failed | 包装层仍使用同一 SQL 事务 |
| 插件恢复可用且原子 | 新文件配旧路径；SQL 回滚后旧 WASM 被覆盖 | failed | 不可变文件暂存与提交路径切换 |
| >100k 三类对象完整与内存可控 | 旧大数集仅覆盖 audit；未测导出内存 | incomplete | DLQ/audit/run 三类内容对账、内存曲线 |
| CH async/HTTP 吞吐 | 新 client 的 async=false；镜像无 curl，HTTP 0 行 | failed | 成功行数对账、实际 sink/协议、请求级 async 设置 |
| 容量与发布基线完整 | checkpoint 微基准外推 pipeline 容量；缺 digest/峰值/flush 等 | incomplete | 按 T3.6 原验收测定，不挪走必选项 |
| IT-3 全部通过 | 上述反例；旧 CI 仅配置/扫描器 smoke | failed | 本记录与 spec 16 项逐条重新闭合 |

两项产品决议：schema evolution 已由用户于 2026-09-06 明确选择 additive-only 单独排期；
ClickHouse 画像范围等待用户对当前异步问题的答复，不阻塞本轮恢复修复。

## Round 1/5 验收闭合

证据目录：[it3-restore-20260906](../../evidence/it3-restore-20260906/manifest.json)。
该 manifest 记录实际 Go 1.26.5 darwin/arm64、Podman 后端镜像 ID/digest、当前 dirty
源码文件哈希；未把本机测试写成 Linux release 认证。原审查反例保留在上方，不再作为当前结果。

| Criterion | Evidence (command/file/run) | Result | Residual or blocker |
| --- | --- | --- | --- |
| v1/v2/unversioned 可恢复；legacy WASM 缺失显式失败 | `TestBackupRestoreConformance/*/legacy_v1_and_unversioned`；三脚本日志 | passed | v1 需原 WASM 文件，runbook 已说明 |
| ID-keyed/legacy/orphan versions；ID/版本间隙/时间保真 | `TestBackupRestoreConformance` 逐字段对账；固定 41/47 与 3/7；三 backend | passed | 时间比较以实际时间点为准，保留各 backend 原有精度 |
| 原始/SecretFieldStore 恢复单事务；真实约束错误/早晚故障可重试 | `fidelity_and_rollback`；wipe、checkpoint、connections 故障；重复 version ID | passed | COMMIT 传输错误可能结局未知，保留 artifact 并对账 |
| 插件移址、旧文件不被覆盖、输入校验、真实运行时加载 | `TestPluginArtifactRoundTrips`、`TestInvalidArtifactsLeaveStateUntouched`、`TestRestoredPluginLoadsWithExtism` | passed | 不宣称 fixture 已认证任意插件业务功能 |
| 恢复后序列与版本可继续分配 | 三 backend `assertNextGeneratedIDs` | passed | PG 序列故障后可有安全间隙，不后退 |
| 可执行维护入口，不启动 HTTP/pipeline/janitor | 三 backend `TestBackupMaintenanceCLI` 使用新编译二进制，子进程成功/失败/重试 | passed | 维护前停止其他 writer 由运维执行；不是在线跨系统快照 |
| Redis 状态与元数据配套演练 | `CONTAINER_CLI=podman bash hack/e2e-backup-restore-state.sh`；generation=7、offset=42、3 类状态 bytes/index/TTL | passed | 单独 RDB，必须与元数据同一停写窗口；本地旧 state 表需物理备份 |
| race、基础回归、差异检查 | `go test -race ./internal/etl/storage/backup ./internal/etl/storage/sqlstore ./internal/etl/storage/factory ./internal/cmd -count=1 -timeout=300s`；`go test ./internal/etl/... ./internal/cmd/... -count=1 -timeout=300s`；`git diff --check` | passed | 未配置远端 connector DSN 的可选用例仍是 skipped，未外推认证 |

三后端脚本均为 `CONTAINER_CLI=podman bash hack/e2e-backup-restore-{sqlite,mysql,postgres}.sh`。
PostgreSQL 实测还修复了 `--storage postgresql` + `ETL_STORAGE_DSN` 路由到旧 backend 的
配置优先级问题。此前测试仅匹配时间字符串造成的时区差异，现按实际时间点对账。

## Round 2/5：密钥保护与产物扫描（已交付）

```text
Round: 2/5
Roadmap item: RA-5 / IT-3 T3.2 residual
Profile/path: standalone control-plane secret persistence; SQLite/MySQL/PostgreSQL backup artifacts
Objective: descriptor secret 与存量迁移的真实产物安全性有可重复证据；portable backup 与 vendor SQL dump 的扫描能拒绝已知明文。
Eligibility: T3.4 原子恢复已闭合；RA-5 是既定顺序中下一 queued 项；用户已授权继续后续迭代。
Scope: storage secret/backup 安全测试、backup/SQL dump fixture 与 scanner、必要的 descriptor 注入和维护入口、CI 与 evidence/runbook。
Non-goals: KMS/Vault、envelope 格式升级、schema evolution、容量优化或 maturity 提升。
Dependencies: 现有 descriptor resolver 与幂等迁移；三个隔离 backend。
Acceptance: 1) descriptor/四 DSN/兜底 WARN 现有用例复核；2) 存量明文检测只读及幂等重加密；3) 真实 portable backup + vendor SQL dump clean 扫描通过、dirty 被拒；4) ciphertext 往返/轮换不回退；5) CI 调用真实产物守护；6) 三 backend 不以缺 DSN 冒充通过。
Data semantics/rollback: 原始密文导出/恢复；不在备份中解密；迁移可中断重跑；不把测试数据写到既有服务。
Evidence: storage/server secret conformance、隔离 artifact e2e、scanner、_gate.yml、下方验收表。
Result: delivered
Residual/follow-up: RA-6 真正流式导出与 >100k 三类逐行对账；RA-8 基线；IT-4。
```

有界增量：先审查现有 secret resolver/迁移与 scanner，再用同一含四条 DSN、嵌套凭据、旧/新 key
的 fixture 生成真实 backup 与三 backend dump，最后接入 CI、对账证据。只补已确定的验收缺口，
不扩展 secret 产品边界。


Round 2 补充反例：`TestEveryDescriptorSecretIsEncryptedMaskedAndPreserved` 实测遍历 78 个
secret 声明，发现 DSN/webhook/部分 username 在 API 未被 mask，回传固定掩码还会覆盖真实值；
`TestDescriptorDSNMaskedInLinearAndDAGSpecs` 证明 pipeline spec 响应同样仍走字段名猜测。
这是 RA-5 “单一真值”原验收内的缺陷，修复范围包含 `server/secrets.go`、connection catalog/ref
的 descriptor 消费，并保持显式非 secret 字段（如 token_url）正常可读。

## Round 2/5 验收闭合（2026-09-07）

证据：[it3-secrets-20260907/manifest.json](../../evidence/it3-secrets-20260907/manifest.json)。
Go 1.26.5 darwin/arm64 + Podman；源码与依赖镜像均记录指纹。所有数据来自隔离 fixture。

| Criterion | Evidence (command/file/run) | Result | Residual or blocker |
| --- | --- | --- | --- |
| descriptor 加密、掩码、占位符回传一致 | `TestEveryDescriptorSecretIsEncryptedMaskedAndPreserved` 实际 HTTP + raw storage，78 字段；线性/DAG DSN 回归 | passed | spec 仍沿用历史首尾字符掩码；connection 为固定占位符 |
| 四 DSN、嵌套 secret、兜底 WARN | 三 backend fixture verify；`schema_secret_conformance_test.go`；日志 WARN 无值 | passed | 显式非 secret descriptor 字段保持可读 |
| 只读检测；无 key 拒绝；中断重跑；幂等 | 实际 `--check-secrets` / `--remediate-secrets`；`TestPlaintextMigrationRequiresKeyAndResumes` | passed | 维护前停其他 writer；错误 SQLite 路径拒绝创建空库 |
| 真实 portable/JSONL/SQL 产物扫描 | `CONTAINER_CLI=podman bash hack/e2e-secret-artifacts.sh {sqlite,mysql,postgres}`；clean 通过，legacy SQL/注入泄漏的真实 portable 拒绝 | passed | 仅保证已知字段/needle，不能探测任意 payload 中的所有秘密 |
| ciphertext 往返、旧 key 可读、新 key 写入 | fixture verify + sealed-state hash compare；既有轮换与 wrong-key 测试 | passed | 旧密文保持原 envelope，需要保留旧 key |
| scanner 转义、分块及错误状态；不回显值 | `python3 hack/test-plaintext-secrets.py`；`TestSecretScanReportDoesNotEchoCredential` | passed | 无读取能力/空 needle 返回 2，不能当 clean |
| CI 调用真实产物检查 | `_gate.yml` 三 backend job，独立 artifact 上传；Ruby YAML 解析和 wiring 校验 | passed | hosted Actions 未跑，未声明其运行通过 |
| race、静态与针对性回归 | `targeted-race.log`、`secret-regressions.log`、`vet.log`；`git diff --check` | passed | 本机工具链证据，非 Linux release 容量认证 |

## Round 3/5：流式导出与大数据完整性（已交付）

```text
Round: 3/5
Roadmap item: RA-6 / IT-3 T3.3
Profile/path: standalone portable control-plane backup; SQLite/MySQL/PostgreSQL
Objective: 真正流式导出全部表，三类各 >100k 行导出/恢复内容完全一致，内存增长受页大小约束。
Eligibility: RA-5 与 T3.4 已闭合；RA-6 是既定下一项；用户授权继续后续迭代。
Scope: storage/sqlstore 全表游标、backup streaming writer/现有 CLI、完整性与故障测试、三 backend e2e、runbook/evidence。
Non-goals: 在线跨系统一致快照、流式 restore、大范围存储重构、吞吐调优、connector/maturity 扩张。
Dependencies: R1 事务恢复/保真度；R2 密钥 preflight；隔离 MySQL/PostgreSQL 实例与本机 Go。
Acceptance: 1) DLQ/audit/run 各 >100000，含孤立归属、同时间戳、ID 间隙；2) 每行内容 hash 与 Counts 对账；3) CLI 流式输出、实测峰值内存；4) 三 backend 与既有恢复脚本回归；5) 读取/写入/计数失败不得发布半份备份。
Data semantics/rollback: 离线停写窗口；包含 orphan DLQ 与 retained/completed tasks；位点/身份/密文原样传输；文件先私有暂存并 fsync，全部成功后 rename；恢复继续单事务，失败保留原库。
Evidence: 新 streaming tests、三 backend 大数集内容与内存 JSON、hack/e2e-backup-restore-*.sh、runbook 与本记录。
Result: delivered
Residual/follow-up: RA-8 实测与 CI；CH 范围待答复；IT-4 按既有优先级。
```

有界增量：全表 keyset 遍历与流式写 JSON（兼容 v2）；逐表 Counts 校验；CLI 改用流式入口；
三 backend 各造 DLQ/audit/run >100k 并逐行校验导出/恢复，测 10k→100k 峰值内存。
restore 仍可加载完整 Snapshot，单独注明此内存边界，不把导出改进外推为流式恢复。

## Round 3/5 验收闭合（2026-09-08）

证据：[it3-backup-volume-20260908/manifest.json](../../evidence/it3-backup-volume-20260908/manifest.json)。
使用实际新编译 CLI；原始 SQL 读库哈希独立于新 backup reader；Go/硬件/源码/依赖镜像均记录。

| Criterion | Evidence (command/file/run) | Result | Residual or blocker |
| --- | --- | --- | --- |
| 三类各 >100000 行、完整内容 | 三次 `hack/e2e-backup-volume.sh`，各 100,037 DLQ/audit/run；全部字段对比生成输入；独立原始 SQL 哈希恢复前后相等 | passed | 非计数或样本替代内容验证 |
| 全表遍历，不受现存 pipeline 与 dispatch limit 影响 | 四种 job 引用、稀疏 ID、同时间戳、2,003 已完成/孤立 task；keyset 页边界测试 | passed | 离线停写是运维前提 |
| 流式导出与内存实测 | CLI `ExportFile`；各 backend 10,003→100,037 行/表，`getrusage` 子进程峰值 RSS | passed | 单页 1,000 行；单条超大 payload 仍需要其大小的内存 |
| JSON 数字与身份保真 | `UseNumber`；每行 >2^53 整数业务键、原始 payload、replay ack、时间/状态/计数逐项验证 | passed | 不改变 runtime 读取 API 的契约 |
| 导出失败不发布半份文件 | `TestStreamingExportDoesNotPublishPartialBackup`，初/末 inventory 故障、读错、截断、取消、坏 JSON、明文；writer error 测试 | passed | 原子发布不提供在线 SQL/Redis 跨系统事务 |
| 三 backend 既有恢复与密钥保护回归 | 三次 `e2e-backup-restore-*.sh` + 三次 `e2e-secret-artifacts.sh`；Extism 实际加载 | passed | 修复了原脚本在 Bash 3.2 + nounset 下空数组报错 |
| race、静态、CI | `go test -race ./internal/etl/storage/... ./internal/cmd`、vet、`git diff --check`、三 backend volume job wiring | passed | hosted Actions 未跑；本机 Go/Podman 证据 |

| Backend | Export RSS：10,003→100,037 行/表 | 大集导出耗时 | 大集 restore 峰值 RSS / 耗时 |
| --- | --- | --- | --- |
| SQLite | 58.0→60.6 MiB | 1.87 s | 723.7 MiB / 4.92 s |
| MySQL | 51.5→53.5 MiB | 2.43 s | 710.4 MiB / 231.88 s |
| PostgreSQL | 54.0→56.2 MiB | 2.03 s | 714.7 MiB / 114.89 s |

MySQL/PostgreSQL 演练在同一主机并发运行，耗时仅说明该维护演练，不能当作受控性能排名。
完整 Snapshot 的 restore 内存边界已写入 runbook，未将其标为流式。

## Round 4/5：构建、启动与恢复资源基线（已交付）

```text
Round: 4/5
Roadmap item: RA-8 / IT-3 T3.6-A
Profile/path: standalone resource baseline; build/runtime provenance and startup/idle/restore measurements
Objective: 通用基准入口输出可重复结构化测量；数字绑定源码、镜像与硬件；测量失败必须失败。
Eligibility: RA-5/RA-6/T3.4 已闭合；RA-8 为既定下一项。T3.5 的 CH 专项答复仍待用户，通用部分可独立推进，未把未答复视为同意。
Scope: hack/bench-baseline.sh 及有界测量 helper、必要的 fixture、_gate.yml、resource-baseline/evidence。
Non-goals: 不启动未批准的 ClickHouse 专项；不调优生产默认值；不更改 runtime 数据语义或 maturity。
Acceptance: 1) 实际当前源码构建矩阵及镜像身份；2) 健康就绪才报告启动成功，错误/超时不得转成数值；3) idle/peak/restore 有实际进程证据；4) 输出可供 CI 严格解析，阈值告警与测量失败区分；5) 平台与未测项目明确。
Data semantics/rollback: 基准使用独立临时服务/目录；不删除固定名称或借用现有服务；备份/恢复遵守离线边界；源码与构建输入只读。
Evidence: 结构化 baseline JSON、容器/image inspect、命令日志、失败路径验证、文档与 CI。
Result: delivered
Residual/follow-up: 同一 RA-8 的 T3.6-B 真正 pipeline 吞吐/三 backend 并发 checkpoint 曲线；CH 决策；IT-3 收口与 IT-4。
```

RA-8 拆为两个有界增量：本轮先修测定入口、构建/镜像及启动资源画像，随后补实际数据路径与
三 backend 并发曲线。两个增量都完成且 T3.5 明确后，才可把 RA-8/T3.6 标为 delivered/done。

Round 4 preflight 补充（2026-09-09）：构建依赖中的 `hack/pack.sh` 下载 floating latest gf，
因此把同一构建入口的 CLI 默认版本绑定到 go.mod，并输出实际版本/hash；未更改 runtime 语义。
Podman 测量显式使用 Docker image format 保留 Dockerfile HEALTHCHECK。首次实跑在 fixture
误把日期分层的 file sink 输出当作平铺文件时失败并返回非零；该次数据不作为基线入库。

## Round 4/5 验收闭合（2026-09-09）

证据：[it3-baseline-20260909/manifest.json](../../evidence/it3-baseline-20260909/manifest.json)。
命令：`CONTAINER_CLI=podman bash hack/bench-baseline.sh --out /tmp/openetl-baseline-20260909-verified --keep-images`。
源码 479 个构建输入的 SHA-256 与当前文件逐项一致；工作区 `packed.go`/前端产物未被改写。

| Criterion | Evidence (command/file/run) | Result | Residual or blocker |
| --- | --- | --- | --- |
| 当前源码与实际打包矩阵 | 四个 Dockerfile build tag；新前端；Go 1.24.13 / gf 2.10.0；source/image/binary/packed hashes | passed | native linux/arm64；未冒充 GoReleaser linux/amd64 或 CGO 认证 |
| 真实健康就绪，失败不能转成时间 | 3 个独立空库进程，TLS 验证、HTTP 200 + status=ok、UID 1001；早退/超时/错误 JSON 单测 | passed | cached image/OS pages，计时包含容器启动及轮询开销 |
| 空载与控制面峰值、离线恢复 | 15 个 idle 样本；16 条 pipeline 共 1,600 行真实写入；3 个新库恢复/启动与完整 checkpoint 对账 | passed | 不是持续吞吐/并发容量；大数据库恢复见 Round 3 |
| 峰值单位正确 | 独立 `process-measure`，Linux 容器内 child self-report 与 wait4 对照；原始 native 值保留 | passed | BusyBox time 的约 4 倍读数已撤回，旧 schema v1 产物被 validator 拒绝 |
| CI 严格输入与 warning 区分 | `test_bench_baseline.py` 12 测试、v2 result validator、YAML/aggregate wiring；合法超预算只警告，缺失/失败拒绝 | passed | hosted Actions 未跑；warning 噪声周期尚未完成 |
| 隔离、清理与差异 | 独立目录/端口/volume；文件日志驱动；仅自身容器/volume 清理；known credential marker 扫描与 diff check | passed | 20 个既有容器未借用或修改；只保留本轮构建镜像供后续测量 |

default/extism/nolua/extism+nolua binary 为 60.38/63.75/59.63/62.94 MiB；默认镜像
133.18 MiB。启动中位数 0.394 s、idle RSS 中位数 48.76 MiB；16-pipeline fixture
峰值 55.98 MiB。离线恢复 0.068–0.077 s、峰值 41.34–42.81 MiB；恢复后启动中位数
0.395 s。以上仅对应该小数据集、ARM VM 和固定配额，未外推生产容量。

T3.6-A 完成不等于 RA-8/IT-3 完成：T3.6-B 的真实路径/三 backend 并发曲线、T3.5 的
CH 决策及 T3.7 的完整验收仍未闭合。原迭代 16 项验收 12–14 仅获得本增量的部分证据。

## 当前领取：Round 5/5

```text
Round: 5/5
Roadmap item: RA-8 / IT-3 T3.6-B
Profile/path: standalone ETL server; real CDC/batch/Kafka paths and SQLite/MySQL/PostgreSQL metadata concurrency
Objective: 用真实 pipeline 和独立目标对账得到吞吐、checkpoint p50/p95/p99、SQL writer pool 排队与稳态/峰值内存曲线。
Eligibility: 同一 active RA-8 下的 T3.6-A 已交付；用户授权继续既有迭代。CH 专项未获答复，仍不开展协议/async/batch/flush 优化画像。
Scope: hack/cmd/capacity-baseline、hack/bench-capacity 入口与有界 helper、既有 SQLite 微基准的误导命名/注释、resource-baseline/evidence/iteration 状态。
Non-goals: 不优化 runtime、默认值或 storage pool；不新增生产 debug API；不认证 distributed；不做 CH 专项；不以直接 SaveCheckpoint 的循环外推 pipeline 数。
Dependencies: 已留存的当前源码 production builder/runtime 镜像；隔离 MySQL/PostgreSQL/Redpanda/MinIO/ClickHouse 容器。
Acceptance: 1) 至少覆盖 CDC→关系型、batch→OLAP、Kafka→对象存储的真实写入及内容对账；2) 同一 streaming workload 在三 metadata backend 的递增并发曲线，实际 checkpoint 延迟和 pool wait；3) 每档稳态/峰值、吞吐、错误/滞后；4) 固定硬件/参数/数据生成/源码/镜像/计量器哈希和原始结果；5) 拐点必须由实测证明，未达到时保留未闭合结论；6) 明文凭据不进入证据、仅清理自有服务。
Data semantics/rollback: 使用现有 NewServer/Runner、secret wrapper、generation fence、checkpoint 与 DLQ 逻辑；外包只读计量；sink ack 后才 checkpoint。数据为隔离合成业务键；结束时停 writer 后按 source/target/位点对账；失败结果不能成为容量通过记录。
Evidence: structured capacity JSON、逐档原始 checkpoint/pool/内存数据、真实 source/target 内容校验、命令与依赖镜像、resource-baseline.md。
Result: active
Residual/follow-up: CH 决策；T3.7 完整验收；IT-4。该轮关闭或交接后达到 5/5，不自动领取第六增量。
```

实现边界：测量进程可复用 production 镜像中的当前 ETL server，注入保持完整 SQL store 接口的
checkpoint 计量 wrapper。该 probe 只增加观测，不更改写入/事务；GoFrame UI/proxy 的常驻成本
由 T3.6-A 单独记录，probe 进程内存不得冒充整个生产部署内存。先用小数集验证对账和计量，
再执行原验收范围的完整负载；参数、重复次数与失败档位全部保留。

Round 5 测量参数冻结（实现预检）：真实 `NewServer` 独立观测进程，Linux Go 1.24.13、
2 CPU / GOMAXPROCS=2 / 1 GiB，保留 auth/TLS/audit/secret/fencing；GoFrame UI/proxy
未链接，其完整打包成本见 Round 4。单路径各 200,000 行、batch 500；并发曲线为
Kafka 单 partition → file_sink，每条独立 consumer group，N=1/2/4/8/16/32/64，
同一 topic 每秒发布 5,000 行，batch 100、flush 100 ms、checkpoint 1 s（仍有 10 batch
触发）、buffer 1,000；先运行 5 s，再观测 20 s，每档 3 次，轮换三 metadata backend
的测量顺序。所有参数可通过脚本重现，短时结果不能外推长稳容量。

每行合成 `id/grp/amount/payload`，生产者逐批记录已确认位点与时刻；结束停写后，
逐字段校验唯一、无缺口的业务键前缀。单路径必须全量一致；超载曲线保留 backlog，
已提交 prefix 必须均在 sink，不能用计划行数代替实际成功数。所有资源使用本轮随机
namespace；只有自有数据可清理。计量器保留原始 checkpoint 样本、writer pool 累计
等待、`/proc` KiB→bytes、CPU 和观测开销，不把总 pool wait 冒充单条 checkpoint wait。

Round 5 预检失败记录（2026-09-09–10，均不计作容量通过）：

- 观测器编译输出与源码临时目录同名、当前 `rpk cluster health` 不接受旧的 `--brokers`
  参数：均在正式测量前修正；失败运行的结果为 `failed`，自有资源已清理。
- 当前 MySQL CDC/go-mysql 与 MySQL 8 的 replica 注册拒绝 48 字符随机 fixture 口令：
  `Failed to register replica; too long 'report-password'`，未建立 binlog 连接。
  本轮容量 fixture 使用该协议可接受的 24 字符随机口令，API token/加密/TLS 等 production
  gate 不变；48 字符兼容性失败保留为有界后续，成功容量数字不覆盖此输入域。
- MySQL sink 优先 `Metadata.Table`，同库不同 `config.table` 的最初 fixture 实际写回源表；
  metrics 报 100,000 written，独立目标读取却为 0，verifier 拒绝。fixture 改为独立目标库、
  同表名后，100,000 行源/目标/生成数据逐字段对账通过。不得使用此前的无效吞吐值。
- ClickHouse 默认 `source_order` 与普通 MergeTree fixture 不匹配，API preflight 返回 400。
  通用 batch→OLAP 测量明确采用 INSERT-only `version_mode=append`、native 协议、普通
  MergeTree、batch 500；不修改产品默认模式，不外推 mutable CDC 或 CH 专项结论。
