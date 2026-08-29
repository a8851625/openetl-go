# IT-1：验证基座与存量收口

> 状态：`queued` | 依赖：无 | 归属 roadmap 条目：RA-4、RA-7、BUG-1、BUG-2、BUG-6、GAP-1、GAP-3、GAP-4、P4 follow-up

## 问题陈述

当前所有认证证据由人工在本机执行 `hack/*.sh`、再手工誊写进 markdown 产生。这条链路有三个
彼此叠加的结构性问题：

1. **证据与 commit 不绑定**，会漂移。ROADMAP 内已两次自我记录该现象：BUG-2「fail 策略容器
   e2e 已跑但证据未入验收矩阵，证据链断裂」导致条目从 `delivered` 回退为 `active`；
   2026-07-26 审计发现「UI 文档记录 108 passed，审计时实际 91 passed / 17 failed」。
2. **单点环境依赖**。BUG-1、BUG-2、BUG-6 三项当前为 `active` 的唯一原因都是「容器 e2e 未跑
   —— 镜像构建被 `go mod download` 网络阻塞」。GAP-1、GAP-3 同样卡在缺 PostgreSQL 实例。
   一个本机网络问题使全部认证工作停摆。
3. **发布不校验测试**。`.github/workflows/release.yml` 在 tag 触发后只执行 checkout →
   setup-go → `hack/check-connector-evidence.sh -strict` → goreleaser，无任何测试步骤；
   `.github/workflows/test.yml` 的七个 job 彼此无 `needs:` 串联，且没有任何一个被 release 引用。
   单测全红的 commit 打上 tag 即可产出 GitHub Release 与 GHCR 镜像。

后果：**任何「已验证」声明当前都不可复现**，因而 IT-2、IT-3 的验收也无法被信任。本迭代是其余
迭代的前置基座，同时顺带清空积压的 `active` backlog —— 因为这些欠账在框架就位后成本骤降。

## 可观察结果

1. 未通过聚合测试门禁的 commit **不能**产出 GitHub Release 或 GHCR 镜像。
2. 核心数据路径的 e2e 在 CI 内以容器方式实跑，任何人在任何机器上 `git checkout <sha> && make e2e`
   都能得到同一结论，不再依赖某台机器的镜像缓存。
3. 认证证据由测试运行产出并与 commit hash 绑定；人工改证据而未重跑测试时门禁失败。
4. BUG backlog 中不再有因「e2e 未跑」而停留在 `active` 的条目。
5. `PathContract.LastCertified` 反映真实的最近一次认证运行，不再是手工填写或空值。

## 验收标准

| # | 验收标准 | 判定方式 |
| --- | --- | --- |
| 1 | 单测失败的 tag 无法发布 | 构造一个故意失败的 tag，release workflow 失败且无 Release/镜像产出，附 run URL |
| 2 | skip 不计为 pass | 构造 storage matrix 中一个 backend 被 skip 的场景，聚合门禁判定为**失败** |
| 3 | 门禁与 tag commit 严格绑定 | 门禁结果对应 tag 所指 commit，不受分支后续提交影响 |
| 4 | ≥8 条核心路径在 CI 容器内实跑通过 | CI run 日志 + 单次全量运行时长记录 |
| 5 | 证据与 commit 绑定校验生效 | 手工修改证据文件而不重跑测试 → `check-connector-evidence.sh` 失败 |
| 6 | `LastCertified` 自动填充 | CI 运行后该字段变化，且与 run 的 commit 一致 |
| 7 | BUG-1 状态与证据一致 | 核对 `hack/e2e-bug1-varchar-pk.sh` 证据是否绑定当前 HEAD；一致则置 `delivered`，否则重跑 |
| 8 | BUG-2 四项验收全闭合 | `fail` / `resume_from_current` / `resnapshot` 三策略 + 瞬时断连不误判，均有容器级记录 |
| 9 | BUG-6 e2e 闭合 | CDC 相位 `ColumnTypes` 经 Kafka → ClickHouse 自动建表使用声明类型 |
| 10 | GAP-1 / GAP-3 PostgreSQL 实例 e2e 通过 | 真实 PG 容器上的 metadata 契约与 `pk_columns_from_metadata` 记录 |
| 11 | GAP-4 mapping-conflict 策略闭合 | ES mapping 冲突按声明策略进入 DLQ 或拒绝，有 e2e 记录 |
| 12 | P4 Doris/Kafka 事实核验完成 | 文档声明与实际运行行为一致，差异已订正 |

## 非目标

- 不迁移全部 69 个 `hack/*.sh`。本迭代只迁移核心路径，其余保留为本地入口。
- 不引入 Kubernetes、外部 CI 基础设施或新的编排依赖。
- 不改变任何既有认证矩阵的**语义门槛**，只改变证据的生产方式。
- 不在本迭代修复 RA-1/2/3/5/6/8 与 GAP-7（属 IT-2、IT-3）。
- 不为提高 CI 通过率而放宽既有断言。

## 交付约束

1. **skip 硬性不等于 pass**。缺容器、缺 DSN、缺服务、缺凭据一律记为 `skipped` 或 `blocked`，
   并在聚合门禁中按失败处理（允许为特定 job 配置显式豁免清单，但豁免必须写进
   `release-checklist.md` 且逐条有理由）。
2. **`hack/*.sh` 不删除**。已迁移路径的脚本改为薄封装或标注「已由 Go e2e 覆盖」，保留本地
   可执行性，避免无 CI 环境时完全失去手段。
3. **CI 单次全量运行时长设上限**。建议 ≤30 分钟；超出必须分层（PR 跑快速集，main/tag 跑全量），
   不得靠删减断言压缩时间。
4. **网络依赖必须可离线降级**。镜像拉取、`go mod download` 需支持代理配置（复用 BUG-1 收口时
   验证过的 goproxy 路径），并在文档记录，避免复现本迭代要解决的那个阻塞。
5. **存量欠账不得借用新框架的通过声明**。BUG-2、BUG-6、GAP-1/3/4 各自的原验收标准不变，
   必须逐条闭合，不能以「已在新 e2e 框架内跑过」笼统覆盖。

## 依赖与前置

| 类型 | 内容 | 状态 |
| --- | --- | --- |
| 迭代依赖 | 无 | — |
| 外部输入 | 可用的容器运行时（`hack/container-cli.sh` 检测 docker/podman） | 本地已具备 |
| 外部输入 | CI runner 可运行容器（GitHub Actions ubuntu-latest 已具备） | 已具备 |
| 外部输入 | 可用的 Go module 代理（网络受限时） | 需确认，见交付约束 4 |
| 待决策 | 无 | — |

## 完成定义（DoD）

1. `tasks.md` 中全部 task 状态为 `done`，且每项有实际执行的证据记录（run URL 或本地命令输出）。
2. 上方 12 项验收标准全部 `passed`。
3. ROADMAP 中 RA-4、RA-7 置 `delivered`；BUG-1/2/6 置 `delivered`；GAP-1/3/4 与 P4 follow-up
   的 e2e 欠账在其条目内标注闭合。
4. `release-checklist.md`、`connector-certification.md` 的证据生产流程说明已更新。
5. `git diff --check` 通过；CI 配置变更不引入任何 secret 到日志或产物。
