# 迭代规划总览（SDD）

> 规划基线：`c2ac396`（v0.2.12-beta.17，2026-08-29）
>
> 唯一执行 backlog 仍是 [docs/ROADMAP.zh.md](../ROADMAP.zh.md)。本目录不复制 roadmap 条目，
> 只把**已经进入当前执行 backlog、尚未实现**的条目组织成可领取、可验证的迭代，并补充
> roadmap 条目层面没有的**技术方案**与**交付约束**。两者冲突时以 ROADMAP 的验收标准为准。
>
> ROADMAP 中标为“候选规划/待决策”的内容（当前包括 ClickHouse `CH-C1`–`CH-C8`）不属于
> 本目录的覆盖范围，也不计入“未实现条目已覆盖”的承诺。候选项只有在用户确认范围与优先级、
> 并新增或更新对应的 `spec.md`/`plan.md`/`tasks.md` 后，才可进入本目录和状态看板。

## 这是什么

本目录是 spec-driven development（SDD）产物。每个迭代一个目录，包含三件套：

| 文件 | 回答什么 | 对应 [AGENTS.md](../../AGENTS.md) 步骤 |
| --- | --- | --- |
| `spec.md` | **WHAT / WHY** —— 问题陈述、可观察结果、验收标准、交付约束、完成定义 | 步骤 1-2（sync + claim 的内容源） |
| `plan.md` | **HOW** —— 技术方案、关键接口与文件、数据语义、风险与回滚、测试策略 | 步骤 4（preflight 的输入） |
| `tasks.md` | **可领取增量** —— 带 ID 的任务表、round 分组、领取记录模板、证据落点 | 步骤 3、7、8（split / reconcile / close） |

`_template/` 是新迭代的模板。

## 迭代总览

| 迭代 | 主题 | 归属 roadmap 条目 | 依赖 | 状态 |
| --- | --- | --- | --- | --- |
| [IT-1](./IT-1-verification-substrate/) | 验证基座与存量收口 | RA-4、RA-7、BUG-1/2/6、GAP-1/3/4、P4 follow-up | 无 | `complete` |
| [IT-2](./IT-2-correctness/) | 正确性（控制面真值 + 数据面身份与顺序） | RA-1、RA-2、RA-3、GAP-7.1/.2/.3 | IT-1 | `complete`（2026-09-05；T2.1–T2.8 全部 done，16 项验收 passed） |
| [IT-3](./IT-3-integrity-capacity/) | 完整性与容量 | RA-5、RA-6、RA-8、PR-1.3 残留 | IT-1 | `complete`（2026-09-16：T3.1–T3.7 全部 done，含 T3.6-B 路径吞吐与三 backend 并发曲线；证据 it3-baseline-20260916） |
| [IT-4](./IT-4-ga-closeout/) | GA 收口评估 | 证据刷新、maturity 对齐、移除 beta 判定 | IT-1 + IT-2 + IT-3 | `complete`（2026-09-16：T4.1–T4.5 done；判定见 [ga-assessment-2026-09-16](../ga-assessment-2026-09-16.md)——standalone 可声明、项目级/distributed 保持 beta） |
| [IT-5](./IT-5-ch-write-contract/) | CH 第一批：写入确认/去重契约 + Kafka metadata envelope + additive-only schema contract | CH-C1、CH-C3、CH-C2 | IT-2（RA-1）、RA-8/T3.6、P3.1+schema 决议 | `complete`（2026-09-19：T5.1–T5.5 全部 done；e2e ch_dedup_crash_window 4/4、kafka_envelope_roundtrip 2/2、schema_contract_enforcement 5/5） |
| [UI-C](./UI-C-wizard-interaction/) | 向导行交互模型收尾（P2-5 残留 + 批量步骤操作） | UC.1 ConfigForm 语义/a11y；UC.2 移动 toast 与批量步骤操作 | 无 | `complete`（2026-09-19：UC.1+UC.2 delivered，E2E 163→172 全绿；以 ROADMAP 为准的回填） |
| [IT-6](./IT-6-composite-pk-snapshot/) | mysql_snapshot_cdc 复合主键快照（源连接器正确性） | 新增 roadmap 条目 IT-6（2026-09-25 立项，线上实战发现） | mysql_snapshot_cdc 现有框架（delivered） | `delivered`（2026-09-26：T6.1–T6.5 全部交付；e2e 复合路径 5/5、真实 MySQL 8 分页/续跑、-e2e.strict 8/8、三 backend drill PASS、3 认证脚本回归 PASS） |
| [UI-D](./UI-D-interaction-mobile-hardening/) | 实测交互可信度与小屏可用性收口（2026-09-24 Playwright 走查） | UI-D.1–D.6：批量确认/可供性、390px 值守、向导发现、刷新/a11y、真实容器收口 | 无 | `delivered`（2026-09-26：真实 app 容器 UI E2E 209/209；完整矩阵见 tasks.md） |
| [UI-E](./UI-E-task-flow-recovery/) | 首次成功与故障恢复的任务型产品闭环（2026-09-24 产品/交互复审） | UI-E.1 匿名五任务研究前置与后续 API/恢复闭环计划 | UI-D delivered；3–5 名目标用户 + 隔离环境；最小 API 契约核对 | `blocked_external`（2026-09-26：缺外部参与者；`research-protocol.md` 已冻结解除条件） |
| [UI-F](./UI-F-control-plane-truthfulness/) | 控制面路由安全与运行状态真实性（2026-09-25 Playwright 补充实测） | F.1–F.7 全部 delivered（收口 265/265） | UI-D.3 delivered；真实 app container；connection impact 需权威 API | `complete`（2026-09-27：F.1 route identity、F.2 Settings route、F.3 删除确认、F.4 状态事实、F.5 小屏 containment、F.6 i18n-a11y、F.7 收口，全部真实容器验证） |
| [UI-G](./UI-G-restart-policy-ux/) | restart_policy 的 UI 配置与 spec 往返保持（2026-09-26 线上恢复复盘） | G.1–G.5 全部 delivered（281/281） | 向导/DAG Designer/文档；不改 reconciler 语义 | `complete`（2026-09-27：向导/DAG 配置、spec 往返保真、详情两态、文档全部真实容器验证） |
| [IT-7](./IT-7-doris-source-order/) | Doris sink 源序写入（sequence column + delete sign） | 新增 roadmap 条目 IT-7（2026-10-30 立项，Doris vs ClickHouse 成熟度核对发现） | `core.SourceOrder` 契约（已存在）；Doris 2.1.11 FE/BE 容器；参照 ClickHouse `prepareSourceOrderRecords`/`e2e-clickhouse-replay-ordering.sh` | `delivered`（T7.1–T7.5 全绿；T7.1 spike 收窄了范围，写侧序列注入 + preflight 门禁 + 排序 e2e + 认证回写均完成） |
| [IT-8](./IT-8-data-integrity-closure/) | 数据完整性闭环（对账 / 对象提交清单 / 单表重同步 / 无主键表快照） | T8.1–T8.5：四条实质功能缺口（2026-10-31 盘点） | 既有连接助手、`SinkCommitMetadataProvider`、lifecycle store、snapshot 游标 | `delivered`（2026-11-01：单测 + `hack/e2e-data-integrity.sh`（MySQL + MinIO）全绿；全量 `go test ./...` 与 `-race` 全绿） |
| [UI-A](./UI-A-frontend-hardening/) | 前端交互与向导逻辑加固（2026-09-12 审计） | 新增 roadmap 条目 UI-A | 无 | `complete`（UI-A.1–A.4 delivered 2026-09-12；缓冲窗口 UI-B.1–B.5 delivered 2026-09-14/15，e2e 158 passed / 0 failed；残留小项入小项池不阻塞） |

### 依赖关系

```text
                    ┌──────────────────────────────────────────┐
                    │ IT-1 验证基座与存量收口                    │
                    │ 发布门禁 + e2e 自动化 + 清空 active backlog │
                    └───────────────┬──────────────────────────┘
                                    │ 解锁：其余迭代的验收可在 CI 复现
                    ┌───────────────┴───────────────┐
                    ▼                               ▼
        ┌───────────────────────┐      ┌───────────────────────┐
        │ IT-2 正确性            │      │ IT-3 完整性与容量      │
        │ 控制面真值 + 身份/顺序  │      │ secret + backup + 基线 │
        └───────────┬───────────┘      └───────────┬───────────┘
                    └───────────────┬───────────────┘
                                    ▼
                    ┌──────────────────────────────┐
                    │ IT-4 GA 收口评估              │
                    │ 证据刷新 + 移除 beta 判定      │
                    └──────────────────────────────┘

  IT-6 mysql_snapshot_cdc 复合主键快照（线上实战：OpenCart 风格复合 PK 表存量缺失）───▶ delivered（2026-09-26）
  IT-7 Doris sink 源序写入（sequence column + delete sign）───▶ delivered（2026-11-03）
  IT-8 数据完整性闭环（对账/对象提交清单/单表重同步/无主键表快照）───▶ delivered（2026-11-01，四条实质功能缺口收口）
  UI-C 向导行交互收尾 ───▶ 随时可做，不阻塞主线
  UI-D 实测交互可信度与小屏可用性收口 ───▶ delivered（2026-09-26，UI E2E 209/209）
  UI-F 控制面路由安全与运行状态真实性 ───▶ delivered（2026-09-27，F.1–F.7 全部交付，收口 265/265）
  UI-G restart_policy 的 UI 配置与 spec 往返保持 ───▶ delivered（2026-09-27，G.1–G.5 全部交付，281/281）
```

IT-2 与 IT-3 之间无代码依赖，文件域基本不重叠（IT-2 集中在 `server`/`sink`/`source`/`orchestrator`，
IT-3 集中在 `storage`/`backup`/CI 基准），在满足「同一时间只推进一个 `active` 主任务」的前提下
可按需调整先后。**IT-2 优先**的理由是它包含唯一会产生静默错值的 RA-1。

## Roadmap 覆盖映射

本轮迭代覆盖 roadmap 中**已进入当前执行 backlog 的全部**未实现条目。逐条对照：

| Roadmap 条目 | 当前状态 | 归属 |
| --- | --- | --- |
| P0：MaxCompute 真实环境认证 | `deferred`（2026-09-17 用户决策移除执行面；实现保留、maturity 维持 experimental） | —（PT-A 目录已删除） |
| BUG-1：`mysql_batch` 字符串主键游标不推进 | `delivered`（2026-09-01 复核：容器 e2e 补全） | IT-1 |
| BUG-2：MySQL CDC binlog 断裂（ERROR 1236）无自动恢复 | `delivered`（三策略容器级闭合） | IT-1 |
| BUG-6：`snapshot_cdc` CDC 阶段不填 `ColumnTypes` | `delivered`（声明类型 e2e 闭合） | IT-1 |
| GAP-1：`postgres_cdc` Metadata 契约 | `delivered`（PG 实例 e2e 闭合） | IT-1 |
| GAP-3：`postgres` sink `pk_columns_from_metadata` | `delivered`（扇出 e2e + auto-create PK 修复） | IT-1 |
| GAP-4：`elasticsearch` mapping-conflict 策略 | `delivered`（mapping-conflict e2e + 校验语义对齐） | IT-1 |
| P4：Doris/Kafka 事实核验 follow-up | bounded follow-up | IT-1 |
| RA-4：release 流水线无测试门禁 | `delivered`（2026-09-03；skip/fail/normal tag run URL 已补齐） | IT-1 |
| RA-7：e2e 证据自动化 | `delivered`（结构化证据+commit 绑定+CI 全绿：run 33767847369；篡改拒绝 33769319891/33769344338） | IT-1 |
| RA-2：`RestoreFromDB` 静默跳过 | `delivered`（2026-09-05；IT-2/T2.1） | IT-2 |
| RA-3：`StartAll` 无视 desired state | `delivered`（2026-09-05；IT-2/T2.2） | IT-2 |
| RA-1：ClickHouse `_version` 非源事件序 | `delivered`（2026-09-05；IT-2/T2.4） | IT-2 |
| GAP-7.1：普通流完整 Key 与组合预检 | `delivered`（2026-09-05；T2.5） | IT-2 |
| GAP-7.2：受控 DLQ replay | `delivered`（2026-09-05；T2.6） | IT-2 |
| GAP-7.3：metadata-PK sink 认证 | `delivered`（2026-09-05；T2.7） | IT-2 |
| RA-5：secret 两套真值来源 | `delivered`（复核 Round 2/5） | IT-3 |
| RA-6：backup 硬截断 100000 | `delivered`（复核 Round 3/5） | IT-3 |
| PR-1.3 残留：非原子 restore、version/ID 保真、WASM artifact、Redis state | `delivered`（复核 Round 1/5） | IT-3 |
| RA-8：实测 resource baseline | `active`（复核 Round 5/5，T3.6-A 已交付） | IT-3 |
| 决策记录：RA-1 优先级 | 已按用户持续交付授权在 IT-1 后完成（2026-09-05） | IT-2 |
| schema evolution：additive-only 单独排期 | 2026-09-06 用户确认 | IT-3 |
| 待决策：ClickHouse 写入吞吐是否立项 | 未决 | IT-3（与 RA-8 合并测定） |
| 项目级发布门槛 / 移除 beta 判定 | — | IT-4 |
| ClickHouse 迭代启发候选 `CH-C1`–`CH-C8` | `CH-C1`/`CH-C3`/`CH-C2` 于 2026-09-17 晋级第一批立项（IT-5）；`CH-C4`–`CH-C8` 保持候选池 | IT-5（第一批）；其余未晋级 |
| UI-D：实测交互可信度与小屏可用性 | `delivered`（2026-09-26：UI-D.1–D.6；真实 app 容器 209/209） | UI-D |
| UI-E：首次成功与故障恢复任务闭环 | `blocked_external`（2026-09-26：UI-E.1 缺 3–5 名外部目标用户；研究协议已就绪） | UI-E |
| UI-F：控制面路由安全与运行状态真实性 | `delivered`（2026-09-27：F.1–F.7，265/265） | UI-F |
| UI-G：restart_policy 的 UI 配置与 spec 往返保持 | `delivered`（2026-09-27：G.1–G.5，281/281） | UI-G（./UI-G-restart-policy-ux/） |
| IT-7：Doris sink 源序写入（sequence column + delete sign） | `delivered`（2026-11-03 关闭：写侧序列注入 + preflight 门禁 + 排序 e2e + 认证回写） | IT-7（./IT-7-doris-source-order/） |
| IT-8：数据完整性闭环（对账/对象提交清单/单表重同步/无主键表快照） | `delivered`（2026-11-01；T8.1–T8.5 全部完成，含真实容器 e2e） | IT-8（./IT-8-data-integrity-closure/） |

**显式不纳入本轮**：ROADMAP「有界后续」及 ClickHouse `CH-C1`–`CH-C8` 候选项（包括
ODPS lookup/source 方向、Feishu 真实环境证据、JS/TS/WASM
parser 示例扩展、复杂多事实 merge）与「明确暂缓或不做」全部条目。它们保持原状态，需要
显式重新排序并补齐迭代三件套才进入执行。

## 怎么用（SDD 循环）

本目录不替代 [AGENTS.md 的 Roadmap-to-Delivery Workflow](../../AGENTS.md#roadmap-to-delivery-workflow)，
而是为它提供输入。一轮完整循环：

```text
1. 选迭代      读 IT-N/spec.md，确认可观察结果、验收标准、交付约束、依赖是否满足
2. 读方案      读 IT-N/plan.md，确认技术路线、允许触碰的文件、数据语义与回滚边界
3. 领取任务    从 IT-N/tasks.md 取当前 round 中最靠前的未完成 task，复制其领取记录模板
               同步把 ROADMAP 对应条目置 active（一次只一个）
4. 实现         AGENTS.md 步骤 4-5：preflight -> 最小垂直切片
5. 验证         AGENTS.md 步骤 6：按 task 的「验收」与「证据落点」执行，记录
               passed/failed/skipped/blocked，skip 与 blocked 不得计为 pass
6. 回填         更新 tasks.md 的任务状态与实际证据；更新 ROADMAP 条目的验收矩阵
7. 收口         一个 round 的全部 task 完成后核对 spec.md 的完成定义；
               全部 round 完成后迭代置 delivered
```

### 与 round 预算的关系

AGENTS.md 默认一次执行请求上限 **5 rounds**。每个迭代的 `tasks.md` 已按此预算把 task 分组为
`Round 1/5` .. `Round 5/5`，一个 round 是一次完整的 claim-to-close 循环。跨迭代继续需要显式
新指令，不从沉默推断授权。

### 状态取值

沿用 ROADMAP 的状态机，不新增：`active` / `blocked_external` / `queued` / `delivered` / `deferred`。
迭代级状态是其全部 task 的聚合：任一 task 未闭合则迭代不得 `delivered`。

## 全局交付约束

以下约束对所有迭代生效，迭代级 `spec.md` 只补充增量约束，不得放宽这些：

1. **语义不倒退**：默认交付语义是 checkpointed at-least-once。任何 retry、replay、checkpoint 或
   sink 改动不得把「可能重复」悄悄变成「可能丢失」。
2. **证据即事实**：`skipped`、`blocked`、「代码存在」、「脚本存在」、maturity 字符串都不等于通过。
   每项声明必须有**当前版本实际执行**的记录。
3. **验收不可中途改写**：实现中发现的相邻工作进入有界后续，不修改当前 task 的验收标准。
4. **不扩大 roadmap**：本目录不新增产品能力。发现新缺口先记入 ROADMAP，再由用户决定是否排期。
5. **优先级不静默变更**：任何提前、插队或重排都需要显式用户决策并在 ROADMAP 留痕。
6. **失败必须可见**：持久化、生命周期、checkpoint、迁移、任务状态错误必须影响 API 结果与
   overall health，不得只写日志或返回零值。
7. **maturity 保守**：证据不完整时使用 `beta` / `experimental` / `production_with_review`，
   不提前改成 `production`。

## 状态看板

在每次收口时更新此表（`—` 表示尚未开始）。

| 迭代 | Round 1 | Round 2 | Round 3 | Round 4 | Round 5+ | 迭代状态 |
| --- | --- | --- | --- | --- | --- | --- |
| IT-1 | T1.1/T1.2 完成 | T1.3/T1.4 完成 | T1.5/T1.6/T1.7/T1.8 完成 | T1.9/T1.10/T1.11 完成（Doris 外部镜像问题保留为 bounded follow-up） | T1.12 完成 | `complete` |
| IT-2 | T2.1 完成：restore failure 持久化/API/health/strict gate | T2.2 完成：desired/observed + reset fencing | T2.3 完成：identity/order 共享契约 | T2.4/T2.5/T2.6 完成 | T2.7 完成：metadata-PK 跨路径认证；T2.8 完成：16 项验收核对 + 证据重绑；迭代 `complete` | `complete` |
| IT-3 | T3.1–T3.4 复核交付；T3.6 通用基线修复中 | T3.3 完成 | T3.4/T3.5 完成 | T3.6-A 基线 + T3.7 CI 接入完成 | T3.6-B 路径吞吐 + 三 backend 并发曲线闭合（2026-09-16，18/18 case）；迭代 `complete` | `complete` |
| IT-4 | T4.1 证据复核全绿 | T4.2 无降级项 | T4.3 三 backend 升级 drill PASS | T4.4 两主链路重认证（b1deefa） | T4.5 显式 GA 判定产出 | `complete` |
| IT-5 | T5.1 完成：dedup token + provider + 错误分类 | T5.2 完成：双协议 crash-window e2e（顺带修复 hook 遮蔽与 replay panic 两个真实 bug） | T5.3 完成：Kafka envelope 全链路 + Redpanda harness | T5.4 完成：contract 类型/存储/enforce 接入 | T5.5 完成：contract e2e 5/5 + 三 backend drill + 全量回归；迭代 `complete` | `complete` |
| UI-C | UC.1 完成：label htmlFor/id + field.* i18n 标签（38 key） | UC.2 完成：移动 toast 提示 + 多选批量删除（C 系列 9 断言） | — | — | — | `complete` |
| UI-D | UI-D.1/UI-D.2：批量确认与可供性 ✅ | UI-D.3：小屏值守布局 ✅ | UI-D.4：向导步骤发现 ✅ | UI-D.5：刷新/a11y ✅ | UI-D.6：真实容器复验/证据收口 ✅（2026-09-26，209/209） | `delivered` |
| UI-E | UI-E.1：用户任务走查（`blocked_external`，等待 3–5 名目标用户） | UI-E.2：首次任务分层（依赖 E.1） | UI-E.3：事实契约 preflight（可独立只读准备，不替代 E.1） | UI-E.4/UI-E.5：恢复/深链/推进（依赖） | UI-E.6：容器回归与回访 | `blocked_external` |
| UI-F | UI-F.1：detail route identity guard（`delivered`） | UI-F.2/UI-F.3：Settings 路由/删除确认（`delivered`） | UI-F.4/UI-F.6：状态事实/i18n-a11y（`delivered`） | UI-F.5：小屏 containment（`delivered`） | UI-F.7：收口复验（`delivered`，265/265） | `delivered` |
| UI-A | UI-A.1–A.4 完成（2026-09-12，e2e 145） | UI-B.1 完成（e2e 148） | UI-B.2 完成（e2e 153） | UI-B.3/B.4 完成（e2e 156/158） | UI-B.5 缓冲轮完成（i18n 全量，e2e 158）；迭代 `complete` | `complete` |
