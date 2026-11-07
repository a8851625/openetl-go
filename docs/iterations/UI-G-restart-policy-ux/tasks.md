# UI-G 任务分解

> 状态取值：`todo` / `active` / `done` / `blocked`。一次只允许一个 `active`。

## Round 分组

| Round | 包含 task | 目标 |
| --- | --- | --- |
| 1/5 | UI-G.2 | 先堵 spec 往返丢失（风险最高、可独立验证）。`done` 2026-09-27。 |
| 2/5 | UI-G.1 | 向导 Safety 步渐进披露 restart_policy 表单。`done` 2026-09-27。 |
| 3/5 | UI-G.3、UI-G.4 | DAG Designer 对等配置 + 详情页两态展示。`done` 2026-09-27。 |
| 4/5 | UI-G.5 | 文档与全量回归收口。`done` 2026-09-27。 |

## 任务表

| ID | 任务 | 依赖 | 状态 | 证据落点 |
| --- | --- | --- | --- | --- |
| UI-G.1 | 向导安全步骤渐进披露 `restart_policy` 配置（EN/ZH，默认等价 `never`） | — | `done`（2026-09-27 Round 2） | `first-task-wizard.tsx`、i18n、Playwright |
| UI-G.2 | 含 `restart_policy` 的 spec 往返不丢失（YAML 导入→表单→提交） | — | `done`（2026-09-27 Round 1） | `syncFromYaml`/`buildSpec`/草稿恢复、往返断言 |
| UI-G.3 | DAG Designer 暴露同等 `restart_policy` 配置 | UI-G.2 | `done`（2026-09-27 Round 3） | DagEditorPage、DAG e2e |
| UI-G.4 | Pipeline 详情展示已配置 restart policy 两态 | — | `done`（2026-09-27 Round 3） | PipelineDetailPage、i18n、浏览器断言 |
| UI-G.5 | 文档与回归收口（etl-config-schema.md、全量 UI 回归） | UI-G.1–UI-G.4 | `done`（2026-09-27 Round 4） | docs、CHANGELOG、`hack/e2e-ui.sh` |

## 任务明细

### UI-G.2 spec 往返保持（Round 1）

**验收**：

1. 向导 YAML 视图导入含 `restart_policy`（mode: on-failure, max_retries: 5, initial_delay_ms: 1000, max_delay_ms: 30000, backoff_multiplier: 2）的 spec 后，`syncFromYaml` 将该字段存入表单状态；
2. 之后任何表单编辑（改 batch_size 等）再提交，API 收到的 spec `restart_policy` 逐值相等；
3. 草稿恢复（localStorage）同样保留该字段；
4. 默认（未配置）路径不新增必填项、不输出 `restart_policy` 键，现有行为不变。

**允许范围**：`web/src/pages/pipelines/first-task-wizard.tsx`（状态 + syncFromYaml + buildSpec + 草稿）、`hack/e2e-ui.sh`、UI-G 文档。

**非目标**：不改 `pipeline.RestartPolicy` 结构、服务端 validate/解析、reconciler；不做通用未知字段透传。

**证据落点**：浏览器往返断言（G2.*）、真实 app container E2E。

**领取记录**：

```text
Round: 1/5
Roadmap item: UI-G / UI-G.2
Profile/path: standalone production Web UI + spec 往返保真
Objective: 含 restart_policy 的 spec 经向导 YAML 导入→表单编辑→提交后字段逐值保留
Scope: web/src/pages/pipelines/first-task-wizard.tsx、hack/e2e-ui.sh、UI-G docs
Non-goals: 不改 RestartPolicy 结构/服务端/reconciler；不做通用未知字段透传
Acceptance: G2.1 导入后表单持有字段；G2.2 表单编辑后提交 spec 逐值相等；G2.3 草稿恢复保留；G2.4 默认路径不输出键
Evidence: browser-first 断言先失败后通过；typecheck/lint/build；真实 app container E2E（269/269）
Result: delivered
```


### Round 1 验收矩阵（UI-G.2，2026-09-27）

| Criterion | Evidence | Result |
| --- | --- | --- |
| 导入的 `restart_policy` 被表单状态持有 | G2.1（`syncFromYaml` → `restartPolicyText`；Safety 步 `wizard-restart-policy` 镜像面显示 on-failure/max_restarts） | pass |
| 表单编辑后提交的 spec 逐值保留 | G2.2（改 batch_size: 77 后创建；`GET /api/v2/pipelines/{id}/spec` 返回 mode=on-failure、max_restarts=5、initial 1000、max 30000、multiplier 2、batch 77） | pass |
| 草稿恢复保留该字段 | G2.3（localStorage `etl_wizard_draft_v1` 的 `restartPolicyText` 含 on-failure/max_restarts） | pass |
| 默认路径不输出键 | G2.4（无 restart_policy 的默认向导提交，spec 中无该键；buildSpec 仅在持有值时输出） | pass |
| 契约边界 | `pipeline.RestartPolicy` 结构/服务端 validate/reconciler 未改；字段名对齐服务端 `max_restarts`（roadmap 原文 max_retries 是笔误，已在 G1 任务记录更正） | pass |
| 前端静态检查 | typecheck/build PASS；lint 0 error / 29 pre-existing warnings | pass |
| 真实 app-container E2E | `E2E_SKIP_BUILD=1` 269/269；最终非 skip 全量 `CONTAINER_CLI=podman ./hack/e2e-ui.sh` 269/269 | pass |

**Round 1 result：`UI-G.2 delivered`。** 下一可领取项为 UI-G.1（Safety 步渐进披露表单化配置），随后 G.3/G.4、G.5 收口。


### Round 2 验收矩阵（UI-G.1，2026-09-27）

| Criterion | Evidence | Result |
| --- | --- | --- |
| Safety 步（runtime options 内）渐进披露表单 | G1.1（mode select + 选定模式后显示 max_restarts/initial/max/backoff 四输入） | pass |
| 默认 `never` 等价未配置，不改变现有行为 | G1.2（默认状态 UI 显示 Not configured；YAML 不含 restart_policy 键）+ G2.4 | pass |
| 表单值逐字进入提交 spec | G1.3（on-failure/5/1000/30000/2 全部出现在生成的 spec YAML） | pass |
| EN/ZH 标签与帮助文本（checkpoint 重放边界） | G1.4（帮助文本含 checkpoint/replay）+ G1.5（ZH 检查点/重放） | pass |
| 确认页摘要显示已配置 policy | G1.6（`wizard-confirm-restart` 含 on-failure 与次数） | pass |
| 单一真相源（JSON 文本） | 结构化表单 ↔ `restartPolicyText` 双向同步；YAML 面板显示只读镜像；G2.1–G2.4 复跑全 PASS | pass |
| 前端静态检查 | typecheck/build PASS；lint 0 error / 29 pre-existing warnings | pass |
| 真实 app-container E2E | `E2E_SKIP_BUILD=1` 275/275；最终非 skip 全量 275/275 | pass |

**Round 2 result：`UI-G.1 delivered`。** 下一可领取项为 UI-G.3 + UI-G.4（DAG Designer 对等配置与详情页两态展示），随后 G.5 收口。


### Round 3 验收矩阵（UI-G.3 + UI-G.4，2026-09-27）

| Criterion | Evidence | Result |
| --- | --- | --- |
| DAG Advanced 抽屉披露 restart-policy 表单（默认 never） | G3.1（`dag-restart-policy-form` + mode select 默认 never） | pass |
| 选定模式后披露四个输入（渐进披露，与向导一致） | G3.2（max_restarts/initial/max/backoff） | pass |
| DAG 导出 YAML 携带配置值（DAG 与线性口径一致） | G3.3（`restart_policy: mode on-failure, max_restarts: 7` 出现在导出 YAML；DAG/线性两分支均输出） | pass |
| 载入含 restart_policy 的 spec 时画布保留该字段 | `loadSpecIntoCanvas` 五字段回填（typecheck/代码审查；G3.3 反向验证导出） | pass |
| 详情页显示已配置 policy（mode/次数/延迟/倍数） | G4.1（`detail-restart-policy-value` 含 on-failure 与参数） | pass |
| 详情页未配置时明确显示「未配置（never）」 | G4.2（默认管线显示 not configured/未配置） | pass |
| EN/ZH 标签 | G4.3（ZH 重启策略标签，无英文泄漏）；G1.5 已覆盖帮助文本 | pass |
| 不新增 restart-attempt telemetry/不猜运行态 | 详情行仅显示存储 spec 中的静态 policy；无 UI 侧重启状态推断 | pass |
| 前端静态检查 | typecheck/build PASS；lint 0 error / 30 warnings（+1 为既有 exhaustive-deps 模式同类） | pass |
| 真实 app-container E2E | `E2E_SKIP_BUILD=1` 281/281；最终非 skip 全量 281/281 | pass |

**Round 3 result：`UI-G.3 + UI-G.4 delivered`。** 仅剩 UI-G.5（docs/etl-config-schema.md 补文档 + CHANGELOG + 全量回归收口）。


### Round 4 验收矩阵（UI-G.5 收口，2026-09-27）

| Criterion | Evidence | Result |
| --- | --- | --- |
| `docs/etl-config-schema.md` 顶层字段表补 `restart_policy` | 字段表新增行 + 示例 YAML 增补；语义说明含默认 never、checkpoint 恢复、at-least-once 重放、与 retry（记录级写入重试）的区别 | pass |
| CHANGELOG 双语 Added 条目 | CHANGELOG.md / CHANGELOG.zh.md Unreleased 增 UI-G 与 UI-F 条目及证据数字 | pass |
| 全量 UI 回归（真实容器、非 skip） | `CONTAINER_CLI=podman ./hack/e2e-ui.sh` 281/281（G1.1–G4.3 + 全部既有断言） | pass |
| 后端未动 | `go build ./...` PASS；`pipeline.RestartPolicy`/reconciler/validate 无代码变更 | pass |
| 前端静态检查 | typecheck/build PASS；lint 0 error / 30 warnings（29 pre-existing + 1 既有同类 exhaustive-deps） | pass |
| git diff --check | 通过 | pass |

**Round 4 result：`UI-G.5 delivered`。UI-G 迭代整体 `delivered`（4/5 rounds，最终 281/281）。** 无残留 follow-up：reconciler 语义、restart 模式集合、checkpoint 契约均未改动；通用未知字段透传仍为明确非目标。

