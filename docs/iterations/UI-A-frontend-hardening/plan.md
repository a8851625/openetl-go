# UI-A 技术方案

## 原则

1. **单一真相源**：向导以结构化 draft state 为 canonical，YAML 是受控视图。这是 P0-5/P0-6 的根治方案。
2. **不静默**：解析失败、模板切换、字段删除、配置覆盖都要显式可见或可撤销。
3. **不伪造**：没有的数据就标注"累计值/不可用"，不硬编码估算冒充时序。
4. 后端契约变更最小化：只有 P0-2 需要后端 stats 补 `last_error`，其余全在前端。

## 分轮方案（对应 tasks.md rounds）

### Round 1：向导配置完整性（P0-5/6/7/8 + P1-16 局部）

- `first-task-wizard.tsx` 引入 `parse state`：每个 JSON 文本框维护 `{ text, parseError }`，ConfigForm 的 onChange 走结构化对象。
- YAML 编辑器：受控视图。用户手改 YAML 后进入 "YAML 优先" 模式：表单冻结只读，提供 "Apply YAML to form"（即现 syncFromYaml）与 "Discard YAML edits"。解析失败禁止 Next/Validate/Create。
- buildSpec 只从结构化 draft 构建；`useEffect(setYamlText)` 仅在 draft 非 dirty-from-yaml 时同步。
- Add transform 默认 `identity` 而非空 project；project 空 fields + keep_unmapped=false 时显示阻断错误 "This config drops all fields"。
- Confirm 页 "Open in DAG editor"：把当前 draft spec 通过 sessionStorage key（`etl_dag_seed_v1`）传递，DagEditorPage 启动时若有 seed 且无 editTarget 则加载为 DAG 草稿；离开不落库。
- 回滚：整个 round 是前端状态机重构，出问题 revert 单 commit。

### Round 2：向导语义与安全（P0-3/9 + P1-6/7/9/14）

- 草稿 localStorage：secret 字段（descriptor.secret === true，含连接内敏感字段）替换为 `__omitted__` 哨兵；恢复草稿时该字段留空 + 显示 "secret 需重新输入"。
- 保存连接下拉只显示 kind 兼容且 type ∈ template.sourceTypes/sinkTypes 的连接；不兼容时分组 "Incompatible (switches template)" 并带确认。
- applyTemplate 前若 draft dirty 则 confirm（自研 ConfirmDialog，不用 window.confirm）。
- 连接 recommendations 默认只展示不自动 setBatchSize/setCheckpointIntervalSec；已被用户改过（touched 标记）的值不再覆盖。
- Sink/Source 下拉对 descriptor maturity === experimental 的类型（maxcompute）显示 "Experimental · writer disabled" 徽标并默认禁用，需展开 "Show experimental" 才可选。
- Preflight 结果渲染分层：valid=true 时若 issues 中存在 source/sink reachability warning，状态行显示 "Validated with reachability warnings — not ready to start"；主按钮变为 "Create without starting"，"Create and start" 需勾选确认。

### Round 3：页面事实一致性（P0-1/2/4 + P1-2/3/5/19）

- Dashboard：移除 15m/24h 假切换；时间范围控件标注 "Cumulative since start"，或仅保留 15m（真实 current metrics 窗口）+ Cumulative 两档真实档位。
- P0-2 后端：runner 启动失败时把错误写入 pipeline status/stats（`last_error`、`last_error_code`），detail Issues tab 渲染条件加 `status === 'failed'`。此增量触碰 internal/etl/server 与 pipeline —— 按 roadmap 证据纪律，push 前需重跑受影响路径 e2e。
- stoppedCount 改为真实分桶（running/failed/completed/paused/scheduled/stopped），Start all 目标只含 stopped；Dashboard paused+stopped 分开显示。
- "View issues" 按钮直达 issues tab；中文硬编码迁入 i18n.ts（pipeline-health 返回结构化 code，展示层翻译）。
- "Production runtime" 改为后端 profile 驱动（复用现有 runtime profile API/health 信息），无数据时显示实际 mode 而非营销文案。
- 移动端向导：步骤条+主体在 <xl 断点单列，步骤条横向滚动但正文 `min-w-0 overflow-x-hidden`，操作栏不溢出。

### Round 4：页面操作安全与收尾（P1-4/17/18 + P2 全部）

- 批量启停：ConfirmDialog 显示目标数量与影响；受控并发（每批 4）；结果汇总面板（成功 N/失败 M + 每条失败原因）。
- 高危操作统一走 ConfirmDialog：Run now、Stop all、checkpoint reset、连接删除、DLQ delete/replay。
- Schedules：一次拉取列表后按需 fetch；API 失败显示 "unavailable" 而非 disabled；"next run" 列改名 "schedule"。
- DLQ dry-run 按钮改名 "Preview impact (≤50 loaded)"；死 Bell 按钮移除；字体栈修正（自托管 Geist 或移除声明改 system-ui 栈）；transform 下拉分组；未知 hash 显示明确 404 提示页。
- ConfigForm label htmlFor 绑定 + 字段描述 i18n 化。

## 数据语义与回滚

- localStorage 草稿 key 不变（`etl_wizard_draft_v1`），新增 secret 哨兵是向后兼容的（旧草稿恢复时照常显示值）。
- DAG seed 用 sessionStorage，不持久化，不与 pipeline storage 交互。
- 后端 last_error 变更不改 API shape（字段已存在于 PipelineStats 类型），只补数据来源。
- 每个 round 独立 commit 组，可单独 revert。

## 测试策略

- 前端：typecheck/build/lint + 针对性 vitest（若引入）否则 Playwright 断言。
- `hack/e2e-ui.sh` 全量回归（112+ 断言）。
- Round 3 后端增量：`go test ./internal/etl/...` + 受影响路径 e2e + 证据门禁重绑（hack/check-connector-evidence.sh -strict）。
