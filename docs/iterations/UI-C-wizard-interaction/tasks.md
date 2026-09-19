# UI-C 任务分解

> 状态取值：`todo` / `active` / `done` / `blocked`。一次只允许一个 `active`。

## Round 分组

| Round | 包含 task | 目标 |
| --- | --- | --- |
| 1/2 | UC.1 | ConfigForm htmlFor/id + 字段 i18n 标签（A） |
| 2/2 | UC.2 | transform 移动提示 + 多选批量删除（B+C）+ e2e 收口 |

## 任务表

| ID | 任务 | 依赖 | 状态 | 证据落点 |
| --- | --- | --- | --- | --- |
| UC.1 | ConfigForm label 绑定与语义标签 | — | `done` | web/src/configFields.tsx + i18n.ts + e2e C1.1–C1.3 |
| UC.2 | transform 移动提示与批量删除 | UC.1 | `done` | first-task-wizard.tsx + e2e C2.0–C2.4b |

## 任务明细

### UC.1 ConfigForm label 绑定与语义标签

**验收**：

1. ConfigForm 全部字段 label htmlFor/id 关联；e2e 断言 click label → input.focus。
2. 字段标签 i18n（en/zh）；悬停 title 显示原始字段名；无 key 时回退字段名。
3. typecheck/lint/build 基线内；`hack/e2e-ui.sh` 全绿。

**证据落点**：`web/src/components/shared/config-form.tsx`、`web/src/i18n.ts`、`hack/e2e-ui.sh` C 系列断言。

### UC.2 transform 移动提示与批量删除

**验收**：

1. ↑/↓ 按钮 title/aria-label 走 i18n；移动后 toast/inline 提示顺序语义变化。
2. transform 多选 checkbox + 批量删除 ConfirmDialog（显示数量）+ 结果反馈。
3. e2e：多选→删除→链长度断言；既有 D2.1b 不回归。

**证据落点**：`web/src/pages/pipelines/first-task-wizard.tsx`、`hack/e2e-ui.sh`。

**领取记录**：

```text
Round: 1-2/2（UC.1+UC.2 一并交付，两轮窗口一次跑完）
Roadmap item: UI-C (UC.1+UC.2)
Profile/path: standalone Web UI
Objective: 向导配置表单可达性（label htmlFor/id 绑定 + 点击聚焦）与语义 i18n 标签（悬停显示原字段名）；transform 移动顺序语义提示 + 多选批量删除（ConfirmDialog + 结果反馈）。
Scope: web/src/configFields.tsx、web/src/i18n.ts（field.* 38 个新 key + wizard.* 10 个）、web/src/pages/pipelines/first-task-wizard.tsx、hack/e2e-ui.sh（C 系列 9 条断言）
Non-goals: transform 停用伪功能（无 enabled 语义）；IA 重构；ConfigForm 动态表单引擎
Acceptance:
1. ConfigForm 全部字段 label htmlFor/id 关联，点击 label 聚焦 input — C1.1/C1.2 PASS
2. 字段标签 i18n（en/zh），悬停 title 显示原始字段名，缺 key 回退字段名 — C1.3 PASS（t() 缺 key 返回 key 本身，即原名）
3. transform 移动后 toast 提示顺序语义变化；↑/↓ title/aria-label 走 t() — C2.1 PASS
4. transform 链多选 + 批量删除 ConfirmDialog（显示数量）+ 结果反馈 — C2.2/C2.3/C2.4/C2.4b PASS
5. typecheck 0 错误；lint 基线 29 warnings 不变；e2e 172/172 全绿（163→172）
Evidence: hack/e2e-ui.sh 全量 172 passed / 0 failed（image openetl-go-etl:ui-c2）；npm run typecheck 0 errors；npm run build 正常（新 asset index-BALtJJHT.js）
Result: delivered（两轮窗口全部完成）
Residual/follow-up: none
```

## 验收矩阵（2026-09-19）

| 验收 | 证据 | 结果 |
| --- | --- | --- |
| 1 ConfigForm label htmlFor/id 绑定 | C1.1（全 label for→id 存在）+ C1.2（click label→focus） | passed |
| 2 字段标签 i18n + 悬停原名 + 回退 | C1.3（title=原名、text≠原名）+ t() 回退机制 | passed |
| 3 移动语义提示 + 按钮 i18n | C2.1（toast 含 downstream/下游） | passed |
| 4 多选批量删除 | C2.0–C2.4b（batch bar/确认数量/仅删所选/结果计数） | passed |
| 5 静态+回归 | typecheck 0 / lint 29 基线 / e2e 172/172 | passed |

**领取记录模板**（复制到 PR / 工作日志）：

```text
Round: <n>/2
Roadmap item: UI-C (UI-C/UC.x)
Profile/path: standalone Web UI
Objective: <one observable outcome>
Scope: <files/components allowed>
Non-goals: transform 停用伪功能；IA 重构
Acceptance: <numbered checks>
Evidence: <commands, e2e, docs>
Result: <delivered|active|blocked_external>
Residual/follow-up: <bounded next item or none>
```
