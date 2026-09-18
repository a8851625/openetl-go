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
| UC.1 | ConfigForm label 绑定与语义标签 | — | `todo` | config-form.tsx + i18n.ts + e2e |
| UC.2 | transform 移动提示与批量删除 | UC.1 | `todo` | first-task-wizard.tsx + e2e |

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
