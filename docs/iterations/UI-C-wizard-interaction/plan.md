# UI-C 技术方案

> 本文是实现输入，不是验收标准。验收以 [spec.md](./spec.md) 为准。

## 方案总览

三个改动点都在既有组件内完成，不新建基础设施：ConfigForm 加 htmlFor/id 绑定与 i18n 标签；向导 transform 链的移动加语义提示、加多选批量删除（平移 PipelinesPage 的 ConfirmDialog 模式）。全部走既有 `t()` 与 `ConfirmDialog`、`data-testid` 惯例。

## 分项设计

### A. ConfigForm htmlFor 绑定 + 字段语义标签

**现状**：`web/src/components/shared/config-form.tsx` 的 label 与 input 未关联；字段标签直接用 field.name。

**目标**：

1. 每个字段生成稳定 id：`cfg-<form-scope>-<field-name>`；label `htmlFor`、input `id`、（select/checkbox 同理）。
2. 标签解析顺序：字段元数据 label（若 plugin schema 提供）→ i18n key `field.<name>` → 回退 field.name 本身。悬停 title 显示 `field.name`（原始名始终可见）。
3. `web/src/i18n.ts` 增补高频字段 key（batch_size、checkpoint_interval_sec、max_retries、backoff 等向导已暴露字段）。

**关键文件**：config-form.tsx、i18n.ts、（如需）plugin schema 类型。

### B. transform 移动语义提示

**现状**：moveTransform（first-task-wizard.tsx:1420）静默交换；按钮 title="Move up/down" 裸英文。

**目标**：移动成功后 toast/inline 提示"步骤顺序已变化，下游处理顺序随之改变，请复核试运行输出"；↑/↓ title/aria-label 走 `wizard.transformMoveUp/Down` i18n key。

### C. transform 链多选批量删除

**目标**：每个 transform 卡片加 checkbox（data-testid=`wizard-transform-select-N`）；选中 ≥1 时浮出批量操作条（已选 N + Delete selected）；删除走 ConfirmDialog（显示数量），完成后结果反馈（成功 N / 失败 M 与原因）；复用 PipelinesPage batchResult 面板样式。

**设计权衡**：否决"批量停用"——transform 无 enabled 语义，做伪开关违背事实一致性原则；只交付真实存在的操作（移动/删除）。

## 风险与回滚

| 风险 | 影响 | 缓解 | 回滚 |
| --- | --- | --- | --- |
| 字段 id 冲突（同页多 ConfigForm） | label 错关联 | id 含 form-scope 唯一段 | — |
| 既有 D2.1b 断言受 checkbox 布局影响 | e2e 回归 | testid 不变，新增元素不打乱既有选择器 | — |

## 测试策略

| 层级 | 范围 | 命令 |
| --- | --- | --- |
| 1 静态 | typecheck/lint | `npm run typecheck && npm run lint` |
| 2 构建 | build | `npm run build` |
| 3 e2e | label 聚焦、i18n 标签、移动提示、批量删除 | `hack/e2e-ui.sh`（新增 C 系列断言） |
