# UI-C：向导行交互模型收尾

> 状态：`queued` | 依赖：无 | 归属 roadmap 条目：UI 小项池（P2-5 残留 + UI-B.7 收口记录的批量步骤操作）

## 问题陈述

UI-A 审计 P2-5 三条发现中，"删除 transform 无风险提示"已于 UI-B.7（2026-09-17，ConfirmDialog）交付，剩余未修：

1. **ConfigForm label 无 htmlFor 绑定**（web/src/components/shared/config-form.tsx）：label 与 input 未关联——点击 label 不聚焦输入框、屏幕阅读器不播报关联字段，可访问性缺陷。
2. **字段名裸露内部命名**：向导配置表单直接显示 `batch_size`、`checkpoint_interval_sec` 等内部字段名，用户需读文档才能理解语义；i18n 基础设施已就绪但未覆盖此处。
3. **transform 上移/下移无风险提示**（web/src/pages/pipelines/first-task-wizard.tsx:1420 moveTransform；按钮 title 仍是裸 "Move up/down"）：改变步骤顺序会改变下游记录语义（如 filter 在 project 前后结果不同），当前无任何提示。

另：UI-B.7 收口时记录"向导 transform 链批量操作（多选步骤统一移动/删除/停用）"为可选残余，PipelinesPage 已有成熟的批量操作模式（ConfirmDialog + 受控并发 + 结果汇总）可平移。

## 可观察结果

1. 点击向导配置表单的任一字段 label，对应输入框聚焦；屏幕阅读器（aria）关联正确。
2. 配置字段显示用户语义标签（中英双语），悬停可见原始字段名。
3. transform 步骤移动后，UI 明确提示顺序变化对下游语义的影响。
4. 向导 transform 链支持多选后统一删除（带确认与结果汇总）；移动/删除/停用操作有明确反馈。

## 验收标准

| # | 验收标准 | 判定方式 |
| --- | --- | --- |
| 1 | ConfigForm 全部字段 label 有 htmlFor/id 绑定 | e2e 断言（click label → 对应 input focus）+ axe 规则 |
| 2 | 字段标签走 i18n（en/zh），悬停 title 显示原始字段名；缺省 key 回退字段名 | e2e 断言 + i18n key 完整性脚本 |
| 3 | transform 移动后 toast/inline 提示顺序语义变化；↑/↓ 按钮 title/aria-label 走 t() | e2e 断言 |
| 4 | transform 链多选（checkbox）+ 批量删除（ConfirmDialog 显示目标数量）+ 结果反馈 | e2e 断言 |
| 5 | typecheck 0 错误；lint 不高于基线 29 warnings；`hack/e2e-ui.sh` 全绿（163+ 新增断言） | CI 命令 |

## 非目标

- 不做 transform 批量"停用"（transform 配置无 enabled 语义，伪功能不做；只做移动/删除）。
- 不改向导信息架构、不新增页面、不动 runtime。
- 不做 ConfigForm 动态表单引擎重构。

## 交付约束

- 单真相源原则：字段标签映射不引入第二套 schema 定义，从 plugin schema/descriptor 已有字段元数据（label/title 若有）取，缺省用 i18n key 约定 `field.<name>`。
- 既有 163 条 e2e 不得回归；D2.1b 既有断言若受交互变化影响须同步适配并注明。

## 依赖与前置

| 类型 | 内容 | 状态 |
| --- | --- | --- |
| 迭代依赖 | 无（可与 IT-5 并行，UI 小项不占 active 槽） | — |
| 外部输入 | 无 | — |

## 完成定义（DoD）

1. tasks.md 全部 task done 且有证据。
2. 验收标准全部 passed。
3. ROADMAP UI-C 条目更新；UI-A tasks.md 交叉引用闭合。
4. `git diff --check` 通过。
