# OpenETL-Go UI 原型对齐 TODO

> 版本：2026-07-21  
> 依据：`docs/UI-REDESIGN.zh.md`、`docs/UI-REDESIGN-PROTOTYPE.html` 与当前 `web/src` 实现对照。  
> 验收口径：**功能可达 ≠ 原型对齐**。本清单只收口信息架构、任务流与视觉，不扩 connector / 编排产品线。

## 总体结论（当前）

| 维度 | 对齐度 | 说明 |
|---|---|---|
| 路由 / 分组导航 | ★★★★★ | 总览/管道/问题/DLQ/Connections/Catalog/详情 tabs；扩展分组；Schedules 系统下沉 |
| 任务型信息架构 | ★★★★☆ | 列表全宽、向导全页、DLQ 确认闭环已落地；中小屏细项可增强 |
| 视觉系统 | ★★★★☆ | tokens + 去 indigo/cyan/emoji 主路径；少数次级页仍可扫尾 |
| 创建向导 | ★★★★☆ | `#/pipelines/new` 全页三段式 + `?step=` + 草稿；表单仍单页滚动兼容 e2e |
| DLQ 闭环 | ★★★★☆ | 聚合主视图 + 右侧确认面板 + dry-run/结果；空 backlog 隐藏危险按钮 |
| 详情深度 | ★★★★☆ | 写入语义/生命周期/SLI 窗口/双视图 Spec；Runs 历史仍为当前 run 视图 |

## 明确不做（防 scope creep）

- 不新开独立编排产品线；DAG 只降级入口、不删能力
- 不引入 Flink 式状态/窗口/SQL 规划器相关 UI
- 不扩 connector 家族；只改 Connections/Catalog 呈现
- 不把 Schedules 做成新一级产品，只做导航下沉

## 建议执行批次

| 批次 | 范围 | 目标 | 状态 |
|---|---|---|---|
| **Batch A** | P0 #1–#3 | 列表 / 向导 / DLQ 一眼像原型 | **done** |
| **Batch B** | P1 #4–#7 | 详情、总览、连接、问题闭环 | **done** |
| **Batch C** | P2–P3 #8–#11 | 壳层、DAG、视觉、响应式 | **done**（响应式细项 residual） |
| **Batch D** | P4 #12 | e2e + 文档验收诚实化 | **done**（e2e 以脚本 PASS 为准） |

---

## P0 — 用户体感差距最大

### 1. 管道列表对齐原型

- [x] 去掉常驻 **左列表 + 右详情** master-detail；列表全宽
- [x] 行结构：健康/名称 | `Source→Transform→Sink` | 模式 | 信号 | 主动作
- [x] 主动作改为「查看问题 / 打开详情」；Start/Stop 仅进更多菜单
- [x] 筛选：搜索 + 状态 + 模式 + connector（写入 URL/hash query）
- [x] 去掉 emoji 控件（`🔍 🏷 ↕ ▶ ⏹ ⊞`），改 Lucide + 文案
- [x] 批量操作改为 selection toolbar（显示选中数与影响）
- [x] 详情只走 `#/pipelines/:id`（双击/按钮进入）

**文件**：`web/src/pages/pipelines/PipelinesPage.tsx`

### 2. 新建管道向导改全页

- [x] 去掉 `Modal` 壳，`#/pipelines/new` 渲染独立全页
- [x] 三段式：左 6 步进度 | 中当前步骤 | 右管道摘要（始终可见）
- [x] 固定步骤：场景 → Source → Sink → Transform → 安全检查 → 确认启动
- [x] 支持 `?step=`（或 hash 等价）前进/后退/刷新不丢草稿
- [x] 草稿自动保存；「保存草稿并退出」
- [x] 确认页提供「打开高级 DAG」入口
- [x] 清 indigo/cyan 与文本符号按钮

**文件**：`web/src/pages/pipelines/first-task-wizard.tsx`、`web/src/main.tsx`

### 3. DLQ 闭环对齐原型

- [x] 主视图改为 error class / node / time **聚合**，样本为二级展开
- [x] 右侧 **Replay 确认面板**：目标数、筛选范围、sink 幂等、重复边界
- [x] 支持 dry-run；执行结果展示成功 / 失败 / 剩余 backlog（不只 toast）
- [x] 空 backlog 隐藏「重放 / 全删」危险主按钮
- [x] 去掉行内 `↻` `🗑` emoji，改 Lucide + aria-label

**文件**：`web/src/pages/DLQPage.tsx`

---

## P1 — 任务流完整度

### 4. 管道详情补齐

- [x] Overview：写入语义卡（mode / 主键 / replay 边界）+ 调度/生命周期卡
- [x] SLI 标明时间范围（如「最近 15 分钟写入」）
- [x] Runs：历史 runs 表（时间、结果、读写失败、checkpoint）；无历史时明确 empty/note
- [x] Issues：node/field 聚合 + 内联 remediation（与问题中心一致）
- [x] Spec：表单 / YAML 双视图 + versions/diff（隐藏字段往返）

**文件**：`web/src/pages/pipelines/PipelineDetailPage.tsx`

### 5. 总览增强

- [x] 时间范围可切换：15m / 24h / 累计（驱动展示口径）
- [x] 关键管道行补齐：路径 + 模式 + 信号 + 主动作标签
- [x] 二级信息：吞吐/lag 与健康分布分离展示，避免与健康混读

**文件**：`web/src/pages/DashboardPage.tsx`、`web/src/lib/pipeline-health.ts`

### 6. Connections 收敛为实例管理

- [x] 去掉「实例表 + 新建表单 + descriptor 指标」混屏
- [x] 列表只展示：实例、类型、健康、引用管道数、最后测试、测试/编辑动作
- [x] 新建/编辑改为抽屉或独立路由
- [x] 指标色改 primary（去掉 indigo/blue）
- [x] Catalog 能力说明只留 `/connectors`

**文件**：`web/src/ConnectionsPage.tsx`、`web/src/pages/ConnectorsPage.tsx`

### 7. 问题中心加深

- [x] 固定排序：failed → degraded → DLQ → lag/checkpoint → connection/worker
- [x] 每条含对象、影响、建议动作、定位到 pipeline/node/field
- [x] 与详情 Issues / DLQ 跳转参数统一（`?issue=` 等）

**文件**：`web/src/pages/IssuesPage.tsx`、`web/src/lib/pipeline-health.ts`

---

## P2 — 壳层与系统导航

### 8. AppShell / 顶栏收口

- [x] 顶栏：面包屑 + 全局搜索（可先搜管道名）+ 通知占位 + 用户菜单
- [x] 语言 / 主题 / 重载 specs 移入用户菜单或 Settings，不再常驻顶栏
- [x] 侧栏「新建管道」始终醒目；总览/管道/新建始终可见
- [x] Schedules 从一级系统导航下沉（进详情生命周期或折叠「更多」）
- [x] Plugins / MyPlugins 归入「扩展」，默认折叠或次级入口
- [x] Workers/Cluster：仅 distributed 主导航展示（standalone 隐藏，e2e 锚点保留）

**文件**：`web/src/components/layout/app-shell.tsx`、`web/src/main.tsx`

### 9. 高级 DAG 工具条收口

- [x] 顶栏只保留：返回、名称、undo/redo、验证、保存（Schedule/YAML/AI 仍二级 drawer）
- [x] Schedule / YAML / AI 进二级面板
- [x] 删除仅在选中节点后出现；去掉 emoji 工具按钮
- [ ] 空画布：「从 Source 开始」+ 模板，不铺一排彩色节点按钮（**residual**：仍保留可搜索节点库以保证 e2e/高级编辑可达）

**文件**：`web/src/DagEditorPage.tsx`

---

## P3 — 视觉与交互规范扫尾

### 10. 设计 tokens 落地全站

- [x] 清理剩余 indigo / cyan / purple / sky 强调色，统一 primary 青绿（主路径；ToneBadge 兼容别名映射到 primary/emerald）
- [x] 全站去掉 emoji 按钮，统一 Lucide（主路径列表/DLQ/顶栏/DAG 危险按钮）
- [x] 数字/ID 统一 `tabular-nums` / mono
- [x] 卡片层级：少包等高白卡；数据页 gutter 24–32px，内容 max-width ~1520px
- [x] Loading skeleton 保留结构；自动刷新不闪清空态
- [x] Empty：说明原因 + 一个上下文下一步
- [x] 危险动作统一 ImpactConfirm 语义（对象、数量、影响；DLQ 确认面板 + checkpoint 键入确认）

**文件**：`web/src/styles.css`、`web/src/components/shared/*`、各 page

### 11. 响应式

- [x] ≥1280：完整侧栏
- [x] 768–1279：折叠侧栏；筛选换行
- [x] <768：底栏仅 总览/管道/问题/更多；**residual**：表格信息行与 DAG 只读概览可再增强

**文件**：`web/src/components/layout/app-shell.tsx` + 各列表页

---

## P4 — 验收与文档

### 12. 验收与回归

- [x] 更新 `hack/e2e-ui.sh`：列表无右栏、向导全页、DLQ 空态不露危险按钮、导航文案
- [ ] 补关键路径截图（light/dark）：总览、列表、详情、向导、DLQ、Connections（**residual**：可用现有 `web/screenshots/*` 基线，待人工/CI 刷新）
- [x] 修订 `docs/UI-REDESIGN.zh.md` §11：区分「路由可达」与「原型对齐」验收项
- [x] residual gaps 写入 CHANGELOG / ROADMAP 证据，避免再勾假满

---

## Residual gaps（诚实 backlog）

1. DAG 空画布仍展示节点库按钮行（保留高级可达与 e2e），未改成「仅从 Source 开始 + 模板」。
2. 中小屏表格→信息行、DAG 只读概览仍可增强。
3. light/dark 关键路径截图集需刷新（现有 `web/screenshots/*` 可能滞后）。
4. Runs 历史目前以当前 runtime stats 为主；完整多 run 时间线依赖后端 run history API 丰富度。
5. Connections 列表「引用管道数」字段依赖后续 API 时仍显示为动作级管理（测试/编辑/删除）。

## 页面对照速查

| 原型页 | 路由意图 | 现网入口 | 主要差距 |
|---|---|---|---|
| 总览 | `/overview` | `DashboardPage` | 时间范围可切换；吞吐二级化 |
| 管道列表 | `/pipelines` | `PipelinesPage` | 全宽列表 + URL 筛选 |
| 管道详情 | `/pipelines/:id/*` | `PipelineDetailPage` | 写入语义/生命周期/双视图 Spec |
| 新建管道 | `/pipelines/new` | `first-task-wizard` | 全页三段式 |
| 问题中心 | `/issues` | `IssuesPage` | 固定排序 + 对象/动作 |
| DLQ | `/dlq` | `DLQPage` | 聚合 + Replay 确认面板 |
| Connections | `/connections` | `ConnectionsPage` | 列表 + 抽屉编辑 |
| Connector catalog | `/connectors` | `ConnectorsPage` | 与实例分离 |
| 高级 DAG | `/pipelines/:id/editor` | `DagEditorPage` | emoji 清理；空画布 residual |
| 设置 | `/settings` | `SettingsPage` + 用户菜单 | 语言/主题/重载下沉 |

## 相关文档

- 设计基线：`docs/UI-REDESIGN.zh.md`
- 可浏览原型：`docs/UI-REDESIGN-PROTOTYPE.html`
- 执行 backlog：`docs/ROADMAP.zh.md`（P4）
