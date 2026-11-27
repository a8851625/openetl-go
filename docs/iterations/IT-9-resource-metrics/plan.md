# IT-9 技术方案

> 验收以 [spec.md](./spec.md) 和 [ROADMAP](../../ROADMAP.zh.md) EV-3 为准；本文是实施输入。
> 已进入实施；实际行为与命令证据随 tasks.md 回填，不能把方案描述当作通过结果。

## 现有实现与接入点

| 接入点 | 当前行为 | 最小改动方向 |
| --- | --- | --- |
| `hack/bench-baseline.sh`／`hack/container-cli.sh` | 统一 docker-first、显式 CONTAINER_CLI 覆盖 | 保留唯一 selector；wrapper 透传新增测量／预算参数 |
| `hack/bench_baseline.py:image_identity` | `Id` 为 provider inspect ID，`bytes=raw["Size"]` | provider 信息保留为诊断；增加从所选归档计算的明确指标 |
| `Baseline.build` | 构建四变体、提取实际 `/app/main` 和前端、记录 packed hash | 逐变体保存临时归档并测量，绑定实际 binary 与归档；不减少矩阵 |
| `Baseline.hardware` | 已记录 server runtime_version、架构、主机资源 | 保留字段；补实际 provider、client／Python 版本与测量方法 |
| `Baseline.measure` | default production／SQLite／TLS 的启动与离线恢复 | 保持完整过程、16×100 数据、3 trials 和身份／checkpoint 验证 |
| `validate_result`／main `--check` | v2 结构检查＋warning | v3 方法／兼容检查，新增显式 policy 和 enforce 开关 |
| `hack/test_bench_baseline.py` | 12 个 readiness／RSS／内容／退出码边界 tests | 保留原语义；增加真实字节 fixtures、v2/v3 与预算负向 cases |
| `.github/workflows/_gate.yml:bench-baseline` | 完整测量、check、上传 JSON／logs，聚合不允许 skip | 增加真实 conformance 与明确预算 mode，保留原聚合依赖 |

本机可见 docker 和 podman 可执行文件，仅凭这一点不能认定有两个实际不同引擎。
CI 和本地 probe 都要记录真实 provider 与服务端版本；同一服务的 CLI 别名不能通过双引擎验收。
Python 开发以 CI 可用版本为基线，不能因本机 Python 较新而依赖 CI 未提供的模块。

## 1. 归档度量与 schema v3

### 规范化指标

为所选镜像记录：

- `image_config_sha256`：归档中实际 configuration JSON 原始字节的摘要；与 engine inspect ID 区分。
- `image_archive_sha256`／`image_archive_bytes`：本次读取的完整保存文件；归档容器／格式不同可导致大小不同，只作可追溯事实。
- `image_uncompressed_layer_bytes`：所选 manifest 每个层条目解码后 tar 流的字节数相加，单位 bytes。
- `layer_inventory`：按 manifest 顺序记录层摘要、diff ID、编码和展开字节；总和必须一致。
- `engine_reported_image_bytes`、provider/version、provider image ID：真实诊断值，不替代规范化指标或跨版本阈值。
- 保留实际 binary／frontend／packed 大小与 SHA。方法标识拟为 `uncompressed-layer-bytes/v1`。

`image_config_sha256`／`image_archive_sha256` 使用 64 位小写十六进制；CLI 的期望 digest 接受并规范化
`sha256:` 前缀。字节计量为整数、实际应用镜像必须为正值；`diff_id` 保留算法前缀，不能用 bool／字符串充当数值。

从 manifest 与期望 config digest／image architecture 选择镜像，不能把整个 archive 或 index 的全部平台相加。
Docker-save 与 OCI archive 的对应关系由固定 fixtures 验证；首轮支持 raw／gzip 层。
若真实受测归档使用其他编码，先记录具体必要前置及测试范围，不默默添加依赖或伪称支持。
未知编码、配置不唯一、hash／diff ID 不匹配、缺 blob 必须失败，不能回退到 inspect.Size。

逐块流式读取，限制 configuration／index 大小和异常展开量；不 extractall，不跟随符号链接或越界路径。
不把 config.Env、私钥、层内容或业务 payload 写入结果。公开报告只保留摘要、方法、尺寸及必要元数据。

### CLI 与报告范围

拟增加一个纯归档入口：`--image-archive <path> --image-config-digest <sha256> --out <owned-dir>`。
它输出独立的 `openetl-image-metrics/v1` 报告，不构建、不启动应用，不冒充 packaging-startup-restore baseline。
默认完整测量继续经 `bench-baseline.sh`，输出 schema v3；其中四种镜像都必须计算规范化指标。

v3 校验必须验证字段／单位／方法、各层总和、内容身份、实际构建矩阵、源码及工具信息。
完整 native 测量的 health／TLS／UID／恢复内容及 cleanup 失败规则保留。
原始归档在验收计算阶段保存用于复核；结果记录其 hash。保留必要 default archive 供跨引擎重放，
其他原始文件按有界 CI retention 保存，不把大体积生成物提交源码目录。

### 兼容与失败

v2 原文件保持原样，默认 `--check` 可以作历史观测读取；旧 image bytes 明确为引擎自报口径。
v2 不补造 v3 configuration／layer 指标，不作为新强制预算依据；v1 错误计时格式继续拒绝。
测量有效与符合预算是不同结果：测量错误任何模式都失败，预算警告不能掩盖测量错误。

## 2. 真实双引擎对账

Round 2 实测前置：本机 Podman 5.8.4 的 `save --format docker-archive` 附带 legacy
`<layer>/layer.tar -> ../<diff-id>.tar` 符号链接，严格 parser 按既定规则拒绝，原始失败报告保留。
继续实测发现 `oci-archive` 会把 Docker config 转换为 OCI config，摘要从 `5ce9794a…` 变为
`a95852cc…`；该 probe 按期望摘要拒绝，不能接受为 conformance。保留两次失败报告。
最终使用 Podman 原生 `docker-archive`，parser 只允许忽略未被 manifest 引用的严格 legacy alias：
`<64 hex>/layer.tar`、size 0、目标必须为已存在的 regular `../<64 hex>.tar`。
不跟随链接；manifest 引用 alias、越界目标、任意 symlink/hardlink、重复路径仍拒绝。
报告记录忽略 alias 数量，完整原始 archive hash 保留。此为真实保存格式的必要兼容前置，
不扩展 codec，不改变 config/layer 身份，也不改变 build 的 Docker format/HEALTHCHECK 契约。

Round 2 的另一实际环境差异：VM 内原生 Linux `podman version` 只有 `Client`，因为该引擎 daemonless；
macOS remote API 才同时返回 `Server`。本机 SSH 命令已证实该 shape。probe 对 native Linux
以 `info.version` 记录真实 engine version，必须和 Linux CLI version 一致，标注 `daemonless-local`。
不同版本或非 Linux 的 client-only 响应不能借此冒充 provider。Docker 仍要求其 Engine component 与 info/server 版本一致。

拟增加 `hack/e2e-bench-image-metrics.sh`，通过现有 CONTAINER_CLI selector 执行一个 provider 的 probe。
输入为任务拥有的同一保存归档、期望配置摘要和输出目录。仅 load／save／inspect 与归档度量，
不需要运行该镜像内 Go 程序。

在 Docker 与 Podman 上分别保存／度量同一 default 镜像，再比较：配置摘要、架构、diff IDs、展开层字节。
归档容器大小、压缩方式、provider inspect ID 可以不同，必须原样记录；不比较不同硬件的 RSS 或启动耗时。
若传输改变 config 或层内容，不能改写期望摘要来接受它；先报告具体失败，再修复受限路径。

CI 优先复用完整 baseline 创建的 default 镜像／归档；Podman 仅为测试工具，不新增生产依赖。
记录两个实际 provider 和完整 server versions；至少一份真正的跨引擎结果，不能把 Docker 28 fixture 当成真实运行。
缺第二个引擎或原始归档时由 CI 维护者提供，完成同一 probe 后解阻；不得静默跳过。

## 3. 预算 policy 与退出码

拟增加 `--budgets <policy.json>` 和 `--enforce-budgets`，policy 文件拟为 `hack/bench-budgets.json`。
policy 包含版本、指标名、方法、单位、架构／variant 适用范围、上限、基线来源及余量说明；记录其 SHA。

先以 default binary 与 `image_uncompressed_layer_bytes` 两个稳定体积指标灰度。
具体值在当前可信 v3 数据后确定，不直接搬用旧 raw.Size 的 200 MiB 上限；只覆盖有有效依据的架构。
其他变体依然完整报告，时间／RSS 保留观测 warning。历史 14 次观测用于噪声背景，不伪装为新口径校准。

| 输入与模式 | 必需行为 |
| --- | --- |
| 有效报告、预算内 | exit 0，结果明确适用 policy |
| 有效报告、选定预算超限、默认观测 | warning、exit 0 |
| 有效报告、选定预算超限、显式 enforce | error、exit 1；列出指标／实测／上限 |
| 缺失、failed、错误单位、异常数值、错误内容身份 | 两种模式都 error、exit 1 |
| enforce 使用 v2、dirty source、缺 policy／缺适用架构／不匹配方法 | error、exit 1，不算预算通过 |
| 时间／RSS 超观测预算但体积预算合格 | warning，体积强制不将这些项改为 blocking |

在干净隔离源码或实际 CI checkout 采集预算依据，记录真正的 commit／输入 fingerprint；
PR merge SHA 与 branch SHA 各有其范围，不相互替换。当前 `.gitignore` 已处理的日志自污染保持回归验证。

## 4. CI、文档与证据

沿用 `bench-baseline` 作业、现有 `gate-passed` 和外层 source + candidate 聚合。
完整四变体测量及 default 恢复照常执行；增加双引擎 probe、v3 checker、正常预算和受控低预算验证。
受控负向 case 明确期待 exit 1，并独立记录；真实基线或未知错误失败不能被忽略。
作业命名／注释、release checklist、资源说明反映“选定体积灰度强制＋时间/RSS warning”的实际范围。

拟交付证据放在本迭代 `evidence/`，不写入旧基线或 P1 记录：

| 文件／产物 | 要证明的内容 |
| --- | --- |
| `acceptance.md` | A1–A9 的 exact command／CI run、pass/fail/skip/block 与残余 |
| `archive-fixtures.md`、parser logs | 确定性数据、真实期望字节、错误编码／路径／hash 拒绝 |
| `docker-image-metrics.json`／`podman-image-metrics.json`／`conformance.json` | 实际不同 provider 与同一镜像内容／规范化体积对账 |
| `baseline-v3.json`、命令日志、source/harness/归档元数据 | 当前四变体与 default native profile 全量测量 |
| `budget-decision.md`、policy、正常／受控超限日志 | 基线与余量、架构范围、退出码、灰度 scope 与回滚 |
| 当前 PR CI 链接 | 原有 source、存储／frontend、exact-candidate 和 Delivery Passed 没有被豁免 |

## 允许文件与数据语义

| 允许写入 | 用途 |
| --- | --- |
| `hack/bench_baseline.py`、`hack/test_bench_baseline.py`、必要的 `hack/bench-baseline.sh` | 测量、CLI、compatibility、negative cases |
| 新 `hack/e2e-bench-image-metrics.sh`、`hack/bench-budgets.json` | 受限 conformance 与 policy |
| `.github/workflows/_gate.yml` | 仅对应测量／预算步骤与必要工具准备；不放宽聚合 |
| `docs/resource-baseline.md`、`docs/release-checklist.md` | 新口径与灰度范围；保留用户既有编辑和历史事实 |
| 本迭代三件套／`evidence/`、ROADMAP、迭代索引 | 实际领取、证据与收口 |

生产源位置、checkpoint、sink ack、DLQ/replay、metadata schema 不修改；无数据库迁移。
benchmark 的临时 SQLite／备份只用于既有隔离 fixture，恢复继续逐项对账。
测量或预算返回失败不删除生产数据；只清理唯一前缀标识的任务资源，cleanup 失败进入结果。
不改 `internal/`、web／公共 UI、Dockerfile／GoReleaser 发布配方或 GA-C1 helper；若出现具体必要前置先记录有界 scope 决策。

## 风险与回滚

| 风险 | 缓解 | 回滚 |
| --- | --- | --- |
| Docker/OCI 选择错误、只加压缩字节或重复统计其他平台 | 固定已知 fixtures、期望 config、层数／diff ID／总和与实际双引擎对账 | 保留新原始证据；暂停强制，修正方法，不用旧 raw.Size 冒充可信值 |
| CLI 别名指向同一服务 | 比较真实 provider/server 信息，拒绝假双引擎证明 | 记录精确外部输入和 unblock probe |
| 新阈值对共享 runner 时间噪声过敏 | 本轮只强制两个稳定体积项，时间／RSS warning | 移除 enforce flag；有效性检查仍必过 |
| 归档解析耗内存或泄露 Env | 流式、有界、无 extractall、输出仅摘要／尺寸；秘密哨兵测试 | 停止相关采集，保留失败记录，不上传私钥／原始 Env |
| 用户未提交资源文档被覆盖 | 编辑前检查 diff，增量追加新口径／证据 | 只回退本增量，保留既有工作树内容 |
| 冷缓存／镜像网络拖延两周窗口 | 固定实际输入、记录失败，预留第五轮收口 | 缺必需 evidence 时交接 active/blocked，不降验收标准 |

预算回滚只撤灰度 flag／policy 应用，不恢复错误计量、不改业务状态、不增 GATE_ALLOWLIST。
v3 报告始终保留实际格式，旧消费者只读 v2；新 consumer 继续兼容 v2 历史观测。

## 验证顺序

以下为实施后命令，不表示当前已存在新增 CLI/script。

1. `PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s hack -p test_bench_baseline.py -v`；相关 shell `bash -n`。
2. 小型固定归档走纯归档 CLI，验证实际输出与已知字节／hash、秘密哨兵和不完整输入。
3. `CONTAINER_CLI=docker ./hack/bench-baseline.sh --out <owned-dir>`；Podman 可用于本地 native 测量；各自事实单独记录。
4. 同一 default 保存镜像，两个实际 provider 运行新 image-metrics probe，保留 conformance JSON；不启动跨架构应用。
5. `python3 hack/bench_baseline.py --check <v3-result> --budgets <policy> --enforce-budgets`，正常结果 0；受控低预算 1；默认 warning 0。
6. 当前 PR 的现有完整 source＋candidate CI 全绿；`git diff --check`；检查证据和用户改动保留情况。

本轮 Python／harness 改动无需新 Go race 测试；既有 CI Go race 与三 storage gates照常运行。
不借 default／SQLite 资源测量宣称其他 runtime／storage／distributed 的新增容量认证。

## EV-3.4 收口的具体前置

Preflight 确认 `hack/cmd/check-connector-evidence/main.go:allowedEvidenceDescendantPath` 当前只允许
P1 文档／证据等指定路径。IT-9 的验收和交付文档不在允许列表中，认证后回填会使 strict source binding
再次失败。EV-3.4 允许最小扩展该 checker 和最近的 `main_test.go`：只加入 IT-9 三件套／限定证据文档、
resource-baseline 与 release-checklist 等本项 bookkeeping；代码／脚本／workflow 仍需重新认证。
这是 A7/A9 的必要前置，不新增 roadmap 项；改动后跑 targeted Go race、strict descendant 负向和完整当前 candidate/CI。

实际旧证据负向已执行：`-strict -commit 191683e` 使用旧 P1 manifest 因 `_gate.yml` 改变而拒绝。
当前 CI run 46024237806 的 36 jobs 与 23-case 候选已全部通过，收口须同步该候选的实际
`connector-evidence.json` 到 `internal/etl/server/evidence/connector-evidence.json`，以及它的七份
`results/tagged/paths/*.json` 到既有 `docs/evidence/` canonical files。此为 A7/A9 的必要证据回填，
追加到本轮允许范围；不改 connector/runtime 代码或 maturity，不手填执行窗口、SHA 或 pass。
拷贝后以真实 candidate source/config ID 跑 strict，再以只含指定 bookkeeping 的 descendant 验证绑定。

## EV-3.4 的已证实有界测试前置

首次 PR #14 run `46017295606` 的 source storage/race/frontend/connector gates 已通过，
但 exact-candidate 的 `hack/e2e-doris-source-order.sh` 在 MySQL 初始化期间的 Unix socket
`SELECT 1` 返回成功后立即执行 GRANT，遇到临时 mysqld 停止／正式 mysqld 尚未启动的窗口：
`ERROR 2002 ... Can't connect ... mysqld.sock`。实际 job `140571144753` 的 command.log 和失败 report 保留。

EV-3.4 仅追加该脚本的启动 readiness 为允许文件：通过 TCP `127.0.0.1` 的已认证 SQL
检查最终 MySQL（初始化临时实例使用 skip-networking），并在超时后明确 fail。
不修改 Doris/source_order、CDC 位置或候选测量语义，不扩大其他 connector scope。
以 shell syntax + 同一 mandatory exact-candidate path 的真实通过作为验收，完整 Delivery Passed 仍必需。
