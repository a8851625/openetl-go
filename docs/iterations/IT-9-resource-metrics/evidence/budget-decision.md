# EV-3.3：预算 policy 与 CLI 验收

Round: 3/5；本增量 `delivered`。CI 灰度与完整聚合仍由 EV-3.4 验收。

采用 [hack/bench-budgets.json](../../../../hack/bench-budgets.json) 的 `2026-11-27.1` policy。
只强制所选 native Linux 架构的 default binary 与规范化 image 两个稳定体积项。
每个上限 = `ceil(observed_bytes × 1.25 / 1048576) × 1048576`；这是初始 25% 余量，
不是从 v2 engine.Size 的 200 MiB 常量推导，也不是生产运行时容量或百分比性能回归门槛。

| 架构／default 指标 | 真实校准 bytes | 上限 bytes（MiB） | 方法／单位 |
| --- | ---: | ---: | --- |
| amd64 binary | 67,285,176 | 84,934,656（81） | `file-bytes/v1`／bytes |
| amd64 image | 147,265,024 | 184,549,376（176） | `uncompressed-layer-bytes/v1`／bytes |
| arm64 binary | 63,963,320 | 80,740,352（77） | `file-bytes/v1`／bytes |
| arm64 image | 141,062,656 | 177,209,344（169） | `uncompressed-layer-bytes/v1`／bytes |

每个架构只有一个当前完整 native v3 校准；启动／恢复各 3 trials，不将其当作跨硬件性能统计。
AMD64 来自 `calibration-v3-amd64.json`，CI merge `653ce171…`（parent `d2d7c1d…`）；
ARM64 来自 `baseline-v3-arm64.json`，干净 branch `d2d7c1d…`。
完整原始 report SHA、source commit/fingerprint、baseline_bytes 与选择器在 policy 中，
由实际文件逐项重算核对。方法的真实双引擎证据见 [image-conformance.md](./image-conformance.md)。

binary 是从实际镜像提取的 `/app/main` 文件字节；启动／恢复过程再核对相同 executable SHA。
image 是 manifest 所选各层解压后 tar 流字节之和，包含 tar metadata、覆盖内容和 whiteouts；
归档传输大小与 engine.Size 仅诊断。它不等于合并后的 rootfs 或进程内存。

| 必需行为 | 实际证据 | 结果 |
| --- | --- | --- |
| 两个 native baseline 的正常 policy 强制成功 | `r3-cli-evidence.json`：amd64／arm64 normal-enforce；`r3-budget-amd64.json`、`r3-budget-arm64.json` | passed；exit 0，data_valid true，selected 2 |
| 同一真实测量、低 image policy 默认 warning | `r3-low-policy.json`：AMD64 image 上限比实测低 1 byte；`r3-budget-warning.json` | passed；exit 0，status warning |
| 同一低预算显式 enforce 失败 | `r3-budget-low-enforce.json` 与 CLI record | passed；exit 1，data_valid true，明确实测／上限 |
| 缺方法／wrong method／unit／缺归档／失败测量任何模式失败 | actual CLI 的 wrong-method 和 missing-archives cases；38 tests 的双模式负向 | passed；exit 1，不能 fallback engine.Size |
| v2 只读兼容、dirty／缺 policy／缺适用架构不能 enforce | actual v2 原文件 observation 0／enforce 1；missing-policy 1；38 tests 的 dirty/architecture cases | passed；旧报告与 policy SHA 运行前后相同 |
| 错 selector／policy／非有限／非正／bool／float／重复 fields 拒绝 | `r3-tests.log` 和 `r3-python312-tests.log`，38 tests each，OK，exit 0 | passed；读取有界、错误不回显秘密哨兵 |
| 时间／RSS 超观测预算、体积正常仍能 enforce 成功 | test `test_time_and_rss_warnings_do_not_become_size_enforcement_failures` | passed；3 observation warnings，exit 0 |
| 退出强制后仍要求有效测量 | actual `low-observation` 0 与 `rollback-retains-validity` 1；38 tests | passed；只撤 enforce flag 不绕过 source/archive/method 检查 |

命令入口：

```sh
python3 hack/bench_baseline.py --check <v3-result.json> --budgets hack/bench-budgets.json
python3 hack/bench_baseline.py --check <v3-result.json> --budgets hack/bench-budgets.json --enforce-budgets
# 可选 --out <new-dir> 保存独立 budget-result.json，不改写输入 baseline。
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s hack -p test_bench_baseline.py -v
```

CLI 原始 argv、exit code、输出 verdict 和 SHA 在 `r3-cli-evidence.json`；它明确标注
受控低 policy 与复制后 wrong-method／missing-artifact negatives，不伪装成真实采集失败。
无显式 policy 时仅验证测量和旧观测警告，status `not_evaluated`；没有适用架构或历史 v2
在观测模式是 `not_applicable`，不能记为预算通过。enforce 必须有明确 policy 和 clean v3。

旧 5 秒启动、250 MiB idle RSS 与 5 秒恢复后启动仍 warning；extism/nolua/extism-nolua
继续完整测量，没有体积强制。本项不覆盖 GoReleaser 全平台发布资产、CGO、吞吐或其他 storage profiles。
回滚灰度时，移除测量／checker 的 `--enforce-budgets`，保留 v3 有效性和 policy 观测；
原始报告、方法与历史 v2 原文件都保留。policy 变更必须另起 version 并复验，不自动 ratchet。

变更：baseline 的有界 source inventory／harness/profile 校验、policy/checker/结构化 audit 与
退出码；最近 CLI tests；versioned policy；本迭代 evidence 与领取状态。
风险面为 v2 reader、v3 归档与 source inventory、warning/enforce 结果和 cleanup 后 writer verdict，
均有实际命令／负向证据。生产 checkpoint、DLQ、sink ack 或 schema 未改变。

VERDICT: DELIVERABLE

## EV-3.4 CI 灰度结果

Run 46024237806 / frozen implementation 191683e 的 Resource Baseline job success；完整 36 jobs success。
正常 policy SHA `428718897d185407fb745dc9b58b85ec099373b5488acb8e853ba8cfae7ae7d0`，selected 2、enforce passed、exit 0。
同一实际 default image 147265024 bytes，对受控 limit 147265023 bytes：enforce failed/exit 1、data_valid true；
移除 enforce flag 后 observation warning/exit 0。三份原始 audit 和 `baseline-v3.json` 保存该结果。
时间/RSS 没有进入 blocking，Docker29.4.2 + native Podman5.7.0 conformance 实际 passed。
最终接受矩阵及原始 archive/candidate artifact 在 [acceptance.md](./acceptance.md)。

## Round 5 最新 source 复验

Source 8dfbf84 / run46032636182 36/36 success；相同 policy SHA42871889... 与 4-variant native v3、
actual dual-provider、normal enforce0/controlled-low enforce1/observation0 继续通过。
当前 baseline source为真实 merge c3c4d8b2...（parent8dfbf84），source inventory SHA7a6dab1d...；
它不会改写两个原始校准和 policy 值，default仍为67285176 binary /147265024 expanded image bytes。
共享 dev MySQL TCP health 的有界测试修正已实际关闭，原失败 run46029555087 保留。
