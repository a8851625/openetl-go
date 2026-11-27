# EV-3.2 验收：真实双引擎对账

Round: 2/5；本增量结果：`delivered`。原 PR 聚合仍失败，留给 EV-3.4 的已记录前置；本项不把它计为 passed。

| 本增量验收 | 证据 | 结果／残余 |
| --- | --- | --- |
| 同一 default 归档经两个真实 provider 后内容／体积一致 | `r2-docker-image-metrics.json`、`r2-podman-image-metrics.json`、`r2-conformance.json`；CLI 重算两份实际归档，exit 0 | passed；image-only，未启动镜像内程序 |
| 真实 provider/version、原始诊断与归档 hash 保留 | Docker Engine 29.4.2，Linux/amd64 CI；Podman 5.8.4 remote API，Linux/arm64 VM，所选镜像仍 linux/amd64 | passed；归档格式／压缩量和 engine.Size 允许不同 |
| wrong config／CLI alias／内容变化／cleanup 负向 | `r2-alias-negative.json`、`r2-wrong-config.json` 实际 CLI exit 1；30 tests 与 owned-tag failure／archive mutation cases | passed；当前 CLI 修正的 native daemonless shape 已通过真实 VM version/info 检查和 tests，完整 native Podman CI probe 留给 EV-3.4 |
| 时间窗口、原始报告与任务资源边界 | report started/finished_at、harness SHA、cleanup；所有删除只指向独立 probe tag | passed；原始归档留在 below owned/artifact roots，未提交镜像生成物 |

实测命令（Docker 部分是首次 CI run 的真实步骤）：

```sh
CONTAINER_CLI=docker ./hack/e2e-bench-image-metrics.sh \
  --image-archive .bench-out/current/image-default.tar --image-config-digest "$IMAGE_CONFIG_DIGEST" \
  --out .bench-out/current/docker-probe
gh run download 46017295606 --name resource-baseline-46017295606 \
  --dir /private/tmp/openetl-it9-r2-ci-baseline
# 在干净 d2d7c1d checkout 运行同一 harness，保留原始 config 与相同 archive hash：
CONTAINER_CLI=podman ./hack/e2e-bench-image-metrics.sh \
  --image-archive /private/tmp/openetl-it9-r2-ci-baseline/.bench-out/current/image-default.tar \
  --image-config-digest "$IMAGE_CONFIG_DIGEST" --image-architecture amd64 \
  --out /private/tmp/openetl-it9-r2-podman-ci-image
python3 hack/bench_baseline.py --compare-image-probes \
  /private/tmp/openetl-it9-r2-ci-baseline/.bench-out/current/docker-probe/result.json \
  /private/tmp/openetl-it9-r2-podman-ci-image/result.json --out /private/tmp/openetl-it9-r2-conformance
```

comparison 输出 `Image-only conformance passed`，exit 0。所选六层共 **147,265,024 bytes**。
精确 config/hash/逐层字节以 `r2-conformance.json` 为准；完整原始归档在下载的 GitHub artifact
和 `/private/tmp/openetl-it9-r2-podman-ci-image/image.tar` 中可复核。

校准输入同时取得：干净 ARM64 source `d2d7c1d40dd8e124b97076ab306077426866d8e1`、
AMD64 CI merge source `653ce1712b2898f19ad130ca140abd3333179aa1`，其 parent 包含该 branch SHA，
二者 build-input fingerprint 相同：`1d8d2668f7d4bee84f1b194fda64f7b5ba8fcb0a1903608bd133d98e7de3245f`。
真实 4-variant / 3-cold / 3-restore / 15-RSS / 16×100 数据和 cleanup 均 passed。
`calibration-v3-amd64.json`、`baseline-v3-arm64.json` 与两份 source-files 为不可改写的校准输入；
这些平台的启动/RSS 数字不作性能排名。

首次 CI [46017295606](https://github.com/a8851625/openetl-go/actions/runs/46017295606) 有 36 jobs，
31 success；Resource Baseline 因 native Linux Podman client-only shape 失败，Doris source-order 因
MySQL 初始化 socket 切换窗口失败，三个 aggregate 因此失败。native baseline 和 Docker probe 都实际 passed。
失败与其余结果保存在 `r2-ci.json`；最终完整 source/candidate/Delivery Passed 仍是 EV-3.4 mandatory。

变更清单：baseline provider/transport/compare CLI、最近 30 tests、新单引擎 wrapper、
`_gate.yml` 的工具准备／probe／归档留存，以及本迭代证据。回归面为镜像 transport、
旧 v2 reader、native Podman 版本响应与 cleanup；真实 archive 和 native baseline 均已检查，当前新 CI 待最终收口。

VERDICT: DELIVERABLE
