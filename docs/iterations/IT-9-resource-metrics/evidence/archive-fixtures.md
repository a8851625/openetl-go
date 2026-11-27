# EV-3.1 验收：归档度量与 v3 兼容

Round: 1/5；结果：`delivered`（仅本增量；真实 native baseline 和双引擎分别在后续增量验收）。

| 本增量验收 | 实际证据 | 结果／残余 |
| --- | --- | --- |
| Docker-save／OCI，raw／gzip，所选平台的配置、层字节、diff ID 一致 | `archive-fixtures.json`：四份 CLI report，每份两层、20,480 展开字节；`r1-parser-tests.log` | passed；fixture 不认证真实引擎 |
| 错 config、缺 blob、digest／descriptor size 错、越界／链接／重复、未知编码、有界读取、归档中途变化拒绝 | `python3 -m unittest discover -s hack -p test_bench_baseline.py -v`：25 tests，OK，exit 0 | passed；outer tar、raw/gzip 是当前支持范围 |
| v3 完整四 variant、归档重算、runtime binary identity；v2 只读、v1 拒绝 | 同一 test suite 的 `V3EvidenceTests`／`ResultTests`；Linux Python 3.12：25 tests，OK，exit 0，见 `r1-python312-tests.log` | passed at fixture/harness scope；实际 3-trial native 测量留给 EV-3.4 |
| 秘密哨兵、malformed JSON／gzip 错误无 payload；cleanup 失败不能成功 | 同一 test suite；image inspect 只持久化必要身份与 HEALTHCHECK，存在性检查只取 ID | passed；不上传运行时 env／私钥／备份 payload |
| 真实保存归档 parser 可读 | `r1-real-archive-metrics.json`；下方实际命令 exit 0 | passed at archive-parser scope；不是当前源码 runtime 认证 |

实际命令：

```sh
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest discover -s hack -p test_bench_baseline.py -v
podman run --rm -v "$PWD/hack:/tests:ro" -w /tests -e PYTHONDONTWRITEBYTECODE=1 \
  docker.io/library/python:3.12-alpine sh -c 'python3 --version && python3 -m unittest discover -s . -p test_bench_baseline.py -v'
python3 hack/bench_baseline.py --image-archive /private/tmp/openetl-p1-ga-candidate-898/candidate-image.tar \
  --image-config-digest sha256:5ce9794ad47c434b5b2dd61a045125929bfe2f8477f196d5ceb11bcdcc333f4e \
  --image-architecture amd64 --out /private/tmp/openetl-it9-r1-final-real-image
bash -n hack/bench-baseline.sh
git diff --check
```

归档来自 P1 source `898af75d1f90a954154d74eb53c3737f38e51b8f`，不是 IT-9 新源码构建。
实际归档 48,193,024 bytes；所选六层展开 147,790,336 bytes；方法 `uncompressed-layer-bytes/v1`。
完整 SHA 和层清单在 JSON 中。归档留在任务临时目录，不提交二进制生成物。

变更清单：`hack/bench_baseline.py`（流式计量、v3 writer/checker、sanitized inspect、cleanup 后校验）；
`hack/test_bench_baseline.py`（原有 12 tests 保留，新增内容／安全／兼容边界）；本迭代领取和证据文档。
风险面为保存格式、四变体报告、旧观测 reader 与恢复 harness；测试覆盖格式和 reader，实际应用验证仍待后续。
七份用户已有文件已按 `/private/tmp/openetl-it9-start-20261127.json` 的 SHA 原样核对。

VERDICT: DELIVERABLE
