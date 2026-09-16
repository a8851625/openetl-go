# Resource Baseline (P5)

Record measured baselines for release notes. Update when packaging, connectors,
or default resource limits change by more than the regression thresholds below.

## How to measure

```bash
CONTAINER_CLI=docker ./hack/bench-baseline.sh --out /tmp/openetl-baseline-run
python3 hack/bench_baseline.py --check /tmp/openetl-baseline-run/result.json
```

Use a new/empty output directory. The shared container selector also supports
`CONTAINER_CLI=podman`. Python 3, ripgrep, OpenSSL and the selected container
runtime are required; a host Go or Node installation is not required.

The script snapshots the current build inputs (including dirty implementation
files), builds the actual `Dockerfile` with fresh frontend assets, and measures
default/extism/nolua/extism+nolua binaries and images. `hack/pack.sh` downloads
the GoFrame CLI version declared in `go.mod` by default and logs its version/hash.
Podman uses Docker image format to retain the declared `HEALTHCHECK`.

Runtime parameters are explicit: native Linux container architecture, standalone
production profile, verified TLS, audit enabled, non-root UID 1001, SQLite,
2 CPU quota / `GOMAXPROCS=2`, and a 1 GiB memory limit. Three independent empty
volumes measure process startup; each waits 10 seconds after health readiness,
then samples `/proc/1/status` five times at one-second intervals. Startup measures
container start through the first HTTP 200 with JSON `status=ok`; it includes
container CLI/port discovery/polling overhead. Images and OS pages are cached;
this is not a machine-reboot cold start.

The restore fixture creates 16 pipelines through the API. Each writes and
reconciles 100 JSON records, is explicitly stopped, and retains its final
checkpoint. After stopping the source app, the real maintenance CLI exports a
backup and restores it into three new volumes. Each restored production process
must become healthy and return the same IDs, desired states, generations and
checkpoints. A separate Go `process-measure` tool records elapsed time and kernel
`wait4` peak RSS, retaining the native value and units (Linux KiB). A child
self-report checks the conversion. The local BusyBox `time` reported about four
times the `/proc` peak; schema v1 measurements using it were withdrawn and the
validator now requires v2. Loaded fixture `VmHWM` is a control-plane observation, not a
sustained concurrent-pipeline capacity measurement.

`result.json`, `source-files.json`, `fixture-state.json` and command logs are the
evidence. Temporary credentials, TLS private keys, backups and binaries remain
in private temporary storage. Only owned containers/volumes are removed. Use
`--keep-images` when a later measurement needs these exact newly built images.
The manifest distinguishes an image config ID from a registry/manifest digest;
local Docker builds may have no registry digest before publishing.

Throughput / checkpoint delay should be measured on the declared production path
(see `docs/path-contract.md`), not on synthetic no-op pipes alone.

## Current evidence — partial RA-8 baseline

The 2026-09-06 IT-3/T3.6 completion claim was withdrawn during revalidation.
There is currently no complete, reproducible release baseline that meets RA-8.
Historical numbers remain in the withdrawn iteration log for traceability; they
must not be used as certified capacity limits or release comparisons.

- The SQLite test used direct `SaveCheckpoint` calls (200 operations per worker),
  not streaming pipelines. It does not establish a pipeline-count threshold or
  a throughput ceiling. In particular, the claimed “more than 10 pipelines”
  recommendation was not supported by that experiment.
- The ClickHouse async setting was applied in another client's session. Subsequent
  inserts did not use it. The HTTP command required `curl`, absent from the named
  image; the reproduced command inserted zero rows while the script calculated
  throughput from the intended count. Both comparisons are invalid.
- Packaging/startup/idle observations lacked the full source fingerprint, image
  digest and hardware/data parameters required for a comparable release baseline.
  Three-backend pipeline curves, peak RSS, p99 and flush/concurrency coverage remain
  unverified. `--help` timing is not startup-to-health timing.

See [the revalidation record](./iterations/IT-3-integrity-capacity/revalidation-2026-09-06.md).
RA-5/RA-6 and the restore repair are delivered. RA-8/T3.6 is complete: Round 4
delivered packaging/startup/restore measurement (2026-09-09), and the T3.6-B
capacity run (2026-09-16, commit `ccc6df1`) closed real-path throughput and the
three-backend concurrency curves — see the capacity section below. The 2026-09-16
baseline rerun reproduces the Round 4 packaging numbers (default binary 60.38 MiB,
image 133.18 MiB).

The [Round 4 manifest](./evidence/it3-baseline-20260909/manifest.json) and
[raw result](./evidence/it3-baseline-20260909/result.json) bind these measurements to
commit `387daf07a3f38ea3f24b72060be16c16c59bcea5`, dirty build-input fingerprint
`ab64d73d7bab31cbfedf108bb81207a5abc31d7524c5df67eb028237f064229a` (479 files),
Go 1.24.13, GoFrame CLI 2.10.0, and the recorded dependency images. Hardware was a
Linux/arm64 Podman 5.8.4 VM with 5 vCPUs and 15.57 GiB RAM, on a macOS client;
20 unrelated containers were running. The measured app had the 2 CPU / 1 GiB
limits described above. This is native ARM evidence, not a linux/amd64 release
certification or a dedicated-host throughput comparison.

| Build tags (`CGO_ENABLED=0`) | Binary MiB | Image MiB |
| --- | --- | --- |
| default | 60.38 | 133.18 |
| extism | 63.75 | 139.93 |
| nolua | 59.63 | 131.68 |
| extism,nolua | 62.94 | 138.30 |

The default image manifest digest is
`sha256:b6cad625c71eb7187ac6abd244baed753bae0e6c1f14c248a865b5d1f09cd0ed`;
the manifest records every variant's full image ID/digest and binary hash. Fresh
frontend assets total 1,046.76 KiB in every variant. CGO/QuickJS is a separate
local build option, omitted from the published GoReleaser variants and this run.

| Runtime observation | Measured value |
| --- | --- |
| Empty process start to healthy JSON, 3 trials | median 0.394 s; range 0.394–0.396 s |
| Empty idle RSS, 15 samples | median 48.76 MiB; range 48.05–49.18 MiB |
| Peak RSS while creating/running/stopping the 16-pipeline fixture | 55.98 MiB (`VmHWM`) |
| Portable backup | 50.46 KiB; 16 pipelines/versions/checkpoints/runs, 48 audit rows, 1 worker |
| Offline restore child process, 3 trials | median 0.075 s; range 0.068–0.077 s |
| Offline restore peak RSS, 3 trials | 41.34–42.81 MiB (native `wait4`, unit verified) |
| Start to healthy JSON with restored state, 3 trials | median 0.395 s; range 0.381–0.413 s |
| Restored content | all 16 IDs, desired states, generations and full checkpoints equal |

This small restore fixture does not establish large-database RTO or memory use.
The separate [three-backend volume drill](./evidence/it3-backup-volume-20260908/manifest.json)
records the >100k-row cases on its own declared platform and toolchain.

| Required measurement | Current status |
| --- | --- |
| Binary/build-tag matrix and image digest/size | Passed for the native ARM Dockerfile matrix above (2026-09-16 rerun: default binary 60.38 MiB / image 133.18 MiB) |
| Cold and restore startup, idle/steady/peak RSS | Passed, including sustained pipeline RSS in the capacity run below |
| Real CDC/batch/Kafka pipeline throughput | Passed 2026-09-16: mysql_cdc→mysql upsert 49,218 rows/s end-to-end; mysql_batch→ClickHouse native 76,640 rows/s; kafka→S3 60,065 rows/s (single pipeline, 2 CPU / 1 GiB app container) |
| SQLite/MySQL/PostgreSQL concurrency with checkpoint p50/p95/p99 | Passed 2026-09-16: 1/4/8/16/32 concurrent pipelines × 3 backends, 20 s observation windows, all `sustained_offered_load=YES` |
| SQLite pipeline capacity/queueing boundary | Passed 2026-09-16: see the inflection analysis below — at 32 pipelines sqlite p95 checkpoint latency reaches 63 ms (p99 247 ms) with 890 acquisition waits; the write-queueing degradation is measurable and starts between 8 and 16 pipelines |
| Optional ClickHouse batch/flush/concurrency/native/HTTP/async profile | Decision recorded 2026-09-05 (T3.5): batch/async profile merged into this baseline; old invalid values replaced by the mysql_batch→ClickHouse native path measurement above |
| CI thresholds | Strict v2 JSON validation and warning budgets wired; full local run and 12 parser/failure tests passed, hosted Actions not run |

## Pipeline capacity curves (2026-09-16, T3.6-B)

Measured with `hack/bench-capacity.sh` against the retained baseline images of
commit `ccc6df1`. Same VM class as above (Linux/arm64 Podman 5.8.4, 5 vCPU,
15.57 GiB RAM); each app container 2 CPU / 1 GiB, `GOMAXPROCS=2`; dependencies
(MySQL 8, PostgreSQL, Redpanda, MinIO, ClickHouse for the path part) get 1 CPU /
1 GiB each (ClickHouse 2 GiB). Each curve point runs N kafka→file_sink pipelines
sharing one topic at 5,000 rows/s per pipeline, 20 s observation window after
5 s warmup, checkpoint_interval 1 s, batch 100, full content verified
(canonical SHA-256 of drained output equals the publisher's). All points hold
`sustained_offered_load` — committed progress ≥95% of offered with backlog
growth ≤5%.

Raw evidence: `docs/evidence/it3-baseline-20260916/` (`baseline-result.json`,
`capacity-result.json`, per-case files and server logs).

| Backend | Pipelines | Committed rows/s (sum) | Checkpoint p50/p95/p99 (ms) | Steady RSS (MiB) | Pool waits |
| --- | --- | --- | --- | --- | --- |
| sqlite | 1 | 5,014 | 1.7 / 3.2 / 18.7 | 59.8 | 2 |
| sqlite | 4 | 20,023 | 1.9 / 5.6 / 8.9 | 81.9 | 87 |
| sqlite | 8 | 39,984 | 1.4 / 6.5 / 13.7 | 108.2 | 198 |
| sqlite | 16 | 80,009 | 0.7 / 11.7 / 50.9 | 163.8 | 337 |
| sqlite | 32 | 160,075 | 0.7 / 63.4 / 247.3 | 310.8 | 890 |
| mysql | 1 | 4,989 | 1.8 / 3.6 / 11.2 | 58.7 | 0 |
| mysql | 4 | 20,022 | 2.5 / 6.6 / 9.4 | 80.3 | 0 |
| mysql | 8 | 39,952 | 3.7 / 11.6 / 15.0 | 106.9 | 0 |
| mysql | 16 | 79,997 | 7.1 / 21.1 / 39.4 | 169.1 | 0 |
| mysql | 32 | 160,625 | 21.9 / 91.0 / 133.5 | 310.7 | 60 |
| postgres | 1 | 5,026 | 0.8 / 1.5 / 6.4 | 59.7 | 0 |
| postgres | 4 | 19,968 | 1.0 / 4.7 / 8.4 | 81.2 | 0 |
| postgres | 8 | 40,008 | 1.4 / 8.7 / 12.1 | 113.9 | 0 |
| postgres | 16 | 80,113 | 3.2 / 13.7 / 18.4 | 161.9 | 0 |
| postgres | 32 | 160,180 | 11.0 / 41.9 / 67.1 | 291.2 | 0 |

**Reading the inflection.** At this offered load (5k rows/s per pipeline) all
three backends still keep up in aggregate — the table is a latency/resource
curve, not a hard ceiling. The distinguisher is tail latency: sqlite stays
comparable to mysql/postgres through 8 pipelines (p95 ≤ 6.5 ms) but its write
queueing becomes visible at 16 (p99 50.9 ms) and pronounced at 32 (p95 63 ms,
p99 247 ms, 890 acquisition waits). MySQL degrades earlier and smoother in p50
(21.9 ms at 32) because every checkpoint is a client round trip. PostgreSQL
keeps the flattest tails (p99 67 ms at 32). Under sustained multi-pipeline
streaming (>8 concurrent pipelines or when checkpoint p95 must stay <10 ms),
MySQL/PostgreSQL remain the recommended metadata backends; the earlier BUG-3
"single sqlite connection queues checkpoint writes" boundary is now backed by
this measured curve instead of a qualitative claim.

**Scope caveats.** Single trial per point (round-robin backend rotation), one
topic shared by all curve pipelines, file_sink (no sink-side bottleneck), TLS
on the API but plain internal transport, 1 GiB app memory limit. Numbers are
for cross-version comparison on the same hardware class, not universal
capacity guarantees; native ARM evidence, not a linux/amd64 release
certification.

## Frontend bundle

```bash
cd web && npm ci && npm run build
du -sh ../resource/public
```

Record total `resource/public` size. Fail release candidate review if size grows
>20% without an explicit exemption in the PR / release notes.

## Storage backend cost notes

| Backend | Idle overhead | Notes |
| --- | --- | --- |
| SQLite | lowest | Single node only; file backup |
| MySQL 8 | Separate service; measure with the workload | Production default in compose |
| PostgreSQL | Separate service; measure with the workload | Prefer when already standardized |

## Regression policy

1. Compare the same architecture and hardware class. The published release
   container targets linux/amd64; native ARM runs remain ARM evidence.
2. If binary/image exceeds threshold, either optimize or document exemption with owner.
3. If idle RSS exceeds threshold under empty load, investigate goroutine/leak before GA.
4. Path throughput regressions belong in reliability notes, not only this table.

The current CI observation budgets are default binary 80 MiB, default image
200 MiB, median start-to-health 5 s, median idle RSS 250 MiB, and median restored
start-to-health 5 s. These are initial absolute budgets, not percentage regressions
against the withdrawn measurements. Valid values over budget emit `::warning::`.
Missing/non-finite values, failed builds, failed health checks, incomplete restore
reconciliation or cleanup failures fail the job. The aggregate gate requires the
job to succeed. A measured version cycle of noise is still required before making
budget breaches blocking.

## Release notes template snippet

```text
Resource baseline (<commit>):
- binary: <size>
- image: <size> (<tag/digest>)
- start_to_health: <sec>
- idle_rss: <MiB>
- frontend_public: <size>
- storage matrix: sqlite=passed mysql=... postgres=...
```
