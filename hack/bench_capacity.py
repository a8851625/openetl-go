#!/usr/bin/env python3
"""RA-8 real pipeline capacity observations; no product/runtime tuning.

Requires an actual bench-baseline --keep-images result. The production source
is verified before compiling the separate observed-server in its exact builder.
All services, data, credentials, ports and names belong to this run only.
"""

import argparse
import base64
import bisect
import json
import math
import os
from pathlib import Path
import platform
import secrets
import shutil
import ssl
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid

from bench_baseline import (BUILD_INPUTS, EXCLUDED_PARTS, MeasurementError, digest, image_identity, json_hash,
                            positive, require, validate_result, wait_ready, write_json)


IMAGES = {
    "mysql": "docker.io/library/mysql:8.0",
    "postgres": "docker.io/library/postgres:16-alpine",
    "broker": "docker.io/redpandadata/redpanda:v24.1.1",
    "objects": "quay.io/minio/minio:RELEASE.2024-07-16T23-46-41Z",
    "olap": "docker.io/clickhouse/clickhouse-server:24.3-alpine",
}
MIB = 1024 * 1024
PATHS = ["mysql_cdc__mysql_upsert", "mysql_batch__clickhouse_native", "kafka__s3"]


def percentile(values, p):
    require(bool(values), "empty latency distribution")
    return sorted(values)[max(0, math.ceil(len(values) * p) - 1)]


def source_position(position):
    return position["source"] if position.get("version") == 1 else position


def kafka_prefix(position):
    if position is None:
        return 0
    source = source_position(position)
    offsets = source["offsets"]
    require(set(offsets) == {"0"} and type(offsets["0"]) is int and offsets["0"] >= 0,
            "checkpoint is not a valid single-partition committed offset")
    return offsets["0"] + 1


def acknowledged_at(producer, ns):
    acks = producer["acks"]
    index = bisect.bisect_right([a["time_unix_ns"] for a in acks], ns) - 1
    return acks[index]["rows"] if index >= 0 else 0


def summarize_observation(raw):
    require(raw["schema_version"] == 1, "unknown observation version")
    elapsed = positive(raw["elapsed_seconds"], "observation elapsed")
    start, end, samples = raw["start"], raw["end"], raw["samples"]
    require(end["time_unix_ns"] > start["time_unix_ns"], "invalid observation clock")
    require(abs((end["time_unix_ns"]-start["time_unix_ns"])/1e9-elapsed) < max(.25, elapsed*.01),
            "wall/monotonic clocks diverged during observation")
    checkpoints = raw["checkpoints"]
    require(checkpoints and not any(c["failed"] for c in checkpoints), "missing/failed real checkpoints")
    for c in checkpoints:
        positive(c["duration_ns"], "checkpoint duration")
        require(start["time_unix_ns"] <= c["start_unix_ns"] <= c["end_unix_ns"] <= end["time_unix_ns"],
                "checkpoint outside observation window")
    require(samples, "missing process samples")
    for s in [start, end, *samples]:
        positive(s["rss_bytes"], "RSS")
        require(s["peak_rss_bytes"] >= s["rss_bytes"], "RSS peak below current value")
    waits = end["pool"]["wait_count"] - start["pool"]["wait_count"]
    wait_ns = end["pool"]["wait_ns"] - start["pool"]["wait_ns"]
    require(waits >= 0 and wait_ns >= 0, "pool counters went backwards")
    latency = [c["duration_ns"] / 1e6 for c in checkpoints]
    cpu = sum(end[k] - start[k] for k in ("user_cpu_seconds", "system_cpu_seconds"))
    require(cpu >= 0, "CPU counters went backwards")
    return {
        "elapsed_seconds": elapsed, "checkpoint_count": len(checkpoints),
        "checkpoint_p50_ms": percentile(latency, .5), "checkpoint_p95_ms": percentile(latency, .95),
        "checkpoint_p99_ms": percentile(latency, .99), "checkpoint_max_ms": max(latency),
        "pool_max_open": end["pool"]["max_open"], "pool_wait_count": waits,
        "pool_wait_seconds": wait_ns / 1e9, "pool_mean_wait_ms": wait_ns / 1e6 / waits if waits else 0,
        "steady_rss_mib": statistics.median(s["rss_bytes"] for s in samples) / MIB,
        "peak_rss_mib": end["peak_rss_bytes"] / MIB,
        "mean_cpu_cores": cpu / elapsed,
        "cgroup_throttled_seconds": ((end["cgroup_cpu"]["throttled_usec"] - start["cgroup_cpu"]["throttled_usec"]) / 1e6
                                     if "throttled_usec" in start["cgroup_cpu"] and "throttled_usec" in end["cgroup_cpu"] else None),
        "observer_bookkeeping_seconds": raw["observer_ns"] / 1e9,
        "boundary_calls_excluded": raw["boundary_calls_excluded"],
    }


class Capacity:
    def __init__(self, args, private):
        self.args, self.private = args, private
        self.root, self.out = args.root.resolve(), args.out.resolve()
        self.prefix = "openetl-capacity-" + uuid.uuid4().hex[:12]
        self.containers, self.volumes = [], []
        self.network = None
        self.sequence = 0
        # go-mysql's COM_REGISTER_SLAVE report-password field is limited by
        # MySQL 8. A 24-character random fixture credential fits that protocol;
        # the observed 48-character failure is retained as a compatibility gap.
        self.password = secrets.token_hex(12)
        self.token = secrets.token_hex(24)
        self.key = base64.b64encode(secrets.token_bytes(32)).decode()
        self.secret_values = [self.password, self.token, self.key]
        self.fixture = private / "fixture"
        self.fixture.mkdir()
        self.result = {"schema_version": 1, "status": "running", "scope": "real-pipeline-capacity-observations",
                       "measured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "cases": [],
                       "parameters": {"trials": args.trials, "concurrency": args.counts, "backends": args.backends,
                                      "keep_going_after_failed_case": args.keep_going,
                                      "part": args.part, "window_seconds": args.seconds, "warmup_seconds": args.warmup,
                                      "offered_rows_per_second_per_pipeline": args.rate, "single_path_rows": args.rows,
                                      "batch_size_curve": 100, "batch_size_path": 500, "flush_interval_ms": 100,
                                      "checkpoint_interval_seconds": 1, "checkpoint_batches_trigger": 10,
                                      "backpressure_buffer": 1000, "sample_interval_seconds": 1,
                                      "app_cpus": 2, "app_memory_bytes": 1024*MIB, "gomaxprocs": 2,
                                      "producer_cpus": 1, "producer_memory_bytes": 512*MIB,
                                      "dependency_cpus_each": 1, "dependency_memory_mib": {"olap": 2048, "other": 1024},
                                      "drain_timeout_seconds": args.drain_timeout,
                                      "fixture": "id=1..N; grp=id%97; amount=id*7; payload=row-%09d- plus 48 x characters"},
                       "limitations": [
                           "Observed ETL server includes real scheduler/standalone worker, auth/TLS, audit, encryption and generation fencing.",
                           "Separate executable omits GoFrame UI/proxy; process RSS is not full packaged deployment RSS.",
                           "Checkpoint timer wraps real SaveCheckpoint. Pool waits include every operation on its database/sql writer pool.",
                           "Bookkeeping time is cumulative callback wall time (including mutex wait), not process CPU; no observer correction is subtracted.",
                           "Peak is kernel VmHWM since process start; steady RSS is median one-second observation samples.",
                           "Native ARM/shared-host observations do not certify AMD64 release or long-run production capacity.",
                           "Single fixed native ClickHouse path only; protocol/async/batch/flush specialty remains unapproved.",
                           "At-least-once semantics unchanged. No crash/replay or connector maturity certification is claimed by this benchmark.",
                           "RA-8 completion additionally requires a demonstrated SQLite queueing boundary and remaining IT-3 decisions/CI closeout.",
                           "Fixture database credentials have 24 random hex characters; MySQL CDC's 48-character report-password failure is outside these successful measurements and remains a recorded compatibility gap.",
                       ]}

    def safe(self, text):
        for value in self.secret_values:
            text = text.replace(value, "<private-fixture-secret>")
        return text

    def run(self, argv, *, input_text=None, check=True, timeout=180):
        self.sequence += 1
        name = f"command-{self.sequence:05d}.log"
        with (self.out / "commands.jsonl").open("a") as f:
            f.write(json.dumps({"sequence": self.sequence, "argv": self.safe(json.dumps(list(map(str, argv)))),
                                "log": name}) + "\n")
        proc = subprocess.run(list(map(str, argv)), cwd=self.root, input=input_text, text=True,
                              stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        (self.out / name).write_text(self.safe(proc.stdout + proc.stderr))
        require(not check or proc.returncode == 0, f"command exited {proc.returncode}; see {name}")
        return proc

    def cli(self, *argv, **kwargs):
        return self.run([self.args.container_cli, *argv], **kwargs).stdout

    def state(self, name):
        return json.loads(self.cli("inspect", "--format", "{{json .State}}", name))

    def create(self, suffix, image, options=(), command=()):
        name = self.prefix + "-" + suffix
        self.cli("create", "--name", name, *options, image, *command)
        self.containers.append(name)
        return name

    def remove(self, name):
        self.cli("rm", "-f", name)
        self.containers.remove(name)

    def save(self):
        write_json(self.out / "result.json", self.result)

    def prepare(self):
        baseline_path = self.args.baseline.resolve()
        baseline = json.loads(baseline_path.read_text())
        validate_result(baseline)
        files = json.loads((baseline_path.parent / "source-files.json").read_text())
        require(json_hash(files) == baseline["source"]["sha256"], "baseline source inventory fingerprint differs")
        current_files = self.run(["rg", "--files", "--hidden", "--no-ignore",
                                 *[arg for part in sorted(EXCLUDED_PARTS) for arg in ("-g", "!" + part)], *BUILD_INPUTS]).stdout
        current_paths = {str(Path(p)) for p in current_files.splitlines()
                         if not set(Path(p).parts) & EXCLUDED_PARTS and not Path(p).name.startswith(".env") and Path(p).name != ".DS_Store"}
        # Go package tests are not linked into either executable. Record their
        # documentation-only drift, while strictly rejecting ANY build-input drift.
        unlinked_test = lambda p: p.startswith("internal/") and p.endswith("_test.go")
        expected_paths = {e["path"] for e in files}
        require({p for p in current_paths if not unlinked_test(p)} == {p for p in expected_paths if not unlinked_test(p)},
                "production build-input inventory changed since retained builder")
        test_drift = []
        for entry in files:
            path = self.root / entry["path"]
            unchanged = path.is_file() and not path.is_symlink() and digest(path.read_bytes()) == entry["sha256"]
            if not unchanged and unlinked_test(entry["path"]):
                test_drift.append(entry["path"])
            else:
                require(unchanged, "source changed since retained builder: " + entry["path"])
        test_drift += sorted(p for p in current_paths-expected_paths if unlinked_test(p))
        self.runtime = baseline["images"]["default"]["tag"]
        self.builder = self.args.builder or self.runtime.rsplit(":", 1)[0] + ":builder"
        runtime = image_identity(json.loads(self.cli("image", "inspect", self.runtime))[0])
        builder = image_identity(json.loads(self.cli("image", "inspect", self.builder))[0])
        require(runtime["image_id"] == baseline["images"]["default"]["image_id"], "retained runtime image changed")
        require(builder["image_id"] == baseline["toolchain"]["builder"]["image_id"], "retained builder image changed")
        self.result.update({"source": baseline["source"], "runtime_image": runtime, "builder_image": builder,
                            "baseline_result_sha256": digest(baseline_path.read_bytes()), "nonlinked_test_drift": test_drift})
        write_json(self.out / "source-files.json", files)
        helper_files = [self.root / "hack/bench-capacity.sh", self.root / "hack/bench_capacity.py",
                        self.root / "hack/test_bench_capacity.py", *sorted((self.root / "hack/cmd/capacity-baseline").glob("*.go"))]
        self.result["harness_sha256"] = {str(p.relative_to(self.root)): digest(p.read_bytes()) for p in helper_files}
        info = json.loads(self.cli("info", "--format", "{{json .}}"))
        self.log_driver = "k8s-file" if "host" in info else "json-file"
        if "host" in info:
            h = info["host"]
            hardware = {"os": h["os"], "architecture": h["arch"], "cpus": h["cpus"], "memory_bytes": h["memTotal"],
                        "kernel": h["kernel"], "runtime_version": info["version"]["Version"]}
        else:
            hardware = {"os": info["OSType"], "architecture": {"aarch64": "arm64", "x86_64": "amd64"}.get(info["Architecture"], info["Architecture"]),
                        "cpus": info["NCPU"], "memory_bytes": info["MemTotal"], "kernel": info["KernelVersion"], "runtime_version": info["ServerVersion"]}
        hardware.update({"client_platform": platform.platform(), "other_running_containers": len(self.cli("ps", "-q").splitlines())})
        self.result["hardware"] = hardware
        require(runtime["architecture"] == hardware["architecture"], "runtime architecture differs")
        self.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2", "-subj", "/CN=localhost",
                  "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1", "-keyout", self.fixture / "tls.key", "-out", self.fixture / "tls.crt"])
        (self.fixture / "tls.key").chmod(0o600)
        (self.fixture / "config.yaml").write_text("etl:\n  enabled: true\nlogger:\n  level: warning\n  stdout: true\n")
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(
            context=ssl.create_default_context(cafile=str(self.fixture / "tls.crt"))))
        self.env = {"ETL_API_TOKEN": self.token, "ETL_SPEC_ENCRYPTION_KEY": self.key, "ETL_PROFILE": "production",
                    "ETL_INSECURE_DEV": "false", "ETL_RESTORE_STRICT": "true", "ETL_AUDIT_ENABLED": "true", "ETL_ROLE": "standalone",
                    "ETL_TLS_CERT": "/bench/tls.crt", "ETL_TLS_KEY": "/bench/tls.key", "GOMAXPROCS": "2",
                    "ETL_SCHEMAS_DIR": "/bench/data/schemas", "ETL_PLUGINS_DIR": "/bench/data/plugins",
                    "CAPACITY_MYSQL_DSN": f"root:{self.password}@tcp(mysql:3306)/capacity?parseTime=true",
                    "CAPACITY_PASSWORD": self.password, "CAPACITY_CLICKHOUSE_URL": "http://olap:8123"}
        self.compile()
        self.network = self.prefix + "-net"
        self.cli("network", "create", self.network)
        self.dependencies()
        self.save()

    def compile(self):
        print("Compiling observed ETL server in verified production builder", flush=True)
        container = self.create("build", self.builder, ["--entrypoint", "sh"], ["-c",
            "mkdir -p /app/hack/cmd/capacity-baseline && cp /tmp/capacity-baseline/*.go /app/hack/cmd/capacity-baseline/ && "
            "CGO_ENABLED=0 go test -count=1 -v ./hack/cmd/capacity-baseline && "
            "CGO_ENABLED=0 go build -p 1 -ldflags='-s -w' -o /tmp/capacity-probe ./hack/cmd/capacity-baseline && "
            "go version && go version -m /tmp/capacity-probe"])
        self.cli("cp", str(self.root / "hack/cmd/capacity-baseline"), container + ":/tmp/capacity-baseline")
        log = self.cli("start", "-a", container, timeout=1200)
        require(self.state(container)["ExitCode"] == 0, "helper compilation/tests failed")
        (self.out / "linux-build-tests.log").write_text(self.safe(log))
        self.cli("cp", container + ":/tmp/capacity-probe", str(self.fixture / "capacity-baseline"))
        self.result["probe_binary"] = {"sha256": digest((self.fixture / "capacity-baseline").read_bytes()),
                                       "bytes": (self.fixture / "capacity-baseline").stat().st_size,
                                       "go_version": self.cli("run", "--rm", "--entrypoint", "go", self.builder, "version").strip()}
        self.remove(container)

    def env_file(self, suffix, values):
        path = self.private / (suffix + ".env")
        path.write_text("".join(k + "=" + str(v) + "\n" for k, v in values.items()))
        path.chmod(0o600)
        return path

    def dependency(self, alias, env, command, mount):
        image = IMAGES[alias]
        identity = image_identity(json.loads(self.cli("image", "inspect", image))[0])
        self.result.setdefault("dependencies", {})[alias] = identity
        volume = self.prefix + "-" + alias + "-data"
        self.cli("volume", "create", volume)
        self.volumes.append(volume)
        options = ["--network", self.network, "--network-alias", alias, "--cpus", "1", "--memory",
                   "2g" if alias == "olap" else "1g", "--log-driver", self.log_driver,
                   "--env-file", str(self.env_file(alias, env)), "-v", volume + ":" + mount]
        if alias == "objects":
            options += ["-p", "127.0.0.1::9000"]
        name = self.create(alias, image, options, command)
        self.cli("start", name)
        return name

    def wait_dependency(self, name, argv, timeout=180):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            require(self.state(name)["Running"], "owned dependency exited")
            p = self.run([self.args.container_cli, "exec", name, *argv], check=False)
            if p.returncode == 0:
                return
            require("unknown flag:" not in p.stderr and p.returncode != 127, "invalid dependency readiness command")
            time.sleep(1)
        raise MeasurementError("dependency readiness timed out: " + name)

    def mysql(self, sql):
        return self.cli("exec", "-i", self.services["mysql"], "sh", "-c",
                        'exec mysql -uroot -p"$MYSQL_ROOT_PASSWORD" --batch --skip-column-names', input_text=sql)

    def postgres(self, sql):
        return self.cli("exec", "-i", self.services["postgres"], "psql", "-U", "capacity", "-d", "capacity", "-v", "ON_ERROR_STOP=1", input_text=sql)

    def clickhouse(self, sql):
        return self.cli("exec", "-i", self.services["olap"], "sh", "-c",
                        'exec clickhouse-client --user capacity --password "$CLICKHOUSE_PASSWORD" --multiquery', input_text=sql)

    def dependencies(self):
        self.services = {}
        if self.args.part != "curves" or "mysql" in self.args.backends:
            self.services["mysql"] = self.dependency("mysql", {"MYSQL_ROOT_PASSWORD": self.password, "MYSQL_ROOT_HOST": "%", "MYSQL_DATABASE": "capacity"},
                ["--server-id=641", "--log-bin=mysql-bin", "--binlog-format=ROW", "--binlog-row-image=FULL", "--gtid-mode=ON", "--enforce-gtid-consistency=ON"], "/var/lib/mysql")
            self.wait_dependency(self.services["mysql"], ["sh", "-c", 'mysqladmin ping -h127.0.0.1 -uroot -p"$MYSQL_ROOT_PASSWORD" --silent'])
            self.mysql("CREATE DATABASE capacity_target;")
        if self.args.part != "paths" and "postgres" in self.args.backends:
            self.services["postgres"] = self.dependency("postgres", {"POSTGRES_USER": "capacity", "POSTGRES_PASSWORD": self.password, "POSTGRES_DB": "capacity"}, [], "/var/lib/postgresql/data")
            self.wait_dependency(self.services["postgres"], ["pg_isready", "-h", "127.0.0.1", "-U", "capacity"])
        self.services["broker"] = self.dependency("broker", {}, ["redpanda", "start", "--overprovisioned", "--smp", "1", "--memory", "512M", "--reserve-memory", "0M", "--check=false", "--node-id", "0",
            "--kafka-addr", "PLAINTEXT://0.0.0.0:9092", "--advertise-kafka-addr", "PLAINTEXT://broker:9092", "--rpc-addr", "0.0.0.0:33145", "--advertise-rpc-addr", "broker:33145"], "/var/lib/redpanda/data")
        self.wait_dependency(self.services["broker"], ["rpk", "cluster", "health"])
        if self.args.part != "curves":
            self.services["objects"] = self.dependency("objects", {"MINIO_ROOT_USER": "capacity", "MINIO_ROOT_PASSWORD": self.password}, ["server", "/data", "--address", ":9000", "--console-address", ":9001"], "/data")
            address = self.cli("port", self.services["objects"], "9000/tcp").strip()
            deadline = time.monotonic() + 60
            while True:
                try:
                    with urllib.request.urlopen("http://" + address + "/minio/health/ready", timeout=2) as r:
                        if r.status == 200:
                            break
                except (OSError, urllib.error.URLError):
                    pass
                require(time.monotonic() < deadline, "MinIO readiness timed out")
                time.sleep(1)
            self.services["olap"] = self.dependency("olap", {"CLICKHOUSE_USER": "capacity", "CLICKHOUSE_PASSWORD": self.password, "CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"}, [], "/var/lib/clickhouse")
            self.wait_dependency(self.services["olap"], ["sh", "-c", 'clickhouse-client --user capacity --password "$CLICKHOUSE_PASSWORD" -q "SELECT 1"'])
            self.clickhouse("CREATE DATABASE capacity;")

    def volume(self, suffix):
        volume = self.prefix + "-" + suffix
        self.cli("volume", "create", volume)
        self.volumes.append(volume)
        self.cli("run", "--rm", "--user", "0", "-v", volume + ":/bench", "-v", str(self.fixture) + ":/fixture:ro",
                 "--entrypoint", "sh", self.runtime, "-c",
                 "cp /fixture/* /bench/ && chmod 755 /bench/capacity-baseline && mkdir -p /bench/data /bench/pipes /bench/output && chown -R 1001:1001 /bench")
        return volume

    def request(self, base, path, data=None):
        req = urllib.request.Request(base + path, headers={"X-API-Token": self.token, "Content-Type": "application/json"},
                                     data=json.dumps(data).encode() if data is not None else None)
        try:
            with self.http.open(req, timeout=30) as r:
                return json.loads(r.read())
        except urllib.error.HTTPError as exc:
            body = self.safe(exc.read().decode(errors="replace"))
            (self.out / "http-failure.json").write_text(json.dumps({"path": path, "code": exc.code, "body": body}))
            raise MeasurementError(f"HTTP {exc.code} at {path}; see http-failure.json") from exc

    def start_server(self, case):
        backend, key = case["backend"], case["id"].replace("-", "_")
        env = dict(self.env, ETL_STORAGE_TYPE="postgresql" if backend == "postgres" else backend)
        if backend == "mysql":
            self.mysql("CREATE DATABASE `" + key + "`;")
            env["ETL_STORAGE_DSN"] = f"root:{self.password}@tcp(mysql:3306)/{key}?parseTime=true"
        elif backend == "postgres":
            self.postgres('CREATE DATABASE "' + key + '";')
            env["ETL_STORAGE_DSN"] = f"postgres://capacity:{self.password}@postgres:5432/{key}?sslmode=disable"
        case["env_path"] = self.env_file(case["id"], env)
        case["volume"] = self.volume(case["id"])
        options = ["--network", self.network, "--cpus", "2", "--memory", "1g", "--log-driver", self.log_driver,
                   "--no-healthcheck", "--env-file", str(case["env_path"]), "-v", case["volume"] + ":/bench",
                   "-p", "127.0.0.1::8001", "--entrypoint", "/bench/capacity-baseline"]
        name = self.create(case["id"], self.runtime, options, ["serve", "--backend", backend])
        case["container"] = name
        self.cli("start", name)
        address = self.cli("port", name, "8001/tcp").strip()
        require(address.startswith("127.0.0.1:") and "\n" not in address, "missing isolated API port")
        base = "https://" + address
        wait_ready(lambda: (200, self.request(base, "/api/v2/health")), lambda: self.state(name)["Running"], 90)
        require(self.cli("exec", name, "id", "-u").strip() == "1001", "wrong process user")
        return base

    def helper(self, case, mode, flags, *, detached=False):
        if mode.startswith("verify-"):
            # Read-only verification runs after the measurement window. Reuse
            # the owned namespace; avoid starting one container per output.
            output = self.cli("exec", "-e", "GOMAXPROCS=1", case["container"], "/bench/capacity-baseline", mode,
                              *map(str, flags), timeout=240)
            return json.loads(output)
        options = ["--network", self.network, "--cpus", "1", "--memory", "512m", "--log-driver", self.log_driver,
                   "--no-healthcheck", "--env-file", str(case["env_path"]), "-e", "GOMAXPROCS=1",
                   "-v", case["volume"] + ":/bench", "--entrypoint", "/bench/capacity-baseline"]
        suffix = case["id"] + "-helper-" + str(self.sequence)
        name = self.create(suffix, self.runtime, options, [mode, *map(str, flags)])
        if detached:
            self.cli("start", name)
            return name
        output = self.cli("start", "-a", name, timeout=240)
        require(self.state(name)["ExitCode"] == 0, "fixture helper failed")
        self.remove(name)
        return json.loads(output)

    def read_container_json(self, case, filename, timeout=20):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            require(self.state(case["container"])["Running"], "observed server exited")
            text = self.cli("exec", case["container"], "cat", "/bench/" + filename, check=False)
            if text.strip():
                return json.loads(text)
            time.sleep(.05)
        raise MeasurementError("missing observation file: " + filename)

    def begin(self, case):
        case["dependencies_before"] = self.dependency_resources(case)
        self.cli("kill", "--signal", "USR2", case["container"])
        start = self.read_container_json(case, "window-start.json")
        case["window_start"] = start
        return start

    def end(self, case):
        self.cli("kill", "--signal", "USR1", case["container"])
        raw = self.read_container_json(case, "observation.json")
        write_json(self.out / (case["id"] + "-observation.json"), raw)
        case["observation_file"] = case["id"] + "-observation.json"
        case["measurement"] = summarize_observation(raw)
        case["dependencies_after"] = self.dependency_resources(case)
        return raw

    def dependency_resources(self, case):
        if case["kind"] == "curve":
            aliases = ["broker"] + ([case["backend"]] if case["backend"] != "sqlite" else [])
        else:
            aliases = {PATHS[0]: ["mysql"], PATHS[1]: ["mysql", "olap"], PATHS[2]: ["broker", "objects"]}[case["path"]]
        result = {}
        for alias in aliases:
            start = time.time_ns()
            proc = self.run([self.args.container_cli, "exec", self.services[alias], "cat", "/sys/fs/cgroup/cpu.stat",
                             "/sys/fs/cgroup/memory.current"], check=False)
            values = {}
            if proc.returncode == 0:
                for line in proc.stdout.splitlines():
                    fields = line.split()
                    if len(fields) == 2:
                        values[fields[0]] = int(fields[1])
                    elif len(fields) == 1:
                        values["memory_current_bytes"] = int(fields[0])
            result[alias] = {"status": "measured" if values else "unavailable", "start_unix_ns": start,
                             "end_unix_ns": time.time_ns(), "cgroup_v2": values}
        return result

    def metrics(self, base, ids):
        raw = self.request(base, "/api/v2/metrics")
        rows = [r for r in raw["pipelines"] if r["id"] in ids]
        require(len(rows) == len(ids), "missing pipeline metrics")
        require(all(r["records_failed"] == 0 and r["records_dlq"] == 0 and r["dlq_file_count"] == 0
                    and r["status"] != "failed" and not r.get("last_error") for r in rows), "pipeline reports failed/DLQ records or a source error")
        return rows

    def pipeline(self, base, name, source, sink, batch):
        unsafe_append = source["type"] == "kafka" and sink["type"] in ("file_sink", "s3")
        spec = {"name": name, "allow_unsafe": unsafe_append, "source": source, "sink": sink, "batch_size": batch,
                "flush_interval_ms": 100, "checkpoint_interval_sec": 1, "backpressure_buffer": 1000,
                "retry": {"max_attempts": 3, "initial_interval_ms": 100, "max_interval_ms": 1000}, "dlq": {"enable": True}}
        created = self.request(base, "/api/v2/pipelines", {"spec": spec})
        return created["id"]

    def start_pipeline(self, base, pipeline):
        self.request(base, "/api/v2/pipelines/" + pipeline + "/start", {})

    def kafka_source(self, topic, group):
        return {"type": "kafka", "config": {"brokers": ["broker:9092"], "topic": topic, "group_id": group,
                                              "format": "json", "initial_offset": "oldest", "on_parse_error": "dlq"}}

    def topic(self, topic):
        self.cli("exec", self.services["broker"], "rpk", "topic", "create", topic, "--partitions", "1", "--replicas", "1", "-X", "brokers=broker:9092")

    def wait_producer(self, name):
        deadline = time.monotonic() + self.args.drain_timeout
        while self.state(name)["Running"]:
            require(time.monotonic() < deadline, "producer did not finish")
            time.sleep(.2)
        require(self.state(name)["ExitCode"] == 0, "producer exited unsuccessfully")
        result = json.loads(self.cli("logs", name))
        self.remove(name)
        return result

    def drain_stop(self, case, base, ids, rows):
        deadline = time.monotonic() + self.args.drain_timeout
        while True:
            metrics = self.metrics(base, ids)
            require(all(m["records_written"] <= rows for m in metrics), "sink has excess records")
            if all(m["records_written"] == rows for m in metrics):
                case["drained"] = True
                break
            if time.monotonic() >= deadline:
                case["drained"] = False
                break
            time.sleep(.5)
        case["before_stop_metrics"] = metrics
        checkpoints = {}
        for pid in ids:
            self.request(base, "/api/v2/pipelines/" + pid + "/stop", {})
            cp = self.request(base, "/api/v2/pipelines/" + pid + "/checkpoint")["checkpoint"]
            require(cp is not None, "missing final checkpoint")
            checkpoints[pid] = cp
        case["final_checkpoints"] = checkpoints
        return checkpoints

    def curve(self, case):
        base = self.start_server(case)
        topic = case["id"]
        self.topic(topic)
        pairs = []
        for i in range(case["concurrency"]):
            name = f"pipeline-{i:03d}"
            pid = self.pipeline(base, name, self.kafka_source(topic, topic + "-" + name),
                                {"type": "file_sink", "config": {"output_dir": "/bench/output/" + name, "format": "jsonl"}}, 100)
            pairs.append((pid, name))
            self.start_pipeline(base, pid)
        ids = [p[0] for p in pairs]
        total = math.ceil((self.args.warmup + self.args.seconds + 5) * self.args.rate)
        publisher = self.helper(case, "publish-kafka", ["--topic", topic, "--rows", total, "--rate", self.args.rate], detached=True)
        time.sleep(self.args.warmup)
        case["start_metrics"] = self.metrics(base, ids)
        self.begin(case)
        time.sleep(self.args.seconds)
        require(self.state(publisher)["Running"], "publisher finished before the observation window")
        raw = self.end(case)
        require(self.args.seconds*.95 <= raw["elapsed_seconds"] <= self.args.seconds+2,
                "observation duration changed (suspend or stalled control)")
        case["end_metrics"] = self.metrics(base, ids)
        producer = self.wait_producer(publisher)
        require(producer["rows"] == total and producer["last_offset"] == total-1, "publisher content/offset incomplete")
        case["producer"] = producer
        final = self.drain_stop(case, base, ids, total)
        verified = []
        for pid, name in pairs:
            prefix = kafka_prefix(final[pid]["position"])
            require(prefix <= total, "checkpoint went beyond published input")
            v = self.helper(case, "verify-files", ["--table", name, "--rows", total, "--minimum", prefix])
            if case["drained"]:
                require(v["full"] and v["canonical_sha256"] == producer["canonical_sha256"] and prefix == total,
                        "drained pipeline/checkpoint content does not match publisher")
            verified.append(dict(v, pipeline_id=pid))
        case["verification"] = verified
        start_rows = {pid: kafka_prefix(raw["start_positions"].get(pid)) for pid in ids}
        end_rows = {pid: kafka_prefix(raw["end_positions"].get(pid)) for pid in ids}
        require(all(end_rows[p] > start_rows[p] for p in ids), "some pipeline made no committed progress")
        emitted_start = acknowledged_at(producer, raw["start"]["time_unix_ns"])
        emitted_end = acknowledged_at(producer, raw["end"]["time_unix_ns"])
        require(emitted_end > emitted_start, "no input published during observation")
        elapsed, n = raw["elapsed_seconds"], len(ids)
        committed = sum(end_rows[p] - start_rows[p] for p in ids)
        offered = (emitted_end - emitted_start)*n
        measurement = case["measurement"]
        measurement.update({"checkpointed_rows_per_second": committed/elapsed, "offered_rows_per_second": offered/elapsed,
                            "committed_progress_ratio": committed/offered,
                            "backlog_start_rows": emitted_start*n-sum(start_rows.values()),
                            "backlog_end_rows": emitted_end*n-sum(end_rows.values()),
                            "per_pipeline_committed_start": start_rows, "per_pipeline_committed_end": end_rows,
                            "source_acked_start": emitted_start, "source_acked_end": emitted_end})
        # A short-window observation criterion, explicitly not an unlimited capacity guarantee.
        measurement["sustained_offered_load"] = (case["drained"] and committed/offered >= .95 and
            offered/elapsed >= self.args.rate*n*.95 and measurement["backlog_end_rows"]-measurement["backlog_start_rows"] <= offered*.05)

    def path(self, case):
        base = self.start_server(case)
        kind, table = case["path"], case["id"].replace("-", "_")
        db_config = {"host": "mysql", "port": 3306, "user": "root", "password": self.password, "database": "capacity"}
        rows = self.args.rows
        seed = None
        if kind.startswith("mysql"):
            self.mysql(f"CREATE TABLE capacity.{table} (id BIGINT PRIMARY KEY, grp INT NOT NULL, amount BIGINT NOT NULL, payload VARCHAR(100) NOT NULL);")
        if kind == PATHS[0]:
            # MySQLSink uses Metadata.Table before config.table. Use an
            # independent database, matching the production CDC path contract.
            target = table
            self.mysql(f"CREATE TABLE capacity_target.{target} LIKE capacity.{table};")
            source = {"type": "mysql_cdc", "config": dict(db_config, server_id=12001+case["trial"], tables=[table])}
            sink = {"type": "mysql", "config": dict(db_config, database="capacity_target", table=target, pk_columns=["id"], batch_mode="upsert")}
        elif kind == PATHS[1]:
            seed = self.helper(case, "seed-mysql", ["--rows", rows, "--table", table])
            source = {"type": "mysql_batch", "config": dict(db_config, table=table, pk_column="id", limit=5000)}
            target = table
            self.clickhouse(f"CREATE TABLE capacity.{target} (id Int64, grp Int32, amount Int64, payload String) ENGINE=MergeTree ORDER BY id;")
            sink = {"type": "clickhouse", "config": {"host": "olap", "port": 9000, "user": "capacity", "password": self.password,
                                                       "database": "capacity", "table": target, "auto_create": False,
                                                       "version_mode": "append", "protocol": "native"}}
        else:
            self.topic(case["id"])
            source = self.kafka_source(case["id"], case["id"] + "-consumer")
            target = case["id"]
            sink = {"type": "s3", "config": {"endpoint": "http://objects:9000", "region": "us-east-1", "bucket": "capacity",
                                                "access_key": "capacity", "secret_key": self.password, "format": "jsonl", "prefix": target+"/"}}
        pid = self.pipeline(base, case["id"], source, sink, 500)
        if kind == PATHS[1]:
            self.begin(case)
            self.start_pipeline(base, pid)
        else:
            self.start_pipeline(base, pid)
            if kind == PATHS[0]:
                deadline = time.monotonic() + 30
                while int(self.mysql("SELECT COUNT(*) FROM information_schema.processlist WHERE COMMAND LIKE 'Binlog Dump%';").strip()) < 1:
                    require(time.monotonic() < deadline, "CDC reader never opened binlog stream")
                    time.sleep(.2)
            else:
                time.sleep(self.args.warmup)
            self.begin(case)
            publisher = self.helper(case, "seed-mysql" if kind == PATHS[0] else "publish-kafka",
                                    ["--rows", rows, "--table", table] if kind == PATHS[0] else ["--rows", rows, "--topic", case["id"]], detached=True)
        deadline = time.monotonic() + self.args.drain_timeout
        producer_checked = False
        while True:
            metrics = self.metrics(base, [pid])
            case["latest_metrics"] = metrics
            if seed is None and not producer_checked:
                state = self.state(publisher)
                if not state["Running"]:
                    require(state["ExitCode"] == 0, "publisher failed before pipeline completion")
                    producer_checked = True
            if metrics[0]["records_written"] >= rows:
                require(metrics[0]["records_written"] == rows, "excess path records")
                break
            require(time.monotonic() < deadline, "single pipeline did not drain")
            time.sleep(.25)
        raw = self.end(case)
        case["measurement"]["end_to_end_rows_per_second"] = rows/raw["elapsed_seconds"]
        case["producer"] = seed if seed else self.wait_producer(publisher)
        require(case["producer"]["rows"] == rows, "incomplete path publisher")
        final = self.drain_stop(case, base, [pid], rows)
        mode = "verify-mysql" if kind == PATHS[0] else "verify-clickhouse" if kind == PATHS[1] else "verify-s3"
        verify_flags = ["--rows", rows, "--table", target]
        if kind == PATHS[0]:
            verify_flags += ["--database", "capacity_target"]
        verification = self.helper(case, mode, verify_flags)
        require(verification["full"] and verification["canonical_sha256"] == case["producer"]["canonical_sha256"], "target differs from full input")
        if kind.startswith("mysql"):
            case["source_verification"] = self.helper(case, "verify-mysql", ["--rows", rows, "--table", table])
            require(case["source_verification"]["canonical_sha256"] == verification["canonical_sha256"], "source differs from target")
        else:
            require(kafka_prefix(final[pid]["position"]) == rows, "object path checkpoint incomplete")
        case["verification"] = [verification]

    def finish_case(self, case):
        container = case.get("container")
        if container in self.containers:
            if case["status"] == "failed" and "window_start" in case and "observation_file" not in case:
                try:
                    self.end(case)
                except Exception as exc:
                    case["failed_observation_error"] = self.safe(str(exc))
            state = self.state(container)
            case["container_state_before_cleanup"] = {k: state.get(k) for k in ("Running", "ExitCode", "OOMKilled", "Error")}
            if state["Running"]:
                self.cli("stop", "--time", "30", container)
            log = self.cli("logs", container, check=False)
            (self.out / (case["id"] + "-server.log")).write_text(self.safe(log))
            case["server_log"] = case["id"] + "-server.log"
            self.remove(container)
        # Helpers may outlive a failed case; remove only names in this case's namespace.
        for name in list(self.containers):
            if name.startswith(self.prefix + "-" + case["id"] + "-helper-"):
                filename = case["id"] + "-failed-helper.log"
                (self.out / filename).write_text(self.safe(self.cli("logs", name, check=False)))
                case["failed_helper_log"] = filename
                self.remove(name)
        volume = case.get("volume")
        if volume in self.volumes:
            self.cli("volume", "rm", volume)
            self.volumes.remove(volume)
        if "env_path" in case:
            case.pop("env_path")
        case.pop("volume", None)
        case.pop("container", None)

    def execute_case(self, case):
        print("Measuring " + case["id"], flush=True)
        case["status"] = "running"
        try:
            before = time.time()
            vm_time = int(self.cli("run", "--rm", "--entrypoint", "date", self.runtime, "+%s").strip())
            after = time.time()
            require(before-2 <= vm_time <= after+2, "VM clock is not synchronized; wait for normal host/VM time synchronization")
            case["clock_check"] = {"host_before_unix_seconds": before, "vm_unix_seconds": vm_time, "host_after_unix_seconds": after}
            if case["kind"] == "curve":
                self.curve(case)
            else:
                self.path(case)
            case["status"] = "passed"
        except Exception as exc:
            case.update(status="failed", error=self.safe(str(exc)))
            if not self.args.keep_going:
                raise
        finally:
            self.finish_case(case)
            self.result["cases"].append(case)
            write_json(self.out / (case["id"] + ".json"), case)
            self.save()
        if case["status"] != "passed":
            print("  FAILED: " + case["error"], flush=True)
            return
        m = case["measurement"]
        print(f"  PASS p95={m['checkpoint_p95_ms']:.3f} ms, RSS={m['steady_rss_mib']:.1f} MiB, pool waits={m['pool_wait_count']}", flush=True)

    def measure(self):
        if self.args.part != "curves":
            for trial in range(self.args.trials):
                for kind in PATHS:
                    self.execute_case({"id": f"path-{PATHS.index(kind)}-r{trial}", "kind": "path", "path": kind,
                                       "backend": "sqlite", "concurrency": 1, "trial": trial})
        if self.args.part != "paths":
            for trial in range(self.args.trials):
                # Rotate backend order across repeats to reduce fixed-order drift.
                backends = self.args.backends[trial % len(self.args.backends):] + self.args.backends[:trial % len(self.args.backends)]
                for n in self.args.counts:
                    for backend in backends:
                        self.execute_case({"id": f"curve-{backend}-n{n:03d}-r{trial}", "kind": "curve", "path": "kafka__file_sink",
                                           "backend": backend, "concurrency": n, "trial": trial})

    def cleanup(self):
        errors = []
        for name in list(reversed(self.containers)):
            try:
                if name in getattr(self, "services", {}).values():
                    (self.out / (name.rsplit("-", 1)[-1] + "-dependency.log")).write_text(self.safe(self.cli("logs", name, check=False)))
                self.remove(name)
            except Exception as exc:
                errors.append(str(exc))
        for volume in list(reversed(self.volumes)):
            try:
                self.cli("volume", "rm", volume)
                self.volumes.remove(volume)
            except Exception as exc:
                errors.append(str(exc))
        if self.network:
            try:
                self.cli("network", "rm", self.network)
                self.network = None
            except Exception as exc:
                errors.append(str(exc))
        self.result["cleanup"] = {"status": "failed" if errors else "passed", "errors": errors}
        require(not errors, "owned resource cleanup failed")


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    p.add_argument("--container-cli", default=os.environ.get("CONTAINER_CLI", "docker"))
    p.add_argument("--baseline", type=Path, required=True, help="bench-baseline --keep-images result.json")
    p.add_argument("--builder", help="retained builder tag; inferred from default runtime tag")
    p.add_argument("--out", type=Path, required=True)
    p.add_argument("--part", choices=["all", "paths", "curves"], default="all")
    p.add_argument("--trials", type=int, default=3)
    p.add_argument("--counts", default="1,2,4,8,16,32,64")
    p.add_argument("--backends", default="sqlite,mysql,postgres")
    p.add_argument("--seconds", type=float, default=20)
    p.add_argument("--warmup", type=float, default=5)
    p.add_argument("--rate", type=int, default=5000)
    p.add_argument("--rows", type=int, default=200000)
    p.add_argument("--drain-timeout", type=float, default=180)
    p.add_argument("--keep-going", action="store_true", help="record a failed case and continue independent cases; overall exit remains nonzero")
    args = p.parse_args()
    args.counts = [int(n) for n in args.counts.split(",")]
    args.backends = args.backends.split(",")
    require(args.trials > 0 and args.rows > 0 and args.rate > 0 and args.seconds >= 2 and args.warmup >= 0,
            "invalid measurement parameters")
    require(args.counts and min(args.counts) > 0 and len(set(args.counts)) == len(args.counts), "invalid concurrency list")
    require(args.backends and len(set(args.backends)) == len(args.backends) and set(args.backends) <= {"sqlite", "mysql", "postgres"}, "invalid backends")
    args.out = args.out.resolve()
    require(not args.out.exists() or not any(args.out.iterdir()), "output directory must be new/empty")
    args.out.mkdir(parents=True, exist_ok=True)
    code = 0
    with tempfile.TemporaryDirectory(prefix="openetl-capacity-private-") as directory:
        bench = Capacity(args, Path(directory))
        try:
            bench.prepare()
            bench.measure()
            bench.result["status"] = "passed" if bench.result["cases"] and all(c["status"] == "passed" for c in bench.result["cases"]) else "failed"
            code = 0 if bench.result["status"] == "passed" else 1
        except BaseException as exc:
            code = 1
            bench.result.update(status="failed", error=bench.safe(str(exc)))
            print("Capacity measurement failed: " + bench.safe(str(exc)), file=sys.stderr, flush=True)
        finally:
            try:
                bench.cleanup()
            except Exception as exc:
                code = 1
                bench.result.update(status="failed", cleanup_error=str(exc))
            bench.save()
    print(f"Result: {args.out / 'result.json'} ({bench.result['status']})", flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())
