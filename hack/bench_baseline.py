#!/usr/bin/env python3
"""RA-8 packaging, health-ready startup and offline restore measurements.

Standard-library-only harness. Run via bench-baseline.sh for shared container
selection. Builds use a private source snapshot and fresh packed UI. Results
cover the native container architecture and this scope, not all release paths.
"""

import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import re
import secrets
import shutil
import signal
import ssl
import statistics
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
import uuid


VARIANTS = {"default": "", "extism": "extism", "nolua": "nolua", "extism-nolua": "extism,nolua"}
BUILD_INPUTS = ["Dockerfile", ".dockerignore", "go.mod", "go.sum", "main.go",
                "internal", "resource", "manifest", "web", "hack/pack.sh", "hack/container-cli.sh"]
EXCLUDED_PARTS = {".git", "node_modules", "data", "logs", "log", ".pi", ".pi-subagents",
                  ".bench-out", "screenshots", "__pycache__"}
MIB = 1024 * 1024


class MeasurementError(RuntimeError):
    pass


def require(condition, message):
    if not condition:
        raise MeasurementError(message)


def digest(data):
    return hashlib.sha256(data).hexdigest()


def json_hash(value):
    return digest(json.dumps(value, sort_keys=True, separators=(",", ":")).encode())


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n")
    temporary.replace(path)


def positive(value, label):
    require(type(value) in (int, float) and math.isfinite(value) and value > 0,
            f"missing, non-finite or non-positive measurement: {label}")
    return value


def image_identity(raw):
    image_id = raw["Id"]
    if not image_id.startswith("sha256:"):
        image_id = "sha256:" + image_id
    return {"image_id": image_id, "manifest_digest": raw.get("Digest") or None,
            "repo_digests": raw.get("RepoDigests") or [], "bytes": raw["Size"],
            "os": raw["Os"], "architecture": raw["Architecture"],
            "healthcheck": raw.get("Config", {}).get("Healthcheck") or raw.get("Healthcheck")}


def wait_ready(probe, is_running, timeout, *, clock=time.monotonic, sleep=time.sleep):
    """Require both a live owned process and HTTP 200 with health.status=ok."""
    deadline = clock() + timeout
    while clock() < deadline:
        require(is_running(), "container exited before health readiness")
        try:
            status, body = probe()
            if status == 200 and isinstance(body, dict) and body.get("status") == "ok":
                require(is_running(), "container exited while checking health readiness")
                return body
        except (OSError, urllib.error.URLError, ValueError):
            pass
        sleep(0.1)
    raise MeasurementError("timed out waiting for live container with HTTP 200 and health.status=ok")


def parse_proc_status(raw):
    values = {}
    for key in ("VmRSS", "VmHWM"):
        match = re.search(rf"^{key}:\s+(\d+)\s+kB$", raw, re.MULTILINE)
        require(match is not None, f"/proc process measurement missing {key}")
        values[key] = positive(int(match[1]) * 1024, key)
    require(values["VmHWM"] >= values["VmRSS"], "VmHWM is below current RSS")
    return {"rss_bytes": values["VmRSS"], "peak_rss_bytes": values["VmHWM"]}


def validate_result(result):
    """Reject failed/missing data before applying warning-only budgets."""
    try:
        require(result["schema_version"] == 2 and result["status"] == "passed", "baseline is not passed v2")
        require(result["scope"] == "packaging-startup-restore", "unexpected baseline scope")
        require(re.fullmatch(r"[0-9a-f]{40}", result["source"]["commit"]) is not None, "missing source commit")
        require(re.fullmatch(r"[0-9a-f]{64}", result["source"]["sha256"]) is not None, "missing source fingerprint")
        for key in ("cpus", "memory_bytes"):
            positive(result["hardware"][key], "hardware." + key)
        require(result["hardware"]["os"] == "linux", "runtime hardware must describe Linux")
        require(result["toolchain"]["go_version"].startswith("go version go"), "missing actual Go toolchain")
        images = result["images"]
        require(set(images) == set(VARIANTS), "incomplete build-tag matrix")
        for variant, item in images.items():
            require(re.fullmatch(r"sha256:[0-9a-f]{64}", item["image_id"]) is not None, "missing image ID")
            require(item["os"] == "linux" and item["architecture"] == result["hardware"]["architecture"],
                    "image and runtime architecture differ")
            for key in ("bytes", "binary_bytes", "frontend_bytes", "packed_bytes"):
                positive(item[key], variant + "." + key)
            for key in ("binary_sha256", "frontend_sha256", "packed_sha256"):
                require(re.fullmatch(r"[0-9a-f]{64}", item[key]) is not None, "missing packaged content fingerprint")
        runtime = result["runtime"]
        require(runtime["image_id"] == images["default"]["image_id"], "measured runtime uses another image")
        require(runtime["uid"] == 1001 and runtime["tls_verified"] is True
                and runtime["profile"] == "production", "wrong production runtime profile")
        expected_trials = result["parameters"]["trials"]
        count = result["parameters"]["restore_pipelines"]
        require(type(expected_trials) is int and expected_trials >= 1, "missing trial count")
        require(type(count) is int and count >= 1, "missing restore dataset")
        require(len(runtime["cold_trials"]) == expected_trials, "incomplete cold-start trials")
        require(len(runtime["restore_trials"]) == expected_trials, "incomplete restore trials")
        for trial in runtime["cold_trials"]:
            positive(trial["start_to_health_seconds"], "cold start")
            require(trial["health"]["status"] == "ok", "cold start never became healthy")
            require(len(trial["idle_samples"]) >= 2, "idle measurement has insufficient samples")
            for sample in trial["idle_samples"]:
                positive(sample["rss_bytes"], "idle RSS")
                positive(sample["peak_rss_bytes"], "idle peak RSS")
        for trial in runtime["restore_trials"]:
            for key in ("start_to_health_seconds", "restore_seconds", "restore_peak_rss_bytes", "rss_bytes"):
                positive(trial[key], "restore." + key)
            require(trial["health"]["status"] == "ok" and trial["pipeline_count"] == count
                    and trial["checkpoint_count"] == count, "restored dataset is incomplete")
            require(trial["identity_sha256"] == runtime["fixture"]["identity_sha256"],
                    "restored API identity/checkpoint content differs")
            resource = trial["restore_resource"]
            require(resource["method"] == "wait4.rusage.ru_maxrss" and resource["platform"] == "linux"
                    and resource["native_rss_unit"] == "KiB" and resource["exit_code"] == 0,
                    "restore resource measurement has wrong method, units or exit code")
            require(resource["native_max_rss"] * 1024 == trial["restore_peak_rss_bytes"]
                    and resource["peak_rss_bytes"] == trial["restore_peak_rss_bytes"]
                    and resource["elapsed_seconds"] == trial["restore_seconds"], "restore resource conversion differs")
        positive(runtime["fixture"]["peak_rss_bytes"], "loaded fixture peak RSS")
        require(runtime["fixture"]["records_verified"] == count * result["parameters"]["records_per_pipeline"],
                "source/sink fixture not reconciled")
    except (KeyError, TypeError, AttributeError) as exc:
        raise MeasurementError(f"incomplete or malformed baseline: {exc}") from exc
    # Observation budgets, not percentage regressions against withdrawn numbers.
    measurements = {
        "default binary MiB": (images["default"]["binary_bytes"] / MIB, 80),
        "default image MiB": (images["default"]["bytes"] / MIB, 200),
        "median start-to-health seconds": (statistics.median(t["start_to_health_seconds"] for t in runtime["cold_trials"]), 5),
        "median idle RSS MiB": (statistics.median(s["rss_bytes"] for t in runtime["cold_trials"] for s in t["idle_samples"]) / MIB, 250),
        "median restored start-to-health seconds": (statistics.median(t["start_to_health_seconds"] for t in runtime["restore_trials"]), 5),
    }
    return [f"{name}: {value:.3f} exceeds observation budget {budget}"
            for name, (value, budget) in measurements.items() if value > budget]


class Baseline:
    def __init__(self, args, private):
        self.args, self.private = args, private
        self.root, self.out = args.root.resolve(), args.out.resolve()
        self.prefix = "openetl-baseline-" + uuid.uuid4().hex[:12]
        self.containers, self.volumes, self.image_tags = [], [], []
        self.sequence = 0
        self.http_sequence = 0
        self.token = secrets.token_hex(24)
        self.env_file = private / "runtime.env"
        self.env_file.write_text("ETL_API_TOKEN=" + self.token + "\nETL_SPEC_ENCRYPTION_KEY=" +
                                 base64.b64encode(secrets.token_bytes(32)).decode() + "\nGOMAXPROCS=2\n")
        self.env_file.chmod(0o600)
        self.context = private / "source"
        self.context.mkdir()
        self.fixture = private / "fixture"
        self.fixture.mkdir()
        self.result = {"schema_version": 2, "status": "running", "scope": "packaging-startup-restore",
                       "measured_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                       "parameters": {"trials": args.trials, "settle_seconds": args.settle_seconds,
                                      "idle_samples": args.idle_samples, "sample_interval_seconds": 1,
                                      "restore_pipelines": args.restore_pipelines, "records_per_pipeline": 100,
                                      "cpus": 2, "memory_bytes": 1024 * MIB, "gomaxprocs": 2},
                       "images": {}, "limitations": [
                           "Fresh process and SQLite volume, cached image/OS pages; not a host reboot cold start.",
                           "Start timer includes container start, port discovery and health polling overhead.",
                           "Runtime image uses Dockerfile, not a published GoReleaser release asset.",
                           "CGO/QuickJS is not published by .goreleaser.yml and is not measured here.",
                           "Loaded control-plane peak is not sustained pipeline throughput/concurrency evidence.",
                           "ClickHouse specialty scope is pending; no specialty benchmark is run."]}

    def run(self, args, *, timeout=120, logged=False, input_text=None, check=True):
        self.sequence += 1
        log = self.out / f"command-{self.sequence:03d}.log"
        with (self.out / "commands.jsonl").open("a") as index:
            index.write(json.dumps({"log": log.name, "argv": [str(a) for a in args]}) + "\n")
        if logged:
            with log.open("w") as output:
                proc = subprocess.run(args, cwd=self.root, text=True, input=input_text,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=timeout)
            data = ""
        else:
            proc = subprocess.run(args, cwd=self.root, text=True, input=input_text,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
            data = proc.stdout
            log.write_text(data + proc.stderr)
        require(not check or proc.returncode == 0, f"command failed ({proc.returncode}); see {log.name}")
        return data

    def cli(self, *args, **kwargs):
        return self.run([self.args.container_cli, *args], **kwargs)

    def inspect(self, kind, name):
        if kind == "container":
            # Full inspect includes the temporary token/encryption key in Env.
            # Persist only process state, which is all these checks consume.
            return {"State": json.loads(self.cli(kind, "inspect", "--format", "{{json .State}}", name))}
        return json.loads(self.cli(kind, "inspect", name))[0]

    def snapshot(self):
        globs = [arg for part in sorted(EXCLUDED_PARTS) for arg in ("-g", "!" + part)]
        raw = self.run(["rg", "--files", "--hidden", "--no-ignore", *globs, *BUILD_INPUTS])
        files = []
        for relative in sorted(set(raw.splitlines())):
            path = Path(relative)
            if set(path.parts) & EXCLUDED_PARTS or path.name.startswith(".env") or path.name == ".DS_Store":
                continue
            source = self.root / path
            require(not source.is_symlink(), f"build input is a symlink: {relative}")
            target = self.context / path
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source, target)
            files.append({"path": relative, "sha256": digest(target.read_bytes()),
                          "bytes": target.stat().st_size, "mode": target.stat().st_mode & 0o777})
        write_json(self.out / "source-files.json", files)
        self.result["source"] = {"commit": self.run(["git", "rev-parse", "HEAD"]).strip(),
                                  "dirty": bool(self.run(["git", "status", "--porcelain"]).strip()),
                                  "sha256": json_hash(files), "files": len(files)}
        self.result["harness_sha256"] = {name: digest((self.root / "hack" / name).read_bytes())
                                           for name in ("bench-baseline.sh", "bench_baseline.py",
                                                        "cmd/process-measure/main.go", "cmd/process-measure/main_test.go")}

    def hardware(self):
        info = json.loads(self.cli("info", "--format", "{{json .}}"))
        self.build_options = ["--pull=false"]
        self.log_driver = "k8s-file" if "host" in info else "json-file"
        self.result["parameters"]["container_log_driver"] = self.log_driver
        if "host" in info:  # Podman server, not macOS client hardware.
            # Podman's OCI default discards Dockerfile HEALTHCHECK. Preserve the
            # same image contract Docker uses, while probing health ourselves.
            self.build_options += ["--format", "docker"]
            host = info["host"]
            hardware = {"os": host["os"], "architecture": host["arch"], "cpus": host["cpus"],
                        "memory_bytes": host["memTotal"], "kernel": host["kernel"],
                        "runtime_version": info["version"]["Version"]}
        else:
            architecture = {"x86_64": "amd64", "aarch64": "arm64"}.get(info["Architecture"], info["Architecture"])
            hardware = {"os": info["OSType"], "architecture": architecture, "cpus": info["NCPU"],
                        "memory_bytes": info["MemTotal"], "kernel": info["KernelVersion"],
                        "runtime_version": info["ServerVersion"]}
        hardware["client_platform"] = platform.platform()
        hardware["other_running_containers"] = len(self.cli("ps", "-q").splitlines())
        self.result["hardware"] = hardware
        self.result["base_images"] = {}
        for tag in ("docker.io/library/node:20-alpine", "docker.io/library/golang:1.24-alpine", "docker.io/library/alpine:3.19"):
            present = self.cli("image", "inspect", tag, check=False)
            if not present.strip() or json.loads(present) == []:
                self.cli("pull", tag, logged=True, timeout=1200)
            self.result["base_images"][tag] = image_identity(self.inspect("image", tag))

    def create(self, suffix, image, options=(), command=()):
        name = self.prefix + "-" + suffix
        self.containers.append(name)
        self.cli("create", "--name", name, *options, image, *command)
        return name

    def remove_container(self, name):
        self.cli("rm", "-f", name)
        self.containers.remove(name)

    def build(self):
        for variant, tags in VARIANTS.items():
            print(f"Building current source: {variant}", flush=True)
            tag = f"localhost/{self.prefix}:{variant}"
            self.image_tags.append(tag)
            self.cli("build", *self.build_options, "--build-arg", f"GO_BUILD_TAGS={tags}",
                     "-t", tag, str(self.context), logged=True, timeout=3600)
            raw_image = self.inspect("image", tag)
            item = image_identity(raw_image)
            require((item["healthcheck"] or {}).get("Test"), "Dockerfile HEALTHCHECK was lost")
            item.update({"tag": tag, "build_tags": tags, "cgo_enabled": False})
            container = self.create("extract-" + variant, tag)
            binary = self.private / (variant + "-main")
            public = self.private / (variant + "-public")
            self.cli("cp", container + ":/app/main", str(binary))
            self.cli("cp", container + ":/app/resource/public", str(public))
            entries = [{"path": str(p.relative_to(public)), "sha256": digest(p.read_bytes()), "bytes": p.stat().st_size}
                       for p in sorted(public.rglob("*")) if p.is_file()]
            require(any(entry["path"] == "index.html" for entry in entries), "fresh UI index not packaged")
            item.update({"binary_bytes": binary.stat().st_size, "binary_sha256": digest(binary.read_bytes()),
                         "frontend_bytes": sum(e["bytes"] for e in entries), "frontend_sha256": json_hash(entries)})
            self.remove_container(container)
            self.result["images"][variant] = item
        builder = f"localhost/{self.prefix}:builder"
        self.image_tags.append(builder)
        self.cli("build", *self.build_options, "--target", "builder", "-t", builder, str(self.context),
                 logged=True, timeout=3600)
        self.result["toolchain"] = {
            "builder": image_identity(self.inspect("image", builder)),
            "go_version": self.cli("run", "--rm", "--entrypoint", "go", builder, "version").strip(),
            "binary_build_info": self.cli("run", "--rm", "--entrypoint", "go", builder, "version", "-m", "/app/main"),
            "pack_recipe": "Dockerfile: SKIP_UI=1 hack/pack.sh; gf URL/output are in build logs",
            "gf_default_download_version": re.search(r"github.com/gogf/gf/v2\s+(\S+)",
                                                       (self.context / "go.mod").read_text())[1],
        }
        packed_container = self.create("packed", builder)
        packed = self.private / "packed.go"
        self.cli("cp", packed_container + ":/app/internal/packed/packed.go", str(packed))
        require(b"gres.Add" in packed.read_bytes(), "frontend was not embedded")
        for item in self.result["images"].values():
            item.update({"packed_bytes": packed.stat().st_size, "packed_sha256": digest(packed.read_bytes())})
        self.remove_container(packed_container)
        # Compile a separate measurement tool; it is never linked into the app.
        # BusyBox time versions differ in MaxRSS conversion, so use wait4 units
        # directly and retain the native value alongside converted bytes.
        meter = self.create("process-measure", builder, ["--entrypoint", "sh"], ["-c",
                            "CGO_ENABLED=0 go test -count=1 -v /tmp/measure.go /tmp/measure_test.go && "
                            "CGO_ENABLED=0 go build -ldflags='-s -w' -o /tmp/process-measure /tmp/measure.go"])
        for source, target in (("main.go", "measure.go"), ("main_test.go", "measure_test.go")):
            self.cli("cp", str(self.root / "hack/cmd/process-measure" / source), meter + ":/tmp/" + target)
        self.cli("start", "-a", meter, logged=True, timeout=180)
        self.cli("cp", meter + ":/tmp/process-measure", str(self.fixture / "process-measure"))
        self.result["toolchain"]["process_measure"] = {
            "source_sha256": digest((self.root / "hack/cmd/process-measure/main.go").read_bytes()),
            "binary_sha256": digest((self.fixture / "process-measure").read_bytes()),
            "method": "wait4.rusage.ru_maxrss", "linux_native_rss_unit": "KiB"}
        self.remove_container(meter)

    def prepare_fixture(self):
        self.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "2",
                  "-subj", "/CN=localhost", "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
                  "-keyout", str(self.fixture / "tls.key"), "-out", str(self.fixture / "tls.crt")])
        self.fixture.joinpath("tls.key").chmod(0o600)
        self.fixture.joinpath("config.yaml").write_text("etl:\n  enabled: true\nlogger:\n  level: warning\n  stdout: true\n")
        self.expected_rows = [{"id": i, "payload": "baseline-" + str(i)} for i in range(100)]
        self.fixture.joinpath("input.jsonl").write_text("".join(json.dumps(r) + "\n" for r in self.expected_rows))
        tls = ssl.create_default_context(cafile=str(self.fixture / "tls.crt"))
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=tls))

    def volume(self, suffix, backup=None):
        name = self.prefix + "-" + suffix
        self.volumes.append(name)
        self.cli("volume", "create", name)
        if backup:
            shutil.copy2(backup, self.fixture / "backup.json")
        # Only setup uses root; measured app/maintenance use image USER etl.
        self.cli("run", "--rm", "--user", "0", "-v", name + ":/bench",
                 "-v", str(self.fixture) + ":/fixture:ro", "--entrypoint", "sh", self.default_image,
                 "-c", "cp /fixture/* /bench/ && mkdir -p /bench/pipes /bench/data /bench/output /bench/logs && chown -R 1001:1001 /bench")
        return name

    def common_options(self, volume):
        return ["--cpus", "2", "--memory", "1g", "--log-driver", self.log_driver,
                "--env-file", str(self.env_file), "-v", volume + ":/bench"]

    def common_flags(self):
        return ["--config", "/bench/config.yaml", "--profile", "production", "--insecure-dev=false",
                "--audit-enabled=true", "--restore-strict=true", "--role", "standalone",
                "--data-dir", "/bench/data", "--sqlite-path", "/bench/data/etl.db", "--storage", "sqlite",
                "--specs-dir", "/bench/pipes", "--log-dir", "/bench/logs", "--host", "0.0.0.0", "--port", "8000",
                "--etl-api-host", "0.0.0.0", "--etl-api-port", "8001", "--tls-cert", "/bench/tls.crt",
                "--tls-key", "/bench/tls.key", "--tls-server-name", "localhost"]

    def request(self, base, path, data=None, *, raw=False):
        request = urllib.request.Request(base + path, headers={"X-API-Token": self.token, "Content-Type": "application/json"},
                                         data=None if data is None else json.dumps(data).encode())
        self.http_sequence += 1
        diagnostic = self.out / f"http-{self.http_sequence:03d}.json"
        try:
            with self.http.open(request, timeout=2) as response:
                body = response.read()
                if not raw:
                    # Fixture response only; never persist auth headers or keys.
                    write_json(diagnostic, {"path": path, "status": response.status, "body": body.decode()})
                return response.status, body if raw else json.loads(body)
        except urllib.error.HTTPError as exc:
            write_json(diagnostic, {"path": path, "status": exc.code, "body": exc.read().decode(errors="replace")})
            raise

    def running(self, container):
        return self.inspect("container", container)["State"]["Running"] is True

    def memory(self, container):
        return parse_proc_status(self.cli("exec", container, "cat", "/proc/1/status"))

    def start(self, volume, suffix):
        container = self.create(suffix, self.default_image,
                                [*self.common_options(volume), "-p", "127.0.0.1::8000", "--entrypoint", "/app/main"],
                                self.common_flags())
        started = time.monotonic()
        self.cli("start", container)
        ports = self.cli("port", container, "8000/tcp").strip().splitlines()
        require(len(ports) == 1 and ports[0].startswith("127.0.0.1:"), "missing isolated loopback port")
        base = "https://" + ports[0]
        health = wait_ready(lambda: self.request(base, "/api/v2/health"), lambda: self.running(container), self.args.ready_timeout)
        elapsed = time.monotonic() - started
        status, page = self.request(base, "/", raw=True)
        require(status == 200 and b"<html" in page.lower(), "packaged UI did not load")
        require(self.cli("exec", container, "id", "-u").strip() == "1001", "runtime is not USER etl")
        return container, base, {"start_to_health_seconds": elapsed, "health": health,
                                 "started_at": self.inspect("container", container)["State"]["StartedAt"]}

    def state(self, base):
        _, listing = self.request(base, "/api/v2/pipelines")
        state = []
        for row in sorted(listing["pipelines"], key=lambda r: r["id"]):
            reference = "/api/v2/pipelines/" + row["id"]
            _, detail = self.request(base, reference)
            _, checkpoint = self.request(base, reference + "/checkpoint")
            require(detail["desired_state"] == "stopped", "fixture desired state is not stopped")
            require(checkpoint["checkpoint"] is not None, "fixture checkpoint is missing")
            cp = checkpoint["checkpoint"]
            position = cp["position"]
            require(position.get("version") == 1 and isinstance(position.get("source"), dict)
                    and position.get("delivery_mode") == "at_least_once", "fixture checkpoint envelope differs")
            require(position["source"].get("offset") == len(self.expected_rows), "fixture did not checkpoint every input record")
            state.append({"id": row["id"], "name": row["name"], "desired_state": detail["desired_state"],
                          "generation": detail["generation"], "checkpoint": cp})
        require(len(state) == self.args.restore_pipelines, "fixture pipeline count differs")
        return state

    def seed(self, container, base):
        for i in range(self.args.restore_pipelines):
            name = f"restore-{i:03d}"
            spec = {"name": name, "source": {"type": "file", "config": {"path": "/bench/input.jsonl", "format": "json"}},
                    "sink": {"type": "file_sink", "config": {"output_dir": "/bench/output/" + name, "format": "jsonl"}},
                    "batch_size": 100, "checkpoint_interval_sec": 1, "backpressure_buffer": 100,
                    "flush_interval_ms": 100, "dlq": {"enable": True}}
            _, created = self.request(base, "/api/v2/pipelines", {"spec": spec})
            reference = "/api/v2/pipelines/" + created["id"]
            self.request(base, reference + "/start", {})
            deadline = time.monotonic() + 30
            while True:
                _, metrics = self.request(base, "/api/v2/metrics")
                row = next(r for r in metrics["pipelines"] if r["id"] == created["id"])
                require(row["records_failed"] == 0 and row["records_dlq"] == 0, "fixture data path failed")
                if row["records_written"] == len(self.expected_rows):
                    break
                require(time.monotonic() < deadline, "fixture data path did not complete")
                time.sleep(0.1)
            self.request(base, reference + "/stop", {})
            copied = self.private / (name + "-output")
            self.cli("cp", container + ":/bench/output/" + name, str(copied))
            # FileSink uses a UTC date hierarchy, not flat output filenames.
            rows = [json.loads(line) for path in copied.rglob("*.jsonl") for line in path.read_text().splitlines()]
            require(sorted(rows, key=lambda r: r["id"]) == self.expected_rows, "file sink content differs from fixture input")
        return self.state(base)

    def maintenance(self, volume, suffix, flag):
        timing_file = "/bench/" + suffix + "-resource.json"
        container = self.create(suffix, self.default_image,
                                [*self.common_options(volume), "--entrypoint", "/bench/process-measure"],
                                ["--output", timing_file, "--", "/app/main", *self.common_flags(), flag, "/bench/backup.json"])
        started = time.monotonic()
        self.cli("start", "-a", container, logged=True, timeout=180)
        wall = time.monotonic() - started
        info = self.inspect("container", container)
        require(info["State"]["ExitCode"] == 0, "offline maintenance did not succeed")
        timing = self.out / (suffix + "-resource.json")
        self.cli("cp", container + ":" + timing_file, str(timing))
        values = json.loads(timing.read_text())
        require(values["method"] == "wait4.rusage.ru_maxrss" and values["exit_code"] == 0
                and values["platform"] == "linux" and values["native_rss_unit"] == "KiB",
                "maintenance resource method/units differ")
        require(values["native_max_rss"] * 1024 == values["peak_rss_bytes"], "maintenance RSS unit conversion differs")
        metrics = {**values, "seconds": positive(values["elapsed_seconds"], "maintenance elapsed"),
                   "peak_rss_bytes": positive(values["peak_rss_bytes"], "maintenance peak RSS"),
                   "container_start_to_exit_seconds": wall}
        return container, metrics

    def measure(self):
        self.default_image = self.result["images"]["default"]["tag"]
        self.prepare_fixture()
        runtime = {"image_id": self.result["images"]["default"]["image_id"], "uid": 1001,
                   "profile": "production", "tls_verified": True, "cold_trials": [], "restore_trials": []}
        self.result["runtime"] = runtime
        for i in range(self.args.trials):
            print(f"Measuring health-ready startup and idle RSS: {i + 1}/{self.args.trials}", flush=True)
            volume = self.volume("cold-" + str(i))
            container, base, trial = self.start(volume, "app-" + str(i))
            require(self.request(base, "/api/v2/pipelines")[1]["pipelines"] == [], "cold trial has preexisting pipelines")
            time.sleep(self.args.settle_seconds)
            trial["idle_samples"] = []
            for _ in range(self.args.idle_samples):
                require(self.running(container), "idle process exited")
                trial["idle_samples"].append(self.memory(container))
                time.sleep(1)
            runtime["cold_trials"].append(trial)
            if i + 1 < self.args.trials:
                self.remove_container(container)
        print("Creating and reconciling the offline restore fixture through the API", flush=True)
        state = self.seed(container, base)
        runtime["fixture"] = {"identity_sha256": json_hash(state), "pipeline_count": len(state),
                               "records_verified": len(state) * len(self.expected_rows), **self.memory(container)}
        write_json(self.out / "fixture-state.json", state)
        self.cli("stop", "-t", "30", container)
        require(not self.running(container), "source writer is still running before backup")
        self.cli("logs", container, logged=True)
        backup_container, backup_metrics = self.maintenance(volume, "backup", "--backup-file")
        backup = self.private / "backup.json"
        self.cli("cp", backup_container + ":/bench/backup.json", str(backup))
        backup_json = json.loads(backup.read_text())
        runtime["backup"] = {**backup_metrics, "bytes": backup.stat().st_size,
                              "sha256": digest(backup.read_bytes()), "counts": backup_json["counts"]}
        for i in range(self.args.trials):
            print(f"Measuring offline restore and restored startup: {i + 1}/{self.args.trials}", flush=True)
            target = self.volume("restore-" + str(i), backup)
            _, restored = self.maintenance(target, "restore-cli-" + str(i), "--restore-file")
            app, url, trial = self.start(target, "restored-app-" + str(i))
            state_after = self.state(url)
            require(state_after == state, "restored pipeline identity/checkpoint differs")
            trial.update({"restore_seconds": restored["seconds"], "restore_peak_rss_bytes": restored["peak_rss_bytes"],
                          "restore_resource": restored,
                          "restore_container_seconds": restored["container_start_to_exit_seconds"],
                          "identity_sha256": json_hash(state_after), "pipeline_count": len(state_after),
                          "checkpoint_count": len(state_after), **self.memory(app)})
            runtime["restore_trials"].append(trial)
            self.cli("logs", app, logged=True)
            self.remove_container(app)

    def cleanup(self):
        failures = []
        for kind, names in (("container", list(reversed(self.containers))), ("volume", list(reversed(self.volumes)))):
            for name in names:
                try:
                    self.cli(kind, "rm", "-f", name)
                except (MeasurementError, subprocess.SubprocessError) as exc:
                    failures.append(str(exc))
        if not self.args.keep_images:
            for tag in reversed(self.image_tags):
                try:
                    self.cli("image", "rm", tag)
                except (MeasurementError, subprocess.SubprocessError) as exc:
                    failures.append(str(exc))
        self.result["cleanup"] = {"passed": not failures, "errors": failures, "images_retained": self.args.keep_images}
        require(not failures, "owned-resource cleanup failed; see result.json and command logs")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parent.parent)
    parser.add_argument("--container-cli", default=os.environ.get("CONTAINER_CLI", "docker"))
    parser.add_argument("--out", type=Path, default=None, help="new empty output directory")
    parser.add_argument("--trials", type=int, default=3)
    parser.add_argument("--settle-seconds", type=float, default=10)
    parser.add_argument("--idle-samples", type=int, default=5)
    parser.add_argument("--restore-pipelines", type=int, default=16)
    parser.add_argument("--ready-timeout", type=float, default=60)
    parser.add_argument("--keep-images", action="store_true", help="retain newly built tags for follow-up measurements")
    parser.add_argument("--check", type=Path, help="validate a result and print warning-only observation budgets")
    args = parser.parse_args()
    if args.check:
        try:
            warnings = validate_result(json.loads(args.check.read_text()))
        except (MeasurementError, ValueError, OSError) as exc:
            print(f"::error::{exc}", file=sys.stderr)
            return 1
        for warning in warnings:
            print("::warning::" + warning)
        print("Baseline measurements valid; observation budget warnings: " + str(len(warnings)))
        return 0
    if args.trials < 1 or args.idle_samples < 2 or args.restore_pipelines < 1 or args.ready_timeout <= 0 or args.settle_seconds < 0:
        parser.error("positive trials/pipelines/timeout, >=2 idle samples and nonnegative settling time are required")
    if args.out is None:
        args.out = Path(os.environ.get("BENCH_OUT_DIR", str(args.root / ".bench-out" / ("baseline-" + uuid.uuid4().hex[:12]))))
    require(not args.out.exists() or not any(args.out.iterdir()), "output directory must be new or empty")
    args.out.mkdir(parents=True, exist_ok=True, mode=0o700)
    def interrupted(signum, frame):
        raise MeasurementError(f"interrupted by signal {signum}")
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    with tempfile.TemporaryDirectory(prefix="openetl-baseline-") as temporary:
        bench = Baseline(args, Path(temporary))
        try:
            bench.snapshot()
            bench.hardware()
            bench.build()
            bench.measure()
            bench.result["status"] = "passed"
            bench.result["warnings"] = validate_result(bench.result)
        except Exception as exc:
            bench.result["status"] = "failed"
            bench.result["error"] = str(exc)
            print(f"Baseline failed: {exc}", file=sys.stderr, flush=True)
        finally:
            try:
                bench.cleanup()
            except Exception as exc:
                bench.result["status"] = "failed"
                bench.result["cleanup_error"] = str(exc)
            write_json(args.out / "result.json", bench.result)
    print(f"Baseline {bench.result['status']}: {args.out / 'result.json'}", flush=True)
    return 0 if bench.result["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
