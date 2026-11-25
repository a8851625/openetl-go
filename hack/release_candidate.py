#!/usr/bin/env python3
"""Build once, certify preserved assets, and promote those same bytes."""
import argparse
import datetime as dt
import hashlib
import json
import os
from pathlib import Path, PurePosixPath
import platform
import re
import shutil
import stat
import struct
import subprocess
import sys
import tarfile
import uuid
import zipfile

ROOT = Path(__file__).resolve().parents[1]
MATRIX = (
    "hack/e2e.sh", "hack/e2e-http-source.sh", "hack/e2e-mysql-postgres.sh",
    "hack/e2e-cdc-mysql.sh", "hack/e2e-cdc-postgres.sh", "hack/e2e-snapshot-cdc.sh",
    "hack/e2e-clickhouse.sh", "hack/e2e-snapshot-cdc-clickhouse.sh",
    "hack/e2e-kafka.sh", "hack/e2e-kafka-raw-ods.sh",
    "hack/e2e-kafka-multitable-clickhouse.sh", "hack/e2e-debezium-mysql.sh",
    "hack/e2e-postgres-cdc.sh", "hack/e2e-s3-minio.sh",
    "hack/e2e-doris.sh", "hack/e2e-doris-source-order.sh",
)
PRIMARY = ("hack/e2e-path-mysql-cdc-mysql.sh", "hack/e2e-clickhouse-replay-ordering.sh")
P1 = {
    "p1-health": "hack/health_ui_e2e.py",
    "p1-references": "hack/connection_reference_ui_e2e.py",
    "p1-recovery": "hack/recovery_ui_e2e.py",
}
PATHS = (
    "mysql_cdc__mysql_upsert", "mysql_snap_cdc__ch_rmt",
    "mysql_snap_cdc__ch_rmt_composite_pk", "ch_dedup_crash_window",
    "ch_dedup_crash_window_multitable", "kafka_envelope_roundtrip",
    "schema_contract_enforcement",
)
REQUIRED = (*MATRIX, *PRIMARY, "tagged", *P1, "readiness")
PLATFORMS = {("linux", "amd64"), ("linux", "arm64"), ("darwin", "amd64"),
             ("darwin", "arm64"), ("windows", "amd64")}
FIXTURE_IMAGES = (
    "docker.io/library/mysql:8.0", "docker.io/library/postgres:16-alpine",
    "docker.io/library/redis:7-alpine", "docker.io/redpandadata/redpanda:v24.1.1",
    "docker.io/clickhouse/clickhouse-server:24.3",
    "docker.io/clickhouse/clickhouse-server:24.3-alpine",
    "quay.io/minio/minio:RELEASE.2024-07-16T23-46-41Z",
    "docker.io/apache/doris:fe-2.1.11", "docker.io/apache/doris:be-2.1.11",
)


def utc():
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def digest(value):
    value = value.removeprefix("sha256:")
    if not re.fullmatch(r"[a-f0-9]{64}", value):
        raise ValueError("invalid image identity")
    return "sha256:" + value


def command(args, *, env=None, log=None, cwd=ROOT, check=True):
    if log is None:
        r = subprocess.run(args, cwd=cwd, env=env, text=True, capture_output=True)
    else:
        with Path(log).open("w") as f:
            r = subprocess.run(args, cwd=cwd, env=env, text=True, stdout=f, stderr=subprocess.STDOUT)
    if check and r.returncode:
        raise RuntimeError(f"command failed ({r.returncode}): {args[0]} {args[1:3]} "
                           + (f"(see {log})" if log else r.stderr[-4000:]))
    return r


def cli():
    # Keep the repository's docker-first selection, unless explicitly forced.
    value = os.environ.get("CONTAINER_CLI") or shutil.which("docker") or shutil.which("podman")
    if not value:
        raise RuntimeError("container CLI unavailable")
    return value


def inspect_image(image):
    body = command([cli(), "image", "inspect", image]).stdout
    info = json.loads(body)[0]
    return {"id": digest(info["Id"]), "architecture": info["Architecture"],
            "labels": info.get("Config", {}).get("Labels") or {}}


def input_hash(root=ROOT):
    # Pack/UI outputs are generated from these frozen source inputs.
    names = command(["git", "ls-files", "-z"], cwd=root).stdout.split("\0")
    h = hashlib.sha256()
    for name in sorted(filter(None, names)):
        if name == "internal/packed/packed.go" or name.startswith("resource/public/"):
            continue
        path = root / name
        if path.is_file():
            h.update(name.encode() + b"\0" + bytes.fromhex(sha(path)))
    return h.hexdigest()


def save(path, value):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(value, indent=2) + "\n")
    tmp.replace(path)


def owned_path(base, relative):
    path = (Path(base) / relative).resolve()
    if not path.is_relative_to(Path(base).resolve()):
        raise ValueError("candidate path escapes its directory")
    return path


def safe_members(names):
    for name in names:
        p = PurePosixPath(name)
        if p.is_absolute() or ".." in p.parts:
            raise ValueError("unsafe archive member")


def binary_header(body, goos, goarch):
    if goos == "linux":
        if body[:4] != b"\x7fELF" or len(body) < 20:
            return False
        endian = "<" if body[5] == 1 else ">"
        return struct.unpack(endian + "H", body[18:20])[0] == {"amd64": 62, "arm64": 183}[goarch]
    if goos == "darwin":
        if body[:4] not in (b"\xcf\xfa\xed\xfe", b"\xfe\xed\xfa\xcf") or len(body) < 8:
            return False
        endian = "<" if body[:4] == b"\xcf\xfa\xed\xfe" else ">"
        return struct.unpack(endian + "I", body[4:8])[0] == {"amd64": 0x1000007, "arm64": 0x100000C}[goarch]
    if goos == "windows" and body[:2] == b"MZ" and len(body) >= 64:
        offset = struct.unpack("<I", body[60:64])[0]
        return len(body) >= offset + 6 and body[offset:offset + 4] == b"PE\0\0" \
            and struct.unpack("<H", body[offset + 4:offset + 6])[0] == 0x8664
    return False


def archive_binary(path, member, *, destination=None):
    if str(path).endswith(".zip"):
        with zipfile.ZipFile(path) as z:
            names = z.namelist()
            safe_members(names)
            if any(stat.S_ISLNK(info.external_attr >> 16) for info in z.infolist()):
                raise ValueError("candidate archive must not contain links")
            matches = [n for n in names if PurePosixPath(n).name == member]
            if len(matches) != 1:
                raise ValueError("archive must contain one matching executable")
            data = z.read(matches[0])
            configs = [n for n in names if n.endswith("manifest/config/config.yaml")]
            config_data = z.read(configs[0]) if destination and len(configs) == 1 else None
    else:
        with tarfile.open(path, "r:*") as t:
            members = t.getmembers()
            safe_members(m.name for m in members)
            if any(m.issym() or m.islnk() for m in members):
                raise ValueError("candidate archive must not contain links")
            matches = [m for m in members if m.isfile() and PurePosixPath(m.name).name == member]
            if len(matches) != 1:
                raise ValueError("archive must contain one matching executable")
            data = t.extractfile(matches[0]).read()
            names = [m.name for m in members]
            configs = [m for m in members if m.isfile() and m.name.endswith("manifest/config/config.yaml")]
            config_data = t.extractfile(configs[0]).read() if destination and len(configs) == 1 else None
    if len(configs) != 1 \
            or not any(PurePosixPath(n).name == "LICENSE" for n in names):
        raise ValueError("archive is missing its runtime config or license")
    if destination:
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_bytes(data)
        destination.chmod(0o755)
        # The packed release executable still reads its distribution config.
        # Preserve the config from this same archive, never from the checkout.
        config_path = destination.parent / "manifest/config/config.yaml"
        config_path.parent.mkdir(parents=True, exist_ok=True)
        config_path.write_bytes(config_data)
    return data


def candidate(base, *, files=True):
    base = Path(base).resolve()
    data = json.loads((base / "candidate.json").read_text())
    if data.get("format") != "openetl-candidate/v1" \
            or not re.fullmatch(r"[a-f0-9]{40}", data.get("source", {}).get("commit", "")):
        raise ValueError("invalid candidate source/format")
    if data["source"].get("dirty") is not False:
        raise ValueError("dirty source cannot certify a release candidate")
    expected = {(variant, goos, arch) for variant in ("default", "extism") for goos, arch in PLATFORMS}
    keys = {(b["variant"], b["goos"], b["goarch"]) for b in data["binaries"]}
    if keys != expected or len(data["binaries"]) != len(expected) or len(data["archives"]) != len(expected):
        raise ValueError("candidate platform/variant matrix is incomplete")
    digest(data["image"]["id"])
    selected = next(b for b in data["binaries"] if (b["variant"], b["goos"], b["goarch"]) ==
                    ("default", "linux", data["image"]["architecture"]))
    if selected["sha256"] != data["image"]["binary_sha256"]:
        raise ValueError("image and archive executable identities disagree")
    if files:
        for asset in data["archives"]:
            if sha(owned_path(base, asset["path"])) != asset["sha256"]:
                raise ValueError("archive hash changed")
        checksum = owned_path(base, data["checksum_file"]["path"])
        if sha(checksum) != data["checksum_file"]["sha256"]:
            raise ValueError("checksum file hash changed")
        rows = [line.split("  ", 1) for line in checksum.read_text().splitlines()]
        expected_sums = {Path(a["path"]).name: a["sha256"] for a in data["archives"]}
        if len(rows) != len(expected_sums) or any(len(row) != 2 for row in rows) \
                or {row[1]: row[0] for row in rows} != expected_sums:
            raise ValueError("checksum file does not match the preserved archives")
        if sha(owned_path(base, data["image"]["archive"])) != data["image"]["archive_sha256"]:
            raise ValueError("image archive hash changed")
        for binary in data["binaries"]:
            body = archive_binary(owned_path(base, binary["archive"]), binary["member"])
            if hashlib.sha256(body).hexdigest() != binary["sha256"] \
                    or not binary_header(body[:4096], binary["goos"], binary["goarch"]):
                raise ValueError("archive executable identity/platform mismatch")
    return data


def runtime_binary(base, data, *, image=False):
    goos = "linux" if image else {"Darwin": "darwin", "Linux": "linux", "Windows": "windows"}[platform.system()]
    arch = data["image"]["architecture"] if image else {"x86_64": "amd64", "AMD64": "amd64", "aarch64": "arm64", "arm64": "arm64"}[platform.machine()]
    binary = next(b for b in data["binaries"] if (b["variant"], b["goos"], b["goarch"]) == ("default", goos, arch))
    path = Path(base) / "runtime" / (goos + "_" + arch) / binary["member"]
    archive_binary(owned_path(base, binary["archive"]), binary["member"], destination=path)
    if sha(path) != binary["sha256"]:
        raise ValueError("extracted executable hash mismatch")
    return path, binary["sha256"]


def load(base, data):
    command([cli(), "load", "-i", str(owned_path(base, data["image"]["archive"]))])
    info = inspect_image(data["image"]["id"])
    if info["id"] != data["image"]["id"] or info["architecture"] != data["image"]["architecture"] \
            or info["labels"].get("org.opencontainers.image.revision") != data["source"]["commit"]:
        raise ValueError("loaded image source/identity mismatch")
    r = command([cli(), "run", "--rm", "--entrypoint", "sha256sum", data["image"]["id"],
                 data["image"]["binary_path"]])
    if r.stdout.split()[0] != data["image"]["binary_sha256"]:
        raise ValueError("image executable hash mismatch")
    return info


def build(base, snapshot, arch):
    base = Path(base).resolve()
    if base.exists() and any(base.iterdir()):
        raise ValueError("candidate output must be a new empty directory; existing bytes are never rebuilt in place")
    base.mkdir(parents=True, exist_ok=True)
    if command(["git", "status", "--porcelain"]).stdout:
        raise ValueError("candidate build requires a clean isolated source checkout")
    revision = command(["git", "rev-parse", "HEAD"]).stdout.strip()
    frozen = input_hash()
    started = utc()
    command(["npm", "ci", "--no-audit", "--no-fund"], cwd=ROOT / "web", log=base / "npm-ci.log")
    command(["npm", "run", "build"], cwd=ROOT / "web", log=base / "ui-build.log")
    node = "const fs=require('node:fs'),YAML=require(process.argv[1]);process.stdout.write(JSON.stringify(YAML.parse(fs.readFileSync(process.argv[2],'utf8'))));"
    cfg = json.loads(command(["node", "-e", node, str(ROOT / "web/node_modules/yaml"),
                              str(ROOT / ".goreleaser.yml")]).stdout)
    cfg["dist"] = str(base / "dist")
    for item in cfg.get("dockers", []):
        item["goarch"] = arch
        item["image_templates"] = ["openetl-candidate:" + uuid.uuid4().hex[:16]]
        item["build_flag_templates"] = ["--platform=linux/" + arch,
            "--label=org.opencontainers.image.revision=" + revision,
            "--label=org.opencontainers.image.version={{.Version}}"]
    config = base / "goreleaser.json"
    save(config, cfg)  # JSON is a YAML subset; no second configuration authority.
    env = {**os.environ, "SKIP_UI": "1", "GOTOOLCHAIN": "go1.24.13"}
    tool = os.environ.get("GORELEASER", "goreleaser")
    args = [tool, "release", "--skip=publish", "--clean", "--parallelism=2",
            "--timeout=45m", "--config", str(config)]
    if snapshot:
        args.append("--snapshot")
    command(args, env=env, log=base / "build.log")
    if input_hash() != frozen:
        raise ValueError("source inputs changed during candidate generation")
    metadata = json.loads((base / "dist/metadata.json").read_text())
    artifacts = json.loads((base / "dist/artifacts.json").read_text())
    binaries, archives = [], []
    for item in artifacts:
        if item.get("type") not in ("Binary", "Archive"):
            continue
        path = Path(item["path"])
        if not path.is_absolute():
            path = ROOT / path
        relative = str(path.resolve().relative_to(base))
        variant = "extism" if "extism" in item["name"] else "default"
        record = {"variant": variant, "goos": item["goos"], "goarch": item["goarch"],
                  "path": relative, "sha256": sha(path), "name": item["name"]}
        (binaries if item["type"] == "Binary" else archives).append(record)
    for binary in binaries:
        matching = next(a for a in archives if (a["variant"], a["goos"], a["goarch"]) ==
                        (binary["variant"], binary["goos"], binary["goarch"]))
        binary["archive"], binary["member"] = matching["path"], Path(binary["path"]).name
    image_record = next(a for a in artifacts if a.get("type") == "Docker Image")
    image = inspect_image(image_record["name"])
    image_binary = next(b for b in binaries if (b["variant"], b["goos"], b["goarch"]) == ("default", "linux", arch))
    image.update({"archive": "candidate-image.tar", "binary_path": "/app/openetl-go",
                  "binary_sha256": image_binary["sha256"]})
    save_args = [cli(), "save", "-o", str(base / image["archive"])]
    if Path(cli()).name == "podman":
        save_args += ["--format", "docker-archive"]
    command([*save_args, image["id"]])
    image["archive_sha256"] = sha(base / image["archive"])
    data = {"format": "openetl-candidate/v1", "source": {"commit": revision, "dirty": False, "input_sha256": frozen},
            "version": metadata["version"], "build": {"started_at": started, "finished_at": utc(),
            "go": command(["go", "version"], env=env).stdout.strip(),
            "goreleaser": command([tool, "--version"]).stdout.strip()},
            "image": image, "binaries": binaries, "archives": archives,
            "checksum_file": {"path": "dist/checksums.txt", "sha256": sha(base / "dist/checksums.txt")}}
    save(base / "candidate.json", data)
    candidate(base)
    load(base, data)
    print("Candidate captured:", revision, image["id"], "10 archives; build/publish separated")


def case_dir(base, name):
    return Path(base) / "results" / name.replace("/", "__")


def case_artifact_hashes(folder, name):
    folder = Path(folder)
    paths = []
    if name == "tagged":
        paths = [folder / "paths" / (path_id + ".json") for path_id in PATHS]
    elif name in P1 or name == "readiness":
        paths = [folder / "probe/result.json"]
        paths += [p for p in (folder / "probe").iterdir()
                  if p.is_file() and p.suffix in (".json", ".log", ".js", ".png") and p not in paths]
    return {str(p.relative_to(folder)): sha(p) for p in sorted(paths)}


def test_env(base, data):
    env = {k: v for k, v in os.environ.items() if not k.startswith("ETL_")}
    for key in ("MYSQL_DSN", "POSTGRES_DSN", "MYSQL_HOST", "PGHOST", "POSTGRES_HOST"):
        env.pop(key, None)
    binary, expected = runtime_binary(base, data)
    env.update({"IMAGE": data["image"]["id"], "E2E_SKIP_BUILD": "1",
                "OPENETL_IMAGE_BINARY": data["image"]["binary_path"],
                "OPENETL_BIN": str(binary), "E2E_EXPECTED_BINARY_SHA256": expected,
                "E2E_CANDIDATE_REQUIRED": "1", "TESTCONTAINERS_RYUK_DISABLED": "true",
                "GOTOOLCHAIN": "go1.24.13"})
    return env, expected


def dependency_images():
    result = {}
    for name in FIXTURE_IMAGES:
        r = command([cli(), "image", "inspect", name], check=False)
        if r.returncode == 0:
            result[name] = digest(json.loads(r.stdout)[0]["Id"])
    return result


def run_case(base, name):
    if name not in REQUIRED:
        raise ValueError("unknown mandatory candidate case")
    data = candidate(base)
    if command(["git", "rev-parse", "HEAD"]).stdout.strip() != data["source"]["commit"] \
            or input_hash() != data["source"]["input_sha256"]:
        raise ValueError("test source differs from the candidate's frozen inputs")
    load(base, data)
    env, expected = test_env(base, data)
    folder = case_dir(base, name)
    if folder.exists():
        raise ValueError("case output already exists; preserve it and use a new run directory")
    folder.mkdir(parents=True)
    source = folder / "source"
    command(["git", "worktree", "add", "--detach", str(source), data["source"]["commit"]])
    env["E2E_EVIDENCE_ROOT"] = str(folder / "paths")
    started, result, error, code = utc(), "failed", "", None
    try:
        if name in MATRIX + PRIMARY:
            args = ["bash", str(source / name)]
        elif name == "tagged":
            args = ["go", "test", "-tags=e2e", "-count=1", "-p=1", "-timeout=40m",
                    "./internal/etl/e2e/...", "-e2e.strict", "-v"]
        elif name in P1:
            args = [sys.executable, str(source / P1[name]), "--cli", cli(),
                    "--image", data["image"]["id"], "--out", str(folder / "probe")]
        else:
            args = [sys.executable, str(source / "hack/candidate_readiness_e2e.py"),
                    "--cli", cli(), "--image", data["image"]["id"], "--out", str(folder / "probe"),
                    "--manifest", str(Path(base) / "connector-evidence.json")]
        completed = command(args, env=env, log=folder / "command.log", cwd=source, check=False)
        code = completed.returncode
        if code != 0:
            raise RuntimeError("mandatory case exited " + str(code))
        text = (folder / "command.log").read_text()
        if re.search(r"(?m)^\s*(?:SKIP:|\[SKIP\]|=== SKIPPED)", text):
            result = "skipped"
            raise RuntimeError("mandatory case reported a skip")
        if name == "tagged":
            for path_id in PATHS:
                evidence = json.loads((folder / "paths" / (path_id + ".json")).read_text())
                if evidence.get("result") != "passed" or not evidence.get("checks") \
                        or any(c["result"] != "passed" for c in evidence["checks"]) \
                        or evidence["commit"] != data["source"]["commit"] \
                        or evidence.get("asset", {}).get("binary_sha256") != expected:
                    raise ValueError("tagged path did not certify this candidate: " + path_id)
        if name in P1 or name == "readiness":
            probe = json.loads((folder / "probe/result.json").read_text())
            if probe["result"] != "passed" or not probe["checks"] \
                    or any(c["result"] != "passed" for c in probe["checks"]) \
                    or digest(probe["image_id"]) != data["image"]["id"]:
                raise ValueError("browser/readiness evidence did not certify this image")
        if inspect_image(data["image"]["id"])["id"] != data["image"]["id"]:
            raise ValueError("candidate image changed during certification")
        candidate(base)
        result = "passed"
    except Exception as exc:
        error = str(exc)
    finally:
        # The detached test checkout is task-owned; preserve logs and generated
        # path evidence outside it. User checkouts are never reset or cleaned.
        command(["git", "worktree", "remove", "--force", str(source)], check=False)
    report = {"name": name, "source_commit": data["source"]["commit"], "image_id": data["image"]["id"],
              "binary_sha256": expected if name == "tagged" else data["image"]["binary_sha256"],
              "started_at": started, "finished_at": utc(),
              "result": result, "exit_code": code, "error": error,
              "log_sha256": sha(folder / "command.log") if (folder / "command.log").exists() else None,
              "artifact_sha256": case_artifact_hashes(folder, name) if result == "passed" else {},
              "container_runtime": command([cli(), "--version"]).stdout.strip(),
              "dependency_images": dependency_images()}
    save(folder / "result.json", report)
    print(name, result, error)
    if result != "passed":
        raise RuntimeError("candidate certification failed: " + name)


def reports(base, names):
    data = candidate(base)
    out = []
    for name in names:
        folder = case_dir(base, name)
        value = json.loads((folder / "result.json").read_text())
        if value["name"] != name or value["source_commit"] != data["source"]["commit"] \
                or value["image_id"] != data["image"]["id"] or value["result"] != "passed" \
                or value["binary_sha256"] not in ({b["sha256"] for b in data["binaries"] if b["variant"] == "default"}
                                                  if name == "tagged" else {data["image"]["binary_sha256"]}) \
                or value["exit_code"] != 0 or sha(folder / "command.log") != value["log_sha256"] \
                or value.get("artifact_sha256") != case_artifact_hashes(folder, name):
            raise ValueError("missing, failed or mismatched mandatory evidence: " + name)
        start = dt.datetime.fromisoformat(value["started_at"].replace("Z", "+00:00"))
        end = dt.datetime.fromisoformat(value["finished_at"].replace("Z", "+00:00"))
        if end < start or start < dt.datetime.fromisoformat(data["build"]["finished_at"].replace("Z", "+00:00")) \
                or end > dt.datetime.now(dt.timezone.utc) + dt.timedelta(minutes=5):
            raise ValueError("invalid candidate evidence execution window")
        if dt.datetime.now(dt.timezone.utc) - end > dt.timedelta(days=30):
            raise ValueError("mandatory candidate evidence expired; rerun the case on the same bytes")
        out.append(value)
    return out


def emit_evidence(base):
    data = candidate(base)
    runs = {r["name"]: r for r in reports(base, MATRIX)}
    manifest = json.loads((ROOT / "internal/etl/server/evidence/connector-evidence.json").read_text())
    manifest["certified_commit"] = data["source"]["commit"]
    manifest["certified_image"] = data["image"]["id"]
    manifest["certified_binary_sha256"] = data["image"]["binary_sha256"]
    for record in manifest["records"]:
        executed = [runs[name] for name in record["scripts"]]
        record.update({"commit": manifest["certified_commit"], "image": manifest["certified_image"],
                       "binary_sha256": manifest["certified_binary_sha256"],
                       "started_at": min(r["started_at"] for r in executed),
                       "finished_at": max(r["finished_at"] for r in executed), "verified": True})
        finish = dt.datetime.fromisoformat(record["finished_at"].replace("Z", "+00:00"))
        record["expires_at"] = (finish + dt.timedelta(hours=manifest["policy"]["max_age_hours"])).isoformat().replace("+00:00", "Z")
        record["dependencies"] = {"go": data["build"]["go"], "container_runtime": executed[0]["container_runtime"],
                                  "fixture_images": json.dumps({k: v for r in executed for k, v in r["dependency_images"].items()}, sort_keys=True)}
        record["note"] = "Executed on this preserved candidate; raw per-script logs and image identities are retained in its certification bundle."
    save(Path(base) / "connector-evidence.json", manifest)


def collect(base):
    data = candidate(base)
    values = reports(base, REQUIRED)
    emit_evidence(base)
    tagged = case_dir(base, "tagged") / "paths"
    command(["bash", str(ROOT / "hack/check-connector-evidence.sh"), "-strict",
             "-manifest", str(Path(base) / "connector-evidence.json"), "-path-evidence", str(tagged),
             "-commit", data["source"]["commit"], "-image", data["image"]["id"]],
            log=Path(base) / "strict-evidence.log")
    report = {"format": "openetl-candidate-certification/v1", "source_commit": data["source"]["commit"],
              "candidate_sha256": sha(Path(base) / "candidate.json"), "image_id": data["image"]["id"],
              "image_binary_sha256": data["image"]["binary_sha256"], "result": "passed",
              "started_at": min(v["started_at"] for v in values), "finished_at": utc(),
              "cases": values, "connector_evidence_sha256": sha(Path(base) / "connector-evidence.json")}
    save(Path(base) / "certification.json", report)
    verify(base)
    print("Candidate certification passed:", len(values), "mandatory cases; seven tagged path artifacts")


def verify(base):
    data = candidate(base)
    report = json.loads((Path(base) / "certification.json").read_text())
    if report.get("format") != "openetl-candidate-certification/v1" or report.get("result") != "passed" \
            or report["source_commit"] != data["source"]["commit"] \
            or report["candidate_sha256"] != sha(Path(base) / "candidate.json") \
            or report["image_id"] != data["image"]["id"] \
            or report["image_binary_sha256"] != data["image"]["binary_sha256"] \
            or report["connector_evidence_sha256"] != sha(Path(base) / "connector-evidence.json"):
        raise ValueError("candidate/report identity mismatch")
    if {v["name"] for v in report["cases"]} != set(REQUIRED) or len(report["cases"]) != len(REQUIRED):
        raise ValueError("mandatory certification matrix is incomplete")
    if reports(base, REQUIRED) != report["cases"]:
        raise ValueError("certification case evidence changed")
    finish = dt.datetime.fromisoformat(report["finished_at"].replace("Z", "+00:00"))
    if dt.datetime.now(dt.timezone.utc) - finish > dt.timedelta(days=30):
        raise ValueError("candidate certification expired; recertify the same bytes")
    return data


def bundle(base):
    verify(base)
    base = Path(base).resolve()
    path = base / "candidate-bundle.tar.gz"
    with tarfile.open(path, "w:gz") as t:
        for child in sorted(base.iterdir()):
            if child.name not in ("candidate-bundle.tar.gz", "candidate-bundle.sha256"):
                t.add(child, arcname=child.name, recursive=True)
    (base / "candidate-bundle.sha256").write_text(sha(path) + "  candidate-bundle.tar.gz\n")


def promote(base, destination, *, release_tag=None, insecure=False, image_only=False):
    data = verify(base)
    if release_tag:
        tag_commit = command(["git", "rev-parse", release_tag + "^{commit}"]).stdout.strip()
        if tag_commit != data["source"]["commit"]:
            raise ValueError("release tag does not name the certified candidate source")
    load(base, data)
    before = data["image"]["id"]
    command([cli(), "tag", before, destination])
    args = [cli(), "push"]
    if insecure and Path(cli()).name == "podman":
        args += ["--tls-verify=false"]
    command([*args, destination])
    pull = [cli(), "pull"]
    if insecure and Path(cli()).name == "podman":
        pull += ["--tls-verify=false"]
    command([*pull, destination])
    info = json.loads(command([cli(), "image", "inspect", destination]).stdout)[0]
    if digest(info["Id"]) != before:
        raise ValueError("registry promotion changed candidate image bytes")
    actual = command([cli(), "run", "--rm", "--entrypoint", "sha256sum", destination,
                      data["image"]["binary_path"]]).stdout.split()[0]
    if actual != data["image"]["binary_sha256"]:
        raise ValueError("promoted executable hash mismatch")
    receipt = {"source_commit": data["source"]["commit"], "candidate_sha256": sha(Path(base) / "candidate.json"),
               "image_id": before, "destination": destination, "registry_digests": info.get("RepoDigests", []),
               "binary_sha256": actual, "promoted_at": utc(), "result": "passed"}
    if not receipt["registry_digests"]:
        raise ValueError("registry did not return an immutable digest")
    save(Path(base) / "promotion.json", receipt)
    if release_tag and not image_only:
        assets = [str(owned_path(base, a["path"])) for a in data["archives"]]
        assets += [str(Path(base) / n) for n in ("candidate-bundle.tar.gz", "candidate-bundle.sha256",
                   "candidate.json", "certification.json", "connector-evidence.json", "promotion.json")]
        assets += [str(Path(base) / "dist/checksums.txt")]
        args = ["gh", "release", "create", release_tag, "--verify-tag", "--title", "OpenETL-Go " + data["version"],
                "--generate-notes"]
        if "-" in data["version"]:
            args.append("--prerelease")
        command([*args, *assets])
    print("Promoted preserved bytes:", before, receipt["registry_digests"])


def main():
    p = argparse.ArgumentParser(description=__doc__)
    sub = p.add_subparsers(dest="action", required=True)
    for action in ("build", "load", "run", "emit-evidence", "collect", "verify", "bundle", "promote"):
        s = sub.add_parser(action)
        s.add_argument("--dir", required=True)
        if action == "build":
            s.add_argument("--snapshot", action="store_true")
            s.add_argument("--arch", choices=("amd64", "arm64"), default="amd64")
        if action == "run":
            s.add_argument("--name", required=True, choices=REQUIRED)
        if action == "promote":
            s.add_argument("--destination", required=True)
            s.add_argument("--release-tag")
            s.add_argument("--insecure", action="store_true")
            s.add_argument("--image-only", action="store_true")
    args = p.parse_args()
    if args.action == "build":
        build(args.dir, args.snapshot, args.arch)
    elif args.action == "load":
        load(args.dir, candidate(args.dir))
    elif args.action == "run":
        run_case(args.dir, args.name)
    elif args.action == "emit-evidence":
        emit_evidence(args.dir)
    elif args.action == "collect":
        collect(args.dir)
    elif args.action == "verify":
        verify(args.dir)
        print("Candidate assets and mandatory results verified")
    elif args.action == "bundle":
        bundle(args.dir)
    elif args.action == "promote":
        promote(args.dir, args.destination, release_tag=args.release_tag, insecure=args.insecure, image_only=args.image_only)


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("Candidate gate failed:", error, file=sys.stderr)
        sys.exit(1)
