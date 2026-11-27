#!/usr/bin/env python3
"""RA-8 packaging, health-ready startup and offline restore measurements.

Standard-library-only harness. Run via bench-baseline.sh for shared container
selection. Builds use a private source snapshot and fresh packed UI. Results
cover the native container architecture and this scope, not all release paths.
"""

import argparse
import base64
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import platform
import posixpath
import re
import secrets
import shutil
import signal
import ssl
import statistics
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
import uuid
import zlib


VARIANTS = {"default": "", "extism": "extism", "nolua": "nolua", "extism-nolua": "extism,nolua"}
BUILD_INPUTS = ["Dockerfile", ".dockerignore", "go.mod", "go.sum", "main.go",
                "internal", "resource", "manifest", "web", "hack/pack.sh", "hack/container-cli.sh"]
EXCLUDED_PARTS = {".git", "node_modules", "data", "logs", "log", ".pi", ".pi-subagents",
                  ".bench-out", "screenshots", "__pycache__"}
MIB = 1024 * 1024
IMAGE_METHOD = "uncompressed-layer-bytes/v1"
READ_CHUNK = MIB
MAX_METADATA_BYTES = 8 * MIB
MAX_ARCHIVE_BYTES = 8 * 1024 * MIB
MAX_LAYER_BYTES = 4 * 1024 * MIB
MAX_IMAGE_BYTES = 8 * 1024 * MIB
MAX_MEMBERS = 100000
MAX_LAYERS = 4096
INDEX_TYPES = {"application/vnd.oci.image.index.v1+json",
               "application/vnd.docker.distribution.manifest.list.v2+json"}
MANIFEST_TYPES = {"application/vnd.oci.image.manifest.v1+json",
                  "application/vnd.docker.distribution.manifest.v2+json"}
CONFIG_TYPES = {"application/vnd.oci.image.config.v1+json",
                "application/vnd.docker.container.image.v1+json"}
RAW_LAYER_TYPES = {"application/vnd.oci.image.layer.v1.tar",
                   "application/vnd.oci.image.layer.nondistributable.v1.tar",
                   "application/vnd.docker.image.rootfs.diff.tar"}
GZIP_LAYER_TYPES = {"application/vnd.oci.image.layer.v1.tar+gzip",
                    "application/vnd.oci.image.layer.nondistributable.v1.tar+gzip",
                    "application/vnd.docker.image.rootfs.diff.tar.gzip",
                    "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip"}


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


def byte_count(value, label, *, allow_zero=False):
    require(type(value) is int and value >= (0 if allow_zero else 1),
            "missing or invalid byte count: " + label)
    return value


def normalize_sha256(value):
    require(isinstance(value, str), "SHA256 digest must be text")
    value = value.strip().lower()
    if value.startswith("sha256:"):
        value = value[7:]
    require(re.fullmatch(r"[0-9a-f]{64}", value) is not None, "invalid SHA256 digest")
    return value


def _hash_file(source):
    h, size = hashlib.sha256(), 0
    while body := source.read(READ_CHUNK):
        size += len(body)
        require(size <= MAX_ARCHIVE_BYTES, "image archive exceeds measurement limit")
        h.update(body)
    return h.hexdigest(), size


def _member_name(name):
    require(isinstance(name, str) and "\\" not in name and "\x00" not in name
            and not name.startswith("/") and ".." not in name.split("/"),
            "unsafe image archive path")
    name = posixpath.normpath(name)
    require(name != ".." and not name.startswith("../"), "unsafe image archive path")
    return name


class _BoundedArchiveFile:
    """Bound metadata reads before tarfile processes PAX/long-name headers."""
    def __init__(self, source):
        self.source = source

    def read(self, size=-1):
        require(0 <= size <= MAX_METADATA_BYTES, "image archive metadata exceeds measurement limit")
        return self.source.read(size)

    def seek(self, *args):
        return self.source.seek(*args)

    def tell(self):
        return self.source.tell()

    def seekable(self):
        return True

    def readable(self):
        return True


class _HashingBlob:
    def __init__(self, source):
        self.source, self.h, self.size = source, hashlib.sha256(), 0

    def read(self, size=-1):
        require(0 <= size <= MAX_METADATA_BYTES, "unbounded layer read")
        body = self.source.read(size)
        self.size += len(body)
        require(self.size <= MAX_LAYER_BYTES, "encoded image layer exceeds measurement limit")
        self.h.update(body)
        return body


def _unique_object(pairs):
    value = {}
    for key, item in pairs:
        require(key not in value, "duplicate field in image metadata")
        value[key] = item
    return value


def _metadata_json(body):
    def invalid_constant(_):
        raise MeasurementError("non-finite value in image metadata")
    try:
        return json.loads(body.decode("utf-8"), object_pairs_hook=_unique_object,
                          parse_constant=invalid_constant)
    except (ValueError, UnicodeError) as exc:
        raise MeasurementError("invalid image metadata JSON") from exc


def _tar_header(body):
    if len(body) != 512:
        return False
    if body == bytes(512):
        return True
    try:
        tarfile.TarInfo.frombuf(body, "utf-8", "surrogateescape")
        return True
    except tarfile.HeaderError:
        return False


def measure_image_archive(path, expected_config=None, expected_architecture=None):
    """Measure one selected image, without extraction or using inspect.Size."""
    path = Path(path)
    require(not path.is_symlink() and path.is_file(), "image archive must be a regular file")
    expected = normalize_sha256(expected_config) if expected_config is not None else None
    require(expected is not None or expected_architecture is not None,
            "image selection needs an expected configuration digest or architecture")
    try:
        with path.open("rb") as source:
            before = os.fstat(source.fileno())
            require(before.st_size <= MAX_ARCHIVE_BYTES, "image archive exceeds measurement limit")
            archive_sha, archive_size = _hash_file(source)
            source.seek(0)
            # Engine save produces a raw tar container. Layer encodings are separate.
            with tarfile.open(fileobj=_BoundedArchiveFile(source), mode="r:") as archive:
                members = {}
                for count, member in enumerate(archive):
                    require(count < MAX_MEMBERS, "too many image archive members")
                    name = _member_name(member.name)
                    if member.isdir():
                        continue
                    require(member.isfile(), "image archive links and special files are unsupported")
                    require(name != "." and name not in members, "duplicate or invalid image archive member")
                    require(0 <= member.size <= MAX_LAYER_BYTES, "image archive member exceeds measurement limit")
                    members[name] = member

                def blob(name, maximum=MAX_METADATA_BYTES):
                    name = _member_name(name)
                    require(name in members, "image archive is missing a required blob")
                    member = members[name]
                    require(member.size <= maximum, "image metadata exceeds measurement limit")
                    with archive.extractfile(member) as item:
                        body = item.read(member.size)
                    require(len(body) == member.size, "truncated image metadata")
                    claimed = None
                    if re.fullmatch(r"blobs/sha256/[0-9a-f]{64}", name):
                        claimed = name.rsplit("/", 1)[1]
                    elif re.fullmatch(r"[0-9a-f]{64}\.json", name):
                        claimed = name[:-5]
                    require(claimed is None or digest(body) == claimed, "image metadata digest mismatch")
                    return body

                def descriptor(item):
                    require(isinstance(item, dict), "invalid image descriptor")
                    sha = normalize_sha256(item.get("digest"))
                    require(item["digest"] == "sha256:" + sha, "unsupported image digest algorithm")
                    name = "blobs/sha256/" + sha
                    require(name in members, "image archive is missing a descriptor blob")
                    require(type(item.get("size")) is int and item["size"] == members[name].size,
                            "image descriptor size mismatch")
                    return name, sha

                choices = []

                def add_choice(config_body, layers, archive_format):
                    sha = digest(config_body)
                    if expected is not None and sha != expected:
                        return
                    config = _metadata_json(config_body)
                    require(isinstance(config, dict), "image configuration must be an object")
                    architecture = config.get("architecture")
                    require(isinstance(architecture, str) and architecture
                            and isinstance(config.get("os"), str) and config["os"],
                            "image configuration lacks platform")
                    if expected_architecture is not None and architecture != expected_architecture:
                        return
                    rootfs = config.get("rootfs")
                    require(isinstance(rootfs, dict) and rootfs.get("type") == "layers"
                            and isinstance(rootfs.get("diff_ids"), list), "invalid image rootfs metadata")
                    diff_ids = rootfs["diff_ids"]
                    require(isinstance(layers, list) and len(layers) == len(diff_ids)
                            and len(layers) <= MAX_LAYERS, "image layer/rootfs count mismatch")
                    require(all(isinstance(d, str) and re.fullmatch(r"sha256:[0-9a-f]{64}", d)
                                for d in diff_ids), "invalid image diff ID")
                    choices.append((sha, config["os"], architecture, diff_ids, layers, archive_format))

                if "manifest.json" in members:
                    manifest = _metadata_json(blob("manifest.json"))
                    require(isinstance(manifest, list) and 0 < len(manifest) <= MAX_LAYERS,
                            "invalid Docker save manifest")
                    for item in manifest:
                        require(isinstance(item, dict) and isinstance(item.get("Config"), str)
                                and isinstance(item.get("Layers"), list), "invalid Docker image entry")
                        layers = [{"path": _member_name(name), "media_type": None, "digest": None}
                                  for name in item["Layers"]]
                        add_choice(blob(item["Config"]), layers, "docker-save")
                else:
                    layout = _metadata_json(blob("oci-layout"))
                    require(isinstance(layout, dict) and layout.get("imageLayoutVersion") == "1.0.0",
                            "unsupported OCI layout")
                    visited = set()

                    def walk(index, depth=0):
                        require(depth <= 8 and isinstance(index, dict)
                                and index.get("schemaVersion") == 2
                                and isinstance(index.get("manifests"), list)
                                and len(index["manifests"]) <= MAX_LAYERS, "invalid OCI image index")
                        for entry in index["manifests"]:
                            require(isinstance(entry, dict), "invalid OCI descriptor")
                            p = entry.get("platform")
                            if expected_architecture is not None and isinstance(p, dict) \
                                    and p.get("architecture") not in (None, expected_architecture):
                                continue
                            name, sha = descriptor(entry)
                            if sha in visited:
                                continue
                            visited.add(sha)
                            media = entry.get("mediaType")
                            if media in INDEX_TYPES:
                                walk(_metadata_json(blob(name)), depth + 1)
                            else:
                                require(media in MANIFEST_TYPES, "unsupported OCI manifest media type")
                                image = _metadata_json(blob(name))
                                require(isinstance(image, dict) and image.get("schemaVersion") == 2
                                        and isinstance(image.get("config"), dict)
                                        and isinstance(image.get("layers"), list), "invalid OCI image manifest")
                                config_entry = image["config"]
                                if expected is not None and config_entry.get("digest") != "sha256:" + expected:
                                    continue
                                require(config_entry.get("mediaType") in CONFIG_TYPES,
                                        "unsupported OCI configuration media type")
                                config_name, _ = descriptor(config_entry)
                                layers = []
                                for layer in image["layers"]:
                                    layer_name, layer_sha = descriptor(layer)
                                    layers.append({"path": layer_name, "media_type": layer.get("mediaType"),
                                                   "digest": layer_sha})
                                add_choice(blob(config_name), layers, "oci")

                    walk(_metadata_json(blob("index.json")))

                require(len(choices) == 1, "image selection is missing or ambiguous")
                config_sha, image_os, architecture, diff_ids, layers, archive_format = choices[0]
                inventory, total = [], 0
                for ordinal, (layer, diff_id) in enumerate(zip(layers, diff_ids)):
                    name = layer["path"]
                    require(name in members, "image archive is missing a layer")
                    member = members[name]
                    with archive.extractfile(member) as raw:
                        prefix = raw.read(512)
                        raw.seek(0)
                        media = layer["media_type"]
                        if media is None:
                            if _tar_header(prefix):
                                encoding = "raw"
                            else:
                                require(prefix.startswith(b"\x1f\x8b"), "unsupported image layer encoding")
                                encoding = "gzip"
                        else:
                            require(media in RAW_LAYER_TYPES | GZIP_LAYER_TYPES,
                                    "unsupported image layer media type/encoding")
                            encoding = "gzip" if media in GZIP_LAYER_TYPES else "raw"
                        encoded = _HashingBlob(raw)
                        payload = gzip.GzipFile(fileobj=encoded, mode="rb") if encoding == "gzip" else encoded
                        h, size, first = hashlib.sha256(), 0, b""
                        try:
                            while body := payload.read(READ_CHUNK):
                                size += len(body)
                                require(size <= MAX_LAYER_BYTES and total + size <= MAX_IMAGE_BYTES,
                                        "expanded image layers exceed measurement limit")
                                h.update(body)
                                if len(first) < 512:
                                    first += body[:512 - len(first)]
                        finally:
                            if encoding == "gzip":
                                payload.close()
                        while encoded.read(READ_CHUNK):
                            pass
                        require(encoded.size == member.size, "truncated encoded image layer")
                        require(_tar_header(first) and size >= 1024 and size % 512 == 0,
                                "image layer is not a complete tar stream")
                        require("sha256:" + h.hexdigest() == diff_id, "uncompressed image layer diff ID mismatch")
                        claimed = layer["digest"]
                        if re.fullmatch(r"blobs/sha256/[0-9a-f]{64}", name):
                            claimed = name.rsplit("/", 1)[1]
                        require(claimed is None or encoded.h.hexdigest() == claimed, "encoded image layer digest mismatch")
                        total += size
                        inventory.append({"ordinal": ordinal, "blob_sha256": encoded.h.hexdigest(),
                                          "blob_bytes": encoded.size, "diff_id": diff_id,
                                          "encoding": encoding, "uncompressed_bytes": size})
            source.seek(0)
            after_sha, after_size = _hash_file(source)
            after = os.fstat(source.fileno())
            require(archive_sha == after_sha and archive_size == after_size
                    and (before.st_size, before.st_mtime_ns, before.st_ctime_ns)
                    == (after.st_size, after.st_mtime_ns, after.st_ctime_ns),
                    "image archive changed during measurement")
        return {"method": IMAGE_METHOD, "unit": "bytes", "archive_format": archive_format,
                "image_config_sha256": config_sha, "image_archive_sha256": archive_sha,
                "image_archive_bytes": archive_size, "image_uncompressed_layer_bytes": total,
                "os": image_os, "architecture": architecture, "layer_inventory": inventory}
    except (OSError, tarfile.TarError, EOFError, ValueError, KeyError, TypeError, zlib.error) as exc:
        raise MeasurementError("invalid or unreadable image archive") from exc


def validate_image_metrics(metrics):
    try:
        require(isinstance(metrics, dict) and metrics["method"] == IMAGE_METHOD and metrics["unit"] == "bytes",
                "image metric method or units differ")
        require(metrics["archive_format"] in ("docker-save", "oci"), "unsupported image archive format")
        for key in ("image_config_sha256", "image_archive_sha256"):
            require(re.fullmatch(r"[0-9a-f]{64}", metrics[key]) is not None, "missing image content digest")
        byte_count(metrics["image_archive_bytes"], "image archive")
        byte_count(metrics["image_uncompressed_layer_bytes"], "expanded image", allow_zero=True)
        require(isinstance(metrics["layer_inventory"], list) and len(metrics["layer_inventory"]) <= MAX_LAYERS,
                "missing image layer inventory")
        total = 0
        for ordinal, layer in enumerate(metrics["layer_inventory"]):
            require(type(layer["ordinal"]) is int and layer["ordinal"] == ordinal, "image layer order differs")
            require(re.fullmatch(r"[0-9a-f]{64}", layer["blob_sha256"]) is not None
                    and re.fullmatch(r"sha256:[0-9a-f]{64}", layer["diff_id"]) is not None,
                    "missing image layer digests")
            require(layer["encoding"] in ("raw", "gzip"), "unsupported image layer encoding")
            byte_count(layer["blob_bytes"], "encoded image layer")
            total += byte_count(layer["uncompressed_bytes"], "expanded image layer")
        require(total == metrics["image_uncompressed_layer_bytes"] and total <= MAX_IMAGE_BYTES,
                "image layer size total differs")
    except (KeyError, TypeError, AttributeError) as exc:
        raise MeasurementError("incomplete image metric evidence") from exc


def sanitize_image_inspect(output):
    """Inspect logs must not persist Config.Env or other unneeded configuration."""
    try:
        records = json.loads(output)
        require(isinstance(records, list) and len(records) == 1 and isinstance(records[0], dict),
                "invalid image inspect response")
        item = records[0]
        selected = {key: item[key] for key in ("Id", "Digest", "RepoDigests", "Size", "Os", "Architecture")
                    if key in item}
        selected["Config"] = {"Healthcheck": item.get("Config", {}).get("Healthcheck")}
        if "Healthcheck" in item:
            selected["Healthcheck"] = item["Healthcheck"]
        return json.dumps([selected], sort_keys=True) + "\n"
    except (ValueError, TypeError, AttributeError) as exc:
        raise MeasurementError("invalid image inspect response; output redacted") from exc


def image_identity(raw):
    image_id = raw["Id"]
    if not image_id.startswith("sha256:"):
        image_id = "sha256:" + image_id
    return {"image_id": image_id, "manifest_digest": raw.get("Digest") or None,
            "repo_digests": raw.get("RepoDigests") or [], "bytes": raw["Size"],
            "engine_reported_image_bytes": raw["Size"],
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


def validate_result(result, artifact_root=None):
    """Check v2 historical observations or v3 measurements; no enforce policy yet."""
    try:
        version = result["schema_version"]
        require(type(version) is int and version in (2, 3) and result["status"] == "passed",
                "baseline is not passed v2/v3")
        require(result["scope"] == "packaging-startup-restore", "unexpected baseline scope")
        require(re.fullmatch(r"[0-9a-f]{40}", result["source"]["commit"]) is not None, "missing source commit")
        require(re.fullmatch(r"[0-9a-f]{64}", result["source"]["sha256"]) is not None, "missing source fingerprint")
        for key in ("cpus", "memory_bytes"):
            positive(result["hardware"][key], "hardware." + key)
        require(result["hardware"]["os"] == "linux", "runtime hardware must describe Linux")
        require(result["toolchain"]["go_version"].startswith("go version go"), "missing actual Go toolchain")
        if version == 3:
            require(type(result["source"]["dirty"]) is bool, "missing actual dirty-source status")
            byte_count(result["source"]["files"], "source file count")
            require(result["hardware"]["container_engine"] in ("docker", "podman")
                    and isinstance(result["hardware"]["runtime_version"], str)
                    and result["hardware"]["runtime_version"], "missing actual container engine/version")
            require(isinstance(result["toolchain"]["container_client_version"], str)
                    and result["toolchain"]["container_client_version"]
                    and isinstance(result["toolchain"]["python_version"], str)
                    and result["toolchain"]["python_version"], "missing actual measurement tool versions")
            require(result["cleanup"]["passed"] is True and result["cleanup"]["errors"] == [],
                    "owned-resource cleanup did not complete")
        images = result["images"]
        require(set(images) == set(VARIANTS), "incomplete build-tag matrix")
        for variant, item in images.items():
            require(re.fullmatch(r"sha256:[0-9a-f]{64}", item["image_id"]) is not None, "missing image ID")
            require(item["os"] == "linux" and item["architecture"] == result["hardware"]["architecture"],
                    "image and runtime architecture differ")
            for key in (("bytes", "binary_bytes", "frontend_bytes", "packed_bytes") if version == 2
                        else ("binary_bytes", "frontend_bytes", "packed_bytes")):
                positive(item[key], variant + "." + key)
                if version == 3:
                    byte_count(item[key], variant + "." + key)
            for key in ("binary_sha256", "frontend_sha256", "packed_sha256"):
                require(re.fullmatch(r"[0-9a-f]{64}", item[key]) is not None, "missing packaged content fingerprint")
            if version == 3:
                validate_image_metrics(item)
                byte_count(item["image_uncompressed_layer_bytes"], "application image layers")
                byte_count(item["engine_reported_image_bytes"], "engine-reported image", allow_zero=True)
                require(item["bytes"] == item["engine_reported_image_bytes"], "legacy image diagnostic alias differs")
                require(item["build_tags"] == VARIANTS[variant] and item["cgo_enabled"] is False,
                        "packaging variant differs")
                relative = item["image_archive_path"]
                require(isinstance(relative, str) and relative == _member_name(relative) and relative != ".",
                        "unsafe measured archive path")
                if artifact_root is not None:
                    root = Path(artifact_root).resolve()
                    path = root / relative
                    require(not path.is_symlink() and path.resolve().is_relative_to(root),
                            "measured image archive escapes artifact root")
                    actual = measure_image_archive(path, item["image_config_sha256"], item["architecture"])
                    require(all(item[key] == value for key, value in actual.items()),
                            "reported image measurements differ from preserved archive")
        runtime = result["runtime"]
        require(runtime["image_id"] == images["default"]["image_id"], "measured runtime uses another image")
        require(runtime["uid"] == 1001 and runtime["tls_verified"] is True
                and runtime["profile"] == "production", "wrong production runtime profile")
        if version == 3:
            require(runtime["image_config_sha256"] == images["default"]["image_config_sha256"]
                    and runtime["image_binary_sha256"] == images["default"]["binary_sha256"],
                    "runtime content identity differs from measured image")
        expected_trials = result["parameters"]["trials"]
        count = result["parameters"]["restore_pipelines"]
        require(type(expected_trials) is int and expected_trials >= 1, "missing trial count")
        require(type(count) is int and count >= 1, "missing restore dataset")
        require(len(runtime["cold_trials"]) == expected_trials, "incomplete cold-start trials")
        require(len(runtime["restore_trials"]) == expected_trials, "incomplete restore trials")
        for trial in runtime["cold_trials"]:
            positive(trial["start_to_health_seconds"], "cold start")
            require(trial["health"]["status"] == "ok", "cold start never became healthy")
            if version == 3:
                require(trial["binary_sha256"] == runtime["image_binary_sha256"], "cold process executable differs")
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
            if version == 3:
                require(trial["binary_sha256"] == runtime["image_binary_sha256"], "restored process executable differs")
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
        "median start-to-health seconds": (statistics.median(t["start_to_health_seconds"] for t in runtime["cold_trials"]), 5),
        "median idle RSS MiB": (statistics.median(s["rss_bytes"] for t in runtime["cold_trials"] for s in t["idle_samples"]) / MIB, 250),
        "median restored start-to-health seconds": (statistics.median(t["start_to_health_seconds"] for t in runtime["restore_trials"]), 5),
    }
    if version == 2:
        # This threshold belongs to its historical engine-dependent definition.
        measurements["default image MiB (legacy engine report)"] = (images["default"]["bytes"] / MIB, 200)
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
        self.result = {"schema_version": 3, "status": "running", "scope": "packaging-startup-restore",
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
        self.result["limitations"].extend([
            "Image bytes is a legacy diagnostic alias of engine_reported_image_bytes, never the v3 image budget.",
            "Uncompressed image layer bytes includes layer tar headers/whiteouts/overwritten content; not merged rootfs or process memory.",
            "Saved archive bytes depends on transport/compression; compare the selected uncompressed layer metric."])

    def run(self, args, *, timeout=120, logged=False, input_text=None, check=True, sanitizer=None):
        self.sequence += 1
        log = self.out / f"command-{self.sequence:03d}.log"
        with (self.out / "commands.jsonl").open("a") as index:
            index.write(json.dumps({"log": log.name, "argv": [str(a) for a in args]}) + "\n")
        if logged:
            require(sanitizer is None, "streamed command cannot require output sanitization")
            with log.open("w") as output:
                proc = subprocess.run(args, cwd=self.root, text=True, input=input_text,
                                      stdout=output, stderr=subprocess.STDOUT, timeout=timeout)
            data = ""
        else:
            proc = subprocess.run(args, cwd=self.root, text=True, input=input_text,
                                  stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
            data = proc.stdout
            if sanitizer is not None:
                try:
                    data = sanitizer(data)
                except MeasurementError:
                    log.write_text("Image inspect response invalid; raw output redacted.\n")
                    raise
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
        return json.loads(self.cli(kind, "inspect", name, sanitizer=sanitize_image_inspect))[0]

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
            hardware["container_engine"] = "podman"
        else:
            architecture = {"x86_64": "amd64", "aarch64": "arm64"}.get(info["Architecture"], info["Architecture"])
            hardware = {"os": info["OSType"], "architecture": architecture, "cpus": info["NCPU"],
                        "memory_bytes": info["MemTotal"], "kernel": info["KernelVersion"],
                        "runtime_version": info["ServerVersion"]}
            hardware["container_engine"] = "docker"
        self.client_version = self.cli("version", "--format", "{{json .Client}}").strip()
        hardware["client_platform"] = platform.platform()
        hardware["other_running_containers"] = len(self.cli("ps", "-q").splitlines())
        self.result["hardware"] = hardware
        self.result["base_images"] = {}
        for tag in ("docker.io/library/node:20-alpine", "docker.io/library/golang:1.24-alpine", "docker.io/library/alpine:3.19"):
            # Existence checks must not log an unsanitized Config.Env either.
            present = self.cli("image", "inspect", "--format", "{{.Id}}", tag, check=False)
            if not present.strip():
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
            archive_path = self.out / ("image-" + variant + ".tar")
            save_args = ["save"]
            if self.result["hardware"]["container_engine"] == "podman":
                save_args += ["--format", "docker-archive"]
            self.cli(*save_args, "-o", str(archive_path), tag, logged=True, timeout=300)
            item.update(measure_image_archive(archive_path, expected_architecture=item["architecture"]))
            item["image_archive_path"] = archive_path.name
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
            "container_client_version": self.client_version,
            "python_version": platform.python_version(),
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
        binary_sha = self.cli("exec", container, "sha256sum", "/app/main").split()[0]
        require(binary_sha == self.result["images"]["default"]["binary_sha256"],
                "running executable differs from measured image")
        return container, base, {"start_to_health_seconds": elapsed, "health": health,
                                 "binary_sha256": binary_sha,
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
                   "image_config_sha256": self.result["images"]["default"]["image_config_sha256"],
                   "image_binary_sha256": self.result["images"]["default"]["binary_sha256"],
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
    parser.add_argument("--image-archive", type=Path, help="measure a selected image in a Docker-save or OCI tar; does not build/start")
    parser.add_argument("--image-config-digest", help="expected configuration SHA256, required for image-archive mode")
    parser.add_argument("--image-architecture", help="optional expected image architecture")
    args = parser.parse_args()
    if args.check and args.image_archive:
        parser.error("check and image-archive modes are mutually exclusive")
    if args.image_config_digest is not None and args.image_archive is None:
        parser.error("image-config-digest requires image-archive")
    if args.image_architecture is not None and args.image_archive is None:
        parser.error("image-architecture requires image-archive")
    if args.check:
        try:
            warnings = validate_result(json.loads(args.check.read_text()), artifact_root=args.check.parent)
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
    if args.image_archive is not None:
        report = {"format": "openetl-image-metrics/v1", "scope": "image-archive", "status": "failed",
                  "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
        try:
            require(args.image_config_digest is not None, "image-archive requires an expected configuration digest")
            report["metrics"] = measure_image_archive(args.image_archive, args.image_config_digest,
                                                      args.image_architecture)
            validate_image_metrics(report["metrics"])
            report["status"] = "passed"
        except MeasurementError as exc:
            report["error"] = str(exc)
            print("Image archive measurement failed: " + str(exc), file=sys.stderr)
        report["finished_at"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        write_json(args.out / "result.json", report)
        print(f"Image archive {report['status']}: {args.out / 'result.json'}")
        return 0 if report["status"] == "passed" else 1
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
            if bench.result["status"] == "passed":
                try:
                    bench.result["warnings"] = validate_result(bench.result, artifact_root=args.out)
                except (MeasurementError, ValueError, OSError) as exc:
                    bench.result["status"] = "failed"
                    bench.result["error"] = str(exc)
                    print("Baseline validation failed: " + str(exc), file=sys.stderr)
            write_json(args.out / "result.json", bench.result)
    print(f"Baseline {bench.result['status']}: {args.out / 'result.json'}", flush=True)
    return 0 if bench.result["status"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
