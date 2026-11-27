#!/usr/bin/env python3
"""Failure-boundary checks for the RA-8 measurement and CI parser."""
import copy
import gzip
import hashlib
import io
import json
from pathlib import Path
import subprocess
import sys
import tarfile
import tempfile
from types import SimpleNamespace
import unittest
from unittest import mock

import bench_baseline as baseline_module
from bench_baseline import (BINARY_METHOD, BUDGET_FORMAT, Baseline, ImageProbe, IMAGE_METHOD, MeasurementError, VARIANTS, canonical_image_content,
                            evaluate_size_budgets, json_hash, validate_budget_policy,
                            compare_image_probes, image_identity, prepare_owned_image_archive, provider_identity,
                            measure_image_archive, parse_proc_status, sanitize_image_inspect,
                            sanitize_engine_info, sanitize_engine_version,
                            validate_image_metrics, validate_result, wait_ready)


def valid_result():
    image = {"image_id": "sha256:" + "a" * 64, "os": "linux", "architecture": "arm64",
             "bytes": 140 * 1024**2, "binary_bytes": 64 * 1024**2, "frontend_bytes": 7000,
             "packed_bytes": 20000, "binary_sha256": "b" * 64, "frontend_sha256": "c" * 64,
             "packed_sha256": "d" * 64}
    sample = {"rss_bytes": 50 * 1024**2, "peak_rss_bytes": 60 * 1024**2}
    return {"schema_version": 2, "status": "passed", "scope": "packaging-startup-restore",
            "source": {"commit": "e" * 40, "sha256": "f" * 64},
            "hardware": {"os": "linux", "architecture": "arm64", "cpus": 2, "memory_bytes": 8 * 1024**3},
            "toolchain": {"go_version": "go version go1.24.13 linux/arm64"},
            "images": {variant: copy.deepcopy(image) for variant in VARIANTS},
            "parameters": {"trials": 1, "restore_pipelines": 16, "records_per_pipeline": 100},
            "runtime": {"image_id": image["image_id"], "uid": 1001, "tls_verified": True, "profile": "production",
                        "cold_trials": [{"start_to_health_seconds": 0.5, "health": {"status": "ok"},
                                         "idle_samples": [sample.copy(), sample.copy()]}],
                        "restore_trials": [{"start_to_health_seconds": 0.5, "restore_seconds": 0.1,
                                            "restore_peak_rss_bytes": 50 * 1024**2, "rss_bytes": 60 * 1024**2,
                                            "health": {"status": "ok"}, "pipeline_count": 16, "checkpoint_count": 16,
                                            "identity_sha256": "1" * 64,
                                            "restore_resource": {"method": "wait4.rusage.ru_maxrss", "platform": "linux",
                                                                 "native_rss_unit": "KiB", "exit_code": 0,
                                                                 "native_max_rss": 50 * 1024, "peak_rss_bytes": 50 * 1024**2,
                                                                 "elapsed_seconds": 0.1}}],
                        "fixture": {"identity_sha256": "1" * 64, "records_verified": 1600, "peak_rss_bytes": 60 * 1024**2}}}


def sha(body):
    return hashlib.sha256(body).hexdigest()


def json_bytes(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode()


def layer_tar(payload):
    stream = io.BytesIO()
    with tarfile.open(fileobj=stream, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        item = tarfile.TarInfo("app/main")
        item.size, item.mtime, item.mode = len(payload), 0, 0o755
        archive.addfile(item, io.BytesIO(payload))
    return stream.getvalue()


def write_tar(path, entries):
    with tarfile.open(path, mode="w", format=tarfile.USTAR_FORMAT) as archive:
        for name, body in entries:
            if isinstance(body, tarfile.TarInfo):
                archive.addfile(body)
            else:
                item = tarfile.TarInfo(name)
                item.size, item.mtime = len(body), 0
                archive.addfile(item, io.BytesIO(body))


def image_fixture(architecture="arm64", *, compressed=False, oci=False,
                  payload=b"known-app-fixture", wrong_diff=False, media_type=None):
    raw = [layer_tar(b"base-fixture"), layer_tar(payload)]
    encoded = [gzip.compress(body, mtime=0) if compressed else body for body in raw]
    diff_ids = ["sha256:" + sha(body) for body in raw]
    config = {"architecture": architecture, "os": "linux", "rootfs": {"type": "layers", "diff_ids": diff_ids},
              "config": {"Env": ["SECRET_SENTINEL=never_publish_this_value"]}}
    if wrong_diff:
        config["rootfs"]["diff_ids"] = ["sha256:" + "0" * 64, *diff_ids[1:]]
    config_body = json_bytes(config)
    config_sha = sha(config_body)
    entries = []
    if not oci:
        config_path = config_sha + ".json"
        entries.append((config_path, config_body))
        names = []
        for ordinal, body in enumerate(encoded):
            name = "blobs/sha256/" + sha(body) if compressed else "layer-" + sha(body) + "/layer.tar"
            entries.append((name, body))
            names.append(name)
        manifest = {"Config": config_path, "RepoTags": ["fixture:current"], "Layers": names}
    else:
        config_path = "blobs/sha256/" + config_sha
        entries.append((config_path, config_body))
        layers = []
        for body in encoded:
            digest = sha(body)
            entries.append(("blobs/sha256/" + digest, body))
            layers.append({"mediaType": media_type or "application/vnd.oci.image.layer.v1.tar" + ("+gzip" if compressed else ""),
                           "digest": "sha256:" + digest, "size": len(body)})
        image = {"schemaVersion": 2,
                 "config": {"mediaType": "application/vnd.oci.image.config.v1+json",
                            "digest": "sha256:" + config_sha, "size": len(config_body)}, "layers": layers}
        body = json_bytes(image)
        image_sha = sha(body)
        entries.append(("blobs/sha256/" + image_sha, body))
        manifest = {"mediaType": "application/vnd.oci.image.manifest.v1+json", "digest": "sha256:" + image_sha,
                    "size": len(body), "platform": {"os": "linux", "architecture": architecture}}
    return {"entries": entries, "manifest": manifest, "config_sha": config_sha,
            "raw": raw, "encoded": encoded, "payload": payload}


def fixture_archive(path, images, *, oci=False, extra=()):
    entries = {}
    for image in images:
        entries.update(image["entries"])
    if oci:
        entries["oci-layout"] = json_bytes({"imageLayoutVersion": "1.0.0"})
        entries["index.json"] = json_bytes({"schemaVersion": 2, "manifests": [i["manifest"] for i in images]})
    else:
        entries["manifest.json"] = json_bytes([i["manifest"] for i in images])
    write_tar(path, [*entries.items(), *extra])


def v3_result(root):
    result = valid_result()
    result["schema_version"] = 3
    result["source"].update({"dirty": False, "files": 1})
    result["hardware"].update({"container_engine": "docker", "runtime_version": "29.fixture"})
    result["toolchain"].update({"container_client_version": "29.fixture", "python_version": "3.fixture"})
    result["cleanup"] = {"passed": True, "errors": [], "images_retained": False}
    result["parameters"].update(cpus=2, memory_bytes=1024**3, gomaxprocs=2)
    result["harness_sha256"] = {name: "1" * 64 for name in
                                ("bench-baseline.sh", "bench_baseline.py", "cmd/process-measure/main.go", "cmd/process-measure/main_test.go")}
    source_files = [{"path": "main.go", "sha256": sha(b"source-fixture"), "bytes": len(b"source-fixture"), "mode": 0o644}]
    (root / "source-files.json").write_text(json.dumps(source_files))
    result["source"]["sha256"] = json_hash(source_files)
    for variant, tags in VARIANTS.items():
        image = image_fixture(payload=("fixture-" + variant).encode(), compressed=True)
        path = root / ("image-" + variant + ".tar")
        fixture_archive(path, [image])
        item = result["images"][variant]
        item.update(measure_image_archive(path, image["config_sha"]))
        item.update({"engine_reported_image_bytes": item["bytes"], "image_archive_path": path.name,
                     "build_tags": tags, "cgo_enabled": False, "binary_bytes": len(image["payload"]),
                     "binary_sha256": sha(image["payload"])})
    default = result["images"]["default"]
    result["runtime"].update({"image_config_sha256": default["image_config_sha256"],
                              "image_binary_sha256": default["binary_sha256"]})
    for key in ("cold_trials", "restore_trials"):
        for trial in result["runtime"][key]:
            trial["binary_sha256"] = default["binary_sha256"]
    return result


def fixture_policy(result):
    arch = result["hardware"]["architecture"]
    return {"format": BUDGET_FORMAT, "version": "test-fixture-1", "rationale": "Synthetic CLI boundary fixture only",
            "calibrations": [{"architecture": arch, "schema_version": 3, "source_dirty": False,
                              "scope": result["scope"], "source_commit": result["source"]["commit"],
                              "source_sha256": result["source"]["sha256"], "report_sha256": "1" * 64}],
            "rules": [{"id": "fixture-" + metric, "metric": metric, "method": method, "unit": "bytes",
                       "os": "linux", "architecture": arch, "variant": "default", "mode": "enforce",
                       "baseline_bytes": result["images"]["default"][metric], "max_bytes": 1024**2}
                      for metric, method in [("binary_bytes", BINARY_METHOD), ("image_uncompressed_layer_bytes", IMAGE_METHOD)]]}


class ReadinessTests(unittest.TestCase):
    def clock(self):
        ticks = iter(i / 10 for i in range(100))
        return lambda: next(ticks)

    def test_dead_process_cannot_pass_with_another_servers_healthy_response(self):
        with self.assertRaisesRegex(MeasurementError, "exited"):
            wait_ready(lambda: (200, {"status": "ok"}), lambda: False, 1)

    def test_exit_during_probe_is_failure(self):
        states = iter([True, False])
        with self.assertRaisesRegex(MeasurementError, "exited"):
            wait_ready(lambda: (200, {"status": "ok"}), lambda: next(states), 1)

    def test_nonhealthy_and_malformed_responses_time_out(self):
        for response in [(503, {"status": "ok"}), (200, {}), (200, {"status": "degraded"}), (200, [])]:
            with self.subTest(response=response), self.assertRaisesRegex(MeasurementError, "timed out"):
                wait_ready(lambda: response, lambda: True, 0.5, clock=self.clock(), sleep=lambda _: None)

    def test_connection_failure_does_not_become_a_time_measurement(self):
        def probe():
            raise ConnectionRefusedError("not listening")
        with self.assertRaisesRegex(MeasurementError, "timed out"):
            wait_ready(probe, lambda: True, 0.5, clock=self.clock(), sleep=lambda _: None)

    def test_retry_until_live_json_health(self):
        responses = iter([(503, {}), (200, {"status": "ok"})])
        self.assertEqual(wait_ready(lambda: next(responses), lambda: True, 1,
                                    clock=self.clock(), sleep=lambda _: None), {"status": "ok"})

    def test_process_rss_is_required_not_empty_zero(self):
        for raw in ["", "VmRSS: 0 kB\nVmHWM: 0 kB", "VmRSS: 2048 kB\nVmHWM: 1024 kB"]:
            with self.subTest(raw=raw), self.assertRaises(MeasurementError):
                parse_proc_status(raw)
        self.assertEqual(parse_proc_status("VmRSS: 1024 kB\nVmHWM: 2048 kB")['rss_bytes'], 1024**2)


class ResultTests(unittest.TestCase):
    def test_healthcheck_inspect_locations_on_docker_and_podman(self):
        base = {"Id": "a" * 64, "Size": 100, "Os": "linux", "Architecture": "arm64"}
        health = {"Test": ["CMD-SHELL", "wget -qO- http://localhost:8000/api/v2/health"]}
        for fields in [{"Config": {"Healthcheck": health}}, {"Config": {}, "Healthcheck": health}]:
            with self.subTest(fields=fields):
                self.assertEqual(image_identity({**base, **fields})["healthcheck"], health)

    def test_valid_result_and_budget_warning(self):
        result = valid_result()
        self.assertEqual(validate_result(result), [])
        result["runtime"]["cold_trials"][0]["start_to_health_seconds"] = 6
        self.assertEqual(len(validate_result(result)), 1)

    def test_old_time_format_and_incorrect_rss_units_are_rejected(self):
        result = valid_result()
        result["schema_version"] = 1
        with self.assertRaises(MeasurementError):
            validate_result(result)
        result = valid_result()
        result["runtime"]["restore_trials"][0]["restore_resource"]["native_max_rss"] *= 4
        with self.assertRaisesRegex(MeasurementError, "conversion"):
            validate_result(result)

    def test_missing_nonfinite_zero_string_and_boolean_are_not_metrics(self):
        for value in [None, float("nan"), float("inf"), 0, -1, "0.59", True]:
            result = valid_result()
            result["runtime"]["cold_trials"][0]["start_to_health_seconds"] = value
            with self.subTest(value=value), self.assertRaises(MeasurementError):
                validate_result(result)

    def test_incomplete_and_mismatched_evidence_cannot_pass(self):
        paths = [("source", "sha256"), ("images", "extism"), ("runtime", "cold_trials"),
                 ("runtime", "restore_trials"), ("toolchain", "go_version")]
        for first, second in paths:
            result = valid_result()
            del result[first][second]
            with self.subTest(path=(first, second)), self.assertRaises(MeasurementError):
                validate_result(result)
        for key, value in [("checkpoint_count", 15), ("pipeline_count", 15), ("identity_sha256", "0" * 64)]:
            result = valid_result()
            result["runtime"]["restore_trials"][0][key] = value
            with self.subTest(key=key), self.assertRaises(MeasurementError):
                validate_result(result)

    def test_cli_measurement_failure_fails_but_threshold_breach_warns(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "result.json"
            result = valid_result()
            result["runtime"]["cold_trials"][0]["start_to_health_seconds"] = 6
            path.write_text(json.dumps(result))
            command = [sys.executable, str(Path(__file__).with_name("bench_baseline.py")), "--check", str(path)]
            passed = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(passed.returncode, 0, passed.stderr)
            self.assertIn("::warning::", passed.stdout)
            result["status"] = "failed"
            path.write_text(json.dumps(result))
            failed = subprocess.run(command, capture_output=True, text=True)
            self.assertNotEqual(failed.returncode, 0)
            self.assertIn("::error::", failed.stderr)


class ArchiveMetricTests(unittest.TestCase):
    def test_docker_and_oci_raw_and_gzip_have_the_same_expanded_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            expected = None
            for oci in (False, True):
                for compressed in (False, True):
                    image = image_fixture(oci=oci, compressed=compressed)
                    path = Path(directory) / f"{oci}-{compressed}.tar"
                    fixture_archive(path, [image], oci=oci)
                    result = measure_image_archive(path, "sha256:" + image["config_sha"])
                    validate_image_metrics(result)
                    self.assertEqual(result["method"], IMAGE_METHOD)
                    self.assertEqual(result["unit"], "bytes")
                    self.assertEqual(result["image_uncompressed_layer_bytes"], sum(len(b) for b in image["raw"]))
                    self.assertEqual(result["image_archive_sha256"], sha(path.read_bytes()))
                    self.assertEqual(result["image_archive_bytes"], path.stat().st_size)
                    self.assertEqual([v["diff_id"] for v in result["layer_inventory"]],
                                     ["sha256:" + sha(b) for b in image["raw"]])
                    expected = expected or result["image_uncompressed_layer_bytes"]
                    self.assertEqual(result["image_uncompressed_layer_bytes"], expected)
                    self.assertNotIn("never_publish_this_value", json.dumps(result))
                    self.assertNotIn("Env", json.dumps(result))

    def test_selects_one_platform_and_rejects_unknown_or_ambiguous_selection(self):
        with tempfile.TemporaryDirectory() as directory:
            for oci in (False, True):
                arm = image_fixture(oci=oci)
                amd = image_fixture("amd64", oci=oci, payload=b"different-platform" * 4000)
                path = Path(directory) / f"multi-{oci}.tar"
                fixture_archive(path, [arm, amd], oci=oci)
                result = measure_image_archive(path, arm["config_sha"])
                self.assertEqual(result["architecture"], "arm64")
                self.assertEqual(result["image_uncompressed_layer_bytes"], sum(map(len, arm["raw"])))
                with self.assertRaisesRegex(MeasurementError, "selection"):
                    measure_image_archive(path, "0" * 64)
                with self.assertRaisesRegex(MeasurementError, "selection"):
                    measure_image_archive(path, arm["config_sha"], "amd64")
                with self.assertRaises(MeasurementError):
                    measure_image_archive(path)
            path = Path(directory) / "ambiguous.tar"
            arm = image_fixture()
            fixture_archive(path, [arm, image_fixture(payload=b"second-arm-image")])
            with self.assertRaisesRegex(MeasurementError, "ambiguous"):
                measure_image_archive(path, expected_architecture="arm64")

    def test_digest_missing_blob_unsafe_path_and_unknown_encoding_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.tar"
            wrong = image_fixture(wrong_diff=True)
            fixture_archive(path, [wrong])
            with self.assertRaisesRegex(MeasurementError, "diff ID"):
                measure_image_archive(path, wrong["config_sha"])
            image = image_fixture()
            missing = copy.deepcopy(image)
            missing["entries"] = missing["entries"][:-1]
            fixture_archive(path, [missing])
            with self.assertRaisesRegex(MeasurementError, "missing"):
                measure_image_archive(path, image["config_sha"])
            for name in ("../escape", "/absolute", "a/../../escape", "a\\escape"):
                fixture_archive(path, [image], extra=[(name, b"not-extracted")])
                with self.subTest(name=name), self.assertRaisesRegex(MeasurementError, "unsafe"):
                    measure_image_archive(path, image["config_sha"])
            unsupported = image_fixture(oci=True, media_type="application/vnd.oci.image.layer.v1.tar+zstd")
            fixture_archive(path, [unsupported], oci=True)
            with self.assertRaisesRegex(MeasurementError, "encoding"):
                measure_image_archive(path, unsupported["config_sha"])
            self.assertFalse((Path(directory) / "escape").exists())

    def test_links_duplicates_malformed_metadata_and_size_limits_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.tar"
            image = image_fixture(compressed=True)
            link = tarfile.TarInfo("link")
            link.type, link.linkname = tarfile.SYMTYPE, "../../secret"
            fixture_archive(path, [image], extra=[("link", link)])
            with self.assertRaisesRegex(MeasurementError, "links"):
                measure_image_archive(path, image["config_sha"])
            fixture_archive(path, [image], extra=[image["entries"][0]])
            with self.assertRaisesRegex(MeasurementError, "duplicate"):
                measure_image_archive(path, image["config_sha"])
            fixture_archive(path, [image])
            with mock.patch.object(baseline_module, "MAX_LAYER_BYTES", 4096), self.assertRaisesRegex(MeasurementError, "limit"):
                measure_image_archive(path, image["config_sha"])
            with mock.patch.object(baseline_module, "MAX_METADATA_BYTES", 64), self.assertRaisesRegex(MeasurementError, "limit"):
                measure_image_archive(path, image["config_sha"])
            path.write_bytes(b"not a tar archive")
            with self.assertRaises(MeasurementError):
                measure_image_archive(path, image["config_sha"])

    def test_changed_archive_is_rejected_after_reading(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.tar"
            image = image_fixture()
            fixture_archive(path, [image])
            original = baseline_module._hash_file
            calls = 0
            def mutate_after_first_hash(source):
                nonlocal calls
                result = original(source)
                calls += 1
                if calls == 1:
                    with path.open("r+b") as output:
                        output.seek(-1, 2)
                        output.write(b"\x01")
                return result
            with mock.patch.object(baseline_module, "_hash_file", side_effect=mutate_after_first_hash), \
                    self.assertRaisesRegex(MeasurementError, "changed"):
                measure_image_archive(path, image["config_sha"])

    def test_oci_descriptor_size_and_encoded_digest_mismatches_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.tar"
            image = image_fixture(oci=True, compressed=True)
            bad_size = copy.deepcopy(image)
            bad_size["manifest"]["size"] += 1
            fixture_archive(path, [bad_size], oci=True)
            with self.assertRaisesRegex(MeasurementError, "descriptor size"):
                measure_image_archive(path, image["config_sha"])
            corrupted = copy.deepcopy(image)
            name, body = corrupted["entries"][0]
            corrupted["entries"][0] = (name, body.replace(b"arm64", b"amd64"))
            fixture_archive(path, [corrupted], oci=True)
            with self.assertRaisesRegex(MeasurementError, "metadata digest"):
                measure_image_archive(path, image["config_sha"])

    def test_truncated_and_corrupt_gzip_and_metadata_do_not_leak_contents(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.tar"
            image = image_fixture(compressed=True)
            for transform in (lambda b: b[:-5], lambda b: b[:-8] + bytes(8)):
                candidate = copy.deepcopy(image)
                name, body = candidate["entries"][-1]
                candidate["entries"][-1] = (name, transform(body))
                fixture_archive(path, [candidate])
                with self.subTest(transform=transform), self.assertRaises(MeasurementError) as error:
                    measure_image_archive(path, image["config_sha"])
                self.assertNotIn("never_publish_this_value", str(error.exception))
            body = b'{"config":{"Env":["never_publish_this_value"]},'
            config_sha = sha(body)
            write_tar(path, [(config_sha + ".json", body),
                             ("manifest.json", json_bytes([{"Config": config_sha + ".json", "Layers": []}]))])
            with self.assertRaisesRegex(MeasurementError, "metadata JSON") as error:
                measure_image_archive(path, config_sha)
            self.assertNotIn("never_publish_this_value", str(error.exception))

    def test_pax_metadata_and_directory_counts_are_bounded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "fixture.tar"
            image = image_fixture()
            directory_entry = tarfile.TarInfo("many-directories")
            directory_entry.type = tarfile.DIRTYPE
            fixture_archive(path, [image], extra=[(directory_entry.name, directory_entry)])
            with mock.patch.object(baseline_module, "MAX_MEMBERS", len(image["entries"]) + 1), \
                    self.assertRaisesRegex(MeasurementError, "too many"):
                measure_image_archive(path, image["config_sha"])
            with tarfile.open(path, mode="w", format=tarfile.PAX_FORMAT) as archive:
                item = tarfile.TarInfo("large-pax")
                item.pax_headers = {"comment": "x" * 1024}
                archive.addfile(item)
            with mock.patch.object(baseline_module, "MAX_METADATA_BYTES", 512), \
                    self.assertRaisesRegex(MeasurementError, "metadata exceeds"):
                measure_image_archive(path, image["config_sha"])

    def test_archive_cli_passes_and_wrong_config_produces_failed_report_without_secrets(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "fixture.tar"
            image = image_fixture(oci=True, compressed=True)
            fixture_archive(path, [image], oci=True)
            command = [sys.executable, str(Path(baseline_module.__file__)), "--image-archive", str(path)]
            good = subprocess.run([*command, "--image-config-digest", image["config_sha"], "--out", str(root / "good")],
                                  capture_output=True, text=True)
            self.assertEqual(good.returncode, 0, good.stderr)
            report = json.loads((root / "good/result.json").read_text())
            self.assertEqual(report["format"], "openetl-image-metrics/v1")
            self.assertEqual(report["status"], "passed")
            self.assertEqual(report["metrics"]["image_uncompressed_layer_bytes"], sum(map(len, image["raw"])))
            bad = subprocess.run([*command, "--image-config-digest", "0" * 64, "--out", str(root / "bad")],
                                 capture_output=True, text=True)
            self.assertEqual(bad.returncode, 1, bad.stderr)
            failed = (root / "bad/result.json").read_text()
            self.assertEqual(json.loads(failed)["status"], "failed")
            self.assertNotIn("never_publish_this_value", failed + good.stdout + bad.stderr)
            missing = subprocess.run([*command, "--out", str(root / "missing")], capture_output=True, text=True)
            self.assertEqual(missing.returncode, 1)
            self.assertEqual(json.loads((root / "missing/result.json").read_text())["status"], "failed")


class V3EvidenceTests(unittest.TestCase):
    def test_v3_checks_preserved_archives_and_v2_remains_readable(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            result = v3_result(root)
            self.assertEqual(validate_result(result, artifact_root=root), [])
            (root / "result.json").write_text(json.dumps(result))
            command = [sys.executable, baseline_module.__file__, "--check", str(root / "result.json")]
            proc = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(proc.returncode, 0, proc.stderr)
            corrupted = root / result["images"]["default"]["image_archive_path"]
            corrupted.write_bytes(b"missing original bytes")
            proc = subprocess.run(command, capture_output=True, text=True)
            self.assertEqual(proc.returncode, 1)
            self.assertEqual(validate_result(valid_result()), [])

    def test_v3_units_inventory_variant_cleanup_and_executable_mismatches_fail(self):
        with tempfile.TemporaryDirectory() as directory:
            result = v3_result(Path(directory))
            mutations = [
                lambda r: r["images"]["default"].update(unit="MiB"),
                lambda r: r["images"]["default"].update(image_uncompressed_layer_bytes=1),
                lambda r: r["images"]["default"].update(build_tags="nolua"),
                lambda r: r["images"]["default"].update(image_archive_path="../escape.tar"),
                lambda r: r["cleanup"].update(passed=False, errors=["failed"]),
                lambda r: r["runtime"]["cold_trials"][0].update(binary_sha256="0" * 64),
                lambda r: r["runtime"].update(image_config_sha256="0" * 64),
                lambda r: r["hardware"].update(container_engine="fake-engine"),
            ]
            for change in mutations:
                candidate = copy.deepcopy(result)
                change(candidate)
                with self.subTest(change=change), self.assertRaises(MeasurementError):
                    validate_result(candidate)

    def test_inspect_sanitization_and_command_log_never_store_env(self):
        raw = json.dumps([{"Id": "sha256:" + "a" * 64, "Size": 10, "Architecture": "arm64", "Os": "linux",
                           "Config": {"Env": ["TOKEN=secret-inspect-sentinel"], "Labels": {"private": "secret-inspect-sentinel"}}}])
        self.assertNotIn("secret-inspect-sentinel", sanitize_image_inspect(raw))
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory); out = root / "out"; out.mkdir(); private = root / "private"; private.mkdir()
            args = SimpleNamespace(root=root, out=out, trials=3, settle_seconds=10, idle_samples=5, restore_pipelines=16)
            baseline = Baseline(args, private)
            with mock.patch.object(baseline_module.subprocess, "run", return_value=SimpleNamespace(stdout=raw, stderr="secret-inspect-sentinel", returncode=0)):
                baseline.run(["fixture", "image", "inspect"], sanitizer=sanitize_image_inspect)
            self.assertNotIn("secret-inspect-sentinel", (out / "command-001.log").read_text())

    def test_cleanup_failure_cannot_be_a_successful_baseline(self):
        class FailedCleanup:
            def __init__(self, args, private):
                self.result = valid_result()
            def snapshot(self): pass
            def hardware(self): pass
            def build(self): pass
            def measure(self): pass
            def cleanup(self):
                raise MeasurementError("owned cleanup failed")
        with tempfile.TemporaryDirectory() as directory:
            out = Path(directory) / "out"
            with mock.patch.object(baseline_module, "Baseline", FailedCleanup), \
                    mock.patch.object(sys, "argv", ["bench", "--out", str(out)]):
                self.assertEqual(baseline_module.main(), 1)
            self.assertEqual(json.loads((out / "result.json").read_text())["status"], "failed")


class EngineProbeTests(unittest.TestCase):
    def test_only_unreferenced_safe_podman_legacy_aliases_are_ignored(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "archive.tar"
            image = image_fixture()
            name = "a" * 64 + "/layer.tar"
            target = sha(image["raw"][0]) + ".tar"
            alias = tarfile.TarInfo(name); alias.type = tarfile.SYMTYPE; alias.linkname = "../" + target
            fixture_archive(path, [image], extra=[(target, image["raw"][0]), (name, alias)])
            actual = measure_image_archive(path, image["config_sha"])
            self.assertEqual(actual["ignored_legacy_layer_aliases"], 1)
            referenced = copy.deepcopy(image); referenced["manifest"]["Layers"][0] = name
            fixture_archive(path, [referenced], extra=[(target, image["raw"][0]), (name, alias)])
            with self.assertRaisesRegex(MeasurementError, "missing a layer"):
                measure_image_archive(path, image["config_sha"])
            for linkname in ("../../" + target, "/" + target, "../" + "f" * 64 + ".tar"):
                alias.linkname = linkname
                fixture_archive(path, [image], extra=[(target, image["raw"][0]), (name, alias)])
                with self.subTest(linkname=linkname), self.assertRaises(MeasurementError):
                    measure_image_archive(path, image["config_sha"])

    def test_provider_comes_from_server_and_info_logs_exclude_private_fields(self):
        info = {"ServerVersion": "29.fixture", "ID": "fixture-docker", "HTTPProxy": "private-engine-sentinel"}
        version = {"Client": {"Version": "29.client"},
                   "Server": {"Version": "29.fixture", "Components": [{"Name": "Engine", "Version": "29.fixture",
                                                                         "Details": {"private": "private-engine-sentinel"}}]}}
        self.assertEqual(provider_identity(info, version)["id"], "docker")
        self.assertNotIn("private-engine-sentinel", sanitize_engine_info(json.dumps(info)))
        self.assertNotIn("private-engine-sentinel", sanitize_engine_version(json.dumps(version)))
        podman_info = {"host": {"os": "linux", "hostname": "fixture-podman"}, "version": {"Version": "5.fixture"}}
        podman_version = {"Client": {"Version": "5.client"}, "Server": {"Version": "5.fixture"}}
        self.assertEqual(provider_identity(podman_info, podman_version)["id"], "podman")
        daemonless = {"Client": {"Version": "5.fixture", "Os": "linux"}}
        self.assertEqual(provider_identity(podman_info, daemonless)["connection"], "daemonless-local")
        for client in ({"Version": "other", "Os": "linux"}, {"Version": "5.fixture", "Os": "darwin"}):
            with self.subTest(client=client), self.assertRaisesRegex(MeasurementError, "daemonless"):
                provider_identity(podman_info, {"Client": client})
        with self.assertRaisesRegex(MeasurementError, "versions disagree"):
            provider_identity(info, {**version, "Server": {**version["Server"], "Version": "different"}})
        with self.assertRaisesRegex(MeasurementError, "unrecognized"):
            provider_identity(info, {**version, "Server": {**version["Server"], "Platform": {"Name": "Podman Engine"}}})

    def test_owned_transport_changes_only_tags_and_selects_one_image(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for oci in (False, True):
                for compressed in (False, True):
                    image = image_fixture(oci=oci, compressed=compressed)
                    original = root / "original.tar"
                    fixture_archive(original, [image, image_fixture("amd64", oci=oci, payload=b"other-platform")], oci=oci)
                    before = original.read_bytes()
                    metrics = measure_image_archive(original, image["config_sha"])
                    owned = root / "owned.tar"
                    actual = prepare_owned_image_archive(original, owned, metrics, "localhost/owned-fixture:selected")
                    self.assertEqual(canonical_image_content(actual), canonical_image_content(metrics))
                    self.assertEqual(original.read_bytes(), before)
                    with tarfile.open(owned) as archive:
                        manifest = json.load(archive.extractfile("manifest.json"))
                    self.assertEqual(len(manifest), 1)
                    self.assertEqual(manifest[0]["RepoTags"], ["localhost/owned-fixture:selected"])

    def test_conformance_recomputes_archives_and_rejects_alias_or_content_change(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            original = root / "input.tar"; image = image_fixture()
            fixture_archive(original, [image])
            metrics = measure_image_archive(original, image["config_sha"])
            reports, directories = [], []
            for provider in ("docker", "podman"):
                directory = root / provider; directory.mkdir(); directories.append(directory)
                fixture_archive(directory / "image.tar", [image])
                report = {"format": "openetl-image-engine-probe/v1", "scope": "image-only", "status": "passed",
                          "cleanup": {"passed": True, "errors": []}, "image_archive_path": "image.tar",
                          "provider": {"id": provider, "server_version": provider + "-fixture",
                                       "server": {"Version": provider + "-fixture"}}, "harness_sha256": "1" * 64,
                          "input_metrics": metrics, "metrics": metrics, "engine_diagnostics": {}}
                reports.append(report)
            self.assertEqual(compare_image_probes(*reports, artifact_roots=directories)["status"], "passed")
            alias = copy.deepcopy(reports[1]); alias["provider"] = reports[0]["provider"]
            with self.assertRaisesRegex(MeasurementError, "aliases"):
                compare_image_probes(reports[0], alias)
            bad = copy.deepcopy(reports[1]); bad["input_metrics"]["image_archive_sha256"] = "0" * 64
            with self.assertRaisesRegex(MeasurementError, "same saved"):
                compare_image_probes(reports[0], bad)
            bad = copy.deepcopy(reports[1]); bad["cleanup"] = {"passed": False, "errors": ["failed"]}
            with self.assertRaisesRegex(MeasurementError, "not passed"):
                compare_image_probes(reports[0], bad)
            (directories[1] / "image.tar").write_bytes(b"lost archive")
            with self.assertRaises(MeasurementError):
                compare_image_probes(*reports, artifact_roots=directories)

    def test_probe_never_starts_image_and_cleans_only_unique_owned_tag(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); out = root / "out"; out.mkdir(); private = root / "private"; private.mkdir()
            original = root / "input.tar"; image = image_fixture(); fixture_archive(original, [image])
            args = SimpleNamespace(root=Path(__file__).resolve().parent.parent, out=out, container_cli="fixture-cli",
                                   image_archive=original, image_config_digest=image["config_sha"], image_architecture=None)
            with mock.patch.object(baseline_module.subprocess, "check_output", return_value="e" * 40):
                probe = ImageProbe(args, private)
            commands = []
            def command(*argv, **options):
                commands.append(argv)
                if argv[0] == "info":
                    return json.dumps({"host": {"hostname": "podman-fixture"}, "version": {"Version": "5.fixture"}}), 0
                if argv[0] == "version":
                    return json.dumps({"Client": {"Version": "5.client"}, "Server": {"Version": "5.fixture"}}), 0
                if argv[:2] == ("image", "inspect") and "--format" not in argv:
                    return json.dumps([{"Id": "sha256:" + image["config_sha"], "Size": 1, "Os": "linux", "Architecture": "arm64",
                                        "RootFS": {"Layers": ["sha256:" + sha(b) for b in image["raw"]]}}]), 0
                if argv[0] == "save":
                    (out / "image.tar").write_bytes((private / "owned-image.tar").read_bytes())
                return "", 0
            with mock.patch.object(probe, "command", side_effect=command):
                probe.measure(); probe.cleanup()
            self.assertEqual(probe.report["status"], "passed")
            self.assertFalse(any(argv[0] in ("run", "start", "create") for argv in commands))
            self.assertIn(("image", "rm", probe.tag), commands)
            self.assertFalse(any(argv[0] == "image" and argv[1] == "rm" and argv[2] != probe.tag for argv in commands))
            probe.load_attempted = True
            with mock.patch.object(probe, "command", side_effect=[("id", 0), MeasurementError("owned rm failed")]), \
                    self.assertRaisesRegex(MeasurementError, "cleanup failed"):
                probe.cleanup()
            self.assertFalse(probe.report["cleanup"]["passed"])


class BudgetTests(unittest.TestCase):
    def run_check(self, root, result, policy=None, *, enforce=False, output=None):
        path = root / "result.json"; path.write_text(json.dumps(result))
        command = [sys.executable, baseline_module.__file__, "--check", str(path)]
        if policy is not None:
            policy_path = root / "policy.json"; policy_path.write_text(json.dumps(policy))
            command += ["--budgets", str(policy_path)]
        if enforce:
            command += ["--enforce-budgets"]
        if output is not None:
            command += ["--out", str(output)]
        return subprocess.run(command, capture_output=True, text=True)

    def test_positive_and_low_budgets_have_distinct_exit_codes_and_preserve_measurements(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); result = v3_result(root); policy = fixture_policy(result)
            for metric in ("binary_bytes", "image_uncompressed_layer_bytes"):
                low = copy.deepcopy(policy)
                for rule in low["rules"]:
                    if rule["metric"] == metric:
                        rule["max_bytes"] = result["images"]["default"][metric] - 1
                before = sha((root / "image-default.tar").read_bytes())
                observed = self.run_check(root, result, low, output=root / (metric + "-warn"))
                self.assertEqual(observed.returncode, 0, observed.stderr)
                self.assertIn("::warning::", observed.stdout)
                audit = json.loads((root / (metric + "-warn") / "budget-result.json").read_text())
                self.assertEqual(audit["status"], "warning"); self.assertTrue(audit["data_valid"])
                enforced = self.run_check(root, result, low, enforce=True, output=root / (metric + "-enforce"))
                self.assertEqual(enforced.returncode, 1, enforced.stderr)
                self.assertIn("::error::", enforced.stderr)
                audit = json.loads((root / (metric + "-enforce") / "budget-result.json").read_text())
                self.assertEqual(audit["status"], "failed"); self.assertTrue(audit["data_valid"])
                self.assertEqual(before, sha((root / "image-default.tar").read_bytes()))
                self.assertEqual(json.loads((root / "result.json").read_text()), result)
            for rule in policy["rules"]:
                rule["max_bytes"] = result["images"]["default"][rule["metric"]]
            proc = self.run_check(root, result, policy, enforce=True, output=root / "normal")
            self.assertEqual(proc.returncode, 0, proc.stderr)
            audit = json.loads((root / "normal/budget-result.json").read_text())
            self.assertEqual(audit["status"], "passed")
            self.assertEqual(audit["policy_sha256"], sha((root / "policy.json").read_bytes()))
            self.assertEqual(len(audit["metrics"]), 2)

    def test_normalized_image_budget_never_falls_back_to_engine_size(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); result = v3_result(root); policy = fixture_policy(result)
            result["images"]["default"].update(bytes=0, engine_reported_image_bytes=0)
            policy["rules"][1]["max_bytes"] = 1
            proc = self.run_check(root, result, policy, enforce=True)
            self.assertEqual(proc.returncode, 1)
            self.assertIn(str(result["images"]["default"]["image_uncompressed_layer_bytes"]), proc.stderr)

    def test_measurement_errors_fail_in_both_modes_and_cannot_be_downgraded_to_warnings(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); result = v3_result(root); policy = fixture_policy(result)
            mutations = [lambda r: r["images"]["default"].pop("method"),
                         lambda r: r["images"]["default"].update(unit="MiB"),
                         lambda r: r["images"]["default"].update(binary_bytes=True),
                         lambda r: r["images"]["default"].update(binary_bytes=0),
                         lambda r: r["images"]["default"].update(binary_bytes=2**100),
                         lambda r: r["images"]["default"].update(bytes=False, engine_reported_image_bytes=0),
                         lambda r: r.update(status="failed"),
                         lambda r: r["parameters"].update(memory_bytes=2 * 1024**3),
                         lambda r: r["parameters"].update(memory_bytes=float(1024**3)),
                         lambda r: r["hardware"].update(memory_bytes=float(8 * 1024**3)),
                         lambda r: r["harness_sha256"].pop("bench_baseline.py")]
            for change in mutations:
                candidate = copy.deepcopy(result); change(candidate)
                for enforce in (False, True):
                    with self.subTest(change=change, enforce=enforce):
                        proc = self.run_check(root, candidate, policy, enforce=enforce)
                        self.assertEqual(proc.returncode, 1, proc.stdout)
                        self.assertIn("::error::", proc.stderr)
            (root / "source-files.json").write_text('[]')
            for enforce in (False, True):
                proc = self.run_check(root, result, policy, enforce=enforce)
                self.assertEqual(proc.returncode, 1)
                self.assertIn("source inventory fingerprint", proc.stderr)

    def test_v2_dirty_missing_policy_and_uncovered_architecture_cannot_enforce(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); result = v3_result(root); policy = fixture_policy(result)
            legacy = valid_result()
            self.assertEqual(self.run_check(root, legacy).returncode, 0)
            self.assertEqual(self.run_check(root, legacy, policy).returncode, 0)
            self.assertEqual(self.run_check(root, legacy, policy, enforce=True).returncode, 1)
            dirty = copy.deepcopy(result); dirty["source"]["dirty"] = True
            observed = self.run_check(root, dirty, policy, output=root / "dirty")
            self.assertEqual(observed.returncode, 0, observed.stderr)
            self.assertFalse(json.loads((root / "dirty/budget-result.json").read_text())["eligible_for_enforcement"])
            self.assertEqual(self.run_check(root, dirty, policy, enforce=True).returncode, 1)
            self.assertEqual(self.run_check(root, result, enforce=True).returncode, 1)
            policy["calibrations"][0]["architecture"] = "amd64"
            for rule in policy["rules"]:
                rule["architecture"] = "amd64"
            observed = self.run_check(root, result, policy, output=root / "uncovered")
            self.assertEqual(observed.returncode, 0, observed.stderr)
            self.assertEqual(json.loads((root / "uncovered/budget-result.json").read_text())["status"], "not_applicable")
            self.assertEqual(self.run_check(root, result, policy, enforce=True).returncode, 1)

    def test_malformed_policy_selectors_methods_limits_and_duplicate_fields_fail(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); result = v3_result(root); policy = fixture_policy(result)
            mutations = [lambda p: p.update(format="v2"), lambda p: p["rules"][0].update(method="engine.Size"),
                         lambda p: p["rules"][0].update(unit="MiB"), lambda p: p["rules"][0].update(variant="extism"),
                         lambda p: p["rules"][0].update(os="windows"), lambda p: p["rules"].pop(),
                         lambda p: p["rules"].append(copy.deepcopy(p["rules"][0]))]
            for value in (None, True, 0, -1, 1.5, float("nan"), float("inf"), 2**100):
                mutations.append(lambda p, value=value: p["rules"][0].update(max_bytes=value))
            for change in mutations:
                candidate = copy.deepcopy(policy); change(candidate)
                for enforce in (False, True):
                    with self.subTest(change=change, enforce=enforce):
                        proc = self.run_check(root, result, candidate, enforce=enforce)
                        self.assertEqual(proc.returncode, 1, proc.stdout)
            path = root / "result.json"; path.write_text(json.dumps(result))
            bad = root / "invalid-policy.json"
            bad.write_text('{"format":"private-policy-sentinel","format":"duplicate"}')
            proc = subprocess.run([sys.executable, baseline_module.__file__, "--check", str(path), "--budgets", str(bad)],
                                  capture_output=True, text=True)
            self.assertEqual(proc.returncode, 1)
            self.assertNotIn("private-policy-sentinel", proc.stdout + proc.stderr)

    def test_time_and_rss_warnings_do_not_become_size_enforcement_failures(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); result = v3_result(root); policy = fixture_policy(result)
            result["runtime"]["cold_trials"][0]["start_to_health_seconds"] = 6
            result["runtime"]["restore_trials"][0]["start_to_health_seconds"] = 6
            for sample in result["runtime"]["cold_trials"][0]["idle_samples"]:
                sample.update(rss_bytes=300 * 1024**2, peak_rss_bytes=310 * 1024**2)
            proc = self.run_check(root, result, policy, enforce=True, output=root / "timing")
            self.assertEqual(proc.returncode, 0, proc.stderr)
            self.assertIn("::warning::", proc.stdout)
            audit = json.loads((root / "timing/budget-result.json").read_text())
            self.assertEqual(audit["status"], "passed"); self.assertEqual(len(audit["warnings"]), 3)
            self.assertEqual(audit["unbudgeted_variants"], sorted(set(VARIANTS) - {"default"}))

    def test_check_output_never_writes_to_existing_or_symlinked_directories(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); result = v3_result(root); policy = fixture_policy(result)
            existing = root / "existing"; existing.mkdir(); (existing / "unrelated.txt").write_text("preserve me")
            proc = self.run_check(root, result, policy, output=existing)
            self.assertEqual(proc.returncode, 1)
            self.assertEqual((existing / "unrelated.txt").read_text(), "preserve me")
            self.assertFalse((existing / "budget-result.json").exists())
            target = root / "target"; target.mkdir(); link = root / "link"; link.symlink_to(target, target_is_directory=True)
            self.assertEqual(self.run_check(root, result, policy, output=link).returncode, 1)
            self.assertFalse(list(target.iterdir()))

    def test_full_writer_preserves_data_validity_and_returns_failed_budget_status(self):
        class FixtureBaseline:
            def __init__(self, args, private): self.result = v3_result(args.out)
            def snapshot(self): pass
            def hardware(self): pass
            def build(self): pass
            def measure(self): pass
            def cleanup(self): pass
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary); fixture_root = root / "fixture"; fixture_root.mkdir()
            policy = fixture_policy(v3_result(fixture_root)); policy["rules"][1]["max_bytes"] = 1
            path = root / "policy.json"; path.write_text(json.dumps(policy))
            for enforce in (False, True):
                out = root / str(enforce)
                argv = ["bench", "--out", str(out), "--budgets", str(path)] + (["--enforce-budgets"] if enforce else [])
                with mock.patch.object(baseline_module, "Baseline", FixtureBaseline), mock.patch.object(sys, "argv", argv):
                    self.assertEqual(baseline_module.main(), 1 if enforce else 0)
                report = json.loads((out / "result.json").read_text())
                self.assertEqual(report["status"], "passed")
                self.assertTrue(report["budget"]["data_valid"])
                self.assertEqual(report["budget"]["status"], "failed" if enforce else "warning")


if __name__ == "__main__":
    unittest.main()
