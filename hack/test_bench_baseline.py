#!/usr/bin/env python3
"""Failure-boundary checks for the RA-8 measurement and CI parser."""
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from bench_baseline import MeasurementError, VARIANTS, image_identity, parse_proc_status, validate_result, wait_ready


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


if __name__ == "__main__":
    unittest.main()
