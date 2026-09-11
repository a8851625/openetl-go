#!/usr/bin/env python3
"""Counterexamples for capacity measurement, not snapshots of the implementation."""

import copy
import unittest

from bench_capacity import acknowledged_at, kafka_prefix, summarize_observation
from bench_baseline import MeasurementError


def fixture():
    sample = {"time_unix_ns": 1000000000, "rss_bytes": 1024, "peak_rss_bytes": 2048,
              "user_cpu_seconds": 1.0, "system_cpu_seconds": 0.5, "cgroup_cpu": {},
              "pool": {"max_open": 1, "wait_count": 1, "wait_ns": 1000}}
    end = copy.deepcopy(sample)
    end.update(time_unix_ns=2000000000, user_cpu_seconds=2.0)
    end["pool"].update(wait_count=2, wait_ns=2000)
    return {"schema_version": 1, "start": sample, "end": end, "samples": [sample],
            "elapsed_seconds": 1.0, "observer_ns": 10, "boundary_calls_excluded": 0,
            "checkpoints": [{"duration_ns": 30, "start_unix_ns": 1500000000, "end_unix_ns": 1500000030, "failed": False}]}


class Measurements(unittest.TestCase):
    def test_actual_distribution_and_units(self):
        result = summarize_observation(fixture())
        self.assertEqual(result["checkpoint_p99_ms"], .00003)
        self.assertEqual(result["pool_wait_count"], 1)
        self.assertEqual(result["pool_wait_seconds"], .000001)
        self.assertEqual(result["mean_cpu_cores"], 1)

    def test_missing_or_failed_measurements_do_not_pass(self):
        mutations = [lambda r: r.update(checkpoints=[]),
                     lambda r: r["checkpoints"][0].update(failed=True),
                     lambda r: r["checkpoints"][0].update(duration_ns=float("nan")),
                     lambda r: r["checkpoints"][0].update(start_unix_ns=99),
                     lambda r: r["end"]["pool"].update(wait_count=0),
                     lambda r: r.update(samples=[]),
                     lambda r: r["end"].update(peak_rss_bytes=1),
                     lambda r: r["end"].update(time_unix_ns=1500000000000),
                     lambda r: r.update(elapsed_seconds=0)]
        for mutate in mutations:
            with self.subTest(mutate=mutate):
                raw = fixture()
                mutate(raw)
                with self.assertRaises(MeasurementError):
                    summarize_observation(raw)

    def test_enveloped_checkpoint_is_last_committed_plus_one(self):
        self.assertEqual(kafka_prefix({"version": 1, "source": {"offsets": {"0": 42}}}), 43)
        self.assertEqual(kafka_prefix({"offsets": {"0": 0}}), 1)
        self.assertEqual(kafka_prefix(None), 0)
        for position in ({"offsets": {"0": -1}}, {"offsets": {"1": 42}}, {"offsets": {"0": 1.5}}):
            with self.assertRaises(MeasurementError):
                kafka_prefix(position)

    def test_offered_load_uses_successful_ack_timeline(self):
        producer = {"rows": 1000, "acks": [{"time_unix_ns": 100, "rows": 10}, {"time_unix_ns": 200, "rows": 20}]}
        self.assertEqual(acknowledged_at(producer, 99), 0)
        self.assertEqual(acknowledged_at(producer, 100), 10)
        self.assertEqual(acknowledged_at(producer, 199), 10)
        self.assertEqual(acknowledged_at(producer, 300), 20)


if __name__ == "__main__":
    unittest.main(verbosity=2)
