#!/usr/bin/env python3
import unittest
import rtla_plugin
import rtla_schema
from arcaflow_plugin_sdk import plugin


class RtlaTest(unittest.TestCase):
    @staticmethod
    def test_serialization():
        plugin.test_object_serialization(
            rtla_plugin.TimerlatInputParams(
                period=100,
                cpus=[0, 2, 4],
                house_keeping=[1, 3, 5],
                duration=10,
                nano=False,
                bucket_size=2,
                entries=128,
                user_threads=True,
            )
        )

        plugin.test_object_serialization(
            rtla_plugin.TimerlatOutput(
                latency_hist=[
                    {
                        "index": 0,
                        "irq-001": 252,
                        "thr-001": 0,
                        "usr-001": 0,
                    },
                    {
                        "index": 1,
                        "irq-001": 679,
                        "thr-001": 0,
                        "usr-001": 0,
                    },
                ],
                stats_per_col=[
                    {
                        "index": "over:",
                        "irq-001": 0,
                        "thr-001": 0,
                        "usr-001": 0,
                    },
                    {
                        "index": "count:",
                        "irq-001": 1000,
                        "thr-001": 1000,
                        "usr-001": 1000,
                    },
                    {
                        "index": "min:",
                        "irq-001": 0,
                        "thr-001": 2,
                        "usr-001": 3,
                    },
                    {
                        "index": "avg:",
                        "irq-001": 0,
                        "thr-001": 6,
                        "usr-001": 8,
                    },
                    {
                        "index": "max:",
                        "irq-001": 6,
                        "thr-001": 15,
                        "usr-001": 19,
                    },
                ],
                total_irq_latency=rtla_schema.latency_stats_schema.unserialize(
                    {
                        "count": 1000,
                        "min": 0,
                        "avg": 0,
                        "max": 6,
                    },
                ),
                total_thr_latency=rtla_schema.latency_stats_schema.unserialize(
                    {
                        "count": 1000,
                        "min": 2,
                        "avg": 6,
                        "max": 15,
                    },
                ),
                total_usr_latency=rtla_schema.latency_stats_schema.unserialize(
                    {
                        "count": 1000,
                        "min": 3,
                        "avg": 8,
                        "max": 19,
                    },
                ),
            )
        )

        plugin.test_object_serialization(
            rtla_plugin.ErrorOutput(error="This is an error")
        )

    def test_functional(self):
        timerlat_input = rtla_plugin.TimerlatInputParams(
            period=100,
            cpus=[0],
            house_keeping=[0],
            duration=3,
            nano=False,
            bucket_size=2,
            entries=10,
            user_threads=True,
        )

        output_id, output_data = rtla_plugin.StartTimerlatStep.run_timerlat(
            params=timerlat_input, run_id="plugin_ci"
        )

        self.assertEqual("success", output_id)
        self.assertEqual(
            output_data,
            rtla_plugin.TimerlatOutput(
                time_unit="",
                latency_hist=[],
                stats_per_col=[],
                total_irq_latency=rtla_schema.LatencyStats(
                    count=None, min=None, avg=None, max=None
                ),
                total_thr_latency=rtla_schema.LatencyStats(
                    count=None, min=None, avg=None, max=None
                ),
                total_usr_latency=rtla_schema.LatencyStats(
                    count=None, min=None, avg=None, max=None
                ),
            ),
        )
        # As of now, the test implementation in the container build automation does not
        # include the privilege escalation and bind mount necessary to gather actual
        # output data, so we are only validating types here based on a no-data return.
        self.assertIsInstance(output_data.latency_hist, list)
        self.assertIsInstance(output_data.stats_per_col, list)
        self.assertIsInstance(output_data.total_irq_latency.min, type(None))
        self.assertIsInstance(output_data.total_thr_latency.avg, type(None))
        self.assertIsInstance(output_data.total_usr_latency.max, type(None))


if __name__ == "__main__":
    unittest.main()
