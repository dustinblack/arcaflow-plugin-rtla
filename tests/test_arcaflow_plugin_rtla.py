#!/usr/bin/env python3
import unittest
import rtla_plugin
from arcaflow_plugin_sdk import plugin


class HelloWorldTest(unittest.TestCase):
    @staticmethod
    def test_serialization():
        plugin.test_object_serialization(rtla_plugin.InputParams("John Doe"))

        plugin.test_object_serialization(rtla_plugin.SuccessOutput("Hello, world!"))

        plugin.test_object_serialization(
            rtla_plugin.ErrorOutput(error="This is an error")
        )

    def test_functional(self):
        input = rtla_plugin.InputParams(name="Example Joe")

        output_id, output_data = rtla_plugin.hello_world(
            params=input, run_id="plugin_ci"
        )

        # The example plugin always returns an error:
        self.assertEqual("success", output_id)
        self.assertEqual(
            output_data,
            rtla_plugin.SuccessOutput("Hello, Example Joe!"),
        )


if __name__ == "__main__":
    unittest.main()
