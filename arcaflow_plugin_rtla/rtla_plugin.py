#!/usr/bin/env python3

import subprocess
import re
import sys
import typing
from threading import Event
from arcaflow_plugin_sdk import plugin, predefined_schemas
from rtla_schema import (
    TimerlatInputParams,
    latency_stats_schema,
    TimerlatOutput,
    ErrorOutput,
)


def run_oneshot_cmd(command_list):
    try:
        cmd_out = subprocess.check_output(
            command_list,
            stderr=subprocess.STDOUT,
            text=True,
        )
    except subprocess.CalledProcessError as err:
        return "error", ErrorOutput(
            f"{err.cmd[0]} failed with return code {err.returncode}:\n{err.output}"
        )
    return "completed", cmd_out


class StartTimerlatStep:
    exit = Event()
    finished_early = False

    @plugin.signal_handler(
        id=predefined_schemas.cancel_signal_schema.id,
        name=predefined_schemas.cancel_signal_schema.display.name,
        description=predefined_schemas.cancel_signal_schema.display.description,
        icon=predefined_schemas.cancel_signal_schema.display.icon,
    )
    def cancel_step(self, _input: predefined_schemas.cancelInput):
        # First, let it know that this is the reason it's exiting.
        self.finished_early = True
        # Now signal to exit.
        self.exit.set()

    @plugin.step_with_signals(
        id="run-timerlat",
        name="Run RTLA Timerlat",
        description=(
            "Runs the RTLA Timerlat data collection and then processes the results "
            "into a machine-readable format"
        ),
        outputs={"success": TimerlatOutput, "error": ErrorOutput},
        signal_handler_method_names=["cancel_step"],
        signal_emitters=[],
        step_object_constructor=lambda: StartTimerlatStep(),
    )
    def run_timerlat(
        self,
        params: TimerlatInputParams,
    ) -> typing.Tuple[str, typing.Union[TimerlatOutput, ErrorOutput]]:

        timerlat_cmd = ["/usr/bin/rtla", "timerlat", "hist"]

        timerlat_cmd.extend(params.to_flags())

        try:
            print("Gathering data... Use Ctrl-C to stop.")
            timerlat_return = subprocess.run(
                timerlat_cmd,
                check=True,
                stdout=subprocess.PIPE,
                text=True,
            )

            # Block here, waiting on the cancel signal
            self.exit.wait(params.duration)

        except subprocess.CalledProcessError as err:
            return "error", ErrorOutput(
                f"{err.cmd[0]} failed with return code {err.returncode}:\n{err.output}"
            )

        # Secondary block interrupt is via the KeyboardInterrupt exception.
        # This enables running the plugin stand-alone without a workflow.
        except (KeyboardInterrupt, SystemExit):
            print("\nReceived keyboard interrupt; Stopping data collection.\n")

        # WIP - Process rtla output

        # Example output with the -u flag (Usr columns included)
        # # RTLA timerlat histogram
        # # Time unit is microseconds (us)
        # # Duration:   0 00:00:04
        # Index   IRQ-001   Thr-001   Usr-001   IRQ-002   Thr-002   Usr-002
        # 0           172         0         0      2138         0         0
        # 1          2805         0         0       651         0         0
        # 2             7        96         0        58      1349         0
        # 3             4        36        46        22      1150       243
        # 4             9        15        76       103       116      1888
        # 5             1        24        12        22        20       451
        # 6             0       531        18         6       135        35
        # 7             1      1798        21         0       137        41
        # 8             1       301        18         0        53       116
        # 9             0        30      1335         0        22        37
        # 10            0        86      1085         0         6       107
        # 11            0        66       156         0         8        48
        # 12            0         6        59         0         2        11
        # 13            0         4       120         0         1         7
        # 14            0         3        27         0         1        10
        # 15            0         1         7         0         0         2
        # 16            0         1        12         0         0         3
        # 17            0         0         3         0         0         0
        # 18            0         0         0         0         0         1
        # 19            0         2         1         0         0         0
        # 20            0         0         2         0         0         0
        # 23            0         0         1         0         0         0
        # 38            0         0         1         0         0         0
        # over:         0         0         0         0         0         0
        # count:     3000      3000      3000      3000      3000      3000
        # min:          0         2         3         0         2         3
        # avg:          0         6         9         0         3         4
        # max:          8        19        38         6        14        18
        # ALL:        IRQ       Thr       Usr
        # count:     6000      6000      6000
        # min:          0         2         3
        # avg:          0         5         7
        # max:          8        19        38

        # total_irq_latency: {
        #     count: 6000,
        #     min: 0,
        #     avg: 0,
        #     max: 0,
        # }
        # total_thr_latency: {
        #     count: 6000,
        #     min: 2,
        #     avg: 5,
        #     max: 19,
        # }
        # total_usr_latency: {
        #     count: 6000,
        #     min: 3,
        #     avg: 7,
        #     max: 38,
        # }

        total_irq_latency = {}
        total_thr_latency = {}
        total_usr_latency = {}
        found_all = False
        for line in timerlat_return.stdout.splitlines():
            if re.match(r"^ALL", line) and not found_all:
                found_all = True
            if found_all and re.match(r"^count", line):
                total_irq_latency["count"] = line.split()[1]
                total_thr_latency["count"] = line.split()[2]
                # TODO make usr values optional
                total_usr_latency["count"] = line.split()[3]
            if found_all and re.match(r"^min", line):
                total_irq_latency["min"] = line.split()[1]
                total_thr_latency["min"] = line.split()[2]
                total_usr_latency["min"] = line.split()[3]
            if found_all and re.match(r"^avg", line):
                total_irq_latency["avg"] = line.split()[1]
                total_thr_latency["avg"] = line.split()[2]
                total_usr_latency["avg"] = line.split()[3]
            if found_all and re.match(r"^max", line):
                total_irq_latency["max"] = line.split()[1]
                total_thr_latency["max"] = line.split()[2]
                total_usr_latency["max"] = line.split()[3]
            else:
                continue

        return "success", TimerlatOutput(
            latency_stats_schema.unserialize(total_irq_latency),
            latency_stats_schema.unserialize(total_thr_latency),
            latency_stats_schema.unserialize(total_usr_latency),
        )


if __name__ == "__main__":
    sys.exit(
        plugin.run(
            plugin.build_schema(
                StartTimerlatStep.run_timerlat,
            )
        )
    )
