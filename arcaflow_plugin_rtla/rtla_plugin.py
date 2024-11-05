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

        timerlat_return = open("output.txt", "w")

        try:
            print("Gathering data... Use Ctrl-C to stop.")
            proc = subprocess.Popen(
                timerlat_cmd,
                start_new_session=True,
                stdout=timerlat_return,
                stderr=subprocess.PIPE,
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
            self.finished_early = True

        # In either the case of a keyboard interrupt or a cancel signal, we need to
        # send the SIGINT to the subprocess.
        if self.finished_early:
            proc.send_signal(2)

        proc.communicate()

        latency_hist = []
        total_irq_latency = {}
        total_thr_latency = {}
        total_usr_latency = {}
        stats_names = ["over:", "count:", "min:", "avg:", "max:"]
        stats_per_col = []
        found_all = False

        with open("output.txt", "r") as file:
            for line in file:
                if re.match(r"^Index", line):
                    cols = line.lower().split()
                if (re.match(r"^\d", line)) or (
                    line.split()[0] in stats_names and not found_all
                ):
                    row_obj = {}
                    for i, col in enumerate(cols):
                        row_obj[col] = line.split()[i]
                    if re.match(r"^\d", line):
                        latency_hist.append(row_obj)
                    else:
                        stats_per_col.append(row_obj)
                if re.match(r"^ALL", line) and not found_all:
                    found_all = True
                if found_all and line.split()[0] in stats_names:
                    if line.split()[0] != "over:":
                        total_irq_latency[line.split()[0][:-1]] = line.split()[1]
                        total_thr_latency[line.split()[0][:-1]] = line.split()[2]
                        if params.user_threads:
                            total_usr_latency[line.split()[0][:-1]] = line.split()[3]
                else:
                    continue

        if params.user_threads:
            return "success", TimerlatOutput(
                latency_hist,
                stats_per_col,
                latency_stats_schema.unserialize(total_irq_latency),
                latency_stats_schema.unserialize(total_thr_latency),
                latency_stats_schema.unserialize(total_usr_latency),
            )

        return "success", TimerlatOutput(
            latency_hist,
            stats_per_col,
            latency_stats_schema.unserialize(total_irq_latency),
            latency_stats_schema.unserialize(total_thr_latency),
        )


if __name__ == "__main__":
    sys.exit(
        plugin.run(
            plugin.build_schema(
                StartTimerlatStep.run_timerlat,
            )
        )
    )
