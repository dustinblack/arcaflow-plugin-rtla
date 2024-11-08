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


def run_oneshot_cmd(command_list: list[str]) -> tuple[str, subprocess.CompletedProcess]:
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
    def __init__(self, exit, finished_early):
        self.exit = exit
        self.finished_early = finished_early

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
        step_object_constructor=lambda: StartTimerlatStep(Event(), False),
    )
    def run_timerlat(
        self,
        params: TimerlatInputParams,
    ) -> typing.Tuple[str, typing.Union[TimerlatOutput, ErrorOutput]]:

        timerlat_cmd = ["/usr/bin/rtla", "timerlat", "hist"]
        timerlat_cmd.extend(params.to_flags())

        try:
            proc = subprocess.Popen(
                timerlat_cmd,
                start_new_session=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )
        except subprocess.CalledProcessError as err:
            return "error", ErrorOutput(
                f"{err.cmd[0]} failed with return code {err.returncode}:\n{err.output}"
            )

        try:
            # Block here, waiting on the cancel signal
            print("Gathering data... Use Ctrl-C to stop.")
            self.exit.wait(params.duration)

        # Secondary block interrupt is via the KeyboardInterrupt exception.
        # This enables running the plugin stand-alone without a workflow.
        except (KeyboardInterrupt, SystemExit):
            print("\nReceived keyboard interrupt; Stopping data collection.\n")
            self.finished_early = True

        # In either the case of a keyboard interrupt or a cancel signal, we need to
        # send the SIGINT to the subprocess.
        if self.finished_early:
            proc.send_signal(2)

        output, _ = proc.communicate()

        # The output from the `rtla timerlat hist` command is columnar in three
        # sections, plus headers. The first section is histogram data, which will
        # display either two or three colums per CPU (three if user threads are
        # enabled). The second section is latency statistics for each of the same
        # columns as the histogram section. The third section is collapsed totals of
        # the statistical data from all CPUs.

        # For the first and second sections, we want to retain the row/columnt format
        # in the output for easy recreation of the data as a table. For the third
        # section, we want to return key:value pairs for each of the IRQ, Thr, and Usr
        # total stats.

        # $ sudo rtla timerlat hist -u -d 1 -c 1,2 -E 10 -b 10
        # # RTLA timerlat histogram
        # # Time unit is microseconds (us)
        # # Duration:   0 00:00:02
        # Index   IRQ-001   Thr-001   Usr-001   IRQ-002   Thr-002   Usr-002
        # 0          1000       944       134       999       998       986
        # 10            0        55       863         0         1        13
        # 20            0         1         3         0         0         0
        # over:         0         0         0         0         0         0
        # count:     1000      1000      1000       999       999       999
        # min:          0         3         4         0         2         3
        # avg:          1         7        10         0         2         3
        # max:          7        21        24         3        11        15
        # ALL:        IRQ       Thr       Usr
        # count:     1999      1999      1999
        # min:          0         2         3
        # avg:          0         4         6
        # max:          7        21        24

        time_unit = ""
        col_headers = []
        latency_hist = []
        total_irq_latency = {}
        total_thr_latency = {}
        total_usr_latency = {}
        stats_names = ["over:", "count:", "min:", "avg:", "max:"]
        stats_per_col = []
        found_all = False

        re_isunit = re.compile(r"^# Time unit is (.+) .*$")
        re_isindex = re.compile(r"^Index")
        re_isdigit = re.compile(r"^\d")
        re_isall = re.compile(r"^ALL")

        for line in output.splitlines():
            if re_isunit.match(line):
                time_unit = re_isunit.match(line).group(1)
            # Capture the column headers
            elif re_isindex.match(line):
                col_headers = line.lower().split()
            # Stats names repeat, so flag when have passed ^ALL
            elif re_isall.match(line):
                found_all = True
            # Either this is a histogram bucket row, or the first time we have seen
            # a row beginning with a stat name
            elif (re_isdigit.match(line)) or (
                line.split()[0] in stats_names and not found_all
            ):
                # Capture the columnar data
                line_list = []
                for element in line.split():
                    try:
                        line_list.append(int(element))
                    except ValueError:
                        line_list.append(element)
                row_obj = dict(zip(col_headers, line_list))
                if re_isdigit.match(line):
                    latency_hist.append(row_obj)
                else:
                    stats_per_col.append(row_obj)
            # Since we've encountered the summary statistics (marked by the line
            # starting with "ALL"), generate key:value pairs instead of columnar data.
            elif found_all and line.split()[0] in stats_names:
                label = line.split()[0][:-1]
                if label != "over":
                    total_irq_latency[label] = line.split()[1]
                    total_thr_latency[label] = line.split()[2]
                    if params.user_threads:
                        total_usr_latency[label] = line.split()[3]

        return "success", TimerlatOutput(
            time_unit,
            latency_hist,
            stats_per_col,
            latency_stats_schema.unserialize(total_irq_latency),
            latency_stats_schema.unserialize(total_thr_latency),
            (
                latency_stats_schema.unserialize(total_usr_latency)
                if params.user_threads
                else None
            ),
        )


if __name__ == "__main__":
    sys.exit(
        plugin.run(
            plugin.build_schema(
                StartTimerlatStep.run_timerlat,
            )
        )
    )
