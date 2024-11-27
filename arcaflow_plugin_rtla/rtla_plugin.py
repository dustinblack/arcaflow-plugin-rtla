#!/usr/bin/env python3

import subprocess
import re
import sys
import typing
import time
from threading import Event
from datetime import datetime
from arcaflow_plugin_sdk import plugin, predefined_schemas
from rtla_schema import (
    TimerlatInputParams,
    latency_stats_schema,
    latency_timeseries_schema,
    TimerlatOutput,
    ErrorOutput,
)


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
            timerlat_proc = subprocess.Popen(
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

        if params.enable_time_series:
            timeseries_dict = {}
            last_uptimestamp = {}
            trace_path = "/sys/kernel/debug/tracing/instances/timerlat_hist/trace_pipe"

            # A delay is needed before reading from the trace_path to ensure the file
            # exists and data is streaming to the fifo. I've tried to avoid a sleep(),
            # but none of the methods I've tested have worked.
            wait_time = 0
            timeout_seconds = 5
            sleep_time = 0.5
            trace_file = None
            while wait_time < timeout_seconds:
                try:
                    trace_file = open(trace_path, "r")
                    break
                except FileNotFoundError:
                    time.sleep(sleep_time)
                    wait_time += sleep_time
                    continue

            if not trace_file:
                print("Unable to read tracer output; Skipping time series collection\n")
            else:
                timeseries_file = open("./timeseries_file", "w")
                try:
                    timeseries_proc = subprocess.Popen(
                        ["cat"],
                        start_new_session=True,
                        stdin=trace_file,
                        stdout=timeseries_file,
                        stderr=subprocess.PIPE,
                        text=True,
                    )
                except subprocess.CalledProcessError as err:
                    return "error", ErrorOutput(
                        f"""{err.cmd[0]} failed with return code {err.returncode}:\n
                        {err.output}"""
                    )
                finally:
                    timeseries_file.close()
                    trace_file.close()

        try:
            # Block here, waiting on the cancel signal
            print("Gathering data... Use Ctrl-C to stop.\n")
            self.exit.wait(params.duration)

        # Secondary block interrupt is via the KeyboardInterrupt exception.
        # This enables running the plugin stand-alone without a workflow.
        except (KeyboardInterrupt, SystemExit):
            print("\nReceived keyboard interrupt; Stopping data collection.\n")
            self.finished_early = True

        # In either the case of a keyboard interrupt or a cancel signal, we need to
        # send the SIGINT to the subprocess.
        if self.finished_early:
            timerlat_proc.send_signal(2)

        # Example time series output from the tracer (truncated)
        # <idle>-0      [009] d. 123.769498: #1002 context    irq timer_latency  458 ns
        #  <...>-625890 [009] .. 123.769499: #1002 context thread timer_latency 666 ns
        #  <...>-625890 [009] .. 123.769500: #1002 context user-ret timer_latency 499 ns
        # <idle>-0      [002] d. 123.769528: #1003 context    irq timer_latency  587 ns
        #  <...>-625883 [002] .. 123.769532: #1003 context thread timer_latency 712 ns
        #  <...>-625883 [002] .. 123.769534: #1003 context user-ret timer_latency 462 ns

        if params.enable_time_series and trace_file:
            # Interrupt the time series collection process and capture the output
            timeseries_proc.send_signal(2)

            # The calculation of the offset from uptime to current time certainly means
            # that our time series is not 100% accurately aligned to the nanosecond, but
            # the relative times will be accurate. We'll accept this as good enough.
            uptime_offset = time.time() - time.monotonic()

            with open("./timeseries_file") as timeseries_output:

                if all(False for _ in timeseries_output):
                    print(
                        "No results reading tracer output; "
                        "Skipping time series collection\n"
                    )

                for line in timeseries_output:
                    # The first object in the line is a string, and it is possible for
                    # it to have spaces in it, so we split first on the expected "["
                    # deliniator for the CPU number field.
                    # FIXME: I can't be 100% sure that a "[" wont' appear in the string
                    # that is the first object of the line.
                    stripped_line = line.split("[")
                    line_list = stripped_line[1].split()

                    # Because the tracer format is dependent on the underlying OS and
                    # cannot be controlled by the container, check the tracer output
                    # format and break gracefully if we don't recognize it
                    try:
                        cpu = int(line_list[0][:-1])
                        uptimestamp = float(line_list[2][:-1])
                        timestamp = (
                            datetime.fromtimestamp(uptimestamp + uptime_offset)
                            .astimezone()
                            .isoformat()
                        )
                        context = str(line_list[5])
                        latency = int(line_list[7])

                    except (IndexError, ValueError) as error:
                        print(
                            "Unknown tracer format; Skipping time series collection: "
                            f"{error}\n{line}"
                        )
                        timeseries_dict = {}
                        break

                    # The trace collects for all CPUs, so skip any CPU we did not select
                    # via the input to the plugin
                    if params.cpus and int(cpu) not in params.cpus:
                        continue
                    # Create a separate time series for each CPU + context combination
                    cpu_context = f"cpu{cpu}_{context}"
                    if cpu_context not in timeseries_dict:
                        timeseries_dict[cpu_context] = []
                    if cpu_context not in last_uptimestamp:
                        last_uptimestamp[cpu_context] = 0
                    if last_uptimestamp[cpu_context] and (
                        uptimestamp - last_uptimestamp[cpu_context]
                        < params.time_series_resolution
                    ):
                        continue
                    last_uptimestamp[cpu_context] = uptimestamp
                    timeseries_dict[cpu_context].append(
                        latency_timeseries_schema.unserialize(
                            {
                                "timestamp": timestamp,
                                "latency_ns": latency,
                            }
                        )
                    )

        timerlat_output = timerlat_proc.stdout.read()

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
        stats_per_col = []
        accumulator = latency_hist
        total_irq_latency = {}
        total_thr_latency = {}
        total_usr_latency = {}

        is_time_unit = re.compile(r"# Time unit is (\w+)")

        output_lines = iter(timerlat_output.splitlines())

        # Phase 1: Get the headers
        for line in output_lines:
            # Get the time unit (user-selectable)
            if is_time_unit.match(line):
                time_unit = is_time_unit.match(line).group(1)
            # Capture the column headers
            elif line.startswith("Index"):
                col_headers = line.lower().split()
                break

        # Phase 2: Collect histogram buckets and column latency statistics
        for line in output_lines:
            line_list = line.split()
            # Collect statistics up until the summary section
            # (We don't process the summary header line itself, we just skip it here.)
            if line_list[0].startswith("ALL"):
                break
            row_obj = {}
            if not line_list[0].isdigit():
                # Stats index values are strings ending in a colon
                row_obj[col_headers[0]] = line_list[0][:-1]
                # When we hit the stats, switch to the other accumulator
                accumulator = stats_per_col
            else:
                # Histogram index values are integers
                row_obj[col_headers[0]] = int(line_list[0])
            # Merge the dicts so that the 'index' column is (probably) first in the
            # output display just for human-friendliness
            row_obj = row_obj | dict(zip(col_headers[1:], map(int, line_list[1:])))
            accumulator.append(row_obj)

        # Phase 3: Get the stats summary as key:value pairs
        for line in output_lines:
            line_list = line.split()
            label = line_list[0][:-1]
            total_irq_latency[label] = line_list[1]
            total_thr_latency[label] = line_list[2]
            if params.user_threads:
                total_usr_latency[label] = line_list[3]

        # Provide the rtla command formatted data as debug output
        print(timerlat_output)

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
            (timeseries_dict if params.enable_time_series else None),
        )


if __name__ == "__main__":
    sys.exit(
        plugin.run(
            plugin.build_schema(
                StartTimerlatStep.run_timerlat,
            )
        )
    )
