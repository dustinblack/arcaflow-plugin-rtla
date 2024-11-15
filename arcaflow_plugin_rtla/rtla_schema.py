#!/usr/bin/env python3

import typing
from dataclasses import dataclass
from arcaflow_plugin_sdk import plugin, schema


def params_to_flags(params: dict) -> str:
    result = []
    for key, value in params.items():
        if not value:
            continue
        result.append(f"-{key}")
        if isinstance(value, list):
            result.append(",".join(str(i) for i in value))
        elif not isinstance(value, bool):
            result.append(str(value))
    return result


@dataclass
class TimerlatInputParams:
    period: typing.Annotated[
        typing.Optional[int],
        schema.name("timerlat period"),
        schema.description("Timerlat period in μs"),
    ] = None
    cpus: typing.Annotated[
        typing.Optional[typing.List[int]],
        schema.name("cpus"),
        schema.description("Run the tracer only on the given cpus"),
    ] = None
    house_keeping: typing.Annotated[
        typing.Optional[typing.List[int]],
        schema.id("house-keeping"),
        schema.name("house-keeping cpus"),
        schema.description("Run rtla control threads only on the given cpus"),
    ] = None
    duration: typing.Annotated[
        typing.Optional[int],
        schema.name("timerlat duration seconds"),
        schema.description("Duration of the session in seconds"),
    ] = None
    nano: typing.Annotated[
        typing.Optional[bool],
        schema.name("display nanoseconds"),
        schema.description("Display data in nanoseconds"),
    ] = None
    bucket_size: typing.Annotated[
        typing.Optional[int],
        schema.id("bucket-size"),
        schema.name("histogram bucket size"),
        schema.description("Set the histogram bucket size (default 1)"),
    ] = None
    entries: typing.Annotated[
        typing.Optional[int],
        schema.min(10),
        schema.max(9999999),
        schema.name("histogram entries"),
        schema.description("Set the number of entries of the histogram (default 256)"),
    ] = None
    user_threads: typing.Annotated[
        typing.Optional[bool],
        schema.id("user-threads"),
        schema.name("use user threads"),
        schema.description(
            "Use rtla user-space threads instead of kernel-space timerlat threads"
        ),
    ] = None
    enable_timeseries: typing.Annotated[
        typing.Optional[bool],
        schema.name("enable timeseries"),
        schema.description("Enable collection of latency timeseries data."),
    ] = False

    def to_flags(self) -> str:
        return params_to_flags(
            {
                "p": self.period,
                "c": self.cpus,
                "H": self.house_keeping,
                "d": self.duration,
                "n": self.nano,
                "b": self.bucket_size,
                "E": self.entries,
                "u": self.user_threads,
            }
        )


@dataclass
class LatencyStats:
    count: typing.Annotated[
        int,
        schema.name("number of measurements"),
        schema.description("Number of latency measurements"),
    ] = None
    min: typing.Annotated[
        int,
        schema.name("minimum latency"),
        schema.description("Minimum latency value"),
    ] = None
    avg: typing.Annotated[
        int,
        schema.name("average latency"),
        schema.description("Average latency value"),
    ] = None
    max: typing.Annotated[
        int,
        schema.name("maximum latency"),
        schema.description("Maximum latency value"),
    ] = None


latency_stats_schema = plugin.build_object_schema(LatencyStats)


@dataclass
class LatencyTimeseries:
    timestamp: typing.Annotated[
        float,
        schema.name("timestamp"),
        schema.description("CPU latency timestamp"),
    ] = None
    latency_ns: typing.Annotated[
        int,
        schema.name("latency in ns"),
        schema.description("CPU latency value in nanoseconds"),
    ] = None


latency_timeseries_schema = plugin.build_object_schema(LatencyTimeseries)


@dataclass
class TimerlatOutput:
    time_unit: typing.Annotated[
        str,
        schema.name("latency unit"),
        schema.description("Time unit for latency values"),
    ] = None
    latency_hist: typing.Annotated[
        typing.List[typing.Dict[str, int]],
        schema.name("latency histogram"),
        schema.description("Histogram of latencies"),
    ] = None
    stats_per_col: typing.Annotated[
        typing.List[typing.Dict[str, typing.Any]],
        schema.name("statistics per column"),
        schema.description("Latency statistics per captured column"),
    ] = None
    total_irq_latency: typing.Annotated[
        LatencyStats,
        schema.name("total irq latency"),
        schema.description("Total IRQ latency"),
    ] = None
    total_thr_latency: typing.Annotated[
        LatencyStats,
        schema.name("total thread latency"),
        schema.description("Total thread latency"),
    ] = None
    total_usr_latency: typing.Annotated[
        typing.Optional[LatencyStats],
        schema.name("total usr latency"),
        schema.description("Total user latency"),
    ] = None
    latency_timeseries: typing.Annotated[
        typing.Optional[typing.Dict[str, typing.List[LatencyTimeseries]]],
        schema.name("latency timeseries"),
        schema.description("Timeseries of latencies for each CPU and context"),
    ] = None


@dataclass
class ErrorOutput:
    error: str
