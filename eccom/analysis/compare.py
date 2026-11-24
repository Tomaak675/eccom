"""Cross-run comparison tables.

``eccom analyze`` summarizes one run; this module tabulates several runs side
by side. Each run is decomposed into the per-stage latencies the event log can
support, given a headline metric (round-trip for request_response, one-way
delivery for one_way), and paired with resource usage measured against the
agent's own idle baseline.

Everything is derived from the raw artifacts (events, system metrics,
scenario), so a run does not need to have been analyzed first and old runs
work as well as new ones.
"""

from __future__ import annotations

import csv
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from eccom.analysis.loading import load_events, load_system_metrics
from eccom.analysis.run import link_capacity_summary, resolve_clock_sync
from eccom.analysis.statistics import (
    MEASUREMENT_PHASE,
    compute_one_way_table,
    RAMP_AXES,
    compute_ramp_table,
    ramp_dimension,
    compute_rtt_table,
    counter_delta,
    latency_stats_ms,
    paired_duration_ms,
)
from eccom.core.config import ScenarioConfig, load_scenario
from eccom.core.events import EventType

logger = logging.getLogger(__name__)

# CPU percent is an average over the interval since the previous sample, so the
# first sample after traffic stops still covers part of it and is discarded.
_SETTLING_SAMPLES = 1

# Samples the agent tagged as its pre-traffic idle window (instrumentation.baseline_s).
_BASELINE_PHASE = "baseline"

# A ramp step counts as sustained while nothing failed and the sender-observed
# latency p95 stays within this factor of the first step's p95.
_KNEE_FACTOR = 2.0
# A ramp step held when the receiver got at least this share of the attempted
# messages (late arrivals included) ...
_RECEIVED_FRACTION = 0.99
# ... and received at least this share of the offered rate within the step's
# scheduled sending time.
_RECEIVED_RATE_FRACTION = 0.95


@dataclass(frozen=True, slots=True)
class Stage:
    """One measurable interval between two events on a single host."""

    name: str
    role: str  # "sender" or "receiver"
    start: EventType
    end: EventType


# Stages available in every scenario. The cross-host delivery leg and the
# round trip are handled separately: they need the clock offset / a different
# pairing, not a same-host event pair.
_STAGES = (
    Stage("build", "sender", EventType.PAYLOAD_CREATED, EventType.SEND_STARTED),
    Stage("send_ack", "sender", EventType.SEND_STARTED, EventType.SEND_COMPLETED),
    Stage("dispatch", "receiver", EventType.MESSAGE_RECEIVED, EventType.HANDLER_STARTED),
    Stage("handler", "receiver", EventType.HANDLER_STARTED, EventType.HANDLER_COMPLETED),
    Stage("reply_send", "receiver", EventType.HANDLER_COMPLETED, EventType.REPLY_SEND_STARTED),
)


def _measurement_ids(events: pd.DataFrame, sender_host: str) -> set[str]:
    """Correlation ids of the measurement phase.

    Only the sender tags phases, so receiver-side stages are scoped by joining
    on these ids rather than by filtering on ``phase``.
    """
    if events.empty:
        return set()
    sends = events[
        (events["host"] == sender_host)
        & (events["event_type"] == EventType.SEND_STARTED)
        & (events["phase"] == MEASUREMENT_PHASE)
    ]
    return set(sends["correlation_id"])


def stage_statistics(
    events: pd.DataFrame, scenario: ScenarioConfig, clock_offset_ns: int
) -> dict[str, dict[str, float] | None]:
    """Latency statistics per stage, plus the scenario's headline metric.

    The headline key is ``rtt`` for request_response and ``delivery`` for
    one_way, so callers can compare runs without knowing the interaction.
    """
    sender, receiver = scenario.topology.sender, scenario.topology.receiver
    ids = _measurement_ids(events, sender)
    hosts = {"sender": events[events["host"] == sender], "receiver": events[events["host"] == receiver]}

    stats: dict[str, dict[str, float] | None] = {}
    for stage in _STAGES:
        stats[stage.name] = latency_stats_ms(
            paired_duration_ms(hosts[stage.role], stage.start, stage.end, ids)
        )

    one_way = compute_one_way_table(events, sender, receiver, clock_offset_ns)
    stats["delivery"] = latency_stats_ms(
        one_way[one_way["phase"] == MEASUREMENT_PHASE]["latency_ms"]
    )
    rtt = compute_rtt_table(events, sender)
    stats["rtt"] = latency_stats_ms(rtt[rtt["phase"] == MEASUREMENT_PHASE]["rtt_ms"])

    stats["headline"] = stats["rtt"] if scenario.interaction == "request_response" else stats["delivery"]
    return stats


def resource_usage(
    run_dir: Path, events: pd.DataFrame, scenario: ScenarioConfig
) -> dict[str, dict[str, float | None]]:
    """Per-host CPU/memory during measurement, against the agent's idle baseline.

    The baseline is every sample taken outside the traffic window — before the
    first message and after the last one (the agent process is already running
    then, so the difference isolates the cost of the traffic itself). Hosts
    whose sampler produced no idle samples report ``None`` for the adjusted
    figures rather than a misleading zero.
    """
    samples_by_host = load_system_metrics(run_dir)
    ids = _measurement_ids(events, scenario.topology.sender)
    usage: dict[str, dict[str, float | None]] = {}

    for host, samples in samples_by_host.items():
        host_events = events[events["host"] == host]
        if host_events.empty or not samples:
            continue
        traffic_start = int(host_events["wall_time_ns"].min())
        traffic_end = int(host_events["wall_time_ns"].max())
        scoped = host_events[host_events["correlation_id"].isin(ids)]
        if scoped.empty:
            continue
        window = (int(scoped["wall_time_ns"].min()), int(scoped["wall_time_ns"].max()))

        during = [s for s in samples if window[0] <= s["wall_time_ns"] <= window[1]]
        tagged = [s for s in samples if s.get("phase") == _BASELINE_PHASE]
        if tagged:
            # instrumentation.baseline_s produced an explicit quiet window
            before, after = tagged, []
        else:
            # Older runs carry no phase tag: fall back to inferring quiet
            # windows from the traffic boundaries.
            before = [s for s in samples if s["wall_time_ns"] < traffic_start]
            after = [s for s in samples if s["wall_time_ns"] > traffic_end][_SETTLING_SAMPLES:]
        usage[host] = _usage_row(during, before, after)
    return usage


def _cpu_over(windows: list[list[dict[str, Any]]]) -> float | None:
    """Mean CPU percent across contiguous sample windows, from cumulative times.

    ``process_cpu_percent`` quantizes to whole percent at 1 Hz, which is
    coarser than an idle agent's real usage, so sampled percentages cannot be
    averaged into anything trustworthy at low load. The cumulative
    ``process_cpu_seconds`` counter has no such floor: the delta across a
    window divided by its wall duration is exact. Returns None for runs
    recorded before that field existed.
    """
    cpu_seconds = 0.0
    wall_seconds = 0.0
    for window in windows:
        delta = counter_delta(window, "process_cpu_seconds")
        if delta is None:
            continue
        cpu_seconds += delta[0]
        wall_seconds += delta[1]
    return 100.0 * cpu_seconds / wall_seconds if wall_seconds > 0 else None


def _usage_row(
    during: list[dict[str, Any]],
    before: list[dict[str, Any]],
    after: list[dict[str, Any]],
) -> dict[str, float | None]:
    baseline = before + after

    def mean(samples: list[dict[str, Any]], key: str) -> float | None:
        return sum(s[key] for s in samples) / len(samples) if samples else None

    def delta(a: float | None, b: float | None) -> float | None:
        return None if a is None or b is None else a - b

    cpu = _cpu_over([during])
    # CPU returns to idle once traffic stops, so both quiet windows count.
    cpu_base = _cpu_over([before, after])
    rss = mean(during, "process_rss_bytes")
    # Memory does not return to idle: the agent buffers every event in memory
    # until close, so post-run RSS reflects the run, not a baseline. Only the
    # pre-traffic window is a true memory baseline.
    rss_base = mean(before, "process_rss_bytes")
    return {
        "samples_measurement": len(during),
        "samples_baseline": len(baseline),
        "process_cpu_percent": cpu,
        "process_cpu_percent_baseline": cpu_base,
        "process_cpu_percent_adjusted": delta(cpu, cpu_base),
        # Sampled percentages are kept only as a fallback for runs recorded
        # before process_cpu_seconds existed; they are quantization-limited.
        "process_cpu_percent_sampled": mean(during, "process_cpu_percent"),
        "process_rss_mb": rss / 1e6 if rss is not None else None,
        "process_rss_mb_baseline": rss_base / 1e6 if rss_base is not None else None,
        "process_rss_mb_adjusted": (
            delta(rss, rss_base) / 1e6 if delta(rss, rss_base) is not None else None
        ),
    }


def ramp_summary(steps: list[dict[str, Any]], interaction: str) -> dict[str, Any] | None:
    """Collapse a ramp's per-step rows into the numbers worth comparing
    across protocols: the last step whose schedule held, the highest rate the
    sender sustained before the latency knee, the step at which the knee
    showed, and the best received rate seen (a step's unique receipts over
    its scheduled sending time).

    A step held (``exact``) when the receiver got at least
    ``_RECEIVED_FRACTION`` of the attempted messages and its received rate
    (receipts within the step's scheduled sending time) reached
    ``_RECEIVED_RATE_FRACTION`` of the offered rate; sender timeouts do not
    count against it. ``miss_step`` is the first step that did not hold,
    ``exact_*`` describe the step before it. The knee is the
    first step that either lost a message or pushed the sender-observed
    latency p95 (send-ack for one-way, RTT for request/response) past
    ``_KNEE_FACTOR`` times the first step's p95; ``sustained_*`` describe the
    step before it. Both report the achieved send rate, the payload size and
    the number of homes of their step, for ramps that grow the rate, the
    payload or the number of simulated homes.
    """
    if not steps:
        return None
    cost_key = "rtt_ms" if interaction == "request_response" else "send_ack_ms"
    floor = (steps[0][cost_key] or {}).get("p95")
    exact = None
    miss = None
    for row in steps:
        held = (
            row["received_fraction"] >= _RECEIVED_FRACTION
            and row["received_rate"] >= _RECEIVED_RATE_FRACTION * row["target_message_rate"]
        )
        if not held:
            miss = row
            break
        exact = row
    sustained = None
    knee = None
    for row in steps:
        cost = row[cost_key]
        healthy = (
            row["messages_failed"] == 0
            and floor is not None
            and cost is not None
            and cost["p95"] <= _KNEE_FACTOR * floor
        )
        if not healthy:
            knee = row
            break
        sustained = row
    return {
        "steps": len(steps),
        "received_fraction": _RECEIVED_FRACTION,
        "received_rate_fraction": _RECEIVED_RATE_FRACTION,
        "exact_message_rate": exact["actual_send_rate"] if exact else None,
        "exact_payload_size_bytes": exact.get("payload_size_bytes") if exact else None,
        "exact_homes": exact.get("homes") if exact else None,
        "exact_step": exact["step"] if exact else None,
        "miss_step": miss["step"] if miss else None,
        "knee_factor": _KNEE_FACTOR,
        "sustained_message_rate": sustained["actual_send_rate"] if sustained else None,
        "sustained_payload_size_bytes": sustained.get("payload_size_bytes") if sustained else None,
        "sustained_homes": sustained.get("homes") if sustained else None,
        "sustained_step": sustained["step"] if sustained else None,
        "knee_step": knee["step"] if knee else None,
        "knee_target_message_rate": knee["target_message_rate"] if knee else None,
        "knee_payload_size_bytes": knee.get("payload_size_bytes") if knee else None,
        "knee_homes": knee.get("homes") if knee else None,
        "peak_received_rate": max(r["received_rate"] for r in steps),
    }


def compare_run(run_dir: Path) -> dict[str, Any]:
    """Stage statistics and resource usage for one run directory."""
    scenario = load_scenario(run_dir / "scenario.yaml")
    events = load_events(run_dir)
    clock = resolve_clock_sync(run_dir, scenario.topology.sender)
    ramp = None
    ramp_steps = None
    if scenario.load.stepped:
        ramp_steps = compute_ramp_table(events, scenario, clock, load_system_metrics(run_dir))
        ramp = ramp_summary(ramp_steps, scenario.interaction)
    clock = clock or {}
    return {
        "run_dir": run_dir.name,
        "scenario": scenario.name,
        "interaction": scenario.interaction,
        # homes runs only: the batching factor the variant ran under, so the
        # comparison can group variants by it
        "aggregation_factor": (
            scenario.load.aggregation_factor if scenario.load.mode == "homes" else None
        ),
        "headline_metric": "rtt" if scenario.interaction == "request_response" else "delivery",
        "stages": stage_statistics(events, scenario, clock.get("offset_ns", 0)),
        "resources": resource_usage(run_dir, events, scenario),
        "link": link_capacity_summary(run_dir, scenario.topology.sender),
        "clock": {
            "method": clock.get("method"),
            "offset_ms": clock.get("offset_ns", 0) / 1e6,
            "uncertainty_ms": clock.get("uncertainty_ns", 0) / 1e6,
        },
        # ramp runs only: the knee summary (tabulated) and the per-step table
        # (drawn by the rate-sweep charts of --plots)
        "ramp": ramp,
        "ramp_steps": ramp_steps,
    }


def compare_runs(run_dirs: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for run_dir in run_dirs:
        try:
            rows.append(compare_run(run_dir))
        except Exception:
            logger.exception("skipping %s", run_dir)
    return rows


# --- rendering -----------------------------------------------------------

_PERCENTILES = ("mean", "median", "p90", "p95", "p99")
_STAGE_ORDER = ("build", "send_ack", "delivery", "rtt", "dispatch", "handler", "reply_send")


def _table(headers: list[str], rows: list[list[str]]) -> str:
    lines = ["| " + " | ".join(headers) + " |", "|" + "|".join(["---"] * len(headers)) + "|"]
    lines += ["| " + " | ".join(row) + " |" for row in rows]
    return "\n".join(lines)


def _num(value: float | None, digits: int = 2) -> str:
    return "-" if value is None else f"{value:.{digits}f}"


def _stat(stats: dict[str, Any] | None, key: str) -> str:
    return "-" if not stats else _num(stats.get(key))


def format_comparison(rows: list[dict[str, Any]]) -> str:
    """Markdown: headline latency, per-stage means, and resource usage."""
    if not rows:
        return "no runs to compare"
    out: list[str] = []

    out.append("## Headline latency (ms)\n")
    out.append(
        _table(
            ["scenario", "metric", "n", *_PERCENTILES, "min", "max"],
            [
                [
                    r["scenario"],
                    r["headline_metric"],
                    str((r["stages"]["headline"] or {}).get("count", "-")),
                    *[_stat(r["stages"]["headline"], p) for p in _PERCENTILES],
                    _stat(r["stages"]["headline"], "min"),
                    _stat(r["stages"]["headline"], "max"),
                ]
                for r in rows
            ],
        )
    )

    present = [s for s in _STAGE_ORDER if any(r["stages"].get(s) for r in rows)]
    out.append("\n## Per-stage mean latency (ms)\n")
    out.append(
        _table(
            ["scenario", *present],
            [[r["scenario"], *[_stat(r["stages"].get(s), "mean") for s in present]] for r in rows],
        )
    )

    out.append("\n## Resource usage (measurement window vs. idle baseline)\n")
    resource_rows: list[list[str]] = []
    for r in rows:
        for host, u in sorted(r["resources"].items()):
            resource_rows.append(
                [
                    r["scenario"],
                    host,
                    _num(u["process_cpu_percent"]),
                    _num(u["process_cpu_percent_baseline"]),
                    _num(u["process_cpu_percent_adjusted"]),
                    _num(u["process_rss_mb"], 1),
                    _num(u["process_rss_mb_baseline"], 1),
                    _num(u["process_rss_mb_adjusted"], 1),
                    f"{u['samples_measurement']}/{u['samples_baseline']}",
                ]
            )
    out.append(
        _table(
            [
                "scenario", "host",
                "cpu %", "cpu % idle", "cpu % adj",
                "rss MB", "rss MB idle", "rss MB adj",
                "samples meas/idle",
            ],
            resource_rows,
        )
    )
    out.append(
        "\n`adj` = measurement window minus the agent's own idle window. The idle "
        "window is the quiet period the agent tags before any traffic "
        "(`instrumentation.baseline_s`); runs without that tag fall back to "
        "inferring it from the traffic boundaries. CPU comes from the cumulative "
        "`process_cpu_seconds` counter, not sampled percentages, which quantize to "
        "whole percent at 1 Hz. `-` means the figure could not be formed: either "
        "the host produced no idle samples, or the run predates that counter."
    )

    links = [r for r in rows if r.get("link")]
    if links:
        out.append("\n## Link capacity (iperf3, before each run)\n")
        out.append(
            _table(
                [
                    "scenario", "sender→receiver Mbit/s", "receiver→sender Mbit/s",
                    "retransmits", "rtt ms", "port",
                ],
                [
                    [
                        r["scenario"],
                        _num(r["link"].get("sender_to_receiver_mbit_s"), 1),
                        _num(r["link"].get("receiver_to_sender_mbit_s"), 1),
                        "-" if r["link"].get("retransmits") is None else str(r["link"]["retransmits"]),
                        _num(r["link"].get("mean_rtt_ms"), 1),
                        str(r["link"].get("port")),
                    ]
                    for r in links
                ],
            )
        )
        out.append(
            "\nOne iperf3 TCP flow per direction between the sender's and the receiver's "
            "host, on the scenario's endpoint port, before any agent started; the figure is "
            "the goodput at the receiving end. `-` means that direction's probe failed (see "
            "the run's `<sender>-link-capacity.json`)."
        )

    ramps = [r for r in rows if r.get("ramp")]
    if ramps:
        dimension = ramp_dimension([r["ramp_steps"] for r in ramps])
        title, unit, digits, exact_key, sustained_key, knee_key = _RAMP_SECTIONS[dimension]
        out.append(f"\n## {title}\n")
        out.append(
            _table(
                [
                    "scenario", "steps", f"exact {unit}", "to step", f"sustained {unit}",
                    "at step", "knee step", f"knee {unit}", "peak received msg/s",
                ],
                [
                    [
                        r["scenario"],
                        str(r["ramp"]["steps"]),
                        _num(r["ramp"].get(exact_key), digits),
                        _step(r["ramp"]["exact_step"]),
                        _num(r["ramp"].get(sustained_key), digits),
                        _step(r["ramp"]["sustained_step"]),
                        _step(r["ramp"]["knee_step"]),
                        _num(r["ramp"].get(knee_key), 0),
                        _num(r["ramp"]["peak_received_rate"], 1),
                    ]
                    for r in ramps
                ],
            )
        )
        out.append(
            "\n`exact` is the last step that held: the receiver got at least "
            f"{100 * _RECEIVED_FRACTION:g} % of the attempted messages and received at least "
            f"{100 * _RECEIVED_RATE_FRACTION:g} % of the offered rate within the step's "
            "scheduled sending time (the last step when every step held). The knee is the first "
            "step that lost a message or pushed the sender-observed latency p95 past "
            f"{_KNEE_FACTOR:g}x the first step's p95; `sustained` is the step before it, "
            "and a `-` knee means no step crossed the line. Both give their step's "
            "achieved send rate, its payload size for a ramp that grows the payload, or "
            "its number of simulated homes for a homes ramp. `peak received` is the "
            "highest per-step count of distinct messages received over the step's "
            "scheduled sending time. Per-step detail is in each "
            "run's `summary.json` under `ramp` and in `plots/ramp_over_time.png`."
        )
        out.extend(_ramp_cpu_tables(ramps, dimension))
    return "\n".join(out)


# Per ramp dimension: the section title, the column unit and its digits,
# then the exact, sustained and knee keys of ``ramp_summary``.
_RAMP_SECTIONS = {
    "rate": (
        "Ramp: rate held, and sustained before the knee", "msg/s", 1,
        "exact_message_rate", "sustained_message_rate", "knee_target_message_rate",
    ),
    "size": (
        "Ramp: payload held, and sustained before the knee", "payload B", 0,
        "exact_payload_size_bytes", "sustained_payload_size_bytes", "knee_payload_size_bytes",
    ),
    "homes": (
        "Ramp: homes held, and sustained before the knee", "homes", 0,
        "exact_homes", "sustained_homes", "knee_homes",
    ),
}


def _step(index: int | None) -> str:
    return "-" if index is None else str(index)

_CPU_METRICS = (
    ("process_cpu_percent", "agent process CPU, % of one core"),
    ("system_cpu_percent", "host CPU, % of all cores"),
)


def _ramp_cpu_tables(ramps: list[dict[str, Any]], dimension: str) -> list[str]:
    """Markdown tables of CPU per ramp step, one per host and metric: the
    swept dimension (offered rate or payload size) down, scenarios across,
    each scenario's busiest step in bold.

    ramps: ``compare_runs()`` rows that carry ``ramp_steps``.
    dimension: ``"rate"`` or ``"size"``, from ``ramp_dimension()``.
    Returns: the section's markdown fragments, empty when no host has data.
    """
    key, unit, _ = RAMP_AXES[dimension]
    hosts: list[str] = []
    for row in ramps:
        for step in row["ramp_steps"]:
            hosts.extend(host for host in step["resources"] if host not in hosts)
    points = sorted({step[key] for row in ramps for step in row["ramp_steps"]})
    tables: list[str] = []
    for host in hosts:
        for metric, title in _CPU_METRICS:
            columns: dict[str, tuple[dict[float, float | None], float | None]] = {}
            for row in ramps:
                values = {
                    step[key]: (step["resources"].get(host) or {}).get(metric)
                    for step in row["ramp_steps"]
                }
                peak = max((v for v in values.values() if v is not None), default=None)
                columns[row["scenario"]] = (values, peak)
            if all(peak is None for _, peak in columns.values()):
                continue
            body = []
            for point in points:
                cells = [_num(point, 0)]
                for values, peak in columns.values():
                    value = values.get(point)
                    text = _num(value, 1)
                    cells.append(f"**{text}**" if value is not None and value == peak else text)
                body.append(cells)
            tables.append(f"\n### {host}: {title}\n")
            tables.append(_table([unit, *columns], body))
    if not tables:
        return []
    return [
        "\n## Ramp: CPU per step\n",
        *tables,
        "\nAgent process CPU is the cumulative process counter over the step's window "
        "(% of one core); host CPU is the mean of the 1 Hz system samples in the window "
        "(% of all cores on that host, every process included, so on the broker host the "
        "broker's share sits in the difference). Bold marks each scenario's busiest step; "
        "`-` means no samples in the window, or a run recorded before that field existed.",
    ]


def write_csv(rows: list[dict[str, Any]], path: Path) -> None:
    """Long-format CSV: one row per run/stage/statistic, for further analysis."""
    fields = ["scenario", "run_dir", "interaction", "stage", "count", *_PERCENTILES, "min", "max", "std"]
    with path.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            for stage, stats in row["stages"].items():
                if stage == "headline" or not stats:
                    continue
                writer.writerow(
                    {
                        "scenario": row["scenario"],
                        "run_dir": row["run_dir"],
                        "interaction": row["interaction"],
                        "stage": stage,
                        **{k: stats[k] for k in ("count", *_PERCENTILES, "min", "max", "std")},
                    }
                )
