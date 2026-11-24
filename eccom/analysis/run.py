"""Analyze one run directory: merge raw per-host events into summary + plots."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from eccom.analysis.loading import load_events, load_json, load_load_report, load_system_metrics
from eccom.analysis.plots import generate_plots, generate_ramp_plot
from eccom.analysis.statistics import (
    MEASUREMENT_PHASE,
    compute_one_way_table,
    compute_rtt_table,
    compute_send_ack_table,
    compute_summary,
    step_tags,
)
from eccom.core.config import ScenarioConfig, load_scenario

logger = logging.getLogger(__name__)


def analyze_run(run_dir: Path) -> dict[str, Any]:
    """Compute summary statistics and plots from the raw event files.

    Writes ``summary.json`` and ``plots/`` into the run directory and returns
    the summary. Operates purely on stored artifacts, so it can be re-run at
    any time (``eccom analyze <run-dir>``).
    """
    scenario = load_scenario(run_dir / "scenario.yaml")
    events = load_events(run_dir)
    load_report = load_load_report(run_dir, scenario.topology.sender)
    clock_sync = resolve_clock_sync(run_dir, scenario.topology.sender)
    system_metrics = load_system_metrics(run_dir)

    summary = compute_summary(events, scenario, load_report, clock_sync, system_metrics)
    summary["link_capacity"] = link_capacity_summary(run_dir, scenario.topology.sender)
    summary_path = run_dir / "summary.json"
    summary_path.write_text(json.dumps(summary, indent=2), encoding="utf-8")
    logger.info("summary written to %s", summary_path)

    latency, metric_label = _plot_table(events, scenario, clock_sync)
    if latency is not None:
        generate_plots(run_dir / "plots", latency, metric_label)
    if scenario.load.stepped:
        _ramp_plot(run_dir, events, scenario, clock_sync, system_metrics)
    return summary


def _ramp_plot(
    run_dir: Path,
    events: pd.DataFrame,
    scenario: ScenarioConfig,
    clock_sync: dict[str, Any] | None,
    system_metrics: dict[str, list[dict[str, Any]]],
) -> None:
    """The time-aligned ramp figure: sender-observed cost (send-ack for
    one-way runs, RTT for request/response) per message with its step tag,
    plus delivery times on the sender's timeline when the clock offset is
    known."""
    sender, receiver = scenario.topology.sender, scenario.topology.receiver
    if scenario.interaction == "request_response":
        cost = compute_rtt_table(events, sender).rename(columns={"rtt_ms": "latency_ms"})
        label = "Round-trip time"
    else:
        cost = compute_send_ack_table(events, sender).rename(
            columns={"send_ack_ms": "latency_ms"}
        )
        label = "Send acknowledgement"
    cost = cost[cost["phase"] == MEASUREMENT_PHASE].merge(
        step_tags(events, sender, scenario.load.senders), on="correlation_id"
    )
    arrivals = None
    if clock_sync is not None:
        one_way = compute_one_way_table(events, sender, receiver, clock_sync["offset_ns"])
        one_way = one_way[one_way["phase"] == MEASUREMENT_PHASE]
        # arrival on the sender's timeline = send time + offset-corrected delivery
        arrivals = (one_way["send_wall_time_ns"] + one_way["latency_ms"] * 1e6).to_numpy()
    generate_ramp_plot(
        run_dir / "plots" / "ramp_over_time.png",
        cost,
        label,
        arrivals,
        system_metrics,
        sender,
        receiver,
    )


def resolve_clock_sync(run_dir: Path, sender_host: str) -> dict[str, Any] | None:
    """The clock relationship used for cross-host latencies.

    The sender's probe file provides the applied offset; its RTT/2 bound is
    tightened by the chrony bound from run.json when that is smaller. The
    probe's offset stays applied either way — chrony bounds the *true* offset
    around zero, so the correction error is bounded by
    |applied offset| + combined_max_error_s. Without a probe, chrony-disciplined
    hosts fall back to offset 0 under the chrony bound alone; a same-host run
    is exact by construction.
    """
    run_metadata = load_json(run_dir / "run.json") or {}
    meta_sync = run_metadata.get("clock_sync") or {}
    combined_s = meta_sync.get("combined_max_error_s")

    probe = load_json(run_dir / f"{sender_host}-clock-sync.json")
    if probe is not None:
        resolved = {
            "method": probe["method"],
            "offset_ns": probe["offset_ns"],
            "uncertainty_ns": probe["uncertainty_ns"],
            "uncertainty_source": "probe",
        }
        if combined_s is not None:
            chrony_bound_ns = abs(probe["offset_ns"]) + int(combined_s * 1e9)
            if chrony_bound_ns < resolved["uncertainty_ns"]:
                resolved["uncertainty_ns"] = chrony_bound_ns
                resolved["uncertainty_source"] = "chrony-tracking"
        return resolved
    if combined_s is not None:
        return {
            "method": "chrony-tracking",
            "offset_ns": 0,
            "uncertainty_ns": int(combined_s * 1e9),
            "uncertainty_source": "chrony-tracking",
        }
    if meta_sync.get("method") == "same_host":
        return {"method": "same_host", "offset_ns": 0, "uncertainty_ns": 0}
    return None


def _plot_table(
    events: pd.DataFrame,
    scenario: ScenarioConfig,
    clock_sync: dict[str, Any] | None,
) -> tuple[pd.DataFrame | None, str]:
    """Pick the latency series to plot: RTT for request/response runs,
    offset-corrected one-way latency for one-way runs."""
    rtt = compute_rtt_table(events, scenario.topology.sender)
    rtt = rtt[rtt["phase"] == MEASUREMENT_PHASE]
    if not rtt.empty:
        return rtt.rename(columns={"rtt_ms": "latency_ms"}), "Round-trip time"
    if clock_sync is not None:
        one_way = compute_one_way_table(
            events,
            scenario.topology.sender,
            scenario.topology.receiver,
            clock_sync["offset_ns"],
        )
        one_way = one_way[one_way["phase"] == MEASUREMENT_PHASE]
        if not one_way.empty:
            return one_way, "One-way latency"
    return None, ""


def link_capacity_summary(run_dir: Path, sender_host: str) -> dict[str, Any] | None:
    """The link probe's figures without the raw iperf3 documents: Mbit/s per
    direction, retransmits and RTT of the forward run, errors per direction.

    run_dir: the run directory.
    sender_host: the sender's topology label (names the record file).
    Returns: the digest, or None when the run has no probe record.
    """
    record = load_json(run_dir / f"{sender_host}-link-capacity.json")
    if not record:
        return None
    directions = record.get("directions") or {}

    def figure(name: str, key: str) -> Any:
        return (directions.get(name) or {}).get(key)

    errors = {name: r["error"] for name, r in directions.items() if "error" in r}
    return {
        "method": record.get("method"),
        "port": record.get("port"),
        "duration_s": record.get("duration_s"),
        "sender_to_receiver_mbit_s": figure("client_to_server", "mbit_s"),
        "receiver_to_sender_mbit_s": figure("server_to_client", "mbit_s"),
        "retransmits": figure("client_to_server", "retransmits"),
        "mean_rtt_ms": figure("client_to_server", "mean_rtt_ms"),
        "errors": errors or None,
    }


def _mbit(value: float | None) -> str:
    return "-" if value is None else f"{value:.1f} Mbit/s"


def format_summary(summary: dict[str, Any]) -> str:
    """Human-readable digest of a run summary for terminal output."""
    counts = summary["counts"]
    rates = summary["rates"]
    lines = [
        f"scenario:  {summary['scenario']}",
        (
            "messages:  attempted={messages_attempted} completed={messages_completed} "
            "failed={messages_failed} timeouts={timeouts} errors={errors} "
            "received={received_by_receiver}"
        ).format(**counts)
        + (f" duplicates={counts['duplicate_receipts']}" if counts.get("duplicate_receipts") else ""),
        (
            f"rate:      requested={_fmt(rates['requested_message_rate'], '/s')} "
            f"actual={_fmt(rates['actual_send_rate_from_events'], '/s')}"
        ),
    ]
    if rtt := summary.get("rtt_ms"):
        lines.append("rtt (ms):  " + _stats_line(rtt))
    if send_ack := summary.get("send_ack_ms"):
        lines.append("send-ack (ms): " + _stats_line(send_ack))
    if one_way := summary.get("one_way_latency_ms"):
        clock = summary.get("clock_sync") or {}
        lines.append(
            "one-way (ms): "
            + _stats_line(one_way)
            + (
                f"  [clock offset {clock.get('offset_ns', 0) / 1e6:.3f} ms "
                f"+/- {clock.get('uncertainty_ns', 0) / 1e6:.3f} ms, "
                f"{clock.get('method')}"
                + (
                    ", bound from chrony"
                    if clock.get("uncertainty_source") == "chrony-tracking"
                    else ""
                )
                + "]"
            )
        )
    if throughput := summary.get("throughput"):
        lines.append(
            f"throughput: {_fmt(throughput['messages_per_second'], ' msg/s')} "
            f"{_fmt(throughput['payload_bytes_per_second'], ' payload B/s')}"
        )
    if link := summary.get("link_capacity"):
        details = f"{link.get('method')} {link.get('duration_s')} s on port {link.get('port')}"
        if link.get("retransmits") is not None:
            details += f", {link['retransmits']} retransmits"
        if link.get("mean_rtt_ms"):
            details += f", rtt {link['mean_rtt_ms']:.1f} ms"
        if link.get("errors"):
            details += f", errors: {link['errors']}"
        lines.append(
            f"link: {_mbit(link.get('sender_to_receiver_mbit_s'))} sender->receiver, "
            f"{_mbit(link.get('receiver_to_sender_mbit_s'))} receiver->sender ({details})"
        )
    if system_metrics := summary.get("system_metrics"):
        for host, metrics in system_metrics.items():
            lines.append(
                f"cpu {host}: system mean={metrics['system_cpu_percent']['mean']:.1f}% "
                f"max={metrics['system_cpu_percent']['max']:.1f}%  "
                f"agent process mean={metrics['process_cpu_percent']['mean']:.1f}% "
                f"max={metrics['process_cpu_percent']['max']:.1f}%  "
                f"({metrics['samples']} samples)"
            )
    if ramp := summary.get("ramp"):
        lines.append(
            "ramp steps (measurement phase; sent/s, acked/s, recv/s = the step's counts over "
            "its scheduled sending time, recv/s counting the receipts within one sending time "
            "of the step's first receipt; recv % = every receipt of the step's messages; "
            "after = received later than that; lat = send-ack for one-way, rtt for "
            "request/response; Mbit/s = sent/received):"
        )
        lines.extend(_ramp_lines(ramp))
    return "\n".join(lines)


def _ramp_lines(ramp: list[dict[str, Any]]) -> list[str]:
    hosts = sorted({host for row in ramp for host in row["resources"]})
    show_homes = any(row.get("homes") is not None for row in ramp)
    show_size = len({row.get("payload_size_bytes") for row in ramp}) > 1
    header = [
        "step", *(["homes"] if show_homes else []), "target/s",
        *(["size B"] if show_size else []),
        "sent/s", "acked/s", "recv/s", "n", "recv %", "after", "lost", "dup",
        "timeouts", "errors", "lat p50", "p95", "p99",
    ]
    for host in hosts:
        header += [f"{host} cpu%", f"{host} Mbit/s", f"{host} lag ms"]

    def cell(value: float | None, digits: int = 1) -> str:
        return "-" if value is None else f"{value:.{digits}f}"

    rows: list[list[str]] = []
    for row in ramp:
        cost = row["send_ack_ms"] or row["rtt_ms"]
        cells = [
            str(row["step"]),
            *([str(row["homes"])] if show_homes else []),
            cell(row["target_message_rate"], 0),
            *([str(row["payload_size_bytes"])] if show_size else []),
            cell(row["sent_rate"]),
            cell(row["completed_rate"]),
            cell(row["received_rate"]),
            str(row["messages_attempted"]),
            cell(100 * row["received_fraction"], 2),
            str(row["received_after_step"]),
            str(row["lost"]),
            str(row["duplicate_receipts"]),
            str(row["timeouts"]),
            str(row["errors"]),
            *(cell(cost[key]) if cost else "-" for key in ("median", "p95", "p99")),
        ]
        for host in hosts:
            usage = row["resources"].get(host) or {}
            cells += [
                cell(usage.get("process_cpu_percent")),
                f"{cell(usage.get('net_sent_mbit_s'), 2)}/{cell(usage.get('net_recv_mbit_s'), 2)}",
                cell(usage.get("loop_lag_ms_max")),
            ]
        rows.append(cells)
    widths = [max(len(header[i]), *(len(r[i]) for r in rows)) for i in range(len(header))]

    def line(cells: list[str]) -> str:
        return "  " + "  ".join(c.rjust(w) for c, w in zip(cells, widths))

    return [line(header), *(line(r) for r in rows)]


def _stats_line(stats: dict[str, float]) -> str:
    return (
        "min={min:.2f} mean={mean:.2f} median={median:.2f} "
        "std={std:.2f} p95={p95:.2f} p99={p99:.2f} max={max:.2f}"
    ).format(**stats)


def _fmt(value: float | None, unit: str) -> str:
    return f"{value:.1f}{unit}" if value is not None else "n/a"
