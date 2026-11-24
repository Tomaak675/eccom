"""Read raw run artifacts (JSONL event files, JSON reports) into memory."""

from __future__ import annotations

import gzip
import json
from bisect import bisect_right
from pathlib import Path
from typing import Any, IO

import pandas as pd

from eccom.core.artifacts import instance_labels, split_label

EVENT_COLUMNS = [
    "event_type",
    "correlation_id",
    "sequence_number",
    "host",
    "phase",
    "monotonic_ns",
    "wall_time_ns",
    "metadata",
]


def jsonl_files(run_dir: Path, suffix: str) -> list[tuple[str, Path]]:
    """The run's ``<label>-<suffix>`` files, plain or gzip-compressed (the
    SSH deployment compresses them on the host before the download).

    suffix: e.g. ``events.jsonl``.
    Returns (label, path) pairs in name order.
    """
    found = {}
    for path in list(run_dir.glob(f"*-{suffix}")) + list(run_dir.glob(f"*-{suffix}.gz")):
        label = path.name.removesuffix(".gz").removesuffix(f"-{suffix}")
        found.setdefault(label, path)
    return sorted(found.items())


def open_text(path: Path) -> IO[str]:
    """A text handle on a plain or gzip-compressed artifact."""
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8")
    return path.open(encoding="utf-8")


def load_events(run_dir: Path) -> pd.DataFrame:
    """Load all per-host event files (``*-events.jsonl[.gz]``) into one frame.

    The ``instance`` column is the sender process that wrote the file
    (``<host>.<i>-events.jsonl``; 0 for a single-process host), so per-process
    rates can be formed even though every process reports the same host.

    Builds the frame column by column and keeps one object per distinct string
    and per distinct metadata dict: a message's correlation id appears in ~6
    events, and a row-per-dict load of a multi-million-message run needs
    ~1.2 KB per event."""
    columns: dict[str, list[Any]] = {name: [] for name in [*EVENT_COLUMNS, "instance"]}
    shared: dict[Any, Any] = {}
    for label, path in jsonl_files(run_dir, "events.jsonl"):
        instance = int(label.rsplit(".", 1)[1]) if "." in label else 0
        with open_text(path) as fh:
            for number, line in enumerate(fh, start=1):
                if line.strip():
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError as exc:
                        raise ValueError(
                            f"{path} line {number} is not valid JSON ({exc.msg}); a cut-off "
                            "last line means the agent's disk filled or it was killed mid-write"
                        ) from exc
                    for name in _SHARED_STRINGS:
                        value = row[name]
                        columns[name].append(shared.setdefault(value, value))
                    for name in _PLAIN:
                        columns[name].append(row[name])
                    columns["metadata"].append(_shared_metadata(row["metadata"], shared))
                    columns["instance"].append(instance)
    return pd.DataFrame(columns)


_SHARED_STRINGS = ("event_type", "correlation_id", "host", "phase")
_PLAIN = ("sequence_number", "monotonic_ns", "wall_time_ns")


def _shared_metadata(metadata: dict[str, Any] | None, shared: dict[Any, Any]) -> dict[str, Any] | None:
    """The one stored copy of an event's metadata dict (the analysis only reads it).

    metadata: the parsed dict, or None.
    shared: cache of distinct values, keyed by the dict's typed items (so 1 and 1.0 stay apart).
    Returns the cached dict equal to ``metadata``, or ``metadata`` itself when
    a value is unhashable."""
    if metadata is None:
        return None
    try:
        key = ("metadata", *((name, type(value), value) for name, value in metadata.items()))
        return shared.setdefault(key, metadata)
    except TypeError:
        return metadata


def load_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def load_load_report(run_dir: Path, sender_host: str) -> dict[str, Any] | None:
    """The sender's load report; with ``load.senders > 1`` the per-instance
    reports (``<sender>.<i>-load-report.json``) summed into one (see
    merge_load_reports)."""
    labels = instance_labels(run_dir, sender_host, "load-report.json")
    reports = [r for label in labels if (r := load_json(run_dir / f"{label}-load-report.json"))]
    if not reports:
        return None
    return reports[0] if len(reports) == 1 else merge_load_reports(reports)


def merge_load_reports(reports: list[dict[str, Any]]) -> dict[str, Any]:
    """Sum the load reports of several sender instances: rates and counts add
    up (each instance generated a share of the same schedule), windows are the
    longest, ramp steps are summed step by step. ``instances`` records how many
    reports went in."""
    counters = ("messages_attempted", "messages_completed", "messages_failed")
    merged: dict[str, Any] = {
        "requested_message_rate": sum(r["requested_message_rate"] for r in reports),
        "actual_message_rate": sum(r["actual_message_rate"] for r in reports),
        **{key: sum(r[key] for r in reports) for key in counters},
        "send_window_s": max(r["send_window_s"] for r in reports),
        "total_duration_s": max(r["total_duration_s"] for r in reports),
        "per_device": None,
        "steps": None,
        "instances": len(reports),
    }
    if all(r.get("steps") for r in reports):
        by_step: dict[int, dict[str, Any]] = {}
        for report in reports:
            for step in report["steps"]:
                acc = by_step.setdefault(
                    step["step"],
                    {
                        "step": step["step"],
                        "target_message_rate": 0.0,
                        "actual_message_rate": 0.0,
                        **{key: 0 for key in counters},
                        "duration_s": 0.0,
                    },
                )
                for key in ("target_message_rate", "actual_message_rate", *counters):
                    acc[key] += step[key]
                acc["duration_s"] = max(acc["duration_s"], step["duration_s"])
        merged["steps"] = [by_step[index] for index in sorted(by_step)]
    return merged


def merge_instance_samples(series: list[list[dict[str, Any]]]) -> list[dict[str, Any]]:
    """Fold the resource samples of several processes on one host into one
    series on the first instance's timeline (as-of join: for every sample of
    instance 0, each other instance contributes its latest sample no older
    than ``_STALE_NS``). Process metrics add up (CPU seconds, CPU %, RSS), the
    event-loop lag is the maximum, host-wide metrics (system CPU, memory, NIC
    counters) and the phase tag are instance 0's."""
    if len(series) == 1:
        return series[0]
    base = sorted(series[0], key=lambda s: s["wall_time_ns"])
    others = [sorted(s, key=lambda x: x["wall_time_ns"]) for s in series[1:]]
    other_times = [[s["wall_time_ns"] for s in other] for other in others]
    merged = []
    for sample in base:
        row = dict(sample)
        t = sample["wall_time_ns"]
        for other, times in zip(others, other_times):
            index = bisect_right(times, t) - 1
            if index < 0 or t - times[index] > _STALE_NS:
                continue
            contribution = other[index]
            for key in ("process_cpu_percent", "process_cpu_seconds", "process_rss_bytes"):
                if key in row and key in contribution:
                    row[key] += contribution[key]
            if "loop_lag_ms_max" in contribution:
                row["loop_lag_ms_max"] = max(
                    row.get("loop_lag_ms_max", 0.0), contribution["loop_lag_ms_max"]
                )
        merged.append(row)
    return merged


_STALE_NS = int(2.5e9)  # an instance's sample counts for at most ~2 sampling intervals


def load_system_metrics(run_dir: Path) -> dict[str, list[dict[str, Any]]]:
    """Per-host resource samples from ``*-system-metrics.jsonl``. Several
    instances of one host (``edge.0``, ``edge.1``, ...) are merged into one
    ``edge`` series (merge_instance_samples)."""
    samples: dict[str, list[dict[str, Any]]] = {}
    by_host: dict[str, list[tuple[int, list[dict[str, Any]]]]] = {}
    for label, path in jsonl_files(run_dir, "system-metrics.jsonl"):
        host, instance = split_label(label)
        with open_text(path) as fh:
            rows = [json.loads(line) for line in fh if line.strip()]
        by_host.setdefault(host, []).append((instance or 0, rows))
    for host, instances in by_host.items():
        samples[host] = merge_instance_samples([rows for _, rows in sorted(instances)])
    return samples
