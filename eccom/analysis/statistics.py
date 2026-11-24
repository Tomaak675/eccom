"""Offline statistics over raw measurement events.

RTT is always computed from *sender-side monotonic* timestamps
(SEND_STARTED -> REPLY_RECEIVED on the same host), so it is valid regardless
of clock synchronization. Receiver-side durations likewise use only
receiver-local monotonic timestamps. Wall-clock timestamps are used only for
placing events on a shared timeline (plots, throughput buckets).
"""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

from eccom.core.config import ScenarioConfig
from eccom.core.events import EventType

MEASUREMENT_PHASE = "measurement"
# A receipt counts as on time when its one-way delivery took at most this
# long; later ones are reported as late. An analysis convention (the client
# timeout of the shipped scenarios), not a scenario setting.


def latency_stats_ms(values: "np.ndarray | pd.Series") -> dict[str, float] | None:
    arr = np.asarray(values, dtype=float)
    if arr.size == 0:
        return None
    return {
        "count": int(arr.size),
        "min": float(arr.min()),
        "mean": float(arr.mean()),
        "median": float(np.median(arr)),
        "std": float(arr.std(ddof=1)) if arr.size > 1 else 0.0,
        "p90": float(np.percentile(arr, 90)),
        "p95": float(np.percentile(arr, 95)),
        "p99": float(np.percentile(arr, 99)),
        "max": float(arr.max()),
    }


def variation_stats(values: "np.ndarray | pd.Series | list[float]") -> dict[str, float]:
    """Sample variance and coefficient of variation of repeated measurements
    x1, x2, ..., for judging how reproducible a measured figure is.

    values: the measurements, at least two.
    Returns mean, variance (n-1 denominator), std (its square root) and cv
        (std / mean as a ratio, not a percentage); nan for a single value.
    """
    arr = np.asarray(values, dtype=float)
    mean = float(arr.mean())
    variance = float(arr.var(ddof=1))
    std = variance ** 0.5
    return {"mean": mean, "variance": variance, "std": std, "cv": std / mean}


def compute_rtt_table(events: pd.DataFrame, sender_host: str) -> pd.DataFrame:
    """One row per completed request: correlation_id, sequence_number, phase,
    send_wall_time_ns, rtt_ms."""
    return _sender_pair_table(events, sender_host, EventType.REPLY_RECEIVED, "rtt_ms")


def compute_send_ack_table(events: pd.DataFrame, sender_host: str) -> pd.DataFrame:
    """One row per acknowledged send (SEND_STARTED -> SEND_COMPLETED on the
    sender's monotonic clock): correlation_id, sequence_number, phase,
    send_wall_time_ns, send_ack_ms."""
    return _sender_pair_table(events, sender_host, EventType.SEND_COMPLETED, "send_ack_ms")


def _sender_pair_table(
    events: pd.DataFrame, sender_host: str, end_type: EventType, value_column: str
) -> pd.DataFrame:
    """SEND_STARTED joined with ``end_type`` per correlation id, both on the
    sender; the duration lands in ``value_column`` (ms) next to the send's
    wall time, so the table can be placed on the shared timeline."""
    columns = ["correlation_id", "sequence_number", "phase", "send_wall_time_ns", value_column]
    if events.empty:
        return pd.DataFrame(columns=columns)
    sender_events = events[events["host"] == sender_host]
    sends = sender_events[sender_events["event_type"] == EventType.SEND_STARTED]
    ends = sender_events[sender_events["event_type"] == end_type]
    joined = sends.set_index("correlation_id").join(
        ends.set_index("correlation_id")[["monotonic_ns"]],
        how="inner",
        rsuffix="_end",
    )
    return pd.DataFrame(
        {
            "correlation_id": joined.index,
            "sequence_number": joined["sequence_number"].to_numpy(),
            "phase": joined["phase"].to_numpy(),
            "send_wall_time_ns": joined["wall_time_ns"].to_numpy(),
            value_column: (joined["monotonic_ns_end"] - joined["monotonic_ns"]).to_numpy() / 1e6,
        }
    ).reset_index(drop=True)


def paired_duration_ms(
    host_events: pd.DataFrame,
    start_type: EventType,
    end_type: EventType,
    correlation_ids: set[str],
) -> pd.Series:
    """Per-correlation-id monotonic duration between two event types on one host."""
    scoped = host_events[host_events["correlation_id"].isin(correlation_ids)]
    starts = scoped[scoped["event_type"] == start_type].set_index("correlation_id")
    ends = scoped[scoped["event_type"] == end_type].set_index("correlation_id")
    joined = starts[["monotonic_ns"]].join(ends[["monotonic_ns"]], how="inner", rsuffix="_end")
    return (joined["monotonic_ns_end"] - joined["monotonic_ns"]) / 1e6


def compute_one_way_table(
    events: pd.DataFrame,
    sender_host: str,
    receiver_host: str,
    clock_offset_ns: int,
) -> pd.DataFrame:
    """One row per delivered message: sender SEND_STARTED wall time joined
    with receiver MESSAGE_RECEIVED wall time, corrected by the measured clock
    offset (receiver clock minus sender clock). Only meaningful when the
    offset's uncertainty is small against the latencies."""
    columns = ["correlation_id", "sequence_number", "phase", "send_wall_time_ns", "latency_ms"]
    if events.empty:
        return pd.DataFrame(columns=columns)
    sends = events[
        (events["host"] == sender_host) & (events["event_type"] == EventType.SEND_STARTED)
    ]
    received = events[
        (events["host"] == receiver_host)
        & (events["event_type"] == EventType.MESSAGE_RECEIVED)
    ]
    joined = sends.set_index("correlation_id").join(
        received.set_index("correlation_id")[["wall_time_ns"]],
        how="inner",
        rsuffix="_received",
    )
    return pd.DataFrame(
        {
            "correlation_id": joined.index,
            "sequence_number": joined["sequence_number"].to_numpy(),
            "phase": joined["phase"].to_numpy(),
            "send_wall_time_ns": joined["wall_time_ns"].to_numpy(),
            "latency_ms": (
                joined["wall_time_ns_received"].to_numpy()
                - joined["wall_time_ns"].to_numpy()
                - clock_offset_ns
            )
            / 1e6,
        }
    ).reset_index(drop=True)


def summarize_system_metrics(
    samples_by_host: dict[str, list[dict[str, Any]]],
) -> dict[str, Any] | None:
    """Per-host CPU/memory aggregates over the agent's whole lifetime
    (warmup + measurement + cooldown; samples are 1 Hz by default, so window
    edges are not worth clipping). system_cpu is host-wide; process_cpu is
    the agent process alone."""
    summary: dict[str, Any] = {}
    for host, samples in samples_by_host.items():
        if not samples:
            continue
        system_cpu = [s["system_cpu_percent"] for s in samples]
        process_cpu = [s["process_cpu_percent"] for s in samples]
        summary[host] = {
            "samples": len(samples),
            "system_cpu_percent": {
                "mean": sum(system_cpu) / len(system_cpu),
                "max": max(system_cpu),
            },
            "process_cpu_percent": {
                "mean": sum(process_cpu) / len(process_cpu),
                "max": max(process_cpu),
            },
            "system_memory_percent_max": max(s["system_memory_percent"] for s in samples),
            "process_rss_bytes_max": max(s["process_rss_bytes"] for s in samples),
        }
    return summary or None


def counter_delta(window: list[dict[str, Any]], key: str) -> tuple[float, float] | None:
    """(counter delta, wall seconds) of a cumulative field across one
    contiguous sample window. None with fewer than two samples or when the
    field is absent (runs recorded before it existed)."""
    if len(window) < 2 or key not in window[0] or key not in window[-1]:
        return None
    return (
        float(window[-1][key] - window[0][key]),
        (window[-1]["wall_time_ns"] - window[0]["wall_time_ns"]) / 1e9,
    )


def window_resource_usage(
    samples: list[dict[str, Any]], start_wall_ns: int, end_wall_ns: int
) -> dict[str, float | None]:
    """One agent's resource usage over the half-open wall-clock window
    [start, end): process CPU (% of one core) and NIC throughput from the
    cumulative counters, host-wide CPU (% of all cores, mean of the window's
    samples), plus the worst event-loop lag any sample in the window saw.
    Counter figures are None when the window holds fewer than two samples."""
    window = [s for s in samples if start_wall_ns <= s["wall_time_ns"] < end_wall_ns]

    def per_second(key: str, scale: float) -> float | None:
        delta = counter_delta(window, key)
        if delta is None or delta[1] <= 0:
            return None
        return scale * delta[0] / delta[1]

    lags = [s["loop_lag_ms_max"] for s in window if "loop_lag_ms_max" in s]
    system_cpu = [s["system_cpu_percent"] for s in window if "system_cpu_percent" in s]
    return {
        "samples": len(window),
        "process_cpu_percent": per_second("process_cpu_seconds", 100.0),
        "system_cpu_percent": sum(system_cpu) / len(system_cpu) if system_cpu else None,
        "net_sent_mbit_s": per_second("net_bytes_sent", 8 / 1e6),
        "net_recv_mbit_s": per_second("net_bytes_recv", 8 / 1e6),
        "loop_lag_ms_max": max(lags) if lags else None,
    }


def step_tags(events: pd.DataFrame, sender_host: str, senders: int = 1) -> pd.DataFrame:
    """ramp and homes modes: which step each message was scheduled in, from
    the PAYLOAD_CREATED metadata the load generator attached (columns
    correlation_id, step, target_rate, target_size, target_homes -- the size
    NaN unless the ramp grows the payload, the homes NaN outside homes
    mode). Empty for the other load modes.

    senders: ``load.senders``; each process tags its own share of the rate,
    so target_rate is scaled back to the whole schedule's offered rate.
    """
    columns = ["correlation_id", "step", "target_rate", "target_size", "target_homes", "instance"]
    if events.empty:
        return pd.DataFrame(columns=columns)
    payloads = events[
        (events["host"] == sender_host) & (events["event_type"] == EventType.PAYLOAD_CREATED)
    ]
    metadata = payloads["metadata"].apply(
        lambda m: m if isinstance(m, dict) and "step" in m else None
    )
    tagged = metadata.notna()
    if not tagged.any():
        return pd.DataFrame(columns=columns)
    return pd.DataFrame(
        {
            "correlation_id": payloads.loc[tagged, "correlation_id"].to_numpy(),
            "step": metadata[tagged].apply(lambda m: int(m["step"])).to_numpy(),
            "target_rate": metadata[tagged]
            .apply(lambda m: float(m["target_rate"]) * senders)
            .to_numpy(),
            "target_size": metadata[tagged]
            .apply(lambda m: m.get("target_size"))
            .astype(float)
            .to_numpy(),
            "target_homes": metadata[tagged]
            .apply(lambda m: m.get("target_homes"))
            .astype(float)
            .to_numpy(),
            # the sender process that scheduled the message, for per-process rates
            "instance": (
                payloads.loc[tagged, "instance"].to_numpy()
                if "instance" in payloads.columns
                else 0
            ),
        }
    )


def _rate_from_times(wall_ns: pd.Series) -> float | None:
    """Events per second implied by n timestamps: (n - 1) / span. Unbiased for
    a cadence (n / span overstates by one interval); None below two events."""
    if len(wall_ns) < 2:
        return None
    span_s = float(wall_ns.max() - wall_ns.min()) / 1e9
    return (len(wall_ns) - 1) / span_s if span_s > 0 else None


def _rate_per_instance(frame: pd.DataFrame) -> float | None:
    """``_rate_from_times`` per sender process (``instance`` column), summed.

    Several processes run the same step offset by their start-up jitter, so
    the pooled span overstates every process's own window and understates
    the rate; each process's cadence over its own span adds up correctly.
    Frames without the column count as one process.
    """
    if "instance" not in frame.columns:
        return _rate_from_times(frame["wall_time_ns"])
    rates = [
        rate
        for _, group in frame.groupby("instance")
        if (rate := _rate_from_times(group["wall_time_ns"])) is not None
    ]
    return sum(rates) if rates else None


# The dimension a ramp sweeps: the step field carrying it, the table unit and
# the chart axis label.
RAMP_AXES = {
    "rate": ("target_message_rate", "offered msg/s", "Offered rate (msg/s, log)"),
    "size": ("payload_size_bytes", "payload B", "Payload size (B, log)"),
    "homes": ("homes", "homes", "Simulated homes"),
}


def ramp_dimension(runs: list[list[dict[str, Any]]]) -> str:
    """``"homes"`` when any run's steps carry a number of homes, ``"size"``
    when any run's steps vary the payload size, else ``"rate"``.

    runs: the ``ramp`` step rows of each compared run.
    """
    if any(step.get("homes") is not None for steps in runs for step in steps):
        return "homes"
    for steps in runs:
        if len({step.get("payload_size_bytes") for step in steps}) > 1:
            return "size"
    return "rate"


def _step_sending_time_s(scenario: ScenarioConfig, step: int) -> float:
    """The scheduled sending time of one step: ``load.step_duration_s``, or
    what ``load.duration_s`` leaves of it for a truncated last step.

    scenario: a stepped-load scenario.
    step: the 0-based step index.
    Returns the seconds the schedule spends sending in that step.
    """
    load = scenario.load
    assert load.step_duration_s is not None  # enforced by config validation
    if load.duration_s is None:
        return load.step_duration_s
    return min(load.step_duration_s, load.duration_s - step * load.step_duration_s)


def compute_ramp_table(
    events: pd.DataFrame,
    scenario: ScenarioConfig,
    clock_sync: dict[str, Any] | None,
    system_metrics: dict[str, list[dict[str, Any]]] | None,
) -> list[dict[str, Any]]:
    """One row per ramp step over the measurement phase: what was offered,
    what the sender achieved, what the receiver saw, the latency distribution
    of that step alone, and both agents' resource usage over the step's
    window.

    Messages are attributed to steps by the generator's per-message tag,
    never by timestamps. The window -- first send of the step up to the first
    send of the next step, or the last send for the final step and for every
    step when ``load.step_pause_s`` separates them -- only scopes the
    resource samples.

    Counts: a message is received once however often it arrives
    (``received_by_receiver`` counts correlation ids, ``duplicate_receipts``
    the extra arrivals) and whenever it arrives. ``received_in_step`` are
    those that arrived inside the step's receive window -- the step's
    sending time, counted on the receiver's clock from the first receipt of
    one of the step's messages, per sender process (the processes of
    ``load.senders`` start apart) -- and ``received_after_step`` those that
    arrived later: during the step's pause, a following step or after the
    run. The pause lets late messages drain into the received count; it
    adds nothing to the rate; ``lost`` never arrived, and
    ``timeouts_delivered`` are the sender's timeouts that arrived anyway.

    Rates: ``sent_rate``, ``completed_rate`` and ``received_rate`` are the
    step's counts over its scheduled sending time, so the three compare with
    the offered rate and with each other. ``received_rate`` counts only
    ``received_in_step``, so a
    step's stragglers are discarded from it and never land in another step's
    numbers; the ``*_fraction`` keys are sent / offered, completed /
    attempted and received / attempted, the last over every receipt of the
    step's messages, late ones included. ``actual_send_rate`` and ``delivered_rate`` remain
    the cadence estimates ``(n - 1) / span`` of the send and first-receipt
    times: the first decides whether the schedule held, the second collapses
    when one receipt straggles and is kept for the record only.
    """
    sender_host, receiver_host = scenario.topology.sender, scenario.topology.receiver
    tags = step_tags(events, sender_host, scenario.load.senders)
    if tags.empty:
        return []
    sender_events = events[events["host"] == sender_host].drop(columns="instance", errors="ignore")
    sends = sender_events[
        (sender_events["event_type"] == EventType.SEND_STARTED)
        & (sender_events["phase"] == MEASUREMENT_PHASE)
    ].merge(tags, on="correlation_id")
    if sends.empty:
        return []
    tags = tags[tags["correlation_id"].isin(set(sends["correlation_id"]))]

    receiver_events = events[events["host"] == receiver_host].drop(columns="instance", errors="ignore")
    receipts = receiver_events[receiver_events["event_type"] == EventType.MESSAGE_RECEIVED].merge(
        tags, on="correlation_id"
    )
    # the first arrival of every message; later ones are duplicates
    received = receipts.sort_values("wall_time_ns").drop_duplicates("correlation_id")
    receipts_per_step = receipts.groupby("step").size()
    first_receipt_ns = received.groupby(["step", "instance"])["wall_time_ns"].transform("min")
    send_ack = compute_send_ack_table(events, sender_host).merge(tags, on="correlation_id")
    rtt = compute_rtt_table(events, sender_host).merge(tags, on="correlation_id")
    delivery = (
        compute_one_way_table(events, sender_host, receiver_host, clock_sync["offset_ns"])
        .merge(tags, on="correlation_id")
        .sort_values("latency_ms")
        .drop_duplicates("correlation_id")
        if clock_sync is not None
        else None
    )
    one_way = scenario.interaction == "one_way"

    steps = [int(step) for step in sorted(sends["step"].unique())]
    first_send = {
        step: int(sends.loc[sends["step"] == step, "wall_time_ns"].min()) for step in steps
    }
    # The sender's outcome events joined to their step once and counted per
    # (step, type): one pass instead of a scan of every sender event per
    # step and type, which was a third of the analysis time at 2 M messages.
    outcome_types = [t.value for t in (EventType.SEND_COMPLETED, EventType.TIMEOUT, EventType.ERROR)]
    outcomes = sender_events.loc[
        sender_events["event_type"].isin(outcome_types), ["correlation_id", "event_type"]
    ].merge(tags[["correlation_id", "step"]], on="correlation_id")
    outcome_counts = outcomes.groupby(["step", "event_type"]).size()
    timeouts_delivered = (
        outcomes[
            (outcomes["event_type"] == EventType.TIMEOUT.value)
            & outcomes["correlation_id"].isin(set(received["correlation_id"]))
        ]
        .groupby("step")
        .size()
    )

    def sender_count(step: int, event_type: EventType) -> int:
        return int(outcome_counts.get((step, event_type.value), 0))

    rows: list[dict[str, Any]] = []
    for index, step in enumerate(steps):
        step_sends = sends[sends["step"] == step]
        step_rtt = rtt[rtt["step"] == step]
        step_received = received[received["step"] == step]
        step_delivery = delivery[delivery["step"] == step] if delivery is not None else None
        completed = sender_count(step, EventType.SEND_COMPLETED) if one_way else len(step_rtt)
        attempted = len(step_sends)
        target_rate = float(step_sends["target_rate"].iloc[0])
        sending_time_s = _step_sending_time_s(scenario, step)
        in_step = int(
            (
                step_received["wall_time_ns"]
                < first_receipt_ns[step_received.index] + sending_time_s * 1e9
            ).sum()
        )
        end = (
            first_send[steps[index + 1]]
            if index + 1 < len(steps) and not scenario.load.step_pause_s
            else int(step_sends["wall_time_ns"].max())
        )
        tagged_size = step_sends["target_size"].iloc[0]
        if pd.notna(tagged_size):
            payload_size = int(tagged_size)
        else:
            # homes mode mixes sizes within a step and has no payload block
            payload_size = scenario.payload.size_bytes if scenario.payload is not None else None
        tagged_homes = step_sends["target_homes"].iloc[0]
        rows.append(
            {
                "step": step,
                "target_message_rate": target_rate,
                "homes": int(tagged_homes) if pd.notna(tagged_homes) else None,
                "payload_size_bytes": payload_size,
                "step_sending_time_s": sending_time_s,
                "messages_attempted": attempted,
                "messages_completed": completed,
                "messages_failed": attempted - completed,
                "timeouts": sender_count(step, EventType.TIMEOUT),
                "timeouts_delivered": int(timeouts_delivered.get(step, 0)),
                "errors": sender_count(step, EventType.ERROR),
                "received_by_receiver": len(step_received),
                "duplicate_receipts": int(receipts_per_step.get(step, 0)) - len(step_received),
                "received_in_step": in_step,
                "received_after_step": len(step_received) - in_step,
                "lost": attempted - len(step_received),
                "sent_rate": attempted / sending_time_s,
                "completed_rate": completed / sending_time_s,
                "received_rate": in_step / sending_time_s,
                "sent_fraction": attempted / sending_time_s / target_rate,
                "completed_fraction": completed / attempted,
                "received_fraction": len(step_received) / attempted,
                "actual_send_rate": _rate_per_instance(step_sends),
                # receiver clock only: count and window live on one host
                "delivered_rate": _rate_per_instance(step_received),
                "send_ack_ms": latency_stats_ms(send_ack[send_ack["step"] == step]["send_ack_ms"]),
                "rtt_ms": latency_stats_ms(step_rtt["rtt_ms"]),
                "delivery_ms": (
                    latency_stats_ms(step_delivery["latency_ms"])
                    if step_delivery is not None
                    else None
                ),
                "window": {"start_wall_ns": first_send[step], "end_wall_ns": end},
                "resources": {
                    host: window_resource_usage(samples, first_send[step], end)
                    for host, samples in (system_metrics or {}).items()
                },
            }
        )
    return rows


def _requested_rate(scenario: ScenarioConfig) -> float:
    load = scenario.load
    if load.mode == "devices":
        assert scenario.devices is not None  # enforced by config validation
        return sum(1 / device.interval_s for device in scenario.devices)
    if load.mode == "homes":
        assert scenario.home is not None and load.homes is not None  # enforced by config validation
        return scenario.home.message_rate(load.homes, load.aggregation_factor)
    assert load.messages_per_second is not None
    return load.messages_per_second


def compute_summary(
    events: pd.DataFrame,
    scenario: ScenarioConfig,
    load_report: dict[str, Any] | None,
    clock_sync: dict[str, Any] | None = None,
    system_metrics: dict[str, list[dict[str, Any]]] | None = None,
) -> dict[str, Any]:
    """``clock_sync`` (offset_ns/uncertainty_ns/method, from the sender's
    probe file or the same-host fallback) enables cross-host one-way latency;
    without it those stats are None. ``system_metrics`` carries the per-host
    resource samples from load_system_metrics."""
    sender_host = scenario.topology.sender
    receiver_host = scenario.topology.receiver

    sender_events = (
        events[events["host"] == sender_host] if not events.empty else events
    )
    measurement_sends = (
        sender_events[
            (sender_events["event_type"] == EventType.SEND_STARTED)
            & (sender_events["phase"] == MEASUREMENT_PHASE)
        ]
        if not sender_events.empty
        else sender_events
    )
    measurement_ids: set[str] = (
        set(measurement_sends["correlation_id"]) if not measurement_sends.empty else set()
    )

    rtt_all = compute_rtt_table(events, sender_host)
    rtt = rtt_all[rtt_all["phase"] == MEASUREMENT_PHASE]

    def _sender_count(event_type: EventType) -> int:
        if sender_events.empty:
            return 0
        return int(
            (
                (sender_events["event_type"] == event_type)
                & (sender_events["phase"] == MEASUREMENT_PHASE)
            ).sum()
        )

    attempted = len(measurement_sends)
    # "completed" means the interaction finished from the sender's view:
    # reply received (request_response) or send acknowledged (one_way).
    if scenario.interaction == "one_way":
        completed = _sender_count(EventType.SEND_COMPLETED)
    else:
        completed = len(rtt)

    receiver_events = (
        events[events["host"] == receiver_host] if not events.empty else events
    )
    received_count = 0
    duplicate_receipts = 0
    handler_stats = None
    receiver_total_stats = None
    if not receiver_events.empty and measurement_ids:
        received_ids = receiver_events.loc[
            (receiver_events["event_type"] == EventType.MESSAGE_RECEIVED)
            & receiver_events["correlation_id"].isin(measurement_ids),
            "correlation_id",
        ]
        # a message is received once however often it arrives
        received_count = int(received_ids.nunique())
        duplicate_receipts = len(received_ids) - received_count
        handler_stats = latency_stats_ms(
            paired_duration_ms(
                receiver_events,
                EventType.HANDLER_STARTED,
                EventType.HANDLER_COMPLETED,
                measurement_ids,
            )
        )
        receiver_total_stats = latency_stats_ms(
            paired_duration_ms(
                receiver_events,
                EventType.MESSAGE_RECEIVED,
                EventType.REPLY_SEND_STARTED,
                measurement_ids,
            )
        )

    throughput = None
    if scenario.interaction == "one_way":
        # Delivery throughput from the receiver's own clock (window and count
        # both live on one host, so no clock sync is needed).
        received_events = (
            receiver_events[
                (receiver_events["event_type"] == EventType.MESSAGE_RECEIVED)
                & receiver_events["correlation_id"].isin(measurement_ids)
            ]
            if not receiver_events.empty
            else receiver_events
        )
        if len(received_events) > 1:
            duration_s = float(
                (received_events["wall_time_ns"].max() - received_events["wall_time_ns"].min())
                / 1e9
            )
            payload_bytes = int(
                received_events["metadata"]
                .apply(lambda m: (m or {}).get("size_bytes", 0))
                .sum()
            )
            throughput = {
                "duration_s": duration_s,
                "messages_per_second": (
                    len(received_events) / duration_s if duration_s > 0 else None
                ),
                "payload_bytes_per_second": (
                    payload_bytes / duration_s if duration_s > 0 else None
                ),
            }
    elif completed > 0:
        # Reply arrival on the shared timeline = send wall time + RTT; exact
        # for the sender host since both come from the same clock pair.
        reply_wall_ns = rtt["send_wall_time_ns"] + rtt["rtt_ms"] * 1e6
        duration_s = float(
            (reply_wall_ns.max() - rtt["send_wall_time_ns"].min()) / 1e9
        )
        payload_events = sender_events[
            (sender_events["event_type"] == EventType.PAYLOAD_CREATED)
            & sender_events["correlation_id"].isin(set(rtt["correlation_id"]))
        ]
        payload_bytes = int(
            payload_events["metadata"]
            .apply(lambda m: (m or {}).get("size_bytes", 0))
            .sum()
        )
        throughput = {
            "duration_s": duration_s,
            "messages_per_second": completed / duration_s if duration_s > 0 else None,
            "payload_bytes_per_second": payload_bytes / duration_s if duration_s > 0 else None,
        }

    # Sender-side ack latency (SEND_STARTED -> SEND_COMPLETED, one monotonic
    # clock): HTTP one-way = 204 received; MQTT QoS 1/2 = broker PUBACK/PUBCOMP.
    send_ack_stats = None
    if not sender_events.empty and measurement_ids:
        send_ack_stats = latency_stats_ms(
            paired_duration_ms(
                sender_events,
                EventType.SEND_STARTED,
                EventType.SEND_COMPLETED,
                measurement_ids,
            )
        )

    one_way_stats = None
    if clock_sync is not None:
        one_way = compute_one_way_table(
            events, sender_host, receiver_host, clock_sync["offset_ns"]
        )
        one_way_stats = latency_stats_ms(
            one_way[one_way["phase"] == MEASUREMENT_PHASE]["latency_ms"]
        )

    actual_send_rate = None
    if attempted > 1:
        send_window_s = float(
            (measurement_sends["wall_time_ns"].max() - measurement_sends["wall_time_ns"].min())
            / 1e9
        )
        if send_window_s > 0:
            actual_send_rate = attempted / send_window_s

    return {
        "scenario": scenario.name,
        "counts": {
            "messages_attempted": attempted,
            "messages_completed": completed,
            "messages_failed": attempted - completed,
            "timeouts": _sender_count(EventType.TIMEOUT),
            "errors": _sender_count(EventType.ERROR),
            "received_by_receiver": received_count,
            "duplicate_receipts": duplicate_receipts,
        },
        "rates": {
            "requested_message_rate": _requested_rate(scenario),
            # Derived from SEND_STARTED wall timestamps (excludes the trailing
            # inter-send gap, so it can differ slightly from the load report).
            "actual_send_rate_from_events": actual_send_rate,
            "load_generator_report": load_report,
        },
        "rtt_ms": latency_stats_ms(rtt["rtt_ms"]),
        "send_ack_ms": send_ack_stats,
        # Cross-host, clock-offset-corrected; interpret against the
        # uncertainty in clock_sync.
        "one_way_latency_ms": one_way_stats,
        "clock_sync": clock_sync,
        "receiver_handler_ms": handler_stats,
        "receiver_total_ms": receiver_total_stats,
        "throughput": throughput,
        "system_metrics": summarize_system_metrics(system_metrics or {}),
        # stepped load (ramp, homes) only: the per-step decomposition that
        # locates the knee.
        "ramp": (
            compute_ramp_table(events, scenario, clock_sync, system_metrics)
            if scenario.load.stepped
            else None
        ),
    }
