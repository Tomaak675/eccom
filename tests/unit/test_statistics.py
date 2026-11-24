import numpy as np
import pandas as pd
import pytest

from eccom.analysis.statistics import (
    compute_one_way_table,
    compute_rtt_table,
    compute_summary,
    latency_stats_ms,
    variation_stats,
)
from eccom.core.config import ScenarioConfig

MS = 1_000_000  # ns per millisecond


def scenario(**overrides) -> ScenarioConfig:
    raw = {
        "name": "stats-test",
        "protocol": {"type": "http"},
        "payload": {"size_bytes": 100},
        "load": {"messages_per_second": 10, "total_messages": 6},
    }
    raw.update(overrides)
    return ScenarioConfig.model_validate(raw)


def event(
    event_type: str,
    correlation_id: str,
    host: str,
    monotonic_ns: int,
    phase: str | None = "measurement",
    metadata: dict | None = None,
    sequence_number: int = 0,
) -> dict:
    return {
        "event_type": event_type,
        "correlation_id": correlation_id,
        "sequence_number": sequence_number,
        "host": host,
        "phase": phase,
        "monotonic_ns": monotonic_ns,
        # wall == monotonic keeps synthetic timelines simple
        "wall_time_ns": monotonic_ns,
        "metadata": metadata,
    }


def synthetic_events() -> pd.DataFrame:
    """5 completed requests with RTTs of exactly 1..5 ms, plus 1 timeout."""
    rows = []
    for i, rtt_ms in enumerate([1, 2, 3, 4, 5]):
        corr = f"c{i}"
        send_at = i * 100 * MS
        rows.append(event("PAYLOAD_CREATED", corr, "edge", send_at - 1, metadata={"size_bytes": 100}))
        rows.append(event("SEND_STARTED", corr, "edge", send_at, sequence_number=i))
        rows.append(event("REPLY_RECEIVED", corr, "edge", send_at + rtt_ms * MS, sequence_number=i))
        # receiver side: handler takes 0.5 ms, total server time 0.7 ms
        rows.append(event("MESSAGE_RECEIVED", corr, "cloud", send_at, phase=None))
        rows.append(event("HANDLER_STARTED", corr, "cloud", send_at + MS // 10, phase=None))
        rows.append(event("HANDLER_COMPLETED", corr, "cloud", send_at + MS // 10 + MS // 2, phase=None))
        rows.append(event("REPLY_SEND_STARTED", corr, "cloud", send_at + 7 * MS // 10, phase=None))
    rows.append(event("PAYLOAD_CREATED", "t0", "edge", 500 * MS, metadata={"size_bytes": 100}))
    rows.append(event("SEND_STARTED", "t0", "edge", 500 * MS, sequence_number=5))
    rows.append(event("TIMEOUT", "t0", "edge", 505 * MS, sequence_number=5))
    return pd.DataFrame(rows)


def test_latency_stats_of_known_values() -> None:
    stats = latency_stats_ms(np.array([1.0, 2.0, 3.0, 4.0, 5.0]))
    assert stats is not None
    assert stats["count"] == 5
    assert stats["min"] == 1.0
    assert stats["mean"] == 3.0
    assert stats["median"] == 3.0
    assert stats["std"] == pytest.approx(1.5811, abs=1e-3)
    assert stats["p95"] == pytest.approx(4.8)
    assert stats["p99"] == pytest.approx(4.96)
    assert stats["max"] == 5.0


def test_latency_stats_empty_returns_none() -> None:
    assert latency_stats_ms(np.array([])) is None


def test_variation_stats_uses_the_sample_variance() -> None:
    stats = variation_stats([2.0, 4.0, 4.0, 4.0, 5.0, 5.0, 7.0, 9.0])
    assert stats["mean"] == 5.0
    assert stats["variance"] == pytest.approx(32 / 7)
    assert stats["std"] == pytest.approx((32 / 7) ** 0.5)
    assert stats["cv"] == pytest.approx((32 / 7) ** 0.5 / 5)


def test_rtt_table_joins_send_and_reply_by_correlation_id() -> None:
    rtt = compute_rtt_table(synthetic_events(), sender_host="edge")
    assert len(rtt) == 5  # the timeout has no reply and must not appear
    by_corr = rtt.set_index("correlation_id")["rtt_ms"]
    assert by_corr["c0"] == pytest.approx(1.0)
    assert by_corr["c4"] == pytest.approx(5.0)


def test_summary_counts_and_stats() -> None:
    summary = compute_summary(synthetic_events(), scenario(), load_report=None)

    assert summary["counts"] == {
        "messages_attempted": 6,
        "messages_completed": 5,
        "messages_failed": 1,
        "timeouts": 1,
        "errors": 0,
        "received_by_receiver": 5,
        "duplicate_receipts": 0,
    }
    assert summary["rtt_ms"]["mean"] == pytest.approx(3.0)
    assert summary["receiver_handler_ms"]["mean"] == pytest.approx(0.5)
    assert summary["receiver_total_ms"]["mean"] == pytest.approx(0.7)

    throughput = summary["throughput"]
    # window: first send at 0 ms, last reply at 405 ms
    assert throughput["duration_s"] == pytest.approx(0.405)
    assert throughput["messages_per_second"] == pytest.approx(5 / 0.405)
    assert throughput["payload_bytes_per_second"] == pytest.approx(500 / 0.405)


def test_summary_handles_warmup_phase_exclusion() -> None:
    events = synthetic_events()
    warmup_rows = pd.DataFrame(
        [
            event("SEND_STARTED", "w0", "edge", 0, phase="warmup"),
            event("REPLY_RECEIVED", "w0", "edge", 2 * MS, phase="warmup"),
        ]
    )
    summary = compute_summary(pd.concat([warmup_rows, events]), scenario(), load_report=None)
    assert summary["counts"]["messages_attempted"] == 6
    assert summary["counts"]["messages_completed"] == 5


def one_way_events(clock_offset_ns: int) -> pd.DataFrame:
    """3 one-way messages: true one-way latency exactly 5 ms, send-ack 2 ms,
    receiver clock ahead of sender by ``clock_offset_ns``."""
    rows = []
    for i in range(3):
        corr = f"c{i}"
        send_at = i * 100 * MS
        rows.append(event("PAYLOAD_CREATED", corr, "edge", send_at, metadata={"size_bytes": 100}))
        rows.append(event("SEND_STARTED", corr, "edge", send_at, sequence_number=i))
        rows.append(event("SEND_COMPLETED", corr, "edge", send_at + 2 * MS, sequence_number=i))
        rows.append(
            event(
                "MESSAGE_RECEIVED",
                corr,
                "cloud",
                send_at + 5 * MS + clock_offset_ns,
                phase=None,
                metadata={"size_bytes": 100},
            )
        )
    return pd.DataFrame(rows)


def test_one_way_table_corrects_for_clock_offset() -> None:
    offset = 250 * MS  # cloud clock 250 ms ahead
    table = compute_one_way_table(
        one_way_events(offset), "edge", "cloud", clock_offset_ns=offset
    )
    assert len(table) == 3
    assert table["latency_ms"].tolist() == pytest.approx([5.0, 5.0, 5.0])


def test_one_way_summary_counts_and_stats() -> None:
    offset = 250 * MS
    summary = compute_summary(
        one_way_events(offset),
        scenario(interaction="one_way", load={"messages_per_second": 10, "total_messages": 3}),
        load_report=None,
        clock_sync={"method": "probe", "offset_ns": offset, "uncertainty_ns": MS},
    )
    assert summary["counts"]["messages_attempted"] == 3
    assert summary["counts"]["messages_completed"] == 3  # SEND_COMPLETED acks
    assert summary["counts"]["received_by_receiver"] == 3
    assert summary["rtt_ms"] is None
    assert summary["send_ack_ms"]["mean"] == pytest.approx(2.0)
    assert summary["one_way_latency_ms"]["mean"] == pytest.approx(5.0)
    assert summary["clock_sync"]["offset_ns"] == offset
    # delivery window on the receiver clock: 200 ms for 3 messages
    assert summary["throughput"]["messages_per_second"] == pytest.approx(3 / 0.2)


def test_one_way_latency_absent_without_clock_sync() -> None:
    summary = compute_summary(
        one_way_events(250 * MS),
        scenario(interaction="one_way", load={"messages_per_second": 10, "total_messages": 3}),
        load_report=None,
        clock_sync=None,
    )
    assert summary["one_way_latency_ms"] is None
    assert summary["send_ack_ms"]["count"] == 3  # single-clock stat still valid


def test_system_metrics_summary_aggregates_per_host() -> None:
    from eccom.analysis.statistics import summarize_system_metrics

    def sample(system_cpu: float, process_cpu: float) -> dict:
        return {
            "system_cpu_percent": system_cpu,
            "process_cpu_percent": process_cpu,
            "system_memory_percent": 40.0,
            "process_rss_bytes": 100_000_000,
        }

    summary = summarize_system_metrics(
        {
            "edge": [sample(10.0, 2.0), sample(30.0, 6.0)],
            "cloud": [sample(50.0, 20.0)],
            "empty": [],
        }
    )
    assert summary is not None
    assert summary["edge"]["samples"] == 2
    assert summary["edge"]["system_cpu_percent"] == {"mean": 20.0, "max": 30.0}
    assert summary["edge"]["process_cpu_percent"] == {"mean": 4.0, "max": 6.0}
    assert summary["cloud"]["system_cpu_percent"]["max"] == 50.0
    assert "empty" not in summary

    assert summarize_system_metrics({}) is None


def test_summary_with_no_events() -> None:
    empty = pd.DataFrame(
        columns=[
            "event_type",
            "correlation_id",
            "sequence_number",
            "host",
            "phase",
            "monotonic_ns",
            "wall_time_ns",
            "metadata",
        ]
    )
    summary = compute_summary(empty, scenario(), load_report=None)
    assert summary["counts"]["messages_attempted"] == 0
    assert summary["rtt_ms"] is None
    assert summary["throughput"] is None
