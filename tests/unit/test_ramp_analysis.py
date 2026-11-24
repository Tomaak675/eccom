"""Ramp analysis: per-step attribution, resource windows, the knee, the figure."""

import json
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pytest
import yaml

from eccom.analysis.compare import compare_run, format_comparison
from eccom.analysis.plots import generate_ramp_plot
from eccom.analysis.run import format_summary
from eccom.analysis.statistics import (
    compute_ramp_table,
    compute_summary,
    ramp_dimension,
    step_tags,
    window_resource_usage,
)
from eccom.core.config import ScenarioConfig
from tests.unit.test_statistics import MS, event, scenario, synthetic_events

S = 1_000_000_000  # ns per second
SAME_HOST = {"method": "same_host", "offset_ns": 0, "uncertainty_ns": 0}


def ramp_scenario(**overrides) -> ScenarioConfig:
    raw = {
        "name": "ramp-test",
        "interaction": "one_way",
        "protocol": {"type": "http"},
        "payload": {"size_bytes": 100},
        "load": {
            "mode": "ramp",
            "messages_per_second": 10,
            "rate_multiplier": 2,
            "step_duration_s": 1,
            "duration_s": 2,
        },
    }
    raw.update(overrides)
    return ScenarioConfig.model_validate(raw)


def homes_scenario() -> ScenarioConfig:
    """The ramp fixture's rates (10, then 20 msg/s) as one 10 Hz sensor per
    home with one home added per step."""
    return ramp_scenario(
        payload=None,
        home={"sensors": [{"type": "a", "count": 1, "rate_hz": 10, "size_bytes": 100}]},
        load={"mode": "homes", "homes": 1, "homes_increment": 1, "step_duration_s": 1, "duration_s": 2},
    )


def ramp_event_rows(size_ramp: bool = False, homes: bool = False) -> list[dict]:
    """Two steps. Step 0: three sends 100 ms apart from t=0 (10 msg/s);
    step 1: three sends 50 ms apart from t=1 s (20 msg/s). Every send is
    acked after 2 ms and received after 5 ms, except the last message of
    step 1, which times out. One warmup message tagged step 0 must be ignored.
    With ``size_ramp`` every send also carries its step's payload size
    (100 B, then 400 B); with ``homes`` its step's number of homes (1, then
    2) and its sensor."""
    rows: list[dict] = []

    def message(cid: str, send_at: int, step: int, rate: float, phase: str = "measurement",
                acked: bool = True) -> None:
        metadata = {"size_bytes": 100, "step": step, "target_rate": rate}
        if size_ramp:
            metadata["target_size"] = 100 * 4**step
        if homes:
            metadata |= {"target_homes": step + 1, "sensor": "a"}
        rows.append(event("PAYLOAD_CREATED", cid, "edge", send_at, phase, metadata=metadata))
        rows.append(event("SEND_STARTED", cid, "edge", send_at, phase))
        if acked:
            rows.append(event("SEND_COMPLETED", cid, "edge", send_at + 2 * MS, phase))
            rows.append(
                event("MESSAGE_RECEIVED", cid, "cloud", send_at + 5 * MS, None,
                      metadata={"size_bytes": 100})
            )
        else:
            rows.append(event("TIMEOUT", cid, "edge", send_at + 5 * S, phase))

    message("w0", -S, 0, 10, phase="warmup")
    for i in range(3):
        message(f"a{i}", i * 100 * MS, 0, 10)
    message("b0", S, 1, 20)
    message("b1", S + 50 * MS, 1, 20)
    message("b2", S + 100 * MS, 1, 20, acked=False)
    return rows


def ramp_events(size_ramp: bool = False, homes: bool = False) -> pd.DataFrame:
    return pd.DataFrame(ramp_event_rows(size_ramp, homes))


def metric(host: str, at_s: float, cpu_s: float, sent: int, recv: int, lag_ms: float) -> dict:
    return {
        "host": host,
        "phase": "traffic",
        "wall_time_ns": int(at_s * S),
        "monotonic_ns": int(at_s * S),
        "system_cpu_percent": 1.0,
        "process_cpu_percent": 1.0,
        "process_cpu_seconds": cpu_s,
        "system_memory_percent": 20.0,
        "process_rss_bytes": 1,
        "net_bytes_sent": sent,
        "net_bytes_recv": recv,
        "loop_lag_ms_max": lag_ms,
    }


def ramp_metrics() -> dict[str, list[dict]]:
    return {
        # step 0 window [0, 1 s): 0.02 CPU-s over 0.2 s = 10 %, 250 kB out = 10 Mbit/s
        # step 1 window [1.0, 1.1 s): 0.02 CPU-s over 0.05 s = 40 %
        "edge": [
            metric("edge", 0.0, 0.00, 0, 0, 0.2),
            metric("edge", 0.2, 0.02, 250_000, 0, 3.0),
            metric("edge", 1.0, 0.50, 1_000_000, 0, 0.1),
            metric("edge", 1.05, 0.52, 1_062_500, 0, 80.0),
        ],
        # only step 0 has cloud samples: 1 % CPU, 125 kB in = 5 Mbit/s
        "cloud": [
            metric("cloud", 0.0, 0.000, 0, 0, 0.1),
            metric("cloud", 0.2, 0.002, 0, 125_000, 0.1),
        ],
    }


def test_step_tags_come_from_the_payload_metadata() -> None:
    tags = step_tags(ramp_events(), "edge")
    assert set(tags["correlation_id"]) == {"w0", "a0", "a1", "a2", "b0", "b1", "b2"}
    by_id = dict(zip(tags["correlation_id"], tags["step"]))
    assert by_id["a1"] == 0 and by_id["b1"] == 1
    # a constant-rate run carries no tags
    assert step_tags(synthetic_events(), "edge").empty


def test_step_tags_scale_the_per_process_target_to_the_schedule() -> None:
    """With load.senders > 1 every process tags its own share of the rate; the
    analysis reports the schedule's offered rate, i.e. the tag times senders."""
    tags = step_tags(ramp_events(), "edge", senders=2)
    assert set(tags["target_rate"]) == {20.0, 40.0}
    load = {"mode": "ramp", "messages_per_second": 20, "rate_multiplier": 2,
            "step_duration_s": 1, "duration_s": 2, "senders": 2}
    rows = compute_ramp_table(ramp_events(), ramp_scenario(load=load), SAME_HOST, None)
    assert [r["target_message_rate"] for r in rows] == [20, 40]
    assert [r["messages_attempted"] for r in rows] == [3, 3]


def test_ramp_table_attributes_messages_and_resources_per_step() -> None:
    rows = compute_ramp_table(ramp_events(), ramp_scenario(), SAME_HOST, ramp_metrics())
    assert [r["step"] for r in rows] == [0, 1]
    first, second = rows

    assert first["target_message_rate"] == 10 and second["target_message_rate"] == 20
    # untagged sizes fall back to the scenario's payload
    assert first["payload_size_bytes"] == 100 and second["payload_size_bytes"] == 100
    assert first["messages_attempted"] == 3  # the warmup message is excluded
    assert first["messages_completed"] == 3 and first["messages_failed"] == 0
    assert first["actual_send_rate"] == pytest.approx(10.0)
    assert first["delivered_rate"] == pytest.approx(10.0)
    assert first["received_by_receiver"] == 3
    assert first["send_ack_ms"]["mean"] == pytest.approx(2.0)
    assert first["delivery_ms"]["mean"] == pytest.approx(5.0)
    assert first["rtt_ms"] is None  # one-way run

    assert second["messages_attempted"] == 3 and second["messages_completed"] == 2
    assert second["messages_failed"] == 1 and second["timeouts"] == 1 and second["errors"] == 0
    assert second["actual_send_rate"] == pytest.approx(20.0)
    assert second["received_by_receiver"] == 2
    assert second["delivered_rate"] == pytest.approx(20.0)

    # resource windows: [first send, first send of the next step) and, for the
    # last step, [first send, last send)
    assert first["window"] == {"start_wall_ns": 0, "end_wall_ns": S}
    assert second["window"] == {"start_wall_ns": S, "end_wall_ns": S + 100 * MS}
    edge0 = first["resources"]["edge"]
    assert edge0["samples"] == 2
    assert edge0["process_cpu_percent"] == pytest.approx(10.0)
    assert edge0["net_sent_mbit_s"] == pytest.approx(10.0)
    assert edge0["loop_lag_ms_max"] == pytest.approx(3.0)
    assert edge0["system_cpu_percent"] == pytest.approx(1.0)
    assert first["resources"]["cloud"]["process_cpu_percent"] == pytest.approx(1.0)
    assert first["resources"]["cloud"]["net_recv_mbit_s"] == pytest.approx(5.0)
    edge1 = second["resources"]["edge"]
    assert edge1["process_cpu_percent"] == pytest.approx(40.0)
    assert edge1["loop_lag_ms_max"] == pytest.approx(80.0)
    assert second["resources"]["cloud"]["samples"] == 0
    assert second["resources"]["cloud"]["process_cpu_percent"] is None


def test_ramp_rates_are_counts_over_the_scheduled_step_and_ignore_stragglers() -> None:
    """One receipt of step 0 arrives 30 s late, one arrives twice, and the
    step 1 message the sender timed out arrives 7 s late: both stragglers
    leave their step's received rate and count as received after the step,
    the sent rate stays put, the cadence estimate collapses."""
    rows = ramp_event_rows()
    for row in rows:
        if row["event_type"] == "MESSAGE_RECEIVED" and row["correlation_id"] == "a2":
            row["wall_time_ns"] += 30 * S
    received = {"size_bytes": 100}
    rows.append(event("MESSAGE_RECEIVED", "a0", "cloud", 900 * MS, None, metadata=received))
    rows.append(event("MESSAGE_RECEIVED", "b2", "cloud", 8 * S, None, metadata=received))
    first, second = compute_ramp_table(pd.DataFrame(rows), ramp_scenario(), SAME_HOST, None)

    assert first["step_sending_time_s"] == 1
    assert first["sent_rate"] == first["completed_rate"] == 3.0
    assert first["received_rate"] == 2.0
    assert first["sent_fraction"] == pytest.approx(0.3)  # the fixture sends 3 of the 10 offered
    assert first["received_by_receiver"] == 3 and first["duplicate_receipts"] == 1
    assert first["received_in_step"] == 2 and first["received_after_step"] == 1
    assert first["lost"] == 0 and first["received_fraction"] == 1.0
    assert first["delivered_rate"] < 0.1  # (n - 1) / span over the 30 s straggler
    assert first["delivery_ms"]["count"] == 3  # the duplicate is not a fourth delivery

    assert second["timeouts"] == 1 and second["timeouts_delivered"] == 1
    assert second["messages_completed"] == 2 and second["completed_fraction"] == pytest.approx(2 / 3)
    assert second["received_by_receiver"] == 3 and second["lost"] == 0
    assert second["received_in_step"] == 2 and second["received_after_step"] == 1
    assert second["received_rate"] == 2.0

    summary = compute_summary(pd.DataFrame(rows), ramp_scenario(), load_report=None, clock_sync=SAME_HOST)
    assert summary["counts"]["received_by_receiver"] == 6
    assert summary["counts"]["duplicate_receipts"] == 1
    assert "duplicates=1" in format_summary(summary)


def test_the_received_rate_needs_no_clock_offset() -> None:
    row = compute_ramp_table(ramp_events(), ramp_scenario(), None, None)[0]
    assert row["received_in_step"] == 3 and row["received_after_step"] == 0
    assert row["received_by_receiver"] == 3 and row["lost"] == 0
    assert row["received_rate"] == 3.0


def test_the_received_rate_window_is_one_step_from_the_first_receipt() -> None:
    """The step's first receipt is at 5 ms, so a receipt at 1.2 s is outside
    the 1 s window, also when a 0.5 s pause follows the step, and one at
    1.004 s is inside it."""
    rows = ramp_event_rows()
    for row in rows:
        if row["event_type"] == "MESSAGE_RECEIVED" and row["correlation_id"] == "a2":
            row["wall_time_ns"] = 1200 * MS
    load = {"mode": "ramp", "messages_per_second": 10, "rate_multiplier": 2,
            "step_duration_s": 1, "duration_s": 2}
    plain = compute_ramp_table(pd.DataFrame(rows), ramp_scenario(load=load), SAME_HOST, None)[0]
    paused = compute_ramp_table(
        pd.DataFrame(rows), ramp_scenario(load=load | {"step_pause_s": 0.5}), SAME_HOST, None
    )[0]
    assert plain["received_in_step"] == 2 and plain["received_after_step"] == 1
    for row in rows:
        if row["event_type"] == "MESSAGE_RECEIVED" and row["correlation_id"] == "a2":
            row["wall_time_ns"] = 1004 * MS
    inside = compute_ramp_table(pd.DataFrame(rows), ramp_scenario(load=load), SAME_HOST, None)[0]
    assert inside["received_in_step"] == 3 and inside["received_rate"] == 3.0
    assert paused["received_in_step"] == 2 and paused["received_after_step"] == 1
    assert paused["received_by_receiver"] == 3 and paused["received_fraction"] == 1.0


def test_the_received_rate_window_starts_at_each_sender_process_own_first_receipt() -> None:
    """Two sender processes run step 0, the second 2 s after the first: its
    receipts are inside its own window although outside the first's."""
    rows = []
    for instance, start in ((0, 0), (1, 2 * S)):
        for i in range(3):
            cid, at = f"p{instance}m{i}", start + i * 100 * MS
            metadata = {"size_bytes": 100, "step": 0, "target_rate": 10}
            sent = [
                event("PAYLOAD_CREATED", cid, "edge", at, metadata=metadata),
                event("SEND_STARTED", cid, "edge", at),
                event("SEND_COMPLETED", cid, "edge", at + 2 * MS),
            ]
            rows += [row | {"instance": instance} for row in sent]
            rows.append(event("MESSAGE_RECEIVED", cid, "cloud", at + 5 * MS, None) | {"instance": 0})
    load = {"mode": "ramp", "messages_per_second": 20, "rate_multiplier": 2,
            "step_duration_s": 1, "duration_s": 1, "senders": 2}
    row = compute_ramp_table(pd.DataFrame(rows), ramp_scenario(load=load), SAME_HOST, None)[0]
    assert row["received_in_step"] == 6 and row["received_after_step"] == 0


def test_step_pause_ends_every_window_at_the_steps_last_send() -> None:
    """With a drain pause between steps the quiet gap belongs to no step, so
    the resource window must not run on to the next step's first send."""
    load = {"mode": "ramp", "messages_per_second": 10, "rate_multiplier": 2,
            "step_duration_s": 1, "step_pause_s": 0.5, "duration_s": 2}
    rows = compute_ramp_table(ramp_events(), ramp_scenario(load=load), SAME_HOST, ramp_metrics())
    assert rows[0]["window"] == {"start_wall_ns": 0, "end_wall_ns": 200 * MS}
    assert rows[1]["window"] == {"start_wall_ns": S, "end_wall_ns": S + 100 * MS}


def test_rates_are_summed_per_sender_process_not_pooled_over_their_skew() -> None:
    """Two processes run the same 10 msg/s step, the second starting 0.5 s
    later: the aggregate is 20 msg/s, while the pooled first-to-last span
    (5 sends over 0.7 s) would say 7.1."""
    rows = []
    for instance, offset in ((0, 0), (1, 500 * MS)):
        for i in range(3):
            cid = f"i{instance}m{i}"
            at = offset + i * 100 * MS
            meta = {"size_bytes": 100, "step": 0, "target_rate": 10}
            for row in (
                event("PAYLOAD_CREATED", cid, "edge", at, "measurement", metadata=meta),
                event("SEND_STARTED", cid, "edge", at, "measurement"),
                event("SEND_COMPLETED", cid, "edge", at + 2 * MS, "measurement"),
            ):
                rows.append(row | {"instance": instance})
            rows.append(
                event("MESSAGE_RECEIVED", cid, "cloud", at + 5 * MS, None, metadata={"size_bytes": 100})
                | {"instance": 0}
            )
    load = {"mode": "ramp", "messages_per_second": 20, "rate_multiplier": 2,
            "step_duration_s": 1, "duration_s": 1, "senders": 2}
    (row,) = compute_ramp_table(pd.DataFrame(rows), ramp_scenario(load=load), SAME_HOST, None)
    assert row["target_message_rate"] == 20
    assert row["actual_send_rate"] == pytest.approx(20.0)
    assert row["delivered_rate"] == pytest.approx(20.0)
    assert row["messages_attempted"] == 6 and row["received_by_receiver"] == 6


def test_ramp_table_without_clock_sync_or_metrics() -> None:
    rows = compute_ramp_table(ramp_events(), ramp_scenario(), None, None)
    assert len(rows) == 2
    assert rows[0]["delivery_ms"] is None
    assert rows[0]["send_ack_ms"]["count"] == 3
    assert rows[0]["resources"] == {}


def test_size_ramp_rows_carry_the_step_payload_size() -> None:
    rows = compute_ramp_table(ramp_events(size_ramp=True), ramp_scenario(), SAME_HOST, None)
    assert [r["payload_size_bytes"] for r in rows] == [100, 400]
    assert [r["target_message_rate"] for r in rows] == [10, 20]


def test_homes_ramp_rows_carry_the_number_of_homes() -> None:
    scenario = homes_scenario()
    events = ramp_events(homes=True)
    assert set(step_tags(events, "edge")["target_homes"]) == {1.0, 2.0}
    assert step_tags(ramp_events(), "edge")["target_homes"].isna().all()

    rows = compute_ramp_table(events, scenario, SAME_HOST, None)
    assert [r["homes"] for r in rows] == [1, 2]
    assert [r["target_message_rate"] for r in rows] == [10, 20]
    assert [r["payload_size_bytes"] for r in rows] == [None, None]  # mixed sizes, no payload block
    assert ramp_dimension([rows]) == "homes"
    assert ramp_dimension([compute_ramp_table(ramp_events(), ramp_scenario(), SAME_HOST, None)]) == "rate"

    summary = compute_summary(events, scenario, load_report=None, clock_sync=SAME_HOST)
    assert [r["homes"] for r in summary["ramp"]] == [1, 2]
    assert summary["rates"]["requested_message_rate"] == 10  # one home of one 10 Hz sensor
    json.dumps(summary)
    digest = format_summary(summary)
    assert "homes" in digest.splitlines()[-3]  # the step table's header
    assert "homes" not in format_summary(
        compute_summary(ramp_events(), ramp_scenario(), load_report=None, clock_sync=SAME_HOST)
    )


def test_summary_carries_the_ramp_table_only_for_ramp_runs() -> None:
    summary = compute_summary(
        ramp_events(), ramp_scenario(), load_report=None, clock_sync=SAME_HOST,
        system_metrics=ramp_metrics(),
    )
    assert [r["step"] for r in summary["ramp"]] == [0, 1]
    assert summary["counts"]["messages_attempted"] == 6
    assert summary["rates"]["requested_message_rate"] == 10  # the first step
    # the summary must stay JSON-serializable (numpy scalars would break it)
    json.dumps(summary)

    assert compute_summary(synthetic_events(), scenario(), load_report=None)["ramp"] is None


def test_window_resource_usage_without_counters_reports_none() -> None:
    samples = [{"wall_time_ns": 0, "process_rss_bytes": 1}, {"wall_time_ns": S, "process_rss_bytes": 1}]
    usage = window_resource_usage(samples, 0, 2 * S)
    assert usage["samples"] == 2
    assert usage["process_cpu_percent"] is None
    assert usage["system_cpu_percent"] is None
    assert usage["net_sent_mbit_s"] is None
    assert usage["loop_lag_ms_max"] is None


def write_jsonl(path: Path, rows: list[dict[str, Any]]) -> None:
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")


def write_run_dir(path: Path, scenario: ScenarioConfig, rows: list[dict]) -> None:
    """A run directory the analysis can read: scenario, run metadata, both
    hosts' events and the ramp metrics fixture."""
    path.mkdir(parents=True, exist_ok=True)
    (path / "scenario.yaml").write_text(
        yaml.safe_dump(scenario.model_dump(mode="json")), encoding="utf-8"
    )
    (path / "run.json").write_text(
        json.dumps({"clock_sync": {"method": "same_host", "synchronized": True}}),
        encoding="utf-8",
    )
    write_jsonl(path / "edge-events.jsonl", [r for r in rows if r["host"] == "edge"])
    write_jsonl(path / "cloud-events.jsonl", [r for r in rows if r["host"] == "cloud"])
    for host, samples in ramp_metrics().items():
        write_jsonl(path / f"{host}-system-metrics.jsonl", samples)


def test_compare_reports_the_sustained_rate_before_the_knee(tmp_path: Path) -> None:
    write_run_dir(tmp_path, ramp_scenario(), ramp_event_rows())

    result = compare_run(tmp_path)
    ramp = result["ramp"]
    assert ramp["steps"] == 2
    # step 0 received 3 msg/s of the 10 offered, so no step held; step 1 lost a
    # message, so the knee is at 1
    assert ramp["exact_step"] is None
    assert ramp["miss_step"] == 0
    assert ramp["sustained_step"] == 0
    assert ramp["sustained_message_rate"] == pytest.approx(10.0)
    assert ramp["knee_step"] == 1
    assert ramp["knee_target_message_rate"] == 20
    # three distinct receipts in a 1 s step
    assert ramp["peak_received_rate"] == pytest.approx(3.0)

    table = format_comparison([result])
    assert "Ramp: rate held, and sustained before the knee" in table
    assert "| ramp-test | 2 | - | - | 10.0 | 0 | 1 | 20 | 3.0 |" in table
    # CPU per step: offered rate down, one column per scenario, peak in bold;
    # the cloud has no samples in step 1
    assert "### edge: agent process CPU, % of one core" in table
    assert "| 10 | 10.0 |" in table and "| 20 | **40.0** |" in table
    assert "### cloud: host CPU, % of all cores" in table
    assert "| 10 | **1.0** |" in table and "| 20 | - |" in table


def test_a_step_holds_on_received_share_and_received_rate_not_on_timeouts() -> None:
    """Held: >= 99 % of the attempted messages received and >= 95 % of the
    offered rate received in the step; timeouts alone do not end the walk."""
    from eccom.analysis.compare import ramp_summary

    def step(index: int, received_fraction: float, received_rate: float, failed: int = 0) -> dict:
        return {
            "step": index, "target_message_rate": 100.0, "homes": index + 1, "payload_size_bytes": 32,
            "messages_failed": failed, "received_fraction": received_fraction, "received_rate": received_rate,
            "actual_send_rate": 100.0, "send_ack_ms": {"p95": 1.0}, "rtt_ms": None,
        }

    steps = [step(0, 1.0, 100.0, failed=30), step(1, 0.99, 95.0), step(2, 1.0, 94.9), step(3, 1.0, 100.0)]
    ramp = ramp_summary(steps, "one_way")
    assert (ramp["exact_homes"], ramp["miss_step"]) == (2, 2)
    assert ramp_summary([step(0, 0.989, 100.0)], "one_way")["exact_step"] is None


def test_compare_keys_a_payload_size_ramp_on_the_size(tmp_path: Path) -> None:
    scenario = ramp_scenario(
        load={
            "mode": "ramp", "messages_per_second": 10, "size_multiplier": 4,
            "step_duration_s": 1, "duration_s": 2,
        },
    )
    write_run_dir(tmp_path, scenario, ramp_event_rows(size_ramp=True))

    result = compare_run(tmp_path)
    assert [s["payload_size_bytes"] for s in result["ramp_steps"]] == [100, 400]
    assert result["ramp"]["exact_payload_size_bytes"] is None
    assert result["ramp"]["sustained_payload_size_bytes"] == 100
    assert result["ramp"]["knee_payload_size_bytes"] == 400

    table = format_comparison([result])
    assert "Ramp: payload held, and sustained before the knee" in table
    assert "| ramp-test | 2 | - | - | 100 | 0 | 1 | 400 | 3.0 |" in table
    assert "| payload B | ramp-test |" in table


def test_compare_keys_a_homes_ramp_on_the_homes(tmp_path: Path) -> None:
    write_run_dir(tmp_path, homes_scenario(), ramp_event_rows(homes=True))

    result = compare_run(tmp_path)
    assert result["aggregation_factor"] == 1
    assert [s["homes"] for s in result["ramp_steps"]] == [1, 2]
    assert result["ramp"]["exact_homes"] is None
    assert result["ramp"]["sustained_homes"] == 1
    assert result["ramp"]["knee_homes"] == 2

    table = format_comparison([result])
    assert "Ramp: homes held, and sustained before the knee" in table
    assert "| ramp-test | 2 | - | - | 1 | 0 | 1 | 2 | 3.0 |" in table
    assert "| homes | ramp-test |" in table
    assert "| 1 | 10.0 |" in table and "| 2 | **40.0** |" in table


def test_ramp_plot_renders_and_skips_empty_input(tmp_path: Path) -> None:
    rng = np.random.default_rng(1)
    rows = []
    t = 0.0
    for step, rate in enumerate((10, 20, 40, 80)):
        for _ in range(rate):  # one second per step
            rows.append(
                {
                    "send_wall_time_ns": int(t * S),
                    "latency_ms": 20 + step * 5 + rng.random(),
                    "step": step,
                    "target_rate": rate,
                }
            )
            t += 1 / rate
    cost = pd.DataFrame(rows)
    arrivals = cost["send_wall_time_ns"].to_numpy() + 12 * MS
    metrics = {
        "edge": [metric("edge", s, 0.1 * s, 100_000 * s, 0, 1.0) for s in range(5)],
        "cloud": [metric("cloud", s, 0.05 * s, 0, 100_000 * s, 0.5) for s in range(5)],
    }

    path = generate_ramp_plot(
        tmp_path / "ramp_over_time.png", cost, "Send acknowledgement", arrivals, metrics,
        "edge", "cloud",
    )
    assert path is not None and path.stat().st_size > 10_000

    assert generate_ramp_plot(
        tmp_path / "empty.png", cost.iloc[0:0], "x", None, {}, "edge", "cloud"
    ) is None
