"""Cross-run comparison: stage decomposition and baseline-adjusted resources."""

import json
from pathlib import Path

import pytest
import yaml

from eccom.analysis.compare import compare_run, format_comparison, write_csv

BASE_NS = 1_000_000_000_000_000_000
MS = 1_000_000


def at(seconds: float) -> int:
    return BASE_NS + int(seconds * 1e9)


def event(event_type: str, host: str, cid: str, seq: int, seconds: float, phase=None) -> dict:
    return {
        "event_type": event_type,
        "correlation_id": cid,
        "sequence_number": seq,
        "host": host,
        "phase": phase,
        "monotonic_ns": at(seconds),
        "wall_time_ns": at(seconds),
        "metadata": {},
    }


def metric(host: str, seconds: float, cpu_seconds: float, rss_bytes: int) -> dict:
    return {
        "host": host,
        "wall_time_ns": at(seconds),
        "monotonic_ns": at(seconds),
        "system_cpu_percent": 1.0,
        "process_cpu_percent": 1.0,
        "system_memory_percent": 20.0,
        "process_rss_bytes": rss_bytes,
        "process_cpu_seconds": cpu_seconds,
        "net_bytes_sent": 0,
        "net_bytes_recv": 0,
    }


def write_jsonl(path: Path, rows: list[dict]) -> None:
    path.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")


@pytest.fixture
def run_dir(tmp_path: Path) -> Path:
    """A synthetic one-way run: build 1 ms, send-ack 10 ms, delivery 5 ms."""
    scenario = {
        "name": "synthetic",
        "interaction": "one_way",
        "protocol": {"type": "http"},
        "payload": {"size_bytes": 128},
        "load": {"messages_per_second": 1, "duration_s": 20},
    }
    (tmp_path / "scenario.yaml").write_text(yaml.safe_dump(scenario), encoding="utf-8")
    (tmp_path / "run.json").write_text(
        json.dumps({"clock_sync": {"method": "same_host", "synchronized": True}}), encoding="utf-8"
    )

    edge, cloud = [], []
    # one warmup message (excluded from every statistic) plus four measured ones
    for index, (start, phase) in enumerate(
        [(5.0, "warmup"), (10.0, "measurement"), (15.0, "measurement"),
         (20.0, "measurement"), (25.0, "measurement")]
    ):
        cid = f"c{index}"
        edge += [
            event("PAYLOAD_CREATED", "edge", cid, index, start - 0.001, phase),
            event("SEND_STARTED", "edge", cid, index, start, phase),
            event("SEND_COMPLETED", "edge", cid, index, start + 0.010, phase),
        ]
        cloud += [
            event("MESSAGE_RECEIVED", "cloud", cid, index, start + 0.005),
            event("HANDLER_STARTED", "cloud", cid, index, start + 0.0052),
            event("HANDLER_COMPLETED", "cloud", cid, index, start + 0.0053),
        ]
    write_jsonl(tmp_path / "edge-events.jsonl", edge)
    write_jsonl(tmp_path / "cloud-events.jsonl", cloud)

    # Idle before traffic (1% CPU), busy during (10%), idle again after. The
    # first post-traffic sample is dropped as settling, so 30.0 is excluded.
    write_jsonl(
        tmp_path / "edge-system-metrics.jsonl",
        [
            metric("edge", 0.0, 0.00, 100_000_000),
            metric("edge", 1.0, 0.01, 100_000_000),
            metric("edge", 10.0, 1.00, 150_000_000),
            metric("edge", 20.0, 2.00, 150_000_000),
            metric("edge", 30.0, 3.00, 150_000_000),
            metric("edge", 31.0, 3.01, 150_000_000),
            metric("edge", 32.0, 3.02, 150_000_000),
        ],
    )
    return tmp_path


def test_stage_decomposition(run_dir: Path) -> None:
    result = compare_run(run_dir)
    stages = result["stages"]

    assert result["interaction"] == "one_way"
    assert result["headline_metric"] == "delivery"
    # the warmup message is excluded everywhere
    assert stages["send_ack"]["count"] == 4
    assert stages["send_ack"]["mean"] == pytest.approx(10.0)
    assert stages["build"]["mean"] == pytest.approx(1.0)
    assert stages["delivery"]["mean"] == pytest.approx(5.0)
    assert stages["dispatch"]["mean"] == pytest.approx(0.2, abs=0.01)
    assert stages["handler"]["mean"] == pytest.approx(0.1, abs=0.01)
    # one-way scenarios have no round trip, and the headline points at delivery
    assert stages["rtt"] is None
    assert stages["headline"] is stages["delivery"]
    assert "p90" in stages["send_ack"]


def test_cpu_uses_cumulative_counter_against_idle_baseline(run_dir: Path) -> None:
    usage = compare_run(run_dir)["resources"]["edge"]

    # during: 1.0 CPU-second over 10 s of wall time
    assert usage["process_cpu_percent"] == pytest.approx(10.0)
    # baseline: 0.01 + 0.01 CPU-seconds over 1 s + 1 s, both quiet windows
    assert usage["process_cpu_percent_baseline"] == pytest.approx(1.0)
    assert usage["process_cpu_percent_adjusted"] == pytest.approx(9.0)
    assert usage["samples_measurement"] == 2
    assert usage["samples_baseline"] == 4  # 2 before + 3 after - 1 settling


def test_memory_baseline_ignores_the_post_run_window(run_dir: Path) -> None:
    """RSS never returns to idle (events are buffered until close), so only the
    pre-traffic window is a valid memory baseline."""
    usage = compare_run(run_dir)["resources"]["edge"]

    assert usage["process_rss_mb"] == pytest.approx(150.0)
    assert usage["process_rss_mb_baseline"] == pytest.approx(100.0)
    assert usage["process_rss_mb_adjusted"] == pytest.approx(50.0)


def test_tagged_baseline_window_wins_over_the_timing_heuristic(run_dir: Path) -> None:
    """instrumentation.baseline_s tags an explicit quiet window; when present it
    is the baseline, and post-run samples are ignored entirely."""
    samples = [
        metric("edge", 0.0, 0.00, 100_000_000),
        metric("edge", 1.0, 0.02, 100_000_000),
        metric("edge", 2.0, 0.04, 100_000_000),  # tagged: 0.04 CPU-s over 2 s = 2%
        metric("edge", 10.0, 1.00, 150_000_000),
        metric("edge", 20.0, 2.00, 150_000_000),
        metric("edge", 30.0, 9.00, 900_000_000),  # noisy post-run, must be ignored
        metric("edge", 31.0, 9.90, 900_000_000),
    ]
    for sample in samples[:3]:
        sample["phase"] = "baseline"
    for sample in samples[3:]:
        sample["phase"] = "traffic"
    write_jsonl(run_dir / "edge-system-metrics.jsonl", samples)

    usage = compare_run(run_dir)["resources"]["edge"]
    assert usage["samples_baseline"] == 3
    assert usage["process_cpu_percent"] == pytest.approx(10.0)
    assert usage["process_cpu_percent_baseline"] == pytest.approx(2.0)
    assert usage["process_cpu_percent_adjusted"] == pytest.approx(8.0)
    assert usage["process_rss_mb_baseline"] == pytest.approx(100.0)


def test_missing_cpu_counter_reports_none_rather_than_guessing(run_dir: Path) -> None:
    """Runs recorded before process_cpu_seconds existed must not be back-filled
    from the quantized percentages."""
    samples = [
        json.loads(line)
        for line in (run_dir / "edge-system-metrics.jsonl").read_text().splitlines()
    ]
    for sample in samples:
        del sample["process_cpu_seconds"]
    write_jsonl(run_dir / "edge-system-metrics.jsonl", samples)

    usage = compare_run(run_dir)["resources"]["edge"]
    assert usage["process_cpu_percent"] is None
    assert usage["process_cpu_percent_adjusted"] is None
    assert usage["process_rss_mb_adjusted"] == pytest.approx(50.0)  # memory still works


def test_table_and_csv_render(run_dir: Path, tmp_path: Path) -> None:
    rows = [compare_run(run_dir)]
    table = format_comparison(rows)
    assert "Headline latency" in table
    assert "synthetic" in table
    assert "p90" in table

    csv_path = tmp_path / "stages.csv"
    write_csv(rows, csv_path)
    lines = csv_path.read_text(encoding="utf-8").splitlines()
    assert lines[0].startswith("scenario,run_dir,interaction,stage,count")
    stages = {line.split(",")[3] for line in lines[1:]}
    assert {"build", "send_ack", "delivery", "dispatch", "handler"} <= stages
    assert "headline" not in stages  # it is an alias, not a stage
