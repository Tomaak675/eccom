"""Multi-process senders: artifact naming and merging their reports/samples."""

from pathlib import Path

from eccom.analysis.loading import (
    load_load_report,
    load_system_metrics,
    merge_instance_samples,
    merge_load_reports,
)
from eccom.core.artifacts import instance_label, instance_labels, split_label
from eccom.orchestration.ssh import _container_name


def test_instance_labels_keep_single_process_names_unchanged(tmp_path: Path) -> None:
    assert instance_label("edge", 0, 1) == "edge"
    assert instance_label("edge", 2, 3) == "edge.2"
    assert split_label("edge.2") == ("edge", 2)
    assert split_label("edge") == ("edge", None)
    (tmp_path / "edge-load-report.json").write_text("{}")
    assert instance_labels(tmp_path, "edge", "load-report.json") == ["edge"]
    for i in (2, 0, 1):
        (tmp_path / f"cloud.{i}-events.jsonl").write_text("")
    assert instance_labels(tmp_path, "cloud", "events.jsonl") == ["cloud.0", "cloud.1", "cloud.2"]
    assert instance_labels(tmp_path, "nobody", "events.jsonl") == []
    assert _container_name("sender", "20260910T1", 1) == "eccom-sender-20260910T1.1"
    assert _container_name("sender", "20260910T1") == "eccom-sender-20260910T1"


def _report(attempted: int, rate: float, steps: bool) -> dict:
    return {
        "requested_message_rate": rate,
        "actual_message_rate": rate * 0.9,
        "messages_attempted": attempted,
        "messages_completed": attempted - 1,
        "messages_failed": 1,
        "send_window_s": 10.0 + attempted / 100,
        "total_duration_s": 12.0,
        "per_device": None,
        "steps": (
            [
                {"step": 0, "target_message_rate": rate, "actual_message_rate": rate,
                 "messages_attempted": attempted // 2, "messages_completed": attempted // 2,
                 "messages_failed": 0, "duration_s": 5.0},
                {"step": 1, "target_message_rate": rate * 2, "actual_message_rate": rate * 1.5,
                 "messages_attempted": attempted // 2, "messages_completed": attempted // 2 - 1,
                 "messages_failed": 1, "duration_s": 5.0 + attempted / 1000},
            ]
            if steps
            else None
        ),
    }


def test_merge_load_reports_sums_shares_step_by_step() -> None:
    merged = merge_load_reports([_report(100, 5.0, True), _report(200, 5.0, True)])
    assert merged["instances"] == 2
    assert merged["requested_message_rate"] == 10.0
    assert merged["messages_attempted"] == 300 and merged["messages_failed"] == 2
    assert merged["send_window_s"] == 12.0  # the longest instance window
    assert [s["step"] for s in merged["steps"]] == [0, 1]
    assert merged["steps"][1]["target_message_rate"] == 20.0
    assert merged["steps"][1]["messages_attempted"] == 150
    assert merged["steps"][1]["duration_s"] == 5.2
    assert merge_load_reports([_report(10, 1.0, False), _report(10, 1.0, False)])["steps"] is None


def _sample(t_s: float, cpu_s: float, rss: int, lag: float, host_cpu: float = 10.0) -> dict:
    return {
        "host": "edge", "phase": "traffic", "wall_time_ns": int(t_s * 1e9),
        "system_cpu_percent": host_cpu, "process_cpu_percent": cpu_s * 10,
        "process_cpu_seconds": cpu_s, "process_rss_bytes": rss, "loop_lag_ms_max": lag,
        "net_bytes_sent": 1000, "net_bytes_recv": 2000,
    }


def test_merge_instance_samples_adds_process_metrics_on_instance_zeros_timeline() -> None:
    base = [_sample(0.0, 1.0, 100, 1.0), _sample(1.0, 2.0, 100, 1.0), _sample(2.0, 3.0, 100, 1.0)]
    other = [_sample(0.4, 5.0, 50, 9.0, host_cpu=99.0), _sample(1.4, 6.0, 50, 2.0, host_cpu=99.0)]
    merged = merge_instance_samples([base, other])
    assert [s["wall_time_ns"] for s in merged] == [0, int(1e9), int(2e9)]
    # t=0: the other instance has no sample yet -> instance 0 alone
    assert merged[0]["process_cpu_seconds"] == 1.0 and merged[0]["process_rss_bytes"] == 100
    # t=1: as-of join picks the other's 0.4 s sample
    assert merged[1]["process_cpu_seconds"] == 7.0 and merged[1]["process_rss_bytes"] == 150
    assert merged[1]["loop_lag_ms_max"] == 9.0
    # t=2: the other's latest (1.4 s) sample, still fresh
    assert merged[2]["process_cpu_seconds"] == 9.0 and merged[2]["loop_lag_ms_max"] == 2.0
    # host-wide values and NIC counters stay instance 0's
    assert all(s["system_cpu_percent"] == 10.0 and s["net_bytes_sent"] == 1000 for s in merged)
    assert merge_instance_samples([base]) is base


def test_loaders_merge_instance_files(tmp_path: Path) -> None:
    import json

    for i in range(2):
        (tmp_path / f"edge.{i}-load-report.json").write_text(json.dumps(_report(10, 2.0, False)))
        (tmp_path / f"edge.{i}-system-metrics.jsonl").write_text(
            "\n".join(json.dumps(_sample(t, t + i, 10, 0.0)) for t in (0.0, 1.0)) + "\n"
        )
    (tmp_path / "cloud-system-metrics.jsonl").write_text(json.dumps(_sample(0.0, 1.0, 1, 0.0)) + "\n")
    report = load_load_report(tmp_path, "edge")
    assert report["instances"] == 2 and report["messages_attempted"] == 20
    metrics = load_system_metrics(tmp_path)
    assert set(metrics) == {"edge", "cloud"}
    assert [s["process_cpu_seconds"] for s in metrics["edge"]] == [1.0, 3.0]
    assert load_load_report(tmp_path, "cloud") is None
