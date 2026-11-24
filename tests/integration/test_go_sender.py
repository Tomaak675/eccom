"""The Go sender through the real runner (agents as subprocesses) against the
Go receiver and against the Python receiver, for HTTP and MQTT in both
interactions, plus a scenario-5-shaped ramp with pauses: the proof that the
Go sender's artifacts satisfy the analysis contract.

Skipped without the Go agent binary (go/bin/eccom-agent[.exe]) or a Go
toolchain to build it; the MQTT cases also need Docker for the managed broker.
"""

import asyncio
import dataclasses
import json
import shutil
from pathlib import Path

import pytest
import yaml

from eccom.analysis.loading import EVENT_COLUMNS
from eccom.generators.load import LoadReport, StepReport
from eccom.orchestration import runner
from eccom.orchestration.local import LocalProcessDeployment, go_agent_binary
from tests.integration.helpers import docker_available, free_port, load_events

MESSAGES = 30
METRIC_KEYS = {
    "host", "phase", "wall_time_ns", "monotonic_ns", "system_cpu_percent", "process_cpu_percent",
    "process_cpu_seconds", "system_memory_percent", "process_rss_bytes", "net_bytes_sent",
    "net_bytes_recv", "loop_lag_ms_max",
}
REPORT_KEYS = [field.name for field in dataclasses.fields(LoadReport)]
STEP_KEYS = [field.name for field in dataclasses.fields(StepReport)]


@pytest.fixture(scope="module")
def go_agent() -> Path:
    binary = go_agent_binary()
    if not binary.is_file() and shutil.which("go") is None:
        pytest.skip("no Go agent binary and no Go toolchain to build it")
    asyncio.run(LocalProcessDeployment().prepare(frozenset({"go"})))
    return binary


def _write_scenario(
    path: Path, protocol: str, interaction: str, receiver: str, load: dict | None = None
) -> Path:
    scenario = {
        "name": f"go_sender_{protocol}_{interaction}_{receiver}",
        "interaction": interaction,
        "payload": {"size_bytes": 64, "content": "random", "seed": 1},
        "load": load or {"messages_per_second": 100, "total_messages": MESSAGES, "concurrency": 4},
        "sender": {"implementation": "go", "threads": 2},
        "receiver": {"control_port": free_port(), "implementation": receiver},
        "experiment": {"warmup_s": 0.1, "cooldown_s": 0.5},
        "instrumentation": {"system_metrics": True, "system_metrics_interval_s": 0.2, "baseline_s": 0.2},
        # "cloud" names the receiver's role: the runner resolves it (127.0.0.1
        # here) and passes --endpoint-host to both agents.
        "protocol": {"type": protocol, protocol: {"endpoint": {"host": "cloud", "port": free_port()}}},
    }
    path.write_text(yaml.safe_dump(scenario), encoding="utf-8")
    return path


def _run(scenario_path: Path, runs_root: Path) -> Path:
    return asyncio.run(runner.TestRunner(scenario_path, runs_root).run())


def _expected_sender_events(protocol: str, interaction: str) -> set[str]:
    expected = {"PAYLOAD_CREATED", "SEND_STARTED"}
    if interaction == "one_way":
        return expected | {"SEND_COMPLETED"}
    expected.add("REPLY_RECEIVED")
    if protocol == "mqtt":
        expected.add("SEND_COMPLETED")  # the broker's ack is recorded next to the reply
    return expected


def _assert_contract(run_dir: Path, protocol: str, interaction: str, receiver: str) -> None:
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    counts = summary["counts"]
    assert counts["messages_attempted"] == MESSAGES
    assert counts["messages_completed"] == MESSAGES
    assert counts["received_by_receiver"] == MESSAGES
    assert summary["clock_sync"]["method"] == "control-endpoint-ntp-probe"

    environment = json.loads((run_dir / "edge-environment.json").read_text(encoding="utf-8"))
    assert environment["implementation"] == "go" and environment["host_role"] == "edge"
    assert environment["go_max_procs"] == 2 and environment["python_version"] is None
    cloud = json.loads((run_dir / "cloud-environment.json").read_text(encoding="utf-8"))
    assert cloud["implementation"] == receiver

    events = load_events(run_dir / "edge-events.jsonl")
    assert list(events[0]) == EVENT_COLUMNS
    assert {event["event_type"] for event in events} == _expected_sender_events(protocol, interaction)
    assert {event["phase"] for event in events} == {"warmup", "measurement"}
    assert all(event["host"] == "edge" and len(event["correlation_id"]) == 32 for event in events)
    sizes = {event["metadata"]["size_bytes"] for event in events if event["event_type"] == "PAYLOAD_CREATED"}
    assert sizes == {64}

    report = json.loads((run_dir / "edge-load-report.json").read_text(encoding="utf-8"))
    assert list(report) == REPORT_KEYS
    assert report["messages_attempted"] == MESSAGES and report["steps"] is None and report["per_device"] is None
    assert summary["rates"]["load_generator_report"]["messages_completed"] == MESSAGES

    clock_sync = json.loads((run_dir / "edge-clock-sync.json").read_text(encoding="utf-8"))
    assert clock_sync["method"] == "control-endpoint-ntp-probe" and clock_sync["after"] is not None

    metrics = load_events(run_dir / "edge-system-metrics.jsonl")
    assert metrics and METRIC_KEYS <= set(metrics[0])
    assert {sample["phase"] for sample in metrics} <= {"startup", "baseline", "traffic"}
    log = (run_dir / "logs" / "sender.log").read_text(encoding="utf-8")
    assert "measurement phase done" in log and "lanes=2" in log


@pytest.mark.parametrize("interaction", ["one_way", "request_response"])
def test_http_go_sender_against_go_receiver(tmp_path: Path, go_agent: Path, interaction: str) -> None:
    scenario = _write_scenario(tmp_path / "scenario.yaml", "http", interaction, "go")
    _assert_contract(_run(scenario, tmp_path / "runs"), "http", interaction, "go")


def test_http_go_sender_against_python_receiver(tmp_path: Path, go_agent: Path) -> None:
    scenario = _write_scenario(tmp_path / "scenario.yaml", "http", "one_way", "python")
    _assert_contract(_run(scenario, tmp_path / "runs"), "http", "one_way", "python")


@pytest.mark.parametrize("interaction", ["one_way", "request_response"])
def test_mqtt_go_sender_against_go_receiver(tmp_path: Path, go_agent: Path, interaction: str) -> None:
    if not docker_available():
        pytest.skip("docker is not available for the managed broker")
    scenario = _write_scenario(tmp_path / "scenario.yaml", "mqtt", interaction, "go")
    _assert_contract(_run(scenario, tmp_path / "runs"), "mqtt", interaction, "go")


def test_mqtt_go_sender_against_python_receiver(tmp_path: Path, go_agent: Path) -> None:
    if not docker_available():
        pytest.skip("docker is not available for the managed broker")
    scenario = _write_scenario(tmp_path / "scenario.yaml", "mqtt", "request_response", "python")
    _assert_contract(_run(scenario, tmp_path / "runs"), "mqtt", "request_response", "python")


def test_ramp_with_pauses_yields_the_step_table(tmp_path: Path, go_agent: Path) -> None:
    """A linear ramp with a drain pause after
    every step, tagged per step so the analysis builds the per-step table."""
    load = {
        "mode": "ramp", "messages_per_second": 100, "rate_increment": 100, "step_duration_s": 0.5,
        "step_pause_s": 0.3, "duration_s": 1.5, "concurrency": 20,
    }
    scenario = _write_scenario(tmp_path / "scenario.yaml", "http", "one_way", "go", load)
    run_dir = _run(scenario, tmp_path / "runs")

    report = json.loads((run_dir / "edge-load-report.json").read_text(encoding="utf-8"))
    assert [step["step"] for step in report["steps"]] == [0, 1, 2]
    assert [step["target_message_rate"] for step in report["steps"]] == [100, 200, 300]
    assert all(list(step) == STEP_KEYS and step["payload_size_bytes"] is None for step in report["steps"])
    assert all(step["messages_attempted"] > 0 for step in report["steps"])
    # the pauses are not sending time: 1.5 s of sending over ~2.1 s of wall time
    assert report["send_window_s"] >= 2.0

    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    ramp = summary["ramp"]
    assert [row["step"] for row in ramp] == [0, 1, 2]
    assert [row["target_message_rate"] for row in ramp] == [100, 200, 300]
    tags = {
        (event["metadata"]["step"], event["metadata"]["target_rate"])
        for event in load_events(run_dir / "edge-events.jsonl")
        if event["event_type"] == "PAYLOAD_CREATED" and event["phase"] == "measurement"
    }
    assert tags == {(0, 100), (1, 200), (2, 300)}
    assert summary["counts"]["messages_failed"] == 0
