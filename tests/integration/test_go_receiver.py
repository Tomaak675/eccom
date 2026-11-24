"""The Python sender and analysis against the Go receiver, through the real
runner (agents as subprocesses), for HTTP and MQTT in both interactions: the
proof that the Go agent's artifacts satisfy the analysis contract.

Skipped without the Go agent binary (go/bin/eccom-agent[.exe]) or a Go
toolchain to build it; the MQTT cases also need Docker for the managed broker.
"""

import asyncio
import json
import shutil
from pathlib import Path

import pytest
import yaml

from eccom.analysis.loading import EVENT_COLUMNS
from eccom.orchestration import runner
from eccom.orchestration.local import LocalProcessDeployment, go_agent_binary
from tests.integration.helpers import docker_available, free_port, load_events

MESSAGES = 20
METRIC_KEYS = {
    "host", "phase", "wall_time_ns", "monotonic_ns", "system_cpu_percent", "process_cpu_percent",
    "process_cpu_seconds", "system_memory_percent", "process_rss_bytes", "net_bytes_sent",
    "net_bytes_recv", "loop_lag_ms_max",
}


@pytest.fixture(scope="module")
def go_agent() -> Path:
    binary = go_agent_binary()
    if not binary.is_file() and shutil.which("go") is None:
        pytest.skip("no Go agent binary and no Go toolchain to build it")
    asyncio.run(LocalProcessDeployment().prepare(frozenset({"go"})))
    return binary


def _write_scenario(path: Path, protocol: str, interaction: str, endpoint_port: int) -> Path:
    scenario = {
        "name": f"go_{protocol}_{interaction}",
        "interaction": interaction,
        "payload": {"size_bytes": 64, "content": "random", "seed": 1},
        "load": {"messages_per_second": 50, "total_messages": MESSAGES, "concurrency": 4},
        "receiver": {"control_port": free_port(), "implementation": "go"},
        "experiment": {"warmup_s": 0, "cooldown_s": 0.5},
        "instrumentation": {"system_metrics": True, "system_metrics_interval_s": 0.2, "baseline_s": 0.2},
        # "cloud" names the receiver's role: the runner resolves it (127.0.0.1
        # here) and passes --endpoint-host to both agents.
        "protocol": {"type": protocol, protocol: {"endpoint": {"host": "cloud", "port": endpoint_port}}},
    }
    path.write_text(yaml.safe_dump(scenario), encoding="utf-8")
    return path


def _run(scenario_path: Path, runs_root: Path) -> Path:
    return asyncio.run(runner.TestRunner(scenario_path, runs_root).run())


def _assert_contract(run_dir: Path, interaction: str) -> None:
    summary = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))
    counts = summary["counts"]
    assert counts["messages_attempted"] == MESSAGES
    assert counts["messages_completed"] == MESSAGES
    assert counts["received_by_receiver"] == MESSAGES

    environment = json.loads((run_dir / "cloud-environment.json").read_text(encoding="utf-8"))
    assert environment["implementation"] == "go"
    assert environment["go_version"].startswith("go")
    assert environment["python_version"] is None

    events = load_events(run_dir / "cloud-events.jsonl")
    assert list(events[0]) == EVENT_COLUMNS
    expected = {"MESSAGE_RECEIVED", "HANDLER_STARTED", "HANDLER_COMPLETED"}
    if interaction == "request_response":
        expected.add("REPLY_SEND_STARTED")
    assert {event["event_type"] for event in events} == expected
    assert all(event["phase"] is None and event["host"] == "cloud" for event in events)

    metrics = load_events(run_dir / "cloud-system-metrics.jsonl")
    assert metrics and METRIC_KEYS <= set(metrics[0])
    assert {sample["phase"] for sample in metrics} <= {"startup", "baseline", "traffic"}
    assert "receiver ready" in (run_dir / "logs" / "receiver.log").read_text(encoding="utf-8")


@pytest.mark.parametrize("interaction", ["one_way", "request_response"])
def test_http_sender_against_go_receiver(tmp_path: Path, go_agent: Path, interaction: str) -> None:
    scenario = _write_scenario(tmp_path / "scenario.yaml", "http", interaction, free_port())
    run_dir = _run(scenario, tmp_path / "runs")
    _assert_contract(run_dir, interaction)


@pytest.mark.parametrize("interaction", ["one_way", "request_response"])
def test_mqtt_sender_against_go_receiver(tmp_path: Path, go_agent: Path, interaction: str) -> None:
    if not docker_available():
        pytest.skip("docker is not available for the managed broker")
    scenario = _write_scenario(tmp_path / "scenario.yaml", "mqtt", interaction, free_port())
    run_dir = _run(scenario, tmp_path / "runs")
    _assert_contract(run_dir, interaction)
