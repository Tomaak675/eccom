"""Shared helpers for in-process integration tests."""

import asyncio
import dataclasses
import json
import shutil
import socket
import subprocess
from pathlib import Path

import yaml

from eccom.agents.receiver import ReceiverAgent
from eccom.analysis.loading import merge_load_reports
from eccom.core.artifacts import instance_label
from eccom.agents.sender import SenderAgent
from eccom.core.config import ScenarioConfig
from eccom.core.events import EventRecorder
from eccom.generators.load import LoadReport
from eccom.protocols import create_adapter


def free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def docker_available() -> bool:
    """Whether a Docker engine answers (tests needing containers skip otherwise)."""
    if shutil.which("docker") is None:
        return False
    try:
        return subprocess.run(["docker", "info"], capture_output=True, timeout=20).returncode == 0
    except (OSError, subprocess.TimeoutExpired):
        return False


async def run_scenario_in_process(
    scenario: ScenarioConfig, run_dir: Path, scenario_dir: Path | None = None
) -> LoadReport:
    """Wire receiver + sender directly (no subprocesses) and run the load.

    ``scenario_dir`` is the base for TLS file paths (defaults to run_dir)."""
    base_dir = scenario_dir or run_dir
    receiver_recorder = EventRecorder("cloud", run_dir / "cloud-events.jsonl")
    receiver_adapter = create_adapter(scenario.protocol, receiver_recorder, base_dir)
    receiver = ReceiverAgent(scenario, receiver_adapter, receiver_recorder)

    sender_host, instances = scenario.topology.sender, scenario.load.senders
    senders = []
    for instance in range(instances):
        label = instance_label(sender_host, instance, instances)
        recorder = EventRecorder(sender_host, run_dir / f"{label}-events.jsonl")
        adapter = create_adapter(scenario.protocol, recorder, base_dir)
        senders.append((label, recorder, adapter, SenderAgent(scenario, adapter, recorder, instance)))

    try:
        await receiver_adapter.start()
        await receiver.start()
        for _, _, adapter, _ in senders:
            await adapter.start()
        reports = await asyncio.gather(*(agent.run() for _, _, _, agent in senders))
    finally:
        for _, recorder, adapter, _ in senders:
            await adapter.stop()
            recorder.close()
        await receiver_adapter.stop()
        receiver_recorder.close()

    # persist the artifacts a real run would have, so the analyzer can work
    (run_dir / "scenario.yaml").write_text(
        yaml.safe_dump(scenario.model_dump(mode="json")), encoding="utf-8"
    )
    for (label, _, _, _), instance_report in zip(senders, reports):
        (run_dir / f"{label}-load-report.json").write_text(
            json.dumps(dataclasses.asdict(instance_report)), encoding="utf-8"
        )
    if len(reports) == 1:
        report = reports[0]
    else:
        merged = merge_load_reports([dataclasses.asdict(r) for r in reports])
        merged.pop("instances")
        report = LoadReport(**merged)
    # both agents ran in this process: same clock, like a local runner run
    (run_dir / "run.json").write_text(
        json.dumps({"clock_sync": {"method": "same_host", "synchronized": True}}),
        encoding="utf-8",
    )
    return report


def load_events(path: Path) -> list[dict]:
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
