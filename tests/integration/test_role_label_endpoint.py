"""A scenario that names the receiver's topology role as its endpoint host
("cloud") instead of an address, run through the real runner with the Python
agents as subprocesses: the runner resolves the role from the hosts mapping
(local here, so 127.0.0.1) and hands the address to both agents as
--endpoint-host, while the run's scenario copy keeps the label."""

import asyncio
import json
from pathlib import Path

import yaml

from eccom.orchestration import runner
from tests.integration.helpers import free_port

MESSAGES = 10


def test_runner_resolves_the_cloud_role_for_python_agents(tmp_path: Path) -> None:
    scenario = {
        "name": "role_label_http",
        "payload": {"size_bytes": 64},
        "load": {"messages_per_second": 50, "total_messages": MESSAGES},
        "receiver": {"control_port": free_port()},
        "experiment": {"warmup_s": 0, "cooldown_s": 0.5},
        "instrumentation": {"system_metrics": False},
        "protocol": {"type": "http", "http": {"endpoint": {"host": "cloud", "port": free_port()}}},
    }
    path = tmp_path / "scenario.yaml"
    path.write_text(yaml.safe_dump(scenario), encoding="utf-8")

    run_dir = asyncio.run(runner.TestRunner(path, tmp_path / "runs").run())

    counts = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))["counts"]
    assert counts["messages_completed"] == MESSAGES
    assert counts["received_by_receiver"] == MESSAGES
    assert "host: cloud" in (run_dir / "scenario.yaml").read_text(encoding="utf-8")
