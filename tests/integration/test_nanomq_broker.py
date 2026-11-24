"""The managed NanoMQ broker through the real runner (Python agents as
subprocesses): plaintext and TLS, one-way QoS 1 and request/response, with
the generated config and the broker's own log kept in the run dir.

Needs Docker for the broker container; skips otherwise.
"""

import asyncio
import json
import shutil
from pathlib import Path

import pytest
import yaml

from eccom.orchestration import runner
from tests.integration.helpers import docker_available, free_port

MESSAGES = 20
CERTS_DIR = Path(__file__).parents[1] / "certs"


def _write_scenario(path: Path, interaction: str, port: int, tls: bool) -> Path:
    scenario = {
        "name": f"nanomq_{interaction}" + ("_tls" if tls else ""),
        "interaction": interaction,
        "payload": {"size_bytes": 64, "content": "random", "seed": 1},
        "load": {"messages_per_second": 50, "total_messages": MESSAGES, "concurrency": 4},
        "receiver": {"control_port": free_port()},
        "experiment": {"warmup_s": 0, "cooldown_s": 0.5},
        "instrumentation": {"system_metrics": False},
        "protocol": {
            "type": "mqtt",
            "mqtt": {
                # "cloud" names the receiver's role: the runner resolves it (127.0.0.1 here)
                "endpoint": {"host": "cloud", "port": port},
                "client": {"qos": 1},
                "broker": {"implementation": "nanomq", "max_queued_messages": 0},
            },
        },
    }
    if tls:
        shutil.copytree(CERTS_DIR, path.parent / "certs")
        scenario["protocol"]["tls"] = {
            "enabled": True,
            "cert_file": "certs/server.pem",
            "key_file": "certs/server-key.pem",
            "ca_file": "certs/server.pem",
        }
    path.write_text(yaml.safe_dump(scenario), encoding="utf-8")
    return path


@pytest.mark.parametrize(
    "interaction,tls", [("one_way", False), ("request_response", False), ("one_way", True)]
)
def test_nanomq_broker_relays_the_run(tmp_path: Path, interaction: str, tls: bool) -> None:
    if not docker_available():
        pytest.skip("docker is not available for the managed broker")
    port = free_port()
    scenario = _write_scenario(tmp_path / "scenario.yaml", interaction, port, tls)
    run_dir = asyncio.run(runner.TestRunner(scenario, tmp_path / "runs").run())

    counts = json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))["counts"]
    assert counts["messages_attempted"] == MESSAGES
    assert counts["messages_completed"] == MESSAGES
    assert counts["received_by_receiver"] == MESSAGES

    conf = (run_dir / "broker" / "nanomq.conf").read_text(encoding="utf-8")
    assert f'bind = "0.0.0.0:{port}"' in conf
    assert ("listeners.ssl" in conf) == tls
    assert "max_mqueue_len = 1048576" in conf
    log = (run_dir / "broker" / "nanomq.log").read_text(encoding="utf-8")
    assert "NanoMQ Broker is started successfully" in log
    assert "online!" in log  # NanoMQ logs every client's connect at warn level
    assert "msg lost" not in log and "dropped" not in log
