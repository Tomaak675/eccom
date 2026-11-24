"""End-to-end MQTT tests against a real mosquitto broker in Docker.

Skipped automatically when Docker is not available.
"""

import asyncio
import shutil
import socket
import subprocess
import sys
import time
from pathlib import Path

import pytest

# (shutil is used both for docker lookup and for staging TLS cert copies)


@pytest.fixture(scope="module")
def event_loop_policy():
    # paho-mqtt needs add_reader/add_writer: on Windows that means the
    # selector loop (mirrors what `eccom agent` does in production).
    if sys.platform == "win32":
        return asyncio.WindowsSelectorEventLoopPolicy()
    return asyncio.get_event_loop_policy()

from eccom.analysis import analyze_run
from eccom.core.config import ScenarioConfig
from eccom.core.events import EventRecorder
from eccom.core.message import Message, new_correlation_id
from eccom.protocols import create_adapter
from tests.integration.helpers import (
    docker_available,
    free_port,
    load_events,
    run_scenario_in_process,
)


@pytest.fixture(scope="module")
def mqtt_broker():
    """A throwaway mosquitto container; yields (host, port)."""
    if not docker_available():
        pytest.skip("docker is not available")
    port = free_port()
    name = f"eccom-test-broker-{port}"
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    result = subprocess.run(
        [
            "docker", "run", "-d", "--rm", "--name", name,
            "-p", f"{port}:1883",
            "eclipse-mosquitto:2", "mosquitto", "-c", "/mosquitto-no-auth.conf",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"cannot start mosquitto container: {result.stderr.strip()}")
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                break
        except OSError:
            time.sleep(0.2)
    else:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        pytest.skip("mosquitto did not become reachable")
    yield "127.0.0.1", port
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)


CERTS_DIR = Path(__file__).parents[1] / "certs"


@pytest.fixture(scope="module")
def mqtt_tls_broker(tmp_path_factory):
    """A mosquitto container with a TLS listener; yields (host, port)."""
    if not docker_available():
        pytest.skip("docker is not available")
    port = free_port()
    config_dir = tmp_path_factory.mktemp("mosquitto-tls")
    shutil.copyfile(CERTS_DIR / "server.pem", config_dir / "server.pem")
    shutil.copyfile(CERTS_DIR / "server-key.pem", config_dir / "server-key.pem")
    (config_dir / "mosquitto.conf").write_text(
        f"listener {port}\nallow_anonymous true\n"
        "certfile /mosquitto/config/server.pem\n"
        "keyfile /mosquitto/config/server-key.pem\n",
        encoding="utf-8",
        newline="\n",
    )
    name = f"eccom-test-tls-broker-{port}"
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)
    result = subprocess.run(
        [
            "docker", "run", "-d", "--rm", "--name", name, "--user", "root",
            "-p", f"{port}:{port}",
            "-v", f"{config_dir}:/mosquitto/config:ro",
            "eclipse-mosquitto:2",
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        pytest.skip(f"cannot start TLS mosquitto container: {result.stderr.strip()}")
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1):
                break
        except OSError:
            time.sleep(0.2)
    else:
        subprocess.run(["docker", "rm", "-f", name], capture_output=True)
        pytest.skip("TLS mosquitto did not become reachable")
    yield "127.0.0.1", port
    subprocess.run(["docker", "rm", "-f", name], capture_output=True)


def make_scenario(host: str, port: int, **overrides) -> ScenarioConfig:
    raw = {
        "name": "mqtt-integration-test",
        "protocol": {"type": "mqtt", "mqtt": {"endpoint": {"host": host, "port": port}}},
        "payload": {"size_bytes": 512},
        "load": {"messages_per_second": 200, "total_messages": 20, "concurrency": 5},
        "experiment": {"warmup_s": 0, "cooldown_s": 0},
        "instrumentation": {"system_metrics": False},
    }
    for key, value in overrides.items():
        raw[key] = value
    return ScenarioConfig.model_validate(raw)


async def test_mqtt_request_reply_end_to_end(mqtt_broker, tmp_path: Path) -> None:
    scenario = make_scenario(*mqtt_broker)
    report = await run_scenario_in_process(scenario, tmp_path)

    assert report.messages_attempted == 20
    assert report.messages_completed == 20
    assert report.messages_failed == 0

    edge_events = load_events(tmp_path / "edge-events.jsonl")
    cloud_events = load_events(tmp_path / "cloud-events.jsonl")

    def of_type(events: list[dict], event_type: str) -> list[dict]:
        return [e for e in events if e["event_type"] == event_type]

    assert len(of_type(edge_events, "SEND_STARTED")) == 20
    assert len(of_type(edge_events, "SEND_COMPLETED")) == 20  # broker PUBACKs
    assert len(of_type(edge_events, "REPLY_RECEIVED")) == 20
    assert len(of_type(cloud_events, "MESSAGE_RECEIVED")) == 20
    assert len(of_type(cloud_events, "HANDLER_COMPLETED")) == 20
    assert len(of_type(cloud_events, "REPLY_SEND_STARTED")) == 20

    edge_ids = {e["correlation_id"] for e in of_type(edge_events, "SEND_STARTED")}
    cloud_ids = {e["correlation_id"] for e in of_type(cloud_events, "MESSAGE_RECEIVED")}
    assert edge_ids == cloud_ids
    assert len(edge_ids) == 20

    # sequence numbers survive the MQTT user-property round trip
    cloud_sequences = {e["sequence_number"] for e in of_type(cloud_events, "MESSAGE_RECEIVED")}
    assert cloud_sequences == set(range(20))

    summary = analyze_run(tmp_path)
    assert summary["counts"]["messages_completed"] == 20
    assert summary["counts"]["received_by_receiver"] == 20
    assert summary["rtt_ms"]["min"] > 0
    for plot in ("latency_histogram.png", "latency_over_time.png", "throughput_over_time.png"):
        assert (tmp_path / "plots" / plot).exists()


async def test_mqtt_timeouts_are_recorded(mqtt_broker, tmp_path: Path) -> None:
    host, port = mqtt_broker
    scenario = make_scenario(
        host,
        port,
        protocol={
            "type": "mqtt",
            "mqtt": {"endpoint": {"host": host, "port": port}, "client": {"timeout_s": 0.05}},
        },
        load={"messages_per_second": 50, "total_messages": 3, "concurrency": 3},
        receiver={"artificial_processing_delay_ms": 400},
    )
    report = await run_scenario_in_process(scenario, tmp_path)

    assert report.messages_attempted == 3
    assert report.messages_completed == 0

    edge_events = load_events(tmp_path / "edge-events.jsonl")
    assert len([e for e in edge_events if e["event_type"] == "TIMEOUT"]) == 3


async def test_mqtt_over_tls_end_to_end(mqtt_tls_broker, tmp_path: Path) -> None:
    host, port = mqtt_tls_broker
    (tmp_path / "certs").mkdir()
    shutil.copyfile(CERTS_DIR / "server.pem", tmp_path / "certs" / "server.pem")
    shutil.copyfile(CERTS_DIR / "server-key.pem", tmp_path / "certs" / "server-key.pem")
    scenario = make_scenario(
        host,
        port,
        protocol={
            "type": "mqtt",
            "mqtt": {"endpoint": {"host": host, "port": port}},
            "tls": {
                "enabled": True,
                "cert_file": "certs/server.pem",
                "key_file": "certs/server-key.pem",
                "ca_file": "certs/server.pem",
            },
        },
    )
    report = await run_scenario_in_process(scenario, tmp_path)

    assert report.messages_attempted == 20
    assert report.messages_completed == 20
    summary = analyze_run(tmp_path)
    assert summary["counts"]["received_by_receiver"] == 20
    assert summary["rtt_ms"]["count"] == 20


async def test_mqtt_one_way_scenario_end_to_end(mqtt_broker, tmp_path: Path) -> None:
    scenario = make_scenario(*mqtt_broker, interaction="one_way")
    report = await run_scenario_in_process(scenario, tmp_path)

    assert report.messages_attempted == 20
    assert report.messages_completed == 20

    summary = analyze_run(tmp_path)
    assert summary["counts"]["messages_completed"] == 20
    assert summary["counts"]["received_by_receiver"] == 20
    assert summary["rtt_ms"] is None
    # QoS 1: send-ack is the broker PUBACK latency
    assert summary["send_ack_ms"]["count"] == 20
    assert summary["send_ack_ms"]["min"] > 0
    assert summary["one_way_latency_ms"]["count"] == 20


async def test_mqtt_reconnect_per_message_end_to_end(mqtt_broker, tmp_path: Path) -> None:
    """The sender connects, publishes and disconnects once per message."""
    host, port = mqtt_broker
    scenario = make_scenario(
        host,
        port,
        interaction="one_way",
        protocol={
            "type": "mqtt",
            "mqtt": {
                "endpoint": {"host": host, "port": port},
                "client": {"reconnect_per_message": True},
            },
        },
        load={"messages_per_second": 20, "total_messages": 5, "concurrency": 1},
    )
    report = await run_scenario_in_process(scenario, tmp_path)

    assert report.messages_attempted == 5
    assert report.messages_completed == 5
    assert report.messages_failed == 0

    summary = analyze_run(tmp_path)
    assert summary["counts"]["received_by_receiver"] == 5
    # send-ack now spans connect + publish + PUBACK, not the publish alone
    assert summary["send_ack_ms"]["count"] == 5
    assert summary["send_ack_ms"]["min"] > 0


async def test_mqtt_reconnect_per_message_resumes_session_when_not_clean(
    mqtt_broker, tmp_path: Path
) -> None:
    """clean_start is only observable here: both settings must still deliver."""
    host, port = mqtt_broker
    scenario = make_scenario(
        host,
        port,
        interaction="one_way",
        protocol={
            "type": "mqtt",
            "mqtt": {
                "endpoint": {"host": host, "port": port},
                "client": {"reconnect_per_message": True, "clean_start": False},
            },
        },
        load={"messages_per_second": 20, "total_messages": 5, "concurrency": 1},
    )
    report = await run_scenario_in_process(scenario, tmp_path)

    assert report.messages_completed == 5
    assert analyze_run(tmp_path)["counts"]["received_by_receiver"] == 5


@pytest.mark.parametrize("tcp_nodelay", [True, False])
@pytest.mark.parametrize("broker_fixture", ["mqtt_broker", "mqtt_tls_broker"])
async def test_tcp_nodelay_switch_reaches_the_mqtt_socket(
    broker_fixture, tcp_nodelay: bool, request, tmp_path: Path
) -> None:
    """paho's own default is Nagle on (a publish written while the previous one
    is unacknowledged waits for its ACK); protocol.tcp_nodelay must decide the
    socket state, on plain and TLS sockets alike."""
    host, port = request.getfixturevalue(broker_fixture)
    protocol = {
        "type": "mqtt",
        "mqtt": {"endpoint": {"host": host, "port": port}},
        "tcp_nodelay": tcp_nodelay,
    }
    if broker_fixture == "mqtt_tls_broker":
        protocol["tls"] = {
            "enabled": True,
            "cert_file": "certs/server.pem",
            "key_file": "certs/server-key.pem",
            "ca_file": "certs/server.pem",
        }
    scenario = make_scenario(host, port, protocol=protocol)
    recorder = EventRecorder("edge", tmp_path / "edge-events.jsonl")
    adapter = create_adapter(scenario.protocol, recorder, CERTS_DIR.parent)
    await adapter.start()
    try:
        sock = adapter._client._client.socket()  # aiomqtt -> paho -> the (SSL) socket
        assert sock.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY) == int(tcp_nodelay)
    finally:
        await adapter.stop()


async def test_mqtt_one_way_publish_subscribe(mqtt_broker, tmp_path: Path) -> None:
    scenario = make_scenario(*mqtt_broker)
    received: list[Message] = []
    done = asyncio.Event()

    async def handler(message: Message) -> None:
        received.append(message)
        if len(received) == 3:
            done.set()

    receiver_recorder = EventRecorder("cloud", tmp_path / "cloud-events.jsonl")
    receiver_adapter = create_adapter(scenario.protocol, receiver_recorder)
    sender_recorder = EventRecorder("edge", tmp_path / "edge-events.jsonl")
    sender_adapter = create_adapter(scenario.protocol, sender_recorder)
    try:
        await receiver_adapter.start()
        await receiver_adapter.listen(handler)
        await sender_adapter.start()
        for sequence in range(3):
            await sender_adapter.send(Message(new_correlation_id(), sequence, b"telemetry"))
        await asyncio.wait_for(done.wait(), timeout=5)
    finally:
        await sender_adapter.stop()
        await receiver_adapter.stop()

    assert sorted(m.sequence_number for m in received) == [0, 1, 2]
    assert all(m.payload == b"telemetry" for m in received)
