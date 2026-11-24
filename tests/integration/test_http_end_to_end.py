"""End-to-end integration tests: HTTP sender and receiver in one process."""

import asyncio
import shutil
import socket
from pathlib import Path

import pytest

import eccom.protocols.http as http_adapter
from eccom.analysis import analyze_run
from eccom.analysis.compare import compare_run, format_comparison
from eccom.core.config import ScenarioConfig
from eccom.core.events import EventRecorder
from eccom.core.message import Message, new_correlation_id
from eccom.protocols import create_adapter
from tests.integration.helpers import free_port, load_events, run_scenario_in_process

CERTS_DIR = Path(__file__).parents[1] / "certs"


def make_scenario(port: int, **overrides) -> ScenarioConfig:
    raw = {
        "name": "integration-test",
        "protocol": {"type": "http", "http": {"endpoint": {"port": port}}},
        "payload": {"size_bytes": 512},
        "load": {"messages_per_second": 200, "total_messages": 20, "concurrency": 5},
        "experiment": {"warmup_s": 0, "cooldown_s": 0},
        "instrumentation": {"system_metrics": False},
    }
    for key, value in overrides.items():
        raw[key] = value
    return ScenarioConfig.model_validate(raw)


async def test_http_request_response_end_to_end(tmp_path: Path) -> None:
    scenario = make_scenario(free_port())
    report = await run_scenario_in_process(scenario, tmp_path)

    assert report.messages_attempted == 20
    assert report.messages_completed == 20
    assert report.messages_failed == 0

    edge_events = load_events(tmp_path / "edge-events.jsonl")
    cloud_events = load_events(tmp_path / "cloud-events.jsonl")

    def of_type(events: list[dict], event_type: str) -> list[dict]:
        return [e for e in events if e["event_type"] == event_type]

    assert len(of_type(edge_events, "PAYLOAD_CREATED")) == 20
    assert len(of_type(edge_events, "SEND_STARTED")) == 20
    assert len(of_type(edge_events, "REPLY_RECEIVED")) == 20
    assert len(of_type(cloud_events, "MESSAGE_RECEIVED")) == 20
    assert len(of_type(cloud_events, "HANDLER_STARTED")) == 20
    assert len(of_type(cloud_events, "HANDLER_COMPLETED")) == 20
    assert len(of_type(cloud_events, "REPLY_SEND_STARTED")) == 20

    # correlation ids join the edge and cloud logs
    edge_ids = {e["correlation_id"] for e in of_type(edge_events, "SEND_STARTED")}
    cloud_ids = {e["correlation_id"] for e in of_type(cloud_events, "MESSAGE_RECEIVED")}
    assert edge_ids == cloud_ids
    assert len(edge_ids) == 20

    # all sender events are tagged with the measurement phase (no warmup)
    assert all(e["phase"] == "measurement" for e in edge_events)

    # offline analysis produces RTT statistics and plots from the raw events
    summary = analyze_run(tmp_path)
    assert summary["counts"]["messages_completed"] == 20
    assert summary["counts"]["received_by_receiver"] == 20
    rtt = summary["rtt_ms"]
    assert rtt["count"] == 20
    assert 0 < rtt["min"] <= rtt["median"] <= rtt["p95"] <= rtt["max"]
    assert (tmp_path / "summary.json").exists()
    for plot in ("latency_histogram.png", "latency_over_time.png", "throughput_over_time.png"):
        assert (tmp_path / "plots" / plot).exists()


async def test_artificial_delay_is_visible_in_rtt(tmp_path: Path) -> None:
    scenario = make_scenario(
        free_port(),
        load={"messages_per_second": 100, "total_messages": 5, "concurrency": 5},
        receiver={"artificial_processing_delay_ms": 50},
    )
    await run_scenario_in_process(scenario, tmp_path)
    summary = analyze_run(tmp_path)
    # asyncio.sleep can wake slightly early relative to wall time on coarse OS
    # timers (Windows), so assert "clearly above baseline", not ">= 50".
    assert summary["rtt_ms"]["min"] >= 40
    assert summary["receiver_handler_ms"]["min"] >= 40


async def test_timeouts_are_recorded_as_timeout_events(tmp_path: Path) -> None:
    port = free_port()
    scenario = make_scenario(
        port,
        protocol={
            "type": "http",
            "http": {"endpoint": {"port": port}, "client": {"timeout_s": 0.05}},
        },
        load={"messages_per_second": 50, "total_messages": 3, "concurrency": 3},
        receiver={"artificial_processing_delay_ms": 400},
    )
    report = await run_scenario_in_process(scenario, tmp_path)

    assert report.messages_attempted == 3
    assert report.messages_completed == 0
    assert report.messages_failed == 3

    edge_events = load_events(tmp_path / "edge-events.jsonl")
    assert len([e for e in edge_events if e["event_type"] == "TIMEOUT"]) == 3

    summary = analyze_run(tmp_path)
    assert summary["counts"]["timeouts"] == 3
    assert summary["counts"]["messages_completed"] == 0
    assert summary["rtt_ms"] is None


async def test_iot_device_simulation_end_to_end(tmp_path: Path) -> None:
    scenario = make_scenario(
        free_port(),
        payload=None,
        load={"mode": "devices", "duration_s": 0.5},
        devices=[
            {"name": "meter-1", "type": "smart_meter", "interval_s": 0.05},
            {"name": "cam-1", "type": "camera", "interval_s": 0.2, "size_bytes": 2048},
        ],
    )
    report = await run_scenario_in_process(scenario, tmp_path)

    assert report.per_device is not None
    assert report.per_device["meter-1"]["attempted"] >= 5
    assert report.per_device["cam-1"]["attempted"] >= 1
    assert report.messages_failed == 0

    edge_events = load_events(tmp_path / "edge-events.jsonl")
    created = [e for e in edge_events if e["event_type"] == "PAYLOAD_CREATED"]
    by_device: dict[str, list[dict]] = {}
    for event in created:
        by_device.setdefault(event["metadata"]["device"], []).append(event)
    assert set(by_device) == {"meter-1", "cam-1"}
    assert all(e["metadata"]["device_type"] == "camera" for e in by_device["cam-1"])
    assert all(e["metadata"]["size_bytes"] == 2048 for e in by_device["cam-1"])

    summary = analyze_run(tmp_path)
    assert summary["counts"]["messages_completed"] == report.messages_completed
    assert summary["rates"]["load_generator_report"]["per_device"]["meter-1"]["attempted"] >= 5
    # requested rate is derived from device intervals: 1/0.05 + 1/0.2
    assert summary["rates"]["requested_message_rate"] == 25.0


async def test_smart_home_ramp_end_to_end(tmp_path: Path) -> None:
    """homes mode through the real sender and the analysis: one batch of two
    homes per step (50 msg/s per batch), every message tagged with its step,
    number of homes and sensor, the ramp table keyed by homes."""
    scenario = make_scenario(
        free_port(),
        interaction="one_way",
        payload=None,
        home={
            "sensors": [
                {"type": "a", "count": 2, "rate_hz": 20, "size_bytes": 8},
                {"type": "b", "count": 1, "rate_hz": 10, "size_bytes": 32},
            ]
        },
        load={
            "mode": "homes", "homes": 2, "homes_increment": 2, "aggregation_factor": 2,
            "step_duration_s": 0.5, "duration_s": 1.5, "concurrency": 20,
        },
    )
    report = await run_scenario_in_process(scenario, tmp_path)

    assert report.steps is not None
    assert [s.target_message_rate for s in report.steps] == [50, 100, 150]
    assert report.messages_failed == 0
    created = [e for e in load_events(tmp_path / "edge-events.jsonl") if e["event_type"] == "PAYLOAD_CREATED"]
    assert {(e["metadata"]["step"], e["metadata"]["target_homes"]) for e in created} == {(0, 2), (1, 4), (2, 6)}
    # two homes' readings per message: twice the sensor's reading size
    assert {(e["metadata"]["sensor"], e["metadata"]["size_bytes"]) for e in created} == {("a", 16), ("b", 64)}

    summary = analyze_run(tmp_path)
    assert summary["counts"]["received_by_receiver"] == report.messages_attempted
    assert [r["homes"] for r in summary["ramp"]] == [2, 4, 6]
    assert summary["rates"]["requested_message_rate"] == 50
    assert (tmp_path / "plots" / "ramp_over_time.png").exists()
    result = compare_run(tmp_path)
    assert result["aggregation_factor"] == 2
    assert "Ramp: homes held, and sustained before the knee" in format_comparison([result])


async def test_https_request_response_end_to_end(tmp_path: Path) -> None:
    """Same adapter, TLS composed via config; chain verified against the
    self-signed cert acting as its own CA."""
    (tmp_path / "certs").mkdir()
    shutil.copyfile(CERTS_DIR / "server.pem", tmp_path / "certs" / "server.pem")
    shutil.copyfile(CERTS_DIR / "server-key.pem", tmp_path / "certs" / "server-key.pem")
    port = free_port()
    scenario = make_scenario(
        port,
        protocol={
            "type": "http",
            "http": {"endpoint": {"port": port}},
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


async def test_two_sender_instances_share_the_load(tmp_path: Path) -> None:
    """load.senders: each process sends its share into its own artifact files;
    the analysis merges them into one sender view."""
    scenario = make_scenario(
        free_port(),
        interaction="one_way",
        load={"messages_per_second": 200, "total_messages": 20, "concurrency": 5, "senders": 2},
    )
    report = await run_scenario_in_process(scenario, tmp_path)

    assert report.messages_attempted == 20
    assert report.messages_completed == 20
    assert not (tmp_path / "edge-events.jsonl").exists()
    for label in ("edge.0", "edge.1"):
        types = [e["event_type"] for e in load_events(tmp_path / f"{label}-events.jsonl")]
        assert types.count("SEND_STARTED") == 10  # 20 messages split 10 / 10
        assert (tmp_path / f"{label}-load-report.json").exists()

    summary = analyze_run(tmp_path)
    assert summary["counts"]["messages_attempted"] == 20
    assert summary["counts"]["received_by_receiver"] == 20
    assert summary["send_ack_ms"]["count"] == 20
    generator = summary["rates"]["load_generator_report"]
    assert generator["instances"] == 2
    assert generator["requested_message_rate"] == 200  # the shares add up


async def test_http_one_way_end_to_end(tmp_path: Path) -> None:
    scenario = make_scenario(free_port(), interaction="one_way")
    report = await run_scenario_in_process(scenario, tmp_path)

    assert report.messages_attempted == 20
    assert report.messages_completed == 20

    edge_events = load_events(tmp_path / "edge-events.jsonl")
    cloud_events = load_events(tmp_path / "cloud-events.jsonl")
    edge_types = [e["event_type"] for e in edge_events]
    assert edge_types.count("SEND_STARTED") == 20
    assert edge_types.count("SEND_COMPLETED") == 20
    assert "REPLY_RECEIVED" not in edge_types  # no replies in one-way mode
    assert [e["event_type"] for e in cloud_events].count("MESSAGE_RECEIVED") == 20

    summary = analyze_run(tmp_path)
    assert summary["counts"]["messages_completed"] == 20
    assert summary["counts"]["received_by_receiver"] == 20
    assert summary["rtt_ms"] is None
    assert summary["send_ack_ms"]["count"] == 20
    # same-host run: offset 0, so one-way latency is directly valid
    one_way = summary["one_way_latency_ms"]
    assert one_way["count"] == 20
    assert one_way["max"] > 0
    assert summary["clock_sync"]["method"] == "same_host"
    assert summary["throughput"]["messages_per_second"] > 0
    for plot in ("latency_histogram.png", "latency_over_time.png", "throughput_over_time.png"):
        assert (tmp_path / "plots" / plot).exists()


async def test_clock_offset_probe_against_control_server(tmp_path: Path) -> None:
    from eccom.agents.control import ControlServer
    from eccom.instrumentation.clock_sync import combine_clock_probes, probe_clock_offset

    port = free_port()
    control = ControlServer("127.0.0.1", port)
    await control.start()
    try:
        before = await probe_clock_offset(f"http://127.0.0.1:{port}", samples=5)
        after = await probe_clock_offset(f"http://127.0.0.1:{port}", samples=5)
    finally:
        await control.stop()

    combined = combine_clock_probes(before, after)
    # probing our own clock: offset must be ~0 within the reported uncertainty
    assert abs(combined["offset_ns"]) <= max(combined["uncertainty_ns"], 5_000_000)
    assert combined["uncertainty_ns"] < 100_000_000
    assert combined["drift_ns"] is not None


async def test_one_way_send_and_listen(tmp_path: Path) -> None:
    """The one-way interaction (send/listen) works over the same adapter."""
    scenario = make_scenario(free_port())
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
            await sender_adapter.send(Message(new_correlation_id(), sequence, b"payload"))
        await asyncio.wait_for(done.wait(), timeout=5)
    finally:
        await sender_adapter.stop()
        await receiver_adapter.stop()

    assert [m.sequence_number for m in received] == [0, 1, 2]
    assert all(m.payload == b"payload" for m in received)


def _nodelay(transport: asyncio.BaseTransport) -> int:
    return transport.get_extra_info("socket").getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY)


@pytest.mark.parametrize("tcp_nodelay", [True, False])
async def test_tcp_nodelay_switch_reaches_client_and_server_sockets(
    tcp_nodelay: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """asyncio turns Nagle off on every transport it makes; protocol.tcp_nodelay:
    false has to turn it back on, for the aiohttp client and the server alike."""
    touched: list[asyncio.BaseTransport] = []
    keep_nagle = http_adapter._keep_nagle

    def spy(transport: asyncio.BaseTransport) -> None:
        keep_nagle(transport)
        touched.append(transport)

    monkeypatch.setattr(http_adapter, "_keep_nagle", spy)
    port = free_port()
    scenario = make_scenario(
        port,
        protocol={"type": "http", "http": {"endpoint": {"port": port}}, "tcp_nodelay": tcp_nodelay},
    )

    async def handler(message: Message) -> None:
        return None

    receiver_recorder = EventRecorder("cloud", tmp_path / "cloud-events.jsonl")
    receiver_adapter = create_adapter(scenario.protocol, receiver_recorder)
    sender_recorder = EventRecorder("edge", tmp_path / "edge-events.jsonl")
    sender_adapter = create_adapter(scenario.protocol, sender_recorder)
    try:
        await receiver_adapter.start()
        await receiver_adapter.listen(handler)
        await sender_adapter.start()
        await sender_adapter.send(Message(new_correlation_id(), 0, b"payload"))
        # keep-alive: both ends still hold the connection the message went over
        (server_side,) = receiver_adapter._server_runner.server.connections
        assert _nodelay(server_side.transport) == int(tcp_nodelay)
        if tcp_nodelay:
            assert touched == []  # asyncio's own default, nothing was undone
        else:
            client_side = [t for t in touched if t is not server_side.transport]
            assert len(client_side) == 1
            assert _nodelay(client_side[0]) == 0
    finally:
        await sender_adapter.stop()
        await receiver_adapter.stop()
