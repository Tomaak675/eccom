import json
from typing import Any

from eccom.core.config import DeviceConfig
from eccom.generators.devices import DeviceSimulator, build_devices


def config(**kwargs) -> DeviceConfig:
    return DeviceConfig.model_validate(kwargs)


def test_sensor_payloads_are_hardcoded_readings() -> None:
    devices = build_devices(
        [
            config(name="meter-1", type="smart_meter", interval_s=1),
            config(name="temp-1", type="temperature_sensor", interval_s=1),
        ]
    )
    meter_reading = json.loads(devices[0].payload)
    assert meter_reading["type"] == "smart_meter"
    assert meter_reading["device"] == "meter-1"
    assert "active_power_w" in meter_reading

    temperature_reading = json.loads(devices[1].payload)
    assert temperature_reading["type"] == "temperature_sensor"
    assert "temperature_c" in temperature_reading


def test_blob_payloads_have_configured_size_and_are_deterministic() -> None:
    configs = [
        config(name="cam-1", type="camera", interval_s=1),
        config(name="cam-2", type="camera", interval_s=1, size_bytes=2048),
        config(name="g-1", type="generic", interval_s=1, size_bytes=64),
    ]
    first, second = build_devices(configs), build_devices(configs)
    assert len(first[0].payload) == 100_000  # camera default
    assert len(first[1].payload) == 2048
    assert len(first[2].payload) == 64
    # deterministic per device across builds/runs, but distinct between devices
    assert [d.payload for d in first] == [d.payload for d in second]
    assert first[0].payload[:64] != first[1].payload[:64]


async def test_simulator_fires_devices_on_their_intervals() -> None:
    devices = build_devices(
        [
            config(name="fast", type="generic", interval_s=0.02, size_bytes=8),
            config(name="slow", type="smart_meter", interval_s=0.1),
        ]
    )
    calls: list[tuple[bytes, dict[str, Any]]] = []

    async def send(payload: bytes, metadata: dict[str, Any]) -> bool:
        calls.append((payload, metadata))
        return True

    report = await DeviceSimulator(devices, duration_s=0.4).run(send)

    assert report.messages_attempted == len(calls)
    assert report.messages_completed == report.messages_attempted
    assert report.per_device is not None
    # loose bounds: OS timer coarseness must not make this flaky
    assert 8 <= report.per_device["fast"]["attempted"] <= 25
    assert 2 <= report.per_device["slow"]["attempted"] <= 6
    assert report.requested_message_rate == 1 / 0.02 + 1 / 0.1

    fast_calls = [m for _, m in calls if m["device"] == "fast"]
    assert fast_calls and all(m["device_type"] == "generic" for m in fast_calls)
    fast_payloads = {p for p, m in calls if m["device"] == "fast"}
    assert fast_payloads == {devices[0].payload}


async def test_simulator_counts_failures_per_device() -> None:
    devices = build_devices([config(name="d", type="generic", interval_s=0.02, size_bytes=8)])
    outcomes = iter([True, False, True, False, True, False, True, False, True, False] * 5)

    async def send(payload: bytes, metadata: dict[str, Any]) -> bool:
        return next(outcomes)

    report = await DeviceSimulator(devices, duration_s=0.2).run(send)
    assert report.messages_attempted == report.messages_completed + report.messages_failed
    assert report.messages_failed > 0
