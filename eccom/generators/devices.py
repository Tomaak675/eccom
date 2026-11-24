"""Simulated IoT devices firing messages on fixed intervals.

Each device's payload is built once at startup (hardcoded readings for the
sensor types, fixed seeded-random bytes for camera/generic), so firing costs
nothing at send time and repeated runs are reproducible. Devices fire open
loop — a slow network never delays the next fire, matching real sensors that
report on a schedule regardless of backend latency.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from eccom.core.config import DeviceConfig
from eccom.generators.load import LoadReport

logger = logging.getLogger(__name__)

_DEFAULT_CAMERA_SIZE_BYTES = 100_000

# Called per fire with the device payload and device metadata (attached to the
# PAYLOAD_CREATED event); returns True on success. Must not raise.
SendPayload = Callable[[bytes, dict[str, Any]], Awaitable[bool]]


@dataclass(frozen=True, slots=True)
class SimulatedDevice:
    name: str
    type: str
    interval_s: float
    payload: bytes


def build_devices(configs: list[DeviceConfig]) -> list[SimulatedDevice]:
    return [
        SimulatedDevice(
            name=config.name,
            type=config.type,
            interval_s=config.interval_s,
            payload=_build_payload(config),
        )
        for config in configs
    ]


def _build_payload(config: DeviceConfig) -> bytes:
    match config.type:
        case "smart_meter":
            # Hardcoded, realistic smart electricity meter reading.
            reading = {
                "device": config.name,
                "type": "smart_meter",
                "voltage_v": 230.1,
                "current_a": 5.42,
                "active_power_w": 1246.6,
                "energy_total_kwh": 10234.7,
            }
        case "temperature_sensor":
            reading = {
                "device": config.name,
                "type": "temperature_sensor",
                "temperature_c": 21.5,
                "humidity_pct": 48.2,
            }
        case "camera":
            size = config.size_bytes or _DEFAULT_CAMERA_SIZE_BYTES
            return _fixed_random_bytes(config, size)
        case "generic":
            assert config.size_bytes is not None  # enforced by config validation
            return _fixed_random_bytes(config, config.size_bytes)
        case _:  # pragma: no cover - Literal-typed config prevents this
            raise ValueError(f"unknown device type: {config.type}")
    return json.dumps(reading, separators=(",", ":")).encode()


def _fixed_random_bytes(config: DeviceConfig, size: int) -> bytes:
    # Seeding with a string derives a stable, per-device byte blob (string
    # seeding is platform- and run-independent, unlike hash()).
    return random.Random(f"{config.seed}:{config.name}").randbytes(size)


class DeviceSimulator:
    """Runs one firing schedule per device for a fixed duration."""

    def __init__(self, devices: list[SimulatedDevice], duration_s: float) -> None:
        self._devices = devices
        self._duration_s = duration_s

    async def run(self, send_payload: SendPayload) -> LoadReport:
        start = time.perf_counter()
        deadline = start + self._duration_s
        fires: list[tuple[str, asyncio.Task[bool]]] = []

        schedules = [
            asyncio.create_task(self._device_loop(index, device, start, deadline, send_payload, fires))
            for index, device in enumerate(self._devices)
        ]
        await asyncio.gather(*schedules)
        send_window_s = max(time.perf_counter() - start, 1e-9)
        results = await asyncio.gather(*(task for _, task in fires))  # drain in-flight sends
        total_duration_s = time.perf_counter() - start

        per_device = {device.name: {"attempted": 0, "completed": 0} for device in self._devices}
        for (name, _), ok in zip(fires, results):
            per_device[name]["attempted"] += 1
            per_device[name]["completed"] += int(ok)

        attempted = len(fires)
        completed = sum(1 for ok in results if ok)
        return LoadReport(
            requested_message_rate=sum(1 / device.interval_s for device in self._devices),
            actual_message_rate=attempted / send_window_s,
            messages_attempted=attempted,
            messages_completed=completed,
            messages_failed=attempted - completed,
            send_window_s=send_window_s,
            total_duration_s=total_duration_s,
            per_device=per_device,
        )

    async def _device_loop(
        self,
        index: int,
        device: SimulatedDevice,
        start: float,
        deadline: float,
        send_payload: SendPayload,
        fires: list[tuple[str, asyncio.Task[bool]]],
    ) -> None:
        # Deterministic stagger so same-interval devices don't all fire in one
        # synchronized burst at t=0.
        next_fire = start + (index / max(len(self._devices), 1)) * device.interval_s
        metadata = {"device": device.name, "device_type": device.type}
        while next_fire < deadline:
            delay = next_fire - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            fires.append(
                (device.name, asyncio.create_task(self._fire(send_payload, device, metadata)))
            )
            next_fire += device.interval_s

    @staticmethod
    async def _fire(
        send_payload: SendPayload, device: SimulatedDevice, metadata: dict[str, Any]
    ) -> bool:
        try:
            return await send_payload(device.payload, metadata)
        except Exception:
            logger.exception("device %s fire raised unexpectedly", device.name)
            return False
