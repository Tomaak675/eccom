"""Sender agent: generates traffic and records interaction-level events.

Protocol-agnostic — everything protocol-specific is behind the adapter.
Supports three traffic models: a paced stream at a constant or ramped rate
(LoadGenerator), the same schedule carrying the sensor mix of simulated
smart homes (LoadGenerator + HomeTraffic), and simulated IoT devices firing
on their own intervals (DeviceSimulator).
"""

from __future__ import annotations

import itertools
import logging
from collections.abc import Iterator
from typing import Any

from eccom.core.config import ScenarioConfig
from eccom.core.events import EventRecorder, EventType
from eccom.core.message import Message, new_correlation_id
from eccom.core.protocol import ProtocolAdapter, ProtocolTimeoutError
from eccom.generators.devices import DeviceSimulator, build_devices
from eccom.generators.home import HomeStream, HomeTraffic
from eccom.generators.load import LoadGenerator, LoadReport
from eccom.generators.payload import PayloadGenerator

logger = logging.getLogger(__name__)


class SenderAgent:
    def __init__(
        self,
        scenario: ScenarioConfig,
        adapter: ProtocolAdapter,
        recorder: EventRecorder,
        instance: int = 0,
    ) -> None:
        self._scenario = scenario
        self._adapter = adapter
        self._recorder = recorder
        self._timeout_s = scenario.protocol.request_timeout_s
        self._one_way = scenario.interaction == "one_way"
        # With load.senders > 1 this process generates only its share of the
        # schedule; the instances' shares add up to the scenario's load.
        self._load = scenario.load.for_instance(instance)
        # Sequence numbers keep increasing across warmup and measurement;
        # phases are separated by the recorder's phase tag.
        self._sequence = itertools.count()
        self._devices = None
        self._home = None
        self._payloads = None
        if scenario.load.mode == "devices":
            assert scenario.devices is not None  # enforced by config validation
            self._devices = build_devices(scenario.devices)
        elif scenario.load.mode == "homes":
            assert scenario.home is not None  # enforced by config validation
            self._home = HomeTraffic(scenario.home, self._load.aggregation_factor)
            self._home_step: dict[str, Any] | None = None
            self._home_order: Iterator[HomeStream] = iter(())
        else:
            assert scenario.payload is not None  # enforced by config validation
            payload = scenario.payload
            if instance:
                # distinct but still reproducible byte streams per instance
                payload = payload.model_copy(update={"seed": payload.seed + instance})
            self._payloads = PayloadGenerator(payload)

    async def run(self) -> LoadReport:
        """Run warmup (if configured) and the measurement traffic; return the
        measurement-phase load report."""
        warmup_s = self._scenario.experiment.warmup_s
        if warmup_s > 0:
            logger.info("warmup for %.1fs", warmup_s)
            self._recorder.set_phase("warmup")
            await self._run_phase(duration_override_s=warmup_s)

        logger.info("measurement phase started")
        self._recorder.set_phase("measurement")
        report = await self._run_phase(duration_override_s=None)
        logger.info(
            "measurement phase done: attempted=%d completed=%d failed=%d "
            "requested_rate=%.1f/s actual_rate=%.1f/s",
            report.messages_attempted,
            report.messages_completed,
            report.messages_failed,
            report.requested_message_rate,
            report.actual_message_rate,
        )
        return report

    async def _run_phase(self, duration_override_s: float | None) -> LoadReport:
        load = self._load
        if self._devices is not None:
            duration_s = duration_override_s if duration_override_s is not None else load.duration_s
            assert duration_s is not None  # enforced by config validation
            return await DeviceSimulator(self._devices, duration_s).run(self._send_payload)
        if duration_override_s is not None:
            load = load.model_copy(
                update={"duration_s": duration_override_s, "total_messages": None}
            )
        if self._home is not None:
            return await LoadGenerator(load, home_traffic=self._home).run(self._send_home)
        assert self._scenario.payload is not None  # enforced by config validation
        generator = LoadGenerator(load, payload_size_bytes=self._scenario.payload.size_bytes)
        return await generator.run(self._send_uniform)

    async def _send_uniform(self, extra_metadata: dict[str, Any]) -> bool:
        assert self._payloads is not None
        payload = self._payloads.generate(extra_metadata.get("target_size"))
        return await self._send_payload(payload, extra_metadata)

    async def _send_home(self, extra_metadata: dict[str, Any]) -> bool:
        """The next message of the home mix: the generator shares one tag dict
        per step, so a new dict means a new step (or a new phase) whose
        number of homes rebuilds the stream set."""
        assert self._home is not None
        if extra_metadata is not self._home_step:
            self._home_step = extra_metadata
            self._home_order = self._home.fire_order(extra_metadata["target_homes"])
        stream = next(self._home_order)
        return await self._send_payload(stream.payload, {**extra_metadata, "sensor": stream.type})

    async def _send_payload(self, payload: bytes, extra_metadata: dict[str, Any]) -> bool:
        message = Message(new_correlation_id(), next(self._sequence), payload)
        record = self._recorder.record
        record(
            EventType.PAYLOAD_CREATED,
            message.correlation_id,
            message.sequence_number,
            metadata={"size_bytes": len(payload), **extra_metadata},
        )
        record(EventType.SEND_STARTED, message.correlation_id, message.sequence_number)
        try:
            if self._one_way:
                # SEND_COMPLETED is recorded by the adapter (wire boundary).
                await self._adapter.send(message)
                return True
            reply = await self._adapter.send_and_wait_reply(message, self._timeout_s)
        except ProtocolTimeoutError:
            record(EventType.TIMEOUT, message.correlation_id, message.sequence_number)
            return False
        except Exception as exc:
            record(
                EventType.ERROR,
                message.correlation_id,
                message.sequence_number,
                metadata={"error": repr(exc)},
            )
            return False
        record(
            EventType.REPLY_RECEIVED,
            message.correlation_id,
            message.sequence_number,
            metadata={"size_bytes": len(reply.payload)},
        )
        return True
