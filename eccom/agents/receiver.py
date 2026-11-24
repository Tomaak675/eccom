"""Receiver agent: serves requests and records handler-level events."""

from __future__ import annotations

import asyncio

from eccom.core.config import ScenarioConfig
from eccom.core.events import EventRecorder, EventType
from eccom.core.message import Message
from eccom.core.protocol import ProtocolAdapter

_REPLY_PAYLOAD = b""


class ReceiverAgent:
    def __init__(
        self,
        scenario: ScenarioConfig,
        adapter: ProtocolAdapter,
        recorder: EventRecorder,
    ) -> None:
        self._adapter = adapter
        self._recorder = recorder
        self._interaction = scenario.interaction
        self._delay_s = scenario.receiver.artificial_processing_delay_ms / 1000.0

    async def start(self) -> None:
        if self._interaction == "one_way":
            await self._adapter.listen(self._process)
        else:
            await self._adapter.listen_with_reply(self._handle_with_reply)

    async def _handle_with_reply(self, message: Message) -> bytes:
        await self._process(message)
        return _REPLY_PAYLOAD

    async def _process(self, message: Message) -> None:
        record = self._recorder.record
        record(EventType.HANDLER_STARTED, message.correlation_id, message.sequence_number)
        if self._delay_s > 0:
            await asyncio.sleep(self._delay_s)
        record(EventType.HANDLER_COMPLETED, message.correlation_id, message.sequence_number)
