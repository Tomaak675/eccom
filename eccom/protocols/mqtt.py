"""MQTT v5 protocol adapter (aiomqtt / paho).

Correlation metadata travels in MQTT v5 packet properties — Correlation Data
carries the correlation id, Response Topic routes the reply, and a user
property carries the sequence number — so payload bytes are transmitted
unchanged, exactly like the HTTP adapter does with headers.

Topics: requests flow on ``<base_topic>/requests``; each adapter instance
owns a unique reply topic ``<base_topic>/replies/<client-id>``. Both agents
connect to the same broker; there is no server socket on the receiver.

Events: besides the shared wire-boundary events (MESSAGE_RECEIVED,
REPLY_SEND_STARTED), this adapter records SEND_COMPLETED when the broker has
acknowledged the publish (PUBACK/PUBCOMP for QoS 1/2; for QoS 0 it merely
marks the publish call returning).

Connection lifetime: normally one connection is opened in start() and held
for the whole run. With client.reconnect_per_message the sender instead
connects, publishes and disconnects per message, so SEND_COMPLETED prices
session setup too — the MQTT counterpart of HTTP without keep-alive, and the
only mode in which client.clean_start is observable. Receivers keep their
persistent connection in either mode.

protocol.tcp_nodelay decides whether the socket runs with Nagle's algorithm
off (TCP_NODELAY, as asyncio's transports do for the HTTP adapter) or on,
which is paho's own default. See _TCP_NODELAY.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import socket
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

import aiomqtt
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from eccom.core.config import MqttConfig, TlsConfig
from eccom.core.events import EventRecorder, EventType
from eccom.core.tls import client_ssl_context
from eccom.core.message import Message
from eccom.core.protocol import (
    Handler,
    ProtocolAdapter,
    ProtocolError,
    ProtocolTimeoutError,
    ReplyHandler,
)

logger = logging.getLogger(__name__)

SEQUENCE_PROPERTY = "sequence_number"
_CONNECT_TIMEOUT_S = 15.0  # covers a managed broker container still starting up
# paho leaves Nagle's algorithm on: once publishes are spaced closer than the
# acknowledgement time, a small write waits (up to one round trip) for the
# previous one's ACK. asyncio sets TCP_NODELAY on every transport it creates,
# so the HTTP adapter never pays this unless asked to; protocol.tcp_nodelay
# picks the behaviour for both adapters and for the managed broker.
_TCP_NODELAY = ((socket.IPPROTO_TCP, socket.TCP_NODELAY, 1),)


class MqttAdapter(ProtocolAdapter):
    def __init__(
        self,
        config: MqttConfig,
        tls: TlsConfig,
        base_dir: Path,
        recorder: EventRecorder,
        *,
        tcp_nodelay: bool,
    ) -> None:
        self._config = config
        self._tls = tls
        self._base_dir = base_dir
        self._recorder = recorder
        self._socket_options = _TCP_NODELAY if tcp_nodelay else ()
        self._client_id = f"eccom-{uuid4().hex[:12]}"
        self._request_topic = f"{config.base_topic}/requests"
        self._reply_topic = f"{config.base_topic}/replies/{self._client_id}"
        self._client: aiomqtt.Client | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._pending: dict[bytes, asyncio.Future[Message]] = {}
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._reply_handler: ReplyHandler | None = None
        self._oneway_handler: Handler | None = None
        self._stopping = False

    async def start(self) -> None:
        # A reconnect_per_message sender holds no connection between messages,
        # so there is nothing to open here; a receiver with the same scenario
        # still needs a persistent one and opens it in _subscribe_requests().
        if self._config.client.reconnect_per_message:
            return
        client = await self._connect(_CONNECT_TIMEOUT_S)
        self._client = client
        # Subscribing the own reply topic eagerly makes send_and_wait_reply
        # race-free; unused reply topics carry no traffic.
        await client.subscribe(self._reply_topic, qos=self._config.client.qos)
        self._reader_task = asyncio.create_task(self._read_loop())

    async def _connect(self, timeout_s: float) -> aiomqtt.Client:
        """Open one broker connection, retrying until ``timeout_s`` elapses."""
        deadline = time.perf_counter() + timeout_s
        while True:
            client = aiomqtt.Client(
                hostname=self._config.endpoint.host,
                port=self._config.endpoint.port,
                identifier=self._client_id,
                protocol=aiomqtt.ProtocolVersion.V5,
                clean_start=self._config.client.clean_start,
                keepalive=self._config.client.keepalive_s,
                max_inflight_messages=self._config.client.max_inflight_messages,
                socket_options=self._socket_options,
                tls_context=(
                    client_ssl_context(self._tls, self._base_dir)
                    if self._tls.enabled
                    else None
                ),
            )
            try:
                await client.__aenter__()
                return client
            except (aiomqtt.MqttError, OSError) as exc:
                if time.perf_counter() >= deadline:
                    raise ProtocolError(
                        f"cannot connect to MQTT broker at "
                        f"{self._config.endpoint.host}:{self._config.endpoint.port}: {exc!r}"
                    ) from exc
                await asyncio.sleep(0.5)

    async def stop(self) -> None:
        self._stopping = True
        for task in list(self._handler_tasks):
            task.cancel()
        if self._reader_task is not None:
            self._reader_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._reader_task
            self._reader_task = None
        if self._client is not None:
            with contextlib.suppress(aiomqtt.MqttError):
                await self._client.__aexit__(None, None, None)
            self._client = None

    # --- sender side -----------------------------------------------------

    async def send(self, message: Message) -> None:
        await self._publish_request(message, response_topic=None)

    async def send_and_wait_reply(self, message: Message, timeout_s: float) -> Message:
        correlation = message.correlation_id.encode()
        future: asyncio.Future[Message] = asyncio.get_running_loop().create_future()
        self._pending[correlation] = future
        # The publish runs as its own task so RTT is never floored at the
        # broker's PUBACK time (for QoS 1/2 the ack can arrive after the
        # reply); _publish_request bounds itself with the client timeout.
        publish = asyncio.create_task(
            self._publish_request(message, response_topic=self._reply_topic)
        )
        self._track_task(publish)
        try:
            return await asyncio.wait_for(future, timeout_s)
        except TimeoutError as exc:
            if publish.done() and (publish_error := publish.exception()) is not None:
                raise publish_error
            raise ProtocolTimeoutError(f"no reply within {timeout_s}s") from exc
        finally:
            self._pending.pop(correlation, None)

    async def _publish_request(self, message: Message, response_topic: str | None) -> None:
        if not self._config.client.reconnect_per_message:
            await self._publish(self._require_client(), message, response_topic)
            return
        # The connect deliberately sits inside the measured SEND_STARTED ->
        # SEND_COMPLETED interval: pricing session setup is the point of this
        # mode. The disconnect happens after SEND_COMPLETED, so teardown is
        # excluded from the latency.
        client = await self._connect(self._config.client.timeout_s)
        try:
            await self._publish(client, message, response_topic)
        finally:
            with contextlib.suppress(aiomqtt.MqttError):
                await client.__aexit__(None, None, None)

    async def _publish(
        self, client: aiomqtt.Client, message: Message, response_topic: str | None
    ) -> None:
        properties = Properties(PacketTypes.PUBLISH)
        properties.CorrelationData = message.correlation_id.encode()
        properties.UserProperty = [(SEQUENCE_PROPERTY, str(message.sequence_number))]
        if response_topic is not None:
            properties.ResponseTopic = response_topic
        qos = self._config.client.qos
        try:
            await asyncio.wait_for(
                client.publish(
                    self._request_topic, payload=message.payload, qos=qos, properties=properties
                ),
                timeout=self._config.client.timeout_s,
            )
        except TimeoutError as exc:
            raise ProtocolTimeoutError(
                f"broker did not acknowledge publish within {self._config.client.timeout_s}s"
            ) from exc
        except aiomqtt.MqttError as exc:
            raise ProtocolError(f"MQTT publish failed: {exc!r}") from exc
        # For QoS 1/2 the broker has acknowledged the message at this point.
        self._recorder.record(
            EventType.SEND_COMPLETED,
            message.correlation_id,
            message.sequence_number,
            metadata={"qos": qos},
        )

    # --- receiver side ----------------------------------------------------

    async def listen(self, handler: Handler) -> None:
        self._oneway_handler = handler
        await self._subscribe_requests()

    async def listen_with_reply(self, handler: ReplyHandler) -> None:
        self._reply_handler = handler
        await self._subscribe_requests()

    async def _subscribe_requests(self) -> None:
        if self._reply_handler is not None and self._oneway_handler is not None:
            raise ProtocolError("adapter is already listening")
        if self._client is None:
            # reconnect_per_message: start() opened nothing, but a listener
            # needs a standing subscription.
            self._client = await self._connect(_CONNECT_TIMEOUT_S)
            self._reader_task = asyncio.create_task(self._read_loop())
        await self._client.subscribe(self._request_topic, qos=self._config.client.qos)

    # --- message routing ----------------------------------------------------

    async def _read_loop(self) -> None:
        client = self._require_client()
        try:
            async for mqtt_message in client.messages:
                self._dispatch(mqtt_message)
        except aiomqtt.MqttError as exc:
            if not self._stopping:
                logger.warning("MQTT connection lost: %r", exc)

    def _dispatch(self, mqtt_message: aiomqtt.Message) -> None:
        topic = str(mqtt_message.topic)
        if topic == self._reply_topic:
            self._resolve_reply(mqtt_message)
        elif topic == self._request_topic:
            # One task per request so slow handlers don't serialize the
            # receiver (mirrors the HTTP server's concurrency).
            self._track_task(asyncio.create_task(self._handle_request(mqtt_message)))
        else:
            logger.warning("message on unexpected topic %s", topic)

    def _resolve_reply(self, mqtt_message: aiomqtt.Message) -> None:
        correlation = _correlation_data(mqtt_message)
        if correlation is None:
            logger.warning("reply without correlation data ignored")
            return
        future = self._pending.get(correlation)
        if future is None or future.done():
            logger.debug("uncorrelated/late reply for %s ignored", correlation)
            return
        future.set_result(
            Message(
                correlation_id=correlation.decode(),
                sequence_number=_sequence_number(mqtt_message),
                payload=bytes(mqtt_message.payload),  # type: ignore[arg-type]
            )
        )

    async def _handle_request(self, mqtt_message: aiomqtt.Message) -> None:
        correlation = _correlation_data(mqtt_message)
        if correlation is None:
            logger.warning("request without correlation data ignored")
            return
        correlation_id = correlation.decode()
        sequence_number = _sequence_number(mqtt_message)
        payload = bytes(mqtt_message.payload)  # type: ignore[arg-type]
        self._recorder.record(
            EventType.MESSAGE_RECEIVED,
            correlation_id,
            sequence_number,
            metadata={"size_bytes": len(payload)},
        )
        message = Message(correlation_id, sequence_number, payload)
        if self._reply_handler is not None:
            reply = await self._reply_handler(message)
            response_topic = getattr(mqtt_message.properties, "ResponseTopic", None)
            if response_topic is None:
                logger.warning("request %s has no response topic; dropping reply", correlation_id)
                return
            self._recorder.record(
                EventType.REPLY_SEND_STARTED,
                correlation_id,
                sequence_number,
                metadata={"size_bytes": len(reply)},
            )
            properties = Properties(PacketTypes.PUBLISH)
            properties.CorrelationData = correlation
            properties.UserProperty = [(SEQUENCE_PROPERTY, str(sequence_number))]
            try:
                await self._require_client().publish(
                    response_topic,
                    payload=reply,
                    qos=self._config.client.qos,
                    properties=properties,
                )
            except aiomqtt.MqttError as exc:
                logger.warning("publishing reply for %s failed: %r", correlation_id, exc)
        elif self._oneway_handler is not None:
            await self._oneway_handler(message)

    def _require_client(self) -> aiomqtt.Client:
        if self._client is None:
            raise ProtocolError("adapter not started")
        return self._client

    def _track_task(self, task: asyncio.Task[Any]) -> None:
        """Keep a background task referenced; log (don't drop) its failure."""
        self._handler_tasks.add(task)

        def _done(finished: asyncio.Task[Any]) -> None:
            self._handler_tasks.discard(finished)
            if not finished.cancelled() and finished.exception() is not None:
                logger.warning("background MQTT task failed: %r", finished.exception())

        task.add_done_callback(_done)


def _correlation_data(mqtt_message: aiomqtt.Message) -> bytes | None:
    return getattr(mqtt_message.properties, "CorrelationData", None)


def _sequence_number(mqtt_message: aiomqtt.Message) -> int:
    user_properties: list[tuple[str, str]] = getattr(
        mqtt_message.properties, "UserProperty", []
    )
    for key, value in user_properties:
        if key == SEQUENCE_PROPERTY:
            return int(value)
    return -1
