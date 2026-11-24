"""HTTP protocol adapter (aiohttp client + server).

Correlation metadata travels in HTTP headers (X-Correlation-ID /
X-Sequence-Number); payload bytes are the request body, transmitted unchanged.

aiohttp was chosen over FastAPI + httpx deliberately: one dependency covers
client and server, and both sides have lower per-request overhead (no ASGI
validation layer, faster client) — relevant because the Python sender is
expected to be the throughput bottleneck.

The adapter records only wire-boundary events (MESSAGE_RECEIVED,
REPLY_SEND_STARTED, and SEND_COMPLETED for one-way sends once the server's
204 arrives); interaction-level events are recorded by the agents so they
stay protocol-agnostic. REPLY_SEND_COMPLETED is not recorded — aiohttp does
not expose "response fully written" cheaply; client tracing hooks are the
extension point for that.

protocol.tcp_nodelay: asyncio sets TCP_NODELAY on every transport it creates,
so the default (true) costs nothing here. false puts Nagle's algorithm back on
for the client's connections and the server's accepted sockets, so HTTP can be
measured under the TCP behaviour paho and mosquitto default to.
"""

from __future__ import annotations

import asyncio
import logging
import socket
from pathlib import Path
from typing import Any

import aiohttp
from aiohttp import web

from eccom.core.config import HttpConfig, TlsConfig
from eccom.core.events import EventRecorder, EventType
from eccom.core.tls import client_ssl_context, server_ssl_context
from eccom.core.message import Message
from eccom.core.protocol import (
    Handler,
    ProtocolAdapter,
    ProtocolError,
    ProtocolTimeoutError,
    ReplyHandler,
)

logger = logging.getLogger(__name__)

CORRELATION_HEADER = "X-Correlation-ID"
SEQUENCE_HEADER = "X-Sequence-Number"
MESSAGES_PATH = "/messages"
_MAX_REQUEST_BODY_BYTES = 64 * 1024 * 1024


def _keep_nagle(transport: asyncio.BaseTransport) -> None:
    """Undo the TCP_NODELAY asyncio sets on every transport it creates."""
    sock = transport.get_extra_info("socket")
    if sock is not None:
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 0)


async def _keep_nagle_for_response(request: web.Request, response: web.StreamResponse) -> None:
    if request.transport is not None:
        _keep_nagle(request.transport)


class _NagleConnector(aiohttp.TCPConnector):
    """TCPConnector whose connections keep Nagle's algorithm on.

    Every outgoing aiohttp connection passes through _wrap_create_connection
    (a private hook, guarded by the tcp_nodelay integration test); it is the
    first point where the transport, and with it the socket asyncio has just
    set TCP_NODELAY on, exists.
    """

    async def _wrap_create_connection(
        self, *args: Any, **kwargs: Any
    ) -> tuple[asyncio.Transport, Any]:
        transport, protocol = await super()._wrap_create_connection(*args, **kwargs)
        _keep_nagle(transport)
        return transport, protocol


class HttpAdapter(ProtocolAdapter):
    """One adapter class serves both roles: the sender side uses the client
    session, the receiver side starts the server via listen*()."""

    def __init__(
        self,
        config: HttpConfig,
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
        self._tcp_nodelay = tcp_nodelay
        scheme = "https" if tls.enabled else "http"
        self._url = f"{scheme}://{config.endpoint.host}:{config.endpoint.port}{MESSAGES_PATH}"
        self._session: aiohttp.ClientSession | None = None
        self._server_runner: web.AppRunner | None = None
        self._reply_handler: ReplyHandler | None = None
        self._oneway_handler: Handler | None = None
        self._handler_tasks: set[asyncio.Task[None]] = set()

    async def start(self) -> None:
        # The pool cap is the scenario's max_connections; 0 (the default)
        # disables aiohttp's own cap, so in-flight concurrency is governed
        # solely by the load generator and a backlog opens new connections.
        connector_class = aiohttp.TCPConnector if self._tcp_nodelay else _NagleConnector
        connector = connector_class(
            limit=self._config.client.max_connections,
            force_close=not self._config.client.keep_alive,
            ssl=client_ssl_context(self._tls, self._base_dir) if self._tls.enabled else None,
        )
        self._session = aiohttp.ClientSession(connector=connector)

    async def stop(self) -> None:
        for task in list(self._handler_tasks):
            task.cancel()
        if self._session is not None:
            await self._session.close()
            self._session = None
        if self._server_runner is not None:
            await self._server_runner.cleanup()
            self._server_runner = None

    # --- sender side -----------------------------------------------------

    async def send(self, message: Message) -> None:
        session = self._require_session()
        try:
            async with session.post(
                self._url,
                data=message.payload,
                headers=self._headers(message),
                timeout=aiohttp.ClientTimeout(total=self._config.client.timeout_s),
            ) as response:
                await response.read()
                if response.status >= 300:
                    raise ProtocolError(f"unexpected HTTP status {response.status}")
        except (TimeoutError, asyncio.TimeoutError) as exc:
            raise ProtocolTimeoutError(
                f"send timed out after {self._config.client.timeout_s}s"
            ) from exc
        except aiohttp.ClientError as exc:
            raise ProtocolError(f"HTTP send failed: {exc!r}") from exc
        # For one-way HTTP, "send completed" means the server's 204 arrived
        # (transport-level acknowledgement; the handler runs asynchronously).
        self._recorder.record(
            EventType.SEND_COMPLETED, message.correlation_id, message.sequence_number
        )

    async def send_and_wait_reply(self, message: Message, timeout_s: float) -> Message:
        session = self._require_session()
        try:
            async with session.post(
                self._url,
                data=message.payload,
                headers=self._headers(message),
                timeout=aiohttp.ClientTimeout(total=timeout_s),
            ) as response:
                body = await response.read()
                if response.status != 200:
                    raise ProtocolError(f"unexpected HTTP status {response.status}")
                # HTTP correlates request and response on the connection level,
                # so the original correlation_id is authoritative; the echoed
                # header exists for wire-level completeness.
                return Message(message.correlation_id, message.sequence_number, body)
        except (TimeoutError, asyncio.TimeoutError) as exc:
            raise ProtocolTimeoutError(f"no reply within {timeout_s}s") from exc
        except aiohttp.ClientError as exc:
            raise ProtocolError(f"HTTP request failed: {exc!r}") from exc

    @staticmethod
    def _headers(message: Message) -> dict[str, str]:
        return {
            CORRELATION_HEADER: message.correlation_id,
            SEQUENCE_HEADER: str(message.sequence_number),
        }

    def _require_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            raise ProtocolError("adapter not started")
        return self._session

    # --- receiver side ----------------------------------------------------

    async def listen(self, handler: Handler) -> None:
        self._oneway_handler = handler
        await self._start_server()

    async def listen_with_reply(self, handler: ReplyHandler) -> None:
        self._reply_handler = handler
        await self._start_server()

    async def _start_server(self) -> None:
        if self._server_runner is not None:
            raise ProtocolError("adapter is already listening")
        app = web.Application(client_max_size=_MAX_REQUEST_BODY_BYTES)
        app.router.add_post(MESSAGES_PATH, self._handle_request)
        if not self._tcp_nodelay:
            # asyncio turns Nagle off on accepted sockets as well. The first
            # response on a connection is the earliest public hook to turn it
            # back on, and nothing is written on the connection before it.
            app.on_response_prepare.append(_keep_nagle_for_response)
        # access_log=None keeps per-request logging off the measured path.
        self._server_runner = web.AppRunner(app, access_log=None)
        await self._server_runner.setup()
        bind_host = self._config.server.bind_host
        port = self._config.endpoint.port
        ssl_context = (
            server_ssl_context(self._tls, self._base_dir) if self._tls.enabled else None
        )
        site = web.TCPSite(self._server_runner, bind_host, port, ssl_context=ssl_context)
        await site.start()
        logger.info(
            "HTTP%s server listening on %s:%d", "S" if self._tls.enabled else "", bind_host, port
        )

    async def _handle_request(self, request: web.Request) -> web.StreamResponse:
        correlation_id = request.headers.get(CORRELATION_HEADER)
        if correlation_id is None:
            return web.Response(status=400, text=f"missing {CORRELATION_HEADER} header")
        sequence_number = int(request.headers.get(SEQUENCE_HEADER, "-1"))
        payload = await request.read()
        self._recorder.record(
            EventType.MESSAGE_RECEIVED,
            correlation_id,
            sequence_number,
            metadata={"size_bytes": len(payload)},
        )
        message = Message(correlation_id, sequence_number, payload)
        if self._reply_handler is not None:
            reply = await self._reply_handler(message)
            self._recorder.record(
                EventType.REPLY_SEND_STARTED,
                correlation_id,
                sequence_number,
                metadata={"size_bytes": len(reply)},
            )
            return web.Response(body=reply, headers={CORRELATION_HEADER: correlation_id})
        assert self._oneway_handler is not None
        # One-way semantics: acknowledge receipt immediately, process
        # asynchronously (mirrors MQTT, where the broker acks and the
        # subscriber processes independently).
        task = asyncio.create_task(self._oneway_handler(message))
        self._handler_tasks.add(task)
        task.add_done_callback(self._handler_tasks.discard)
        return web.Response(status=204)
