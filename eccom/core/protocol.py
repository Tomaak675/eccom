"""Abstract protocol adapter interface.

A ProtocolAdapter maps the framework's logical operations (send, request/reply,
listen) onto one concrete communication technology. Not every protocol has to
support every operation: the default implementations raise
``UnsupportedOperationError`` so adapters only override what the protocol can
genuinely express (a later ProtocolCapabilities model can formalize this).
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable

from eccom.core.message import Message

# One-way message handler (no reply).
Handler = Callable[[Message], Awaitable[None]]
# Request handler returning the reply payload bytes.
ReplyHandler = Callable[[Message], Awaitable[bytes]]


class ProtocolError(Exception):
    """A protocol-level failure (connection refused, bad status, ...)."""


class ProtocolTimeoutError(ProtocolError):
    """No reply arrived within the requested timeout."""


class UnsupportedOperationError(ProtocolError):
    """The adapter does not support this interaction model."""


class ProtocolAdapter(ABC):
    """Async adapter between the framework and one communication protocol.

    Implementations transmit ``Message.payload`` unchanged and carry
    correlation metadata in protocol-native mechanisms.
    """

    @abstractmethod
    async def start(self) -> None:
        """Acquire connections/resources. Must be called before any operation."""

    @abstractmethod
    async def stop(self) -> None:
        """Release all resources (connections, servers, subscriptions)."""

    async def send(self, message: Message) -> None:
        """Send a one-way message; no application-level reply is expected."""
        raise UnsupportedOperationError(f"{type(self).__name__} does not support send()")

    async def send_and_wait_reply(self, message: Message, timeout_s: float) -> Message:
        """Send a request and wait for the correlated reply.

        Raises ProtocolTimeoutError if no reply arrives within ``timeout_s``.
        """
        raise UnsupportedOperationError(
            f"{type(self).__name__} does not support send_and_wait_reply()"
        )

    async def listen(self, handler: Handler) -> None:
        """Start receiving one-way messages and invoke ``handler`` for each."""
        raise UnsupportedOperationError(f"{type(self).__name__} does not support listen()")

    async def listen_with_reply(self, handler: ReplyHandler) -> None:
        """Start receiving requests; ``handler``'s return value is sent back
        as the correlated reply."""
        raise UnsupportedOperationError(
            f"{type(self).__name__} does not support listen_with_reply()"
        )
