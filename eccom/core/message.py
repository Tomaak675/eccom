"""Logical test message model shared by all protocol adapters."""

from __future__ import annotations

from dataclasses import dataclass
from uuid import uuid4


def new_correlation_id() -> str:
    """Return a globally unique correlation id (32 hex chars, no dashes)."""
    return uuid4().hex


@dataclass(frozen=True, slots=True)
class Message:
    """A single logical test message.

    ``payload`` is opaque to the framework: adapters must transmit it unchanged
    and carry ``correlation_id`` / ``sequence_number`` in protocol-native
    metadata (HTTP headers, MQTT v5 properties, ...), never by rewriting the
    payload bytes.
    """

    correlation_id: str
    sequence_number: int
    payload: bytes
