"""Protocol adapter registry."""

from __future__ import annotations

from pathlib import Path

from eccom.core.config import ProtocolConfig
from eccom.core.events import EventRecorder
from eccom.core.protocol import ProtocolAdapter
from eccom.protocols.http import HttpAdapter
from eccom.protocols.mqtt import MqttAdapter


def create_adapter(
    config: ProtocolConfig,
    recorder: EventRecorder,
    base_dir: Path = Path("."),
) -> ProtocolAdapter:
    """``base_dir`` is the scenario file's directory; TLS file paths are
    resolved against it."""
    match config.type:
        case "http":
            return HttpAdapter(
                config.http, config.tls, base_dir, recorder, tcp_nodelay=config.tcp_nodelay
            )
        case "mqtt":
            return MqttAdapter(
                config.mqtt, config.tls, base_dir, recorder, tcp_nodelay=config.tcp_nodelay
            )
    raise ValueError(f"unknown protocol type: {config.type}")
