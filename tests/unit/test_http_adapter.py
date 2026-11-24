"""The HTTP adapter's client-side connection pool follows the scenario."""

from pathlib import Path

from eccom.core.config import HttpConfig, TlsConfig
from eccom.core.events import EventRecorder
from eccom.protocols.http import HttpAdapter


async def test_pool_cap_reaches_the_connector(tmp_path: Path) -> None:
    recorder = EventRecorder("edge", tmp_path / "edge-events.jsonl")
    try:
        for cap in (0, 100):
            adapter = HttpAdapter(
                HttpConfig(client={"max_connections": cap}), TlsConfig(), tmp_path, recorder, tcp_nodelay=True
            )
            await adapter.start()
            assert adapter._session is not None
            assert adapter._session.connector.limit == cap
            await adapter.stop()
    finally:
        recorder.close()
