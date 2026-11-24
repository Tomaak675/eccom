"""SSL context construction shared by all protocol adapters.

TLS is configuration over the existing adapters, never a separate protocol
implementation; reports may still label runs HTTPS / MQTT+TLS via the
scenario name.
"""

from __future__ import annotations

import ssl
from pathlib import Path

from eccom.core.config import TlsConfig


def client_ssl_context(tls: TlsConfig, base_dir: Path) -> ssl.SSLContext:
    """Client-side context. With ca_file: verify the server chain against it
    (hostname check off). Without: encrypt but don't verify — the handshake
    cost being measured is identical."""
    if tls.ca_file is not None:
        context = ssl.create_default_context(cafile=str(base_dir / tls.ca_file))
        context.check_hostname = False
    else:
        context = ssl.create_default_context()
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
    return context


def server_ssl_context(tls: TlsConfig, base_dir: Path) -> ssl.SSLContext:
    assert tls.cert_file is not None and tls.key_file is not None  # config-validated
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(
        certfile=str(base_dir / tls.cert_file), keyfile=str(base_dir / tls.key_file)
    )
    return context
