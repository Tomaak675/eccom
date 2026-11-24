"""Link-capacity probe: an iperf3 measurement between the sender's and the
receiver's host before the agents start.

What the path can carry, independent of the protocol under test: a one-off
iperf3 server on the receiver's host and a client on the sender's host, a few
seconds in each direction, on the scenario's own endpoint port -- the one port
known to be open between the hosts, and still free until the receiver (or the
broker) binds it. SSH hosts run iperf3 from the agent image with
``--network host``; the local deployment runs the ``iperf3`` binary on PATH.
The record lands in ``<run-dir>/<sender>-link-capacity.json``; a failed probe
is recorded, never fatal.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import Any

from eccom.orchestration.deployment import Deployment

logger = logging.getLogger(__name__)

_CONNECT_ATTEMPTS = 5
_SERVER_GRACE_S = 15.0


class LinkCapacityProbe:
    """One iperf3 round: sender host -> receiver host, then the reverse."""

    retry_delay_s = 1.0  # pause before each client attempt, so the server is up

    def __init__(
        self,
        server: Deployment,
        client: Deployment,
        server_address: str,
        port: int,
        duration_s: float,
        run_id: str,
    ) -> None:
        self._server = server
        self._client = client
        self._server_address = server_address
        self._port = port
        self._duration_s = duration_s
        self._run_id = run_id

    async def measure(self) -> dict[str, Any]:
        """Measure both directions in turn.

        Returns: the record to store: method, port, duration and, per
        direction, either the parsed figures with the raw iperf3 document or
        an ``error`` string.
        """
        record: dict[str, Any] = {
            "method": "iperf3",
            "port": self._port,
            "duration_s": self._duration_s,
            "measured_at_wall_ns": time.time_ns(),
            "directions": {},
        }
        for name, reverse in (("client_to_server", False), ("server_to_client", True)):
            try:
                record["directions"][name] = await self._one_direction(reverse)
            except Exception as exc:
                logger.warning("link capacity probe (%s) failed: %s", name, exc)
                record["directions"][name] = {"error": str(exc)}
        return record

    async def _one_direction(self, reverse: bool) -> dict[str, Any]:
        server_name = f"eccom-iperf-server-{self._run_id}"
        if self._server.container_image() is not None:
            await self._server.run_command(["docker", "rm", "-f", server_name])
        server = asyncio.create_task(
            self._server.run_command(
                _wrap(self._server, server_name, ["iperf3", "-s", "-1", "-p", str(self._port)])
            )
        )
        try:
            output = await self._run_client(reverse)
        finally:
            await self._finish_server(server, server_name)
        parsed = parse_iperf3(output)
        parsed["reverse"] = reverse
        return parsed

    async def _run_client(self, reverse: bool) -> str:
        args = [
            "iperf3", "-c", self._server_address, "-p", str(self._port),
            "-t", f"{self._duration_s:g}", "-J",
        ]
        if reverse:
            args.append("-R")
        client_name = f"eccom-iperf-client-{self._run_id}"
        code, output = 1, ""
        for _ in range(_CONNECT_ATTEMPTS):
            await asyncio.sleep(self.retry_delay_s)
            code, output = await self._client.run_command(_wrap(self._client, client_name, args))
            if code == 0:
                return output
            if "unable to connect" not in output:
                break
        raise RuntimeError(output.strip()[-400:] or f"iperf3 exited with {code}")

    async def _finish_server(self, server: asyncio.Task[tuple[int, str]], name: str) -> None:
        try:
            await asyncio.wait_for(server, timeout=_SERVER_GRACE_S)
        except TimeoutError:
            if self._server.container_image() is not None:
                await self._server.run_command(["docker", "rm", "-f", name])
        except Exception:
            logger.debug("iperf3 server command failed", exc_info=True)


def _wrap(deployment: Deployment, name: str, args: list[str]) -> list[str]:
    """``args`` as the host runs them: inside the agent image on container
    hosts, bare otherwise."""
    image = deployment.container_image()
    if image is None:
        return args
    return [
        "docker", "run", "--rm", "--network", "host", "--name", name,
        "--entrypoint", args[0], image, *args[1:],
    ]


def parse_iperf3(output: str) -> dict[str, Any]:
    """The figures of one iperf3 ``-J`` run.

    output: the client's combined stdout/stderr.
    Returns: ``mbit_s`` (goodput at the receiving end), ``mbit_s_sent``,
    ``retransmits``, ``mean_rtt_ms`` (None when not reported), ``bytes``,
    ``seconds`` and the ``raw`` document.
    Raises RuntimeError when the output is not iperf3 JSON or reports an error.
    """
    start = output.find("{")
    if start < 0:
        raise RuntimeError(f"no iperf3 JSON in output: {output.strip()[-200:]}")
    try:
        data = json.loads(output[start:])
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"unreadable iperf3 JSON: {exc}") from exc
    if data.get("error"):
        raise RuntimeError(data["error"])
    end = data["end"]
    sent, received = end["sum_sent"], end["sum_received"]
    streams = end.get("streams") or [{}]
    rtt_us = (streams[0].get("sender") or {}).get("mean_rtt")
    return {
        "mbit_s": received["bits_per_second"] / 1e6,
        "mbit_s_sent": sent["bits_per_second"] / 1e6,
        "retransmits": sent.get("retransmits"),
        "mean_rtt_ms": rtt_us / 1e3 if rtt_us else None,
        "bytes": received["bytes"],
        "seconds": received["seconds"],
        "raw": data,
    }
