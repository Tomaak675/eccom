"""Out-of-band control endpoint for receiver agents.

The orchestrator talks to agents via this small HTTP
server (readiness checks, graceful shutdown). It is only contacted outside
the measurement window.
"""

from __future__ import annotations

import asyncio
import time

from aiohttp import web

HEALTH_PATH = "/health"
SHUTDOWN_PATH = "/shutdown"
CLOCK_PATH = "/clock"


class ControlServer:
    def __init__(self, bind_host: str, port: int) -> None:
        self._bind_host = bind_host
        self._port = port
        self._runner: web.AppRunner | None = None
        self.shutdown_requested = asyncio.Event()

    async def start(self) -> None:
        app = web.Application()
        app.router.add_get(HEALTH_PATH, self._handle_health)
        app.router.add_post(SHUTDOWN_PATH, self._handle_shutdown)
        app.router.add_get(CLOCK_PATH, self._handle_clock)
        self._runner = web.AppRunner(app, access_log=None)
        await self._runner.setup()
        site = web.TCPSite(self._runner, self._bind_host, self._port)
        await site.start()

    async def stop(self) -> None:
        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None

    async def _handle_health(self, _request: web.Request) -> web.Response:
        return web.json_response({"status": "ok"})

    async def _handle_shutdown(self, _request: web.Request) -> web.Response:
        self.shutdown_requested.set()
        return web.json_response({"status": "shutting down"})

    async def _handle_clock(self, _request: web.Request) -> web.Response:
        """NTP-style timestamps for clock-offset probes (see
        eccom.instrumentation.clock_sync)."""
        receive_wall_time_ns = time.time_ns()
        return web.json_response(
            {
                "receive_wall_time_ns": receive_wall_time_ns,
                "transmit_wall_time_ns": time.time_ns(),
            }
        )
