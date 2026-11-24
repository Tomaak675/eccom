"""Periodic system/process resource sampling (psutil) plus an event-loop lag probe.

Runs as independent asyncio tasks so it never blocks the communication
path; samples are buffered and written as JSONL on stop. A future
NetworkProbe (pcap / SO_TIMESTAMPING / eBPF) would sit next to this class
with the same start/stop lifecycle.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import time
from pathlib import Path
from typing import Any

import psutil

# Period of the event-loop lag probe. 100 wakeups/s cost nothing measurable
# and resolve stalls of a few milliseconds.
_LAG_TICK_S = 0.01


class SystemMetricsSampler:
    def __init__(self, host: str, interval_s: float, output_path: Path) -> None:
        self._host = host
        self._interval_s = interval_s
        self._output_path = output_path
        self._process = psutil.Process()
        self._samples: list[dict[str, Any]] = []
        self._task: asyncio.Task[None] | None = None
        self._lag_task: asyncio.Task[None] | None = None
        self._lag_max_ms = 0.0
        self._phase = "startup"

    async def start(self) -> None:
        # Prime the CPU counters: the first cpu_percent() call always returns 0.
        psutil.cpu_percent(interval=None)
        self._process.cpu_percent(interval=None)
        self._task = asyncio.create_task(self._sample_loop())
        self._lag_task = asyncio.create_task(self._lag_loop())

    def set_phase(self, phase: str) -> None:
        """Tag subsequent samples (mirrors EventRecorder.set_phase)."""
        self._phase = phase

    async def measure_baseline(self, duration_s: float) -> None:
        """Sit idle for ``duration_s``, tagging the samples as the baseline.

        Called once before any traffic so the analyzer has an idle reference
        for this exact process on this exact host.
        """
        if duration_s <= 0:
            return
        self.set_phase("baseline")
        await asyncio.sleep(duration_s)
        self.set_phase("traffic")

    async def stop(self) -> None:
        for task in (self._task, self._lag_task):
            if task is not None:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._task = self._lag_task = None
        self._output_path.parent.mkdir(parents=True, exist_ok=True)
        with self._output_path.open("w", encoding="utf-8", newline="\n") as fh:
            for sample in self._samples:
                fh.write(json.dumps(sample, separators=(",", ":")))
                fh.write("\n")

    async def _sample_loop(self) -> None:
        while True:
            await asyncio.sleep(self._interval_s)
            self._samples.append(self._sample())

    async def _lag_loop(self) -> None:
        """Event-loop lag probe: how late a 10 ms timer actually fires.

        Every eccom timestamp is taken in userspace, so when this process's
        loop is saturated the SEND_* stamps are late by exactly this kind of
        delay and, in the events alone, the inflation is indistinguishable
        from network latency. The worst overshoot since the previous sample
        is recorded as loop_lag_ms_max; it also catches GC pauses. Its floor
        is the OS timer resolution (~0.1 ms on Linux, ~15 ms on Windows).
        """
        while True:
            due = time.perf_counter() + _LAG_TICK_S
            await asyncio.sleep(_LAG_TICK_S)
            lag_ms = (time.perf_counter() - due) * 1000.0
            if lag_ms > self._lag_max_ms:
                self._lag_max_ms = lag_ms

    def _sample(self) -> dict[str, Any]:
        virtual_memory = psutil.virtual_memory()
        net = psutil.net_io_counters()
        cpu_times = self._process.cpu_times()
        loop_lag_ms_max, self._lag_max_ms = self._lag_max_ms, 0.0
        return {
            "host": self._host,
            "phase": self._phase,
            "wall_time_ns": time.time_ns(),
            "monotonic_ns": time.perf_counter_ns(),
            "system_cpu_percent": psutil.cpu_percent(interval=None),
            "process_cpu_percent": self._process.cpu_percent(interval=None),
            # Cumulative CPU seconds. cpu_percent quantizes to whole percent at
            # 1 Hz, which is coarser than an idle agent's actual usage; the
            # delta of this counter over a window resolves it exactly.
            "process_cpu_seconds": cpu_times.user + cpu_times.system,
            "system_memory_percent": virtual_memory.percent,
            "process_rss_bytes": self._process.memory_info().rss,
            "net_bytes_sent": net.bytes_sent,
            "net_bytes_recv": net.bytes_recv,
            "loop_lag_ms_max": loop_lag_ms_max,
        }
