"""Open-loop load generation: a constant rate, or a stepped ramp.

The generator schedules sends on a cadence and never slows the schedule down
to wait for replies (only the concurrency cap can apply backpressure).
Because Python itself may be the bottleneck, the *actual* achieved send rate
is always measured and reported alongside the requested rate. The whole class
is replaceable by e.g. a Go implementation later without touching agents or
protocol adapters.

ramp mode multiplies the rate by ``rate_multiplier`` (or adds
``rate_increment``) and/or multiplies the payload size by ``size_multiplier``
every ``step_duration_s`` on one continuous schedule:
nothing is drained at a step boundary, so the in-flight backlog carries over
from step to step -- which is exactly the queueing a saturation experiment
wants to observe. With ``step_pause_s`` the schedule instead goes quiet for
that long after every step (not after the last), so the backlog drains and
the next step starts from an empty queue; ``duration_s`` still counts
sending time only. Every send is tagged with its step so the analysis
attributes messages to steps exactly rather than by timestamp windows.

homes mode is the same stepped schedule with the rate of each step derived
from its number of simulated homes (``eccom.generators.home``).
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from eccom.core.config import LoadConfig
from eccom.generators.home import HomeTraffic

logger = logging.getLogger(__name__)

# Sends one message end-to-end; returns True on success. Must not raise. The
# metadata is attached to the message's PAYLOAD_CREATED event: {} for
# constant_rate, {"step", "target_rate"} for ramp plus "target_size" when the
# ramp grows the payload, {"step", "target_rate", "target_homes"} for homes
# (mirrors the device tag in devices mode).
SendOne = Callable[[dict[str, Any]], Awaitable[bool]]


@dataclass(frozen=True, slots=True)
class StepReport:
    """One ramp step: what was asked for and what the generator achieved."""

    step: int
    target_message_rate: float
    actual_message_rate: float  # attempted / duration_s
    messages_attempted: int
    messages_completed: int
    messages_failed: int
    duration_s: float  # how long this step was the active schedule
    payload_size_bytes: int | None = None  # ramp with size_multiplier only


@dataclass(frozen=True, slots=True)
class LoadReport:
    requested_message_rate: float  # ramp: the first step's rate
    actual_message_rate: float  # attempted / send window, as measured
    messages_attempted: int
    messages_completed: int
    messages_failed: int
    send_window_s: float  # scheduling window (first send start .. last send start)
    total_duration_s: float  # including waiting for in-flight requests to drain
    # devices mode only: {device name: {"attempted": n, "completed": n}}
    per_device: dict[str, dict[str, int]] | None = None
    # ramp mode only: one entry per step, in order
    steps: list[StepReport] | None = None


@dataclass(slots=True)
class _Step:
    """Per-step counters while the generator runs (completed is bumped by
    in-flight tasks after the step has already been closed)."""

    index: int
    target_rate: float
    opened_at: float  # perf_counter
    metadata: dict[str, Any] = field(default_factory=dict)
    closed_at: float | None = None
    attempted: int = 0
    completed: int = 0
    target_size: int | None = None

    def report(self) -> StepReport:
        assert self.closed_at is not None
        duration_s = self.closed_at - self.opened_at
        return StepReport(
            step=self.index,
            target_message_rate=self.target_rate,
            actual_message_rate=self.attempted / duration_s if duration_s > 0 else 0.0,
            messages_attempted=self.attempted,
            messages_completed=self.completed,
            messages_failed=self.attempted - self.completed,
            duration_s=duration_s,
            payload_size_bytes=self.target_size,
        )


class LoadGenerator:
    def __init__(
        self,
        config: LoadConfig,
        payload_size_bytes: int | None = None,
        home_traffic: HomeTraffic | None = None,
    ) -> None:
        """payload_size_bytes: the first step's payload size; required when the
        ramp sets ``size_multiplier``. home_traffic: the home mix whose rate
        the steps follow; required in homes mode."""
        self._config = config
        self._payload_size_bytes = payload_size_bytes
        self._home_traffic = home_traffic

    async def run(self, send_one: SendOne) -> LoadReport:
        config = self._config
        semaphore = asyncio.Semaphore(config.concurrency)
        # Only in-flight tasks are referenced; finished ones drop out through
        # the done callback, so a long high-rate run never accumulates task
        # objects (or a million-argument gather at the end).
        in_flight: set[asyncio.Task[None]] = set()
        steps: list[_Step] = []
        attempted = 0
        # perf_counter: high resolution on all platforms (monotonic has 15.6 ms
        # granularity on Windows), so the measured windows stay trustworthy.
        start = time.perf_counter()
        next_send = start
        current = self._open_step(steps, 0, start)
        interval = 1.0 / current.target_rate

        while True:
            delay = next_send - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            # If we fall behind schedule (slow event loop, coarse OS timers,
            # concurrency cap), we do not sleep and catch up in a burst; the
            # deviation shows up in actual_message_rate.
            await semaphore.acquire()
            now = time.perf_counter()
            # Re-checked after the wait so a send that only got its slot after
            # the deadline is not scheduled (it would land outside every step).
            if not self._should_schedule(attempted, now - start):
                semaphore.release()
                break
            step_index = self._step_index(now - start)
            if step_index != current.index:
                # The schedule itself is not reset: the backlog carries over.
                if current.closed_at is None:
                    current.closed_at = now
                for skipped in range(current.index + 1, step_index):
                    # Nothing could be scheduled during this step (the loop
                    # was blocked on the concurrency cap the whole time).
                    self._open_step(steps, skipped, now).closed_at = now
                current = self._open_step(steps, step_index, now)
                interval = 1.0 / current.target_rate
            if self._in_pause(now - start):
                # The quiet gap after a step: the step is over, nothing is
                # scheduled until the next one starts, the backlog drains.
                if current.closed_at is None:
                    current.closed_at = now
                semaphore.release()
                next_send = start + (step_index + 1) * self._period_s()
                continue
            task = asyncio.create_task(self._run_one(send_one, semaphore, current))
            in_flight.add(task)
            task.add_done_callback(in_flight.discard)
            current.attempted += 1
            attempted += 1
            next_send += interval

        if current.closed_at is None:
            current.closed_at = time.perf_counter()
        send_window_s = max(time.perf_counter() - start, 1e-9)
        await asyncio.gather(*in_flight)  # drain in-flight requests
        total_duration_s = time.perf_counter() - start

        completed = sum(step.completed for step in steps)
        return LoadReport(
            requested_message_rate=steps[0].target_rate,
            actual_message_rate=attempted / send_window_s,
            messages_attempted=attempted,
            messages_completed=completed,
            messages_failed=attempted - completed,
            send_window_s=send_window_s,
            total_duration_s=total_duration_s,
            steps=[step.report() for step in steps] if config.stepped else None,
        )

    def _should_schedule(self, scheduled: int, elapsed_s: float) -> bool:
        if self._config.total_messages is not None:
            return scheduled < self._config.total_messages
        assert self._config.duration_s is not None
        return self._sending_time_s(elapsed_s) < self._config.duration_s

    def _period_s(self) -> float:
        """One ramp step plus the pause that follows it."""
        assert self._config.step_duration_s is not None  # enforced by config validation
        return self._config.step_duration_s + (self._config.step_pause_s or 0)

    def _step_index(self, elapsed_s: float) -> int:
        if not self._config.stepped:
            return 0
        return int(elapsed_s // self._period_s())

    def _in_pause(self, elapsed_s: float) -> bool:
        """Whether ``elapsed_s`` falls in the quiet gap after a step."""
        if not self._config.stepped or not self._config.step_pause_s:
            return False
        assert self._config.step_duration_s is not None
        return elapsed_s % self._period_s() >= self._config.step_duration_s

    def _sending_time_s(self, elapsed_s: float) -> float:
        """``elapsed_s`` minus the pauses already spent: what duration_s counts."""
        if not self._config.stepped:
            return elapsed_s
        assert self._config.step_duration_s is not None
        index, offset = divmod(elapsed_s, self._period_s())
        return index * self._config.step_duration_s + min(offset, self._config.step_duration_s)

    def _open_step(self, steps: list[_Step], index: int, now: float) -> _Step:
        config = self._config
        size = None
        if config.mode == "ramp":
            assert config.messages_per_second is not None  # enforced by config validation
            rate = (
                config.messages_per_second * (config.rate_multiplier or 1) ** index
                + (config.rate_increment or 0) * index
            )
            metadata: dict[str, Any] = {"step": index, "target_rate": rate}
            if config.size_multiplier is not None:
                assert self._payload_size_bytes is not None  # the sender passes payload.size_bytes
                size = round(self._payload_size_bytes * config.size_multiplier**index)
                metadata["target_size"] = size
        elif config.mode == "homes":
            assert config.homes is not None and config.homes_increment is not None
            assert self._home_traffic is not None  # the sender passes the home mix
            homes = config.homes + config.homes_increment * index
            rate = self._home_traffic.message_rate(homes)
            metadata = {"step": index, "target_rate": rate, "target_homes": homes}
        else:
            assert config.messages_per_second is not None  # enforced by config validation
            rate = config.messages_per_second
            metadata = {}
        step = _Step(index, rate, now, metadata, target_size=size)
        steps.append(step)
        return step

    @staticmethod
    async def _run_one(send_one: SendOne, semaphore: asyncio.Semaphore, step: _Step) -> None:
        try:
            if await send_one(step.metadata):
                step.completed += 1
        except Exception:
            # send_one is expected to handle its own errors; this is a backstop
            # so one bad task can never abort the whole run.
            logger.exception("send task raised unexpectedly")
        finally:
            semaphore.release()
