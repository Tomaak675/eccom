"""Measurement event model and the per-host event recorder.

All latency/throughput statistics are computed offline from these raw events;
protocol adapters and agents only *record*, they never aggregate.
"""

from __future__ import annotations

import json
import threading
import time
from collections import deque
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Any

# The writer thread drains the queue at least this often; what a killed
# process loses is bounded by it.
FLUSH_INTERVAL_S = 0.5


class EventType(StrEnum):
    PAYLOAD_CREATED = "PAYLOAD_CREATED"
    SEND_STARTED = "SEND_STARTED"
    SEND_COMPLETED = "SEND_COMPLETED"
    MESSAGE_RECEIVED = "MESSAGE_RECEIVED"
    HANDLER_STARTED = "HANDLER_STARTED"
    HANDLER_COMPLETED = "HANDLER_COMPLETED"
    REPLY_SEND_STARTED = "REPLY_SEND_STARTED"
    REPLY_SEND_COMPLETED = "REPLY_SEND_COMPLETED"
    REPLY_RECEIVED = "REPLY_RECEIVED"
    TIMEOUT = "TIMEOUT"
    ERROR = "ERROR"


@dataclass(frozen=True, slots=True)
class MeasurementEvent:
    event_type: str
    correlation_id: str
    sequence_number: int
    host: str
    # Experiment phase ("warmup" / "measurement") as seen by the *sender*.
    # Receiver-side events carry None; the analyzer derives their phase by
    # joining on correlation_id.
    phase: str | None
    # Monotonic high-resolution clock (time.perf_counter_ns): only comparable
    # to other events from the same host and process; used for all duration
    # calculations. perf_counter_ns instead of monotonic_ns because the latter
    # has 15.6 ms resolution on Windows (GetTickCount64 in CPython <= 3.12);
    # on Linux both map to CLOCK_MONOTONIC.
    monotonic_ns: int
    # time.time_ns(): comparable across hosts only if their clocks are
    # synchronized (see clock_sync in the environment metadata).
    wall_time_ns: int
    metadata: dict[str, Any] | None = None


def _as_dict(event: MeasurementEvent) -> dict[str, Any]:
    """The event's JSON object, keys in field order (dataclasses.asdict is far slower)."""
    return {
        "event_type": event.event_type,
        "correlation_id": event.correlation_id,
        "sequence_number": event.sequence_number,
        "host": event.host,
        "phase": event.phase,
        "monotonic_ns": event.monotonic_ns,
        "wall_time_ns": event.wall_time_ns,
        "metadata": event.metadata,
    }


class EventRecorder:
    """Records measurement events and streams them to ``output_path`` as JSONL.

    The hot path is two clock reads and one deque append; a writer thread
    drains the deque every FLUSH_INTERVAL_S, serializes and writes. Memory
    stays flat however long the run: a whole run buffered in memory is
    millions of live objects that every cyclic garbage collection traverses.
    The serialization cost moves into the run, on the writer thread.

    Recording happens on one event-loop thread; only the writer thread pops.
    """

    def __init__(self, host: str, output_path: Path) -> None:
        self._host = host
        self._output_path = output_path
        self._phase: str | None = None
        self._pending: deque[MeasurementEvent] = deque()
        self._count = 0
        self._closed = False
        self._error: BaseException | None = None
        output_path.parent.mkdir(parents=True, exist_ok=True)
        self._handle = output_path.open("w", encoding="utf-8", newline="\n")
        self._stop = threading.Event()
        self._writer = threading.Thread(
            target=self._write_loop, name=f"events-{host}", daemon=True
        )
        self._writer.start()

    @property
    def output_path(self) -> Path:
        return self._output_path

    def __len__(self) -> int:
        return self._count

    def set_phase(self, phase: str | None) -> None:
        """Tag subsequent events with an experiment phase (sender side only)."""
        self._phase = phase

    def record(
        self,
        event_type: EventType,
        correlation_id: str,
        sequence_number: int,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        self._pending.append(
            MeasurementEvent(
                event_type=event_type.value,
                correlation_id=correlation_id,
                sequence_number=sequence_number,
                host=self._host,
                phase=self._phase,
                monotonic_ns=time.perf_counter_ns(),
                wall_time_ns=time.time_ns(),
                metadata=metadata,
            )
        )
        self._count += 1

    def close(self) -> None:
        """Stop the writer, flush what is pending and close the file.

        Raises the writer's first file error, if it had one.
        """
        if self._closed:
            return
        self._closed = True
        self._stop.set()
        self._writer.join()
        self._flush()
        self._handle.close()
        if self._error is not None:
            raise self._error

    def _write_loop(self) -> None:
        while not self._stop.wait(FLUSH_INTERVAL_S):
            self._flush()

    def _flush(self) -> None:
        if self._error is not None:
            self._pending.clear()
            return
        lines: list[str] = []
        try:
            while True:
                lines.append(json.dumps(_as_dict(self._pending.popleft()), separators=(",", ":")))
        except IndexError:
            pass
        if not lines:
            return
        try:
            self._handle.write("\n".join(lines) + "\n")
            self._handle.flush()
        except OSError as exc:
            self._error = exc
