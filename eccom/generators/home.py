"""The referential smart home as a traffic source (``load.mode: homes``).

A home is a fixed mix of sensor classes (``home.sensors``: count, rate and
reading size per class). ``homes`` copies of it are forwarded by one gateway
that batches the readings of each sensor slot across ``aggregation_factor``
homes into one message, so the message rate falls by that factor while the
byte rate stays. The schedule itself is the paced open-loop cadence of
:class:`eccom.generators.load.LoadGenerator` at the mix's aggregate rate;
this module supplies that rate per step and hands out the messages in the
order the sensors' own staggered timers would fire them (a merge of the
streams in virtual time), so the mix within every second is exact.
"""

from __future__ import annotations

import heapq
import random
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass

from eccom.core.config import HomeConfig


@dataclass(frozen=True, slots=True)
class HomeStream:
    """One sensor slot's batch of homes: fires every ``interval_s`` with the
    readings of ``pieces`` homes in one payload."""

    type: str
    interval_s: float
    pieces: int
    payload: bytes


class HomeTraffic:
    """The streams and rates of ``homes`` copies of one home under a batching factor."""

    def __init__(self, home: HomeConfig, aggregation_factor: int) -> None:
        self._home = home
        self._factor = aggregation_factor
        self._payloads: dict[tuple[str, int], bytes] = {}

    def message_rate(self, homes: int) -> float:
        """Aggregate messages per second of ``homes`` homes."""
        return self._home.message_rate(homes, self._factor)

    def streams(self, homes: int) -> list[HomeStream]:
        """Every stream of ``homes`` homes: per sensor class, ``count`` slots
        for each batch of homes, the last batch holding the remainder.

        homes: number of simulated homes.
        Returns: the streams, grouped by sensor class.
        """
        full, remainder = divmod(homes, self._factor)
        batches = [self._factor] * full + ([remainder] if remainder else [])
        return [
            HomeStream(
                sensor.type,
                1 / sensor.rate_hz,
                pieces,
                self._payload(sensor.type, pieces, pieces * sensor.size_bytes),
            )
            for sensor in self._home.sensors
            for pieces in batches
            for _ in range(sensor.count)
        ]

    def fire_order(self, homes: int) -> Iterator[HomeStream]:
        """The streams of ``homes`` homes in the order their timers would fire,
        without end: a merge in virtual time with each class's slots staggered
        evenly over one interval, as the device simulator staggers devices.

        homes: number of simulated homes.
        Returns: an endless iterator of streams.
        """
        streams = self.streams(homes)
        per_class = Counter(stream.type for stream in streams)
        slot: Counter[str] = Counter()
        heap: list[tuple[float, int]] = []
        for index, stream in enumerate(streams):
            heap.append((slot[stream.type] / per_class[stream.type] * stream.interval_s, index))
            slot[stream.type] += 1
        heapq.heapify(heap)
        while True:
            fire_at, index = heap[0]
            stream = streams[index]
            heapq.heapreplace(heap, (fire_at + stream.interval_s, index))
            yield stream

    def _payload(self, sensor_type: str, pieces: int, size: int) -> bytes:
        key = (sensor_type, pieces)
        if key not in self._payloads:
            self._payloads[key] = random.Random(
                f"{self._home.seed}:{sensor_type}:{pieces}"
            ).randbytes(size)
        return self._payloads[key]
