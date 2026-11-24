"""The referential smart home as a traffic source: streams, batching, fire order."""

from collections import Counter
from itertools import islice

from eccom.core.config import HomeConfig
from eccom.generators.home import HomeTraffic

REFERENCE = HomeConfig.model_validate(
    {
        "seed": 42,
        "sensors": [
            {"type": "light", "count": 4, "rate_hz": 1, "size_bytes": 4},
            {"type": "smart_plug", "count": 40, "rate_hz": 1, "size_bytes": 16},
            {"type": "temperature", "count": 4, "rate_hz": 1, "size_bytes": 4},
            {"type": "motion", "count": 4, "rate_hz": 1, "size_bytes": 4},
            {"type": "microphone", "count": 4, "rate_hz": 50, "size_bytes": 640},
            {"type": "mmwave", "count": 4, "rate_hz": 10, "size_bytes": 20},
        ],
    }
)
SIZES = {sensor.type: sensor.size_bytes for sensor in REFERENCE.sensors}
COUNTS = {sensor.type: sensor.count for sensor in REFERENCE.sensors}


def test_message_rate_counts_one_message_per_sensor_slot_and_batch() -> None:
    assert REFERENCE.message_rate(1, 1) == 292
    assert REFERENCE.message_rate(8, 8) == 292  # eight homes, one batch per slot
    assert REFERENCE.message_rate(9, 8) == 584  # the ninth home starts a second batch
    assert REFERENCE.message_rate(12, 1) == 3504


def test_streams_batch_homes_per_sensor_slot_with_a_partial_last_batch() -> None:
    traffic = HomeTraffic(REFERENCE, aggregation_factor=2)
    streams = traffic.streams(5)  # batches of 2, 2 and 1 homes
    assert Counter(s.type for s in streams) == {t: 3 * n for t, n in COUNTS.items()}
    pieces = Counter((s.type, s.pieces) for s in streams)
    assert pieces[("microphone", 2)] == 8 and pieces[("microphone", 1)] == 4
    assert all(len(s.payload) == s.pieces * SIZES[s.type] for s in streams)
    assert all(s.interval_s == 1 / 50 for s in streams if s.type == "microphone")
    # one bytes object per (type, pieces), reproducible from the seed
    full_mics = [s for s in streams if s.type == "microphone" and s.pieces == 2]
    assert all(s.payload is full_mics[0].payload for s in full_mics)
    assert HomeTraffic(REFERENCE, 2).streams(5)[0].payload == streams[0].payload
    assert HomeTraffic(REFERENCE, 1).streams(1)[0].payload != streams[0].payload[:4]


def test_fire_order_reproduces_each_class_rate_and_is_deterministic() -> None:
    traffic = HomeTraffic(REFERENCE, aggregation_factor=1)
    one_second = list(islice(traffic.fire_order(1), 292))
    assert Counter(s.type for s in one_second) == {
        "microphone": 200, "smart_plug": 40, "mmwave": 40, "light": 4, "temperature": 4, "motion": 4,
    }
    assert [s.type for s in islice(traffic.fire_order(1), 292)] == [s.type for s in one_second]
    # the 50 Hz class is spread through the second, not bunched at its start
    for quarter in range(4):
        block = one_second[quarter * 73 : (quarter + 1) * 73]
        assert 45 <= sum(1 for s in block if s.type == "microphone") <= 55
