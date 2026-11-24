import asyncio
import time
from typing import Any

from eccom.core.config import HomeConfig, LoadConfig
from eccom.generators.home import HomeTraffic
from eccom.generators.load import LoadGenerator

# a: 2 x 20 Hz, b: 1 x 10 Hz = 50 msg/s per batch of homes
TINY_HOME = HomeConfig.model_validate(
    {
        "sensors": [
            {"type": "a", "count": 2, "rate_hz": 20, "size_bytes": 8},
            {"type": "b", "count": 1, "rate_hz": 10, "size_bytes": 32},
        ]
    }
)


async def test_fixed_count_attempts_exactly_n_messages() -> None:
    calls = 0

    async def send_one(metadata: dict[str, Any]) -> bool:
        nonlocal calls
        calls += 1
        return True

    config = LoadConfig(messages_per_second=1000, total_messages=25, concurrency=4)
    report = await LoadGenerator(config).run(send_one)

    assert calls == 25
    assert report.messages_attempted == 25
    assert report.messages_completed == 25
    assert report.messages_failed == 0
    assert report.requested_message_rate == 1000
    assert report.actual_message_rate > 0


async def test_concurrency_cap_is_respected() -> None:
    in_flight = 0
    max_in_flight = 0

    async def send_one(metadata: dict[str, Any]) -> bool:
        nonlocal in_flight, max_in_flight
        in_flight += 1
        max_in_flight = max(max_in_flight, in_flight)
        await asyncio.sleep(0.02)
        in_flight -= 1
        return True

    config = LoadConfig(messages_per_second=10_000, total_messages=12, concurrency=3)
    report = await LoadGenerator(config).run(send_one)

    assert report.messages_attempted == 12
    assert max_in_flight <= 3


async def test_failures_and_exceptions_are_counted() -> None:
    counter = 0

    async def send_one(metadata: dict[str, Any]) -> bool:
        nonlocal counter
        counter += 1
        if counter % 3 == 1:
            return False
        if counter % 3 == 2:
            raise RuntimeError("boom")  # backstop path: counted as failed
        return True

    config = LoadConfig(messages_per_second=1000, total_messages=9)
    report = await LoadGenerator(config).run(send_one)

    assert report.messages_attempted == 9
    assert report.messages_completed == 3
    assert report.messages_failed == 6


async def test_duration_mode_stops_and_measures_rate() -> None:
    async def send_one(metadata: dict[str, Any]) -> bool:
        return True

    config = LoadConfig(messages_per_second=100, duration_s=0.3, concurrency=2)
    report = await LoadGenerator(config).run(send_one)

    # ~30 messages expected; allow generous slack for coarse OS timers
    assert 10 <= report.messages_attempted <= 45
    assert report.send_window_s >= 0.3
    assert report.actual_message_rate > 0


async def test_constant_rate_passes_empty_metadata_and_reports_no_steps() -> None:
    seen: list[dict[str, Any]] = []

    async def send_one(metadata: dict[str, Any]) -> bool:
        seen.append(metadata)
        return True

    report = await LoadGenerator(
        LoadConfig(messages_per_second=1000, total_messages=3)
    ).run(send_one)

    assert seen == [{}, {}, {}]
    assert report.steps is None


async def test_ramp_multiplies_the_rate_per_step_and_tags_every_send() -> None:
    tags: list[dict[str, Any]] = []

    async def send_one(metadata: dict[str, Any]) -> bool:
        tags.append(metadata)
        return True

    config = LoadConfig(
        mode="ramp",
        messages_per_second=50,
        rate_multiplier=2,
        step_duration_s=0.1,
        duration_s=0.3,
        concurrency=100,
    )
    report = await LoadGenerator(config).run(send_one)

    assert report.requested_message_rate == 50  # the first step
    assert report.steps is not None
    assert [s.step for s in report.steps] == [0, 1, 2]
    assert [s.target_message_rate for s in report.steps] == [50, 100, 200]
    assert report.messages_attempted == len(tags)
    assert report.messages_attempted == sum(s.messages_attempted for s in report.steps)
    # ~5, ~10, ~20 sends per step; coarse timers blur the counts but not the growth
    assert report.steps[0].messages_attempted < report.steps[2].messages_attempted
    for step in report.steps:
        assert step.messages_completed == step.messages_attempted
        assert step.messages_failed == 0
        assert 0.05 <= step.duration_s <= 0.2
        assert step.actual_message_rate > 0
    # every send carries the step it was scheduled in and that step's target
    assert {m["step"] for m in tags} == {0, 1, 2}
    assert all(m["target_rate"] == 50 * 2 ** m["step"] for m in tags)
    # the same dict object is shared within a step, so tagging costs nothing
    assert len({id(m) for m in tags}) == 3


async def test_linear_ramp_adds_the_increment_per_step() -> None:
    tags: list[dict[str, Any]] = []

    async def send_one(metadata: dict[str, Any]) -> bool:
        tags.append(metadata)
        return True

    config = LoadConfig(
        mode="ramp",
        messages_per_second=50,
        rate_increment=25,
        step_duration_s=0.1,
        duration_s=0.3,
        concurrency=100,
    )
    report = await LoadGenerator(config).run(send_one)

    assert report.steps is not None
    assert [s.target_message_rate for s in report.steps] == [50, 75, 100]
    assert all(m["target_rate"] == 50 + 25 * m["step"] for m in tags)


async def test_step_pause_goes_quiet_between_steps_and_keeps_duration_as_sending_time() -> None:
    sends: list[tuple[int, float]] = []
    t0 = time.perf_counter()

    async def send_one(metadata: dict[str, Any]) -> bool:
        sends.append((metadata["step"], time.perf_counter() - t0))
        return True

    config = LoadConfig(
        mode="ramp",
        messages_per_second=50,
        rate_increment=50,
        step_duration_s=0.1,
        step_pause_s=0.1,
        duration_s=0.3,
        concurrency=100,
    )
    report = await LoadGenerator(config).run(send_one)

    assert report.steps is not None
    assert [s.target_message_rate for s in report.steps] == [50, 100, 150]
    # every step is its sending window only, the pauses are nobody's
    assert all(0.05 <= s.duration_s <= 0.15 for s in report.steps)
    # three sending windows plus two pauses, no pause after the last step
    assert 0.45 <= report.send_window_s <= 0.7
    # nothing is sent inside a pause: step k's sends start at k * 0.2 s
    for step, at in sends:
        assert step * 0.2 - 0.02 <= at <= step * 0.2 + 0.13, (step, at)


async def test_size_ramp_grows_the_payload_per_step_at_a_constant_rate() -> None:
    tags: list[dict[str, Any]] = []

    async def send_one(metadata: dict[str, Any]) -> bool:
        tags.append(metadata)
        return True

    config = LoadConfig(
        mode="ramp",
        messages_per_second=50,
        size_multiplier=4,
        step_duration_s=0.1,
        duration_s=0.3,
        concurrency=100,
    )
    report = await LoadGenerator(config, payload_size_bytes=128).run(send_one)

    assert report.steps is not None
    assert [s.target_message_rate for s in report.steps] == [50, 50, 50]
    assert [s.payload_size_bytes for s in report.steps] == [128, 512, 2048]
    assert all(m["target_rate"] == 50 for m in tags)
    assert all(m["target_size"] == 128 * 4 ** m["step"] for m in tags)


async def test_homes_ramp_follows_the_home_mix_rate_and_tags_the_homes() -> None:
    tags: list[dict[str, Any]] = []

    async def send_one(metadata: dict[str, Any]) -> bool:
        tags.append(metadata)
        return True

    config = LoadConfig(
        mode="homes",
        homes=2,
        homes_increment=2,
        aggregation_factor=2,
        step_duration_s=0.1,
        duration_s=0.3,
        concurrency=100,
    )
    traffic = HomeTraffic(TINY_HOME, aggregation_factor=2)
    report = await LoadGenerator(config, home_traffic=traffic).run(send_one)

    assert report.requested_message_rate == 50  # the first step: one batch of two homes
    assert report.steps is not None
    assert [s.target_message_rate for s in report.steps] == [50, 100, 150]
    assert {(m["step"], m["target_homes"]) for m in tags} == {(0, 2), (1, 4), (2, 6)}
    assert all(m["target_rate"] == 25 * m["target_homes"] for m in tags)
    assert len({id(m) for m in tags}) == 3  # one tag dict per step, as in ramp mode


async def test_ramp_records_steps_the_blocked_schedule_could_not_reach() -> None:
    """With concurrency 1 and a send slower than a whole step, the schedule
    skips step 1 entirely; it must still appear (empty) so the analysis sees
    a contiguous step sequence, and nothing may be scheduled past duration_s."""

    async def send_one(metadata: dict[str, Any]) -> bool:
        await asyncio.sleep(0.25)
        return True

    config = LoadConfig(
        mode="ramp",
        messages_per_second=1000,
        rate_multiplier=2,
        step_duration_s=0.1,
        duration_s=0.3,
        concurrency=1,
    )
    report = await LoadGenerator(config).run(send_one)

    assert report.steps is not None
    assert [s.step for s in report.steps] == [0, 1, 2]
    assert report.steps[0].messages_attempted == 1
    assert report.steps[1].messages_attempted == 0
    assert report.steps[1].duration_s == 0
    assert report.steps[1].actual_message_rate == 0
    assert report.steps[2].messages_attempted == 1
    assert report.messages_attempted == 2
    assert report.messages_completed == 2
