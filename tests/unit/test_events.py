import json
from pathlib import Path

from eccom.core.events import EventRecorder, EventType
from eccom.core.message import new_correlation_id


def test_correlation_ids_are_unique() -> None:
    ids = {new_correlation_id() for _ in range(10_000)}
    assert len(ids) == 10_000


def test_recorder_captures_required_fields(tmp_path: Path) -> None:
    recorder = EventRecorder("edge", tmp_path / "edge-events.jsonl")
    recorder.set_phase("warmup")
    recorder.record(EventType.SEND_STARTED, "abc", 0)
    recorder.set_phase("measurement")
    recorder.record(EventType.REPLY_RECEIVED, "abc", 0, metadata={"size_bytes": 3})
    recorder.close()

    lines = (tmp_path / "edge-events.jsonl").read_text(encoding="utf-8").splitlines()
    events = [json.loads(line) for line in lines]
    assert len(events) == 2

    first, second = events
    assert first["event_type"] == "SEND_STARTED"
    assert first["correlation_id"] == "abc"
    assert first["sequence_number"] == 0
    assert first["host"] == "edge"
    assert first["phase"] == "warmup"
    assert first["metadata"] is None
    assert second["phase"] == "measurement"
    assert second["metadata"] == {"size_bytes": 3}

    # monotonic timestamps never decrease; wall time is a plausible epoch value
    assert second["monotonic_ns"] >= first["monotonic_ns"]
    assert first["wall_time_ns"] > 1_500_000_000 * 10**9


def test_recorder_streams_before_close(tmp_path: Path, monkeypatch) -> None:
    """Events reach the file while the run is in progress: memory stays flat
    and a killed process loses at most one flush interval."""
    import time

    from eccom.core import events as events_module

    monkeypatch.setattr(events_module, "FLUSH_INTERVAL_S", 0.01)
    recorder = EventRecorder("edge", tmp_path / "edge-events.jsonl")
    try:
        for i in range(500):
            recorder.record(EventType.SEND_STARTED, f"c{i}", i)
        deadline = time.monotonic() + 2
        while time.monotonic() < deadline:
            lines = (tmp_path / "edge-events.jsonl").read_text(encoding="utf-8").splitlines()
            if len(lines) == 500:
                break
            time.sleep(0.01)
        assert len(lines) == 500
        assert len(recorder) == 500
    finally:
        recorder.close()
    assert len((tmp_path / "edge-events.jsonl").read_text(encoding="utf-8").splitlines()) == 500


def test_recorder_close_is_idempotent(tmp_path: Path) -> None:
    recorder = EventRecorder("edge", tmp_path / "events.jsonl")
    recorder.record(EventType.ERROR, "x", 1, metadata={"error": "boom"})
    recorder.close()
    recorder.close()
    lines = (tmp_path / "events.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1
