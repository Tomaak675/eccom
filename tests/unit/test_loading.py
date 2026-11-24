"""Artifact loading: a truncated events file is reported by name and line."""

from pathlib import Path

import pytest

from eccom.analysis.loading import load_events


def test_gzipped_artifacts_load_like_plain_ones(tmp_path: Path) -> None:
    """The SSH deployment gzips the JSONL files on the host before the
    download; the loaders read either form and keep the instance label."""
    import gzip
    import json

    from eccom.analysis.loading import load_system_metrics

    line = {"event_type": "SEND_STARTED", "correlation_id": "a", "sequence_number": 0, "host": "edge",
            "phase": "measurement", "monotonic_ns": 1, "wall_time_ns": 1, "metadata": None}
    with gzip.open(tmp_path / "edge.3-events.jsonl.gz", "wt", encoding="utf-8") as fh:
        fh.write(json.dumps(line) + "\n")
    (tmp_path / "cloud-events.jsonl").write_text(json.dumps(line | {"host": "cloud"}) + "\n", encoding="utf-8")
    events = load_events(tmp_path)
    assert sorted(events["host"]) == ["cloud", "edge"]
    assert int(events.loc[events["host"] == "edge", "instance"].iloc[0]) == 3

    sample = {"host": "edge", "phase": "traffic", "wall_time_ns": 5, "monotonic_ns": 5, "process_cpu_percent": 1.0}
    with gzip.open(tmp_path / "edge-system-metrics.jsonl.gz", "wt", encoding="utf-8") as fh:
        fh.write(json.dumps(sample) + "\n")
    assert load_system_metrics(tmp_path)["edge"][0]["wall_time_ns"] == 5


def test_shared_metadata_keeps_value_types_apart(tmp_path: Path) -> None:
    """Equal metadata dicts share one object, but 600 and 600.0 are not merged."""
    import json

    base = {"event_type": "PAYLOAD_CREATED", "sequence_number": 0, "host": "edge",
            "phase": "measurement", "monotonic_ns": 1, "wall_time_ns": 1}
    metadata = [{"step": 1, "target_rate": 600}, {"step": 1, "target_rate": 600.0}, {"step": 1, "target_rate": 600}]
    lines = [json.dumps(base | {"correlation_id": str(i), "metadata": m}) for i, m in enumerate(metadata)]
    (tmp_path / "edge-events.jsonl").write_text("\n".join(lines) + "\n", encoding="utf-8")
    loaded = list(load_events(tmp_path)["metadata"])
    assert [type(m["target_rate"]) for m in loaded] == [int, float, int]
    assert loaded[0] is loaded[2]


def test_truncated_events_file_is_named_with_its_line(tmp_path: Path) -> None:
    good = '{"event_type":"SEND_STARTED","correlation_id":"a","sequence_number":0,"host":"edge",'
    good += '"phase":null,"monotonic_ns":1,"wall_time_ns":1,"metadata":null}\n'
    (tmp_path / "cloud-events.jsonl").write_text(good + good[:40], encoding="utf-8")
    with pytest.raises(ValueError, match=r"cloud-events.jsonl line 2 .*disk filled"):
        load_events(tmp_path)
