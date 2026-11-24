from pathlib import Path

import numpy as np

from eccom.analysis.plots import (
    generate_comparison_plots,
    monotone_cubic,
    sender_cost_series,
    variant_labels,
)


def _stats(base: float) -> dict[str, float]:
    return {
        "count": 61,
        "min": base * 0.9,
        "mean": base,
        "median": base * 0.98,
        "std": base * 0.05,
        "p90": base * 1.05,
        "p95": base * 1.1,
        "p99": base * 1.2,
        "max": base * 1.3,
    }


def _row(name: str, interaction: str, base: float) -> dict:
    one_way = interaction == "one_way"
    return {
        "run_dir": f"20260905T000000-{name}",
        "scenario": name,
        "interaction": interaction,
        "headline_metric": "delivery" if one_way else "rtt",
        "stages": {
            "send_ack": _stats(base) if one_way else None,
            "rtt": None if one_way else _stats(base),
            "delivery": _stats(base / 2),
            "headline": _stats(base / 2 if one_way else base),
        },
        "resources": {},
        "clock": {"method": "probe", "offset_ms": -0.5, "uncertainty_ms": 4.0},
    }


def test_variant_labels_strip_prefix_and_prettify() -> None:
    names = [
        "s1_http",
        "s1_mqtt_qos1",
        "s1_http_no_keepalive",
        "s1_mqtt_cold_start",
        "s1_http_req_rsp",
    ]
    assert variant_labels(names) == [
        "HTTP",
        "MQTT QoS 1",
        "HTTP no keep-alive",
        "MQTT cold start",
        "HTTP req/rsp",
    ]


def test_variant_labels_single_run_keeps_name() -> None:
    assert variant_labels(["s1_mqtts_qos1"]) == ["s1 MQTTS QoS 1"]


def test_generate_comparison_plots_writes_both_charts(tmp_path: Path) -> None:
    rows = [
        _row("s1_http", "one_way", 23.5),
        _row("s1_mqtt_qos1", "one_way", 23.5),
        _row("s1_http_req_rsp", "request_response", 23.5),
    ]
    written = generate_comparison_plots(tmp_path, rows)
    names = {path.name for path in written}
    assert names == {"compare_sender_cost.png", "compare_delivery.png"}
    for path in written:
        assert path.stat().st_size > 10_000  # a real rendered chart, not a stub


def test_generate_comparison_plots_empty_rows(tmp_path: Path) -> None:
    assert generate_comparison_plots(tmp_path, []) == []


def test_sender_cost_plots_rtt_for_request_response_even_with_a_transport_ack() -> None:
    """MQTT request/response records both PUBACK (send_ack) and the reply
    (rtt); the sender-cost chart must show the round trip, like HTTP req/rsp."""
    mqtt = _row("s1_mqtt_req_rsp", "request_response", 27.5)
    mqtt["stages"]["send_ack"] = _stats(25.7)
    rows = [_row("s1_mqtt_qos1", "one_way", 26.4), mqtt]
    series, labels = sender_cost_series(rows, ["MQTT QoS 1", "MQTT req/rsp"])
    assert [s["mean"] for s in series] == [26.4, 27.5]
    assert labels == ["MQTT QoS 1", "MQTT req/rsp\n(RTT)"]


def _ramp_steps(rates: list[float], delivered: list[float | None], median: list[float | None]) -> list[dict]:
    return [
        {
            "step": i,
            "target_message_rate": rate,
            "received_rate": d,
            "delivery_ms": None if m is None else {"median": m, "p95": m * 1.5},
        }
        for i, (rate, d, m) in enumerate(zip(rates, delivered, median))
    ]


def test_ramp_sweep_charts_are_written_for_ramp_rows(tmp_path: Path) -> None:
    rates = [10.0, 20.0, 40.0, 80.0, 160.0]
    rows = [_row("s2_http", "one_way", 23.0), _row("s2_mqtt_qos1", "one_way", 24.0)]
    rows[0]["ramp_steps"] = _ramp_steps(rates, [10, 20, 40, 30, 5], [12, 12, 12, 12, 13])
    # a step without receiver data
    rows[1]["ramp_steps"] = _ramp_steps(rates, [10, 20, 40, 60, None], [12, 12, 23, 24, None])
    written = {p.name for p in generate_comparison_plots(tmp_path, rows)}
    assert {"compare_ramp_delivered_rate.png", "compare_ramp_delivery_latency.png"} <= written
    assert (tmp_path / "compare_ramp_delivered_rate.png").stat().st_size > 0

    # rows without a ramp table (constant-rate runs) get no sweep charts
    plain = generate_comparison_plots(tmp_path / "plain", [_row("s1_http", "one_way", 23.0)])
    assert not any("ramp" in p.name for p in plain)


def test_ramp_sweep_charts_use_payload_size_when_the_ramp_grows_it(tmp_path: Path) -> None:
    sizes = [128, 512, 2048, 8192]
    rows = [_row("s3_http", "one_way", 25.0), _row("s3_mqtt_qos1", "one_way", 26.0)]
    for row in rows:
        row["ramp_steps"] = _ramp_steps([10.0] * 4, [10, 10, 10, 8], [13, 13, 20, 90])
        for step, size in zip(row["ramp_steps"], sizes):
            step["payload_size_bytes"] = size
    written = {p.name for p in generate_comparison_plots(tmp_path, rows)}
    assert {"compare_ramp_delivered_rate.png", "compare_ramp_delivery_latency.png"} <= written


def test_homes_ramps_get_a_sweep_per_factor_and_a_ceiling_chart(tmp_path: Path) -> None:
    rows = []
    for factor in (1, 2):
        for name, knee in (("http", 3), ("mqtt_qos1", None)):
            row = _row(f"s6_{name}_x{factor}", "one_way", 23.0)
            row["aggregation_factor"] = factor
            row["ramp_steps"] = _ramp_steps(
                [292.0 * i for i in (1, 2, 3, 4)], [292, 584, 876, 1100], [12, 12, 13, 40]
            )
            for step, homes in zip(row["ramp_steps"], (factor * i for i in (1, 2, 3, 4))):
                step["homes"] = homes
            sustained = factor * (knee if knee is not None else 4)
            row["ramp"] = {"exact_homes": sustained, "miss_step": knee}
            rows.append(row)
    written = {p.name for p in generate_comparison_plots(tmp_path, rows)}
    assert {
        "compare_ramp_delivered_rate_x1.png", "compare_ramp_delivery_latency_x1.png",
        "compare_ramp_delivered_rate_x2.png", "compare_ramp_delivery_latency_x2.png",
        "compare_homes_ceiling.png",
    } <= written
    assert "compare_ramp_delivered_rate.png" not in written
    assert (tmp_path / "compare_homes_ceiling.png").stat().st_size > 10_000


def test_monotone_cubic_passes_through_points_without_overshoot() -> None:
    x = np.array([0.0, 1.0, 2.0, 3.0, 4.0])
    y = np.array([1.0, 2.0, 8.0, 8.5, 8.6])  # monotone, with a sharp knee
    at_points = monotone_cubic(x, y, x)
    assert np.allclose(at_points, y)
    dense = monotone_cubic(x, y, np.linspace(0, 4, 401))
    assert np.all(np.diff(dense) >= -1e-9)  # stays monotone: no invented bumps
    assert dense.min() >= 1.0 - 1e-9 and dense.max() <= 8.6 + 1e-9
    two = monotone_cubic(np.array([1.0, 3.0]), np.array([2.0, 6.0]), np.array([2.0]))
    assert np.allclose(two, [4.0])
