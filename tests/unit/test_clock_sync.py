from eccom.instrumentation.clock_sync import combine_clock_probes, offset_and_rtt

MS = 1_000_000


def test_offset_and_rtt_symmetric_path() -> None:
    # Receiver clock is +100 ms ahead; 10 ms each way; 1 ms server processing.
    t0 = 0
    t1 = 10 * MS + 100 * MS
    t2 = 11 * MS + 100 * MS
    t3 = 21 * MS
    offset, rtt = offset_and_rtt(t0, t1, t2, t3)
    assert offset == 100 * MS
    assert rtt == 20 * MS


def test_offset_error_is_bounded_by_half_rtt() -> None:
    # Fully asymmetric path (20 ms out, 0 ms back): worst case error = rtt/2.
    true_offset = 100 * MS
    t0 = 0
    t1 = 20 * MS + true_offset
    t2 = t1
    t3 = 20 * MS
    offset, rtt = offset_and_rtt(t0, t1, t2, t3)
    assert abs(offset - true_offset) <= rtt // 2


def test_combine_probes_widens_uncertainty_by_drift() -> None:
    before = {"offset_ns": 100 * MS, "uncertainty_ns": 1 * MS, "min_rtt_ns": 2 * MS}
    after = {"offset_ns": 104 * MS, "uncertainty_ns": 2 * MS, "min_rtt_ns": 4 * MS}
    combined = combine_clock_probes(before, after)
    assert combined["offset_ns"] == 102 * MS
    assert combined["drift_ns"] == 4 * MS
    assert combined["uncertainty_ns"] == 2 * MS + 2 * MS  # max(unc) + drift/2


def test_combine_probes_without_after_snapshot() -> None:
    before = {"offset_ns": 5 * MS, "uncertainty_ns": 1 * MS, "min_rtt_ns": 2 * MS}
    combined = combine_clock_probes(before, None)
    assert combined["offset_ns"] == 5 * MS
    assert combined["uncertainty_ns"] == 1 * MS
    assert combined["drift_ns"] is None


# --- chrony tracking parsing and the tightened uncertainty bound ---------------

import json
from pathlib import Path

from eccom.analysis.run import resolve_clock_sync
from eccom.instrumentation.clock_sync import chrony_max_error_s, parse_chrony_tracking

# Real `chronyc tracking` output (an NTP-synced host and one on a hypervisor
# PTP clock). "fast"/"slow" and the ppm lines exercise the sign
# handling and the field filtering.
_EDGE_TRACKING = """\
Reference ID    : D91FCA64 (ntp.nic.cz)
Stratum         : 2
Ref time (UTC)  : Sat Sep 05 16:26:54 2026
System time     : 0.000109380 seconds fast of NTP time
Last offset     : +0.000026369 seconds
RMS offset      : 0.000086033 seconds
Frequency       : 38.779 ppm slow
Residual freq   : +0.018 ppm
Skew            : 0.515 ppm
Root delay      : 0.006372227 seconds
Root dispersion : 0.000311262 seconds
Update interval : 64.8 seconds
Leap status     : Normal
"""

_CLOUD_TRACKING = """\
Reference ID    : 50484330 (PHC0)
Stratum         : 1
Ref time (UTC)  : Sat Sep 05 16:27:16 2026
System time     : 0.000000924 seconds slow of NTP time
Last offset     : -0.000000336 seconds
RMS offset      : 0.000001832 seconds
Frequency       : 2.929 ppm fast
Residual freq   : -0.001 ppm
Skew            : 0.138 ppm
Root delay      : 0.000000001 seconds
Root dispersion : 0.000016530 seconds
Update interval : 8.0 seconds
Leap status     : Normal
"""


def test_parse_chrony_tracking_fast_clock() -> None:
    tracking = parse_chrony_tracking(_EDGE_TRACKING)
    assert tracking is not None
    assert tracking["reference"] == "ntp.nic.cz"
    assert tracking["stratum"] == 2
    assert tracking["system_time_offset_s"] == 0.000109380  # "fast" => positive
    assert tracking["root_delay_s"] == 0.006372227
    assert tracking["leap_status"] == "Normal"


def test_parse_chrony_tracking_slow_clock_is_negative() -> None:
    tracking = parse_chrony_tracking(_CLOUD_TRACKING)
    assert tracking is not None
    assert tracking["system_time_offset_s"] == -0.000000924


def test_parse_chrony_tracking_rejects_non_tracking_output() -> None:
    assert parse_chrony_tracking("506 Cannot talk to daemon") is None
    assert parse_chrony_tracking("") is None


def test_chrony_max_error_is_offset_plus_half_delay_plus_dispersion() -> None:
    tracking = parse_chrony_tracking(_EDGE_TRACKING)
    expected = 0.000109380 + 0.006372227 / 2 + 0.000311262
    assert abs(chrony_max_error_s(tracking) - expected) < 1e-12


def test_chrony_max_error_requires_synchronized_clock() -> None:
    tracking = parse_chrony_tracking(_EDGE_TRACKING)
    tracking["leap_status"] = "Not synchronised"
    assert chrony_max_error_s(tracking) is None
    tracking = parse_chrony_tracking(_EDGE_TRACKING)
    tracking["stratum"] = 0
    assert chrony_max_error_s(tracking) is None


def _write_run_dir(
    tmp_path: Path,
    probe: dict | None,
    combined_max_error_s: float | None,
) -> Path:
    if probe is not None:
        (tmp_path / "edge-clock-sync.json").write_text(json.dumps(probe), encoding="utf-8")
    clock_sync: dict = {"method": None, "combined_max_error_s": combined_max_error_s}
    (tmp_path / "run.json").write_text(
        json.dumps({"clock_sync": clock_sync}), encoding="utf-8"
    )
    return tmp_path


def test_resolve_prefers_chrony_bound_when_tighter(tmp_path: Path) -> None:
    probe = {"method": "control-endpoint-ntp-probe", "offset_ns": -1 * MS, "uncertainty_ns": 12 * MS}
    resolved = resolve_clock_sync(_write_run_dir(tmp_path, probe, 0.0036), "edge")
    # offset stays the probe's; bound = |offset| + combined chrony error
    assert resolved["offset_ns"] == -1 * MS
    assert resolved["uncertainty_ns"] == 1 * MS + int(0.0036 * 1e9)
    assert resolved["uncertainty_source"] == "chrony-tracking"


def test_resolve_keeps_probe_bound_when_tighter(tmp_path: Path) -> None:
    probe = {"method": "control-endpoint-ntp-probe", "offset_ns": 0, "uncertainty_ns": 2 * MS}
    resolved = resolve_clock_sync(_write_run_dir(tmp_path, probe, 0.010), "edge")
    assert resolved["uncertainty_ns"] == 2 * MS
    assert resolved["uncertainty_source"] == "probe"


def test_resolve_falls_back_to_chrony_without_probe(tmp_path: Path) -> None:
    resolved = resolve_clock_sync(_write_run_dir(tmp_path, None, 0.0036), "edge")
    assert resolved["offset_ns"] == 0
    assert resolved["uncertainty_ns"] == int(0.0036 * 1e9)
    assert resolved["method"] == "chrony-tracking"


def test_resolve_same_host_still_exact(tmp_path: Path) -> None:
    (tmp_path / "run.json").write_text(
        json.dumps({"clock_sync": {"method": "same_host", "synchronized": True}}),
        encoding="utf-8",
    )
    resolved = resolve_clock_sync(tmp_path, "edge")
    assert resolved == {"method": "same_host", "offset_ns": 0, "uncertainty_ns": 0}
