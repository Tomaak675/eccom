"""NTP-style clock offset measurement between sender and receiver hosts.

Perfectly synchronized clocks do not exist; what the framework provides is a
*measured* offset with an explicit error bound. The sender probes the
receiver's control endpoint: for each probe it records its own send/receive
wall times (t0, t3) and the receiver's receive/transmit wall times (t1, t2)
and computes

    offset = ((t1 - t0) + (t2 - t3)) / 2      # receiver_clock - sender_clock
    rtt    = (t3 - t0) - (t2 - t1)

The probe with the smallest RTT wins; its worst-case offset error is RTT/2
(reached only under fully asymmetric routing). Probing before and after the
run exposes clock drift, which widens the reported uncertainty. The analyzer
subtracts the offset from cross-host wall-time deltas and reports one-way
latencies together with this uncertainty — a latency is only meaningful when
it is large against the bound. For WAN setups, run chrony/NTP on both hosts
to keep the true error well below the probe bound.
"""

from __future__ import annotations

import asyncio
import re
import time
from typing import Any

import aiohttp

from eccom.agents.control import CLOCK_PATH

PROBE_METHOD = "control-endpoint-ntp-probe"
CHRONY_METHOD = "chrony-tracking"


def offset_and_rtt(t0: int, t1: int, t2: int, t3: int) -> tuple[int, int]:
    """Offset (receiver clock minus sender clock) and RTT from one exchange."""
    offset = ((t1 - t0) + (t2 - t3)) // 2
    rtt = (t3 - t0) - (t2 - t1)
    return offset, rtt


async def probe_clock_offset(
    control_base_url: str, samples: int = 15, timeout_s: float = 5.0
) -> dict[str, Any]:
    """Run ``samples`` probes against a control endpoint; keep the best one."""
    url = control_base_url.rstrip("/") + CLOCK_PATH
    measurements: list[tuple[int, int]] = []  # (rtt, offset)
    async with aiohttp.ClientSession() as session:
        for _ in range(samples):
            t0 = time.time_ns()
            async with session.get(
                url, timeout=aiohttp.ClientTimeout(total=timeout_s)
            ) as response:
                data = await response.json()
            t3 = time.time_ns()
            offset, rtt = offset_and_rtt(
                t0, data["receive_wall_time_ns"], data["transmit_wall_time_ns"], t3
            )
            measurements.append((rtt, offset))
            await asyncio.sleep(0.02)
    best_rtt, best_offset = min(measurements)
    return {
        "offset_ns": best_offset,
        "uncertainty_ns": max(best_rtt // 2, 0),
        "min_rtt_ns": best_rtt,
        "samples": len(measurements),
        "measured_at_wall_ns": time.time_ns(),
    }


def combine_clock_probes(
    before: dict[str, Any], after: dict[str, Any] | None
) -> dict[str, Any]:
    """Merge pre-run and post-run probes into the run's clock sync record."""
    if after is None:
        return {
            "method": PROBE_METHOD,
            "offset_ns": before["offset_ns"],
            "uncertainty_ns": before["uncertainty_ns"],
            "drift_ns": None,
            "before": before,
            "after": None,
        }
    drift_ns = after["offset_ns"] - before["offset_ns"]
    return {
        "method": PROBE_METHOD,
        "offset_ns": (before["offset_ns"] + after["offset_ns"]) // 2,
        # Drift between the snapshots is unmodelled error: widen the bound.
        "uncertainty_ns": max(before["uncertainty_ns"], after["uncertainty_ns"])
        + abs(drift_ns) // 2,
        "drift_ns": drift_ns,
        "before": before,
        "after": after,
    }


# --- host clock discipline (chrony) --------------------------------------------
#
# The probe above bounds the offset by RTT/2 of the measured WAN link, because
# it can only synchronize over that link. A host disciplined by chronyd against
# a nearby reference carries a much tighter bound over a much shorter path:
# |host clock - true time| <= |system time offset| + root_delay/2 + root_dispersion.
# When BOTH hosts report that, the true inter-host offset is bounded by the sum
# of the two per-host errors — independently of the measured link's asymmetry.
# The deployments snapshot `chronyc tracking` per host into run.json, and the
# analyzer uses whichever bound (probe or chrony) is tighter.

def parse_chrony_tracking(text: str) -> dict[str, Any] | None:
    """Parse ``chronyc tracking`` output into the fields the analyzer needs.

    Parses the labeled human-readable format (stable across chrony 3.x/4.x)
    rather than ``-c`` CSV, whose column order is positional and undocumented.
    Returns None when the text does not look like tracking output.
    """
    fields: dict[str, str] = {}
    for line in text.splitlines():
        key, sep, value = line.partition(":")
        if sep:
            fields[key.strip()] = value.strip()
    if "Stratum" not in fields or "System time" not in fields:
        return None

    def seconds(name: str) -> float | None:
        # e.g. "0.000109380 seconds fast of NTP time" / "0.006372227 seconds"
        raw = fields.get(name, "")
        parts = raw.split()
        try:
            value = float(parts[0])
        except (IndexError, ValueError):
            return None
        return -value if " slow" in raw else value

    try:
        stratum = int(fields["Stratum"])
    except ValueError:
        return None
    match = re.search(r"\(([^)]+)\)", fields.get("Reference ID", ""))
    return {
        "reference": match.group(1) if match else None,
        "stratum": stratum,
        "system_time_offset_s": seconds("System time"),
        "root_delay_s": seconds("Root delay"),
        "root_dispersion_s": seconds("Root dispersion"),
        "leap_status": fields.get("Leap status"),
    }


def chrony_max_error_s(tracking: dict[str, Any]) -> float | None:
    """Worst-case error of the host clock against true time, from tracking data.

    |offset| + root_delay/2 + root_dispersion: the root_delay/2 term is the
    same path-asymmetry bound the WAN probe pays, but over the short path to
    the host's own time source. None when the host is not synchronized or a
    field is missing.
    """
    if tracking.get("leap_status") != "Normal" or tracking.get("stratum", 0) <= 0:
        return None
    offset = tracking.get("system_time_offset_s")
    delay = tracking.get("root_delay_s")
    dispersion = tracking.get("root_dispersion_s")
    if offset is None or delay is None or dispersion is None:
        return None
    return abs(offset) + delay / 2 + dispersion
