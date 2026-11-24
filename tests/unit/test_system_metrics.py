import asyncio
import json
import time
from pathlib import Path

from eccom.instrumentation.system_metrics import SystemMetricsSampler


async def test_samples_carry_the_worst_event_loop_stall_since_the_previous_sample(
    tmp_path: Path,
) -> None:
    path = tmp_path / "edge-system-metrics.jsonl"
    sampler = SystemMetricsSampler("edge", interval_s=0.05, output_path=path)
    await sampler.start()
    await asyncio.sleep(0.15)  # a few quiet samples
    time.sleep(0.2)  # block the loop: nothing, the lag probe included, can run
    await asyncio.sleep(0.15)  # the stall lands in one of these samples
    await sampler.stop()

    samples = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]
    assert len(samples) >= 3
    lags = [s["loop_lag_ms_max"] for s in samples]
    assert max(lags) >= 150  # the 200 ms block, minus the tick already elapsed
    assert min(lags) < 50  # quiet samples sit at the OS timer floor
    for key in ("process_cpu_seconds", "net_bytes_sent", "net_bytes_recv", "process_rss_bytes"):
        assert key in samples[0]
