"""Agent process entry points.

The same code runs on Edge and Cloud; only ``role`` and the scenario decide
the behavior. A receiver serves until its control endpoint (or SIGTERM)
requests shutdown; a sender runs the configured load to completion.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import signal
from dataclasses import asdict
from pathlib import Path
from typing import Literal

from eccom.agents.control import ControlServer
from eccom.agents.receiver import ReceiverAgent
from eccom.agents.sender import SenderAgent
from eccom.core.artifacts import instance_label
from eccom.core.config import ScenarioConfig, load_scenario
from eccom.core.events import EventRecorder
from eccom.instrumentation.clock_sync import combine_clock_probes, probe_clock_offset
from eccom.instrumentation.environment import write_environment_file
from eccom.instrumentation.system_metrics import SystemMetricsSampler
from eccom.protocols import create_adapter

logger = logging.getLogger(__name__)

Role = Literal["sender", "receiver"]


async def run_agent(
    role: Role,
    scenario_path: Path,
    run_dir: Path,
    peer_control: str | None = None,
    instance: int = 0,
    endpoint_host: str | None = None,
) -> None:
    """``instance`` numbers this process among ``load.senders`` sender
    processes (0-based); it selects the schedule share and the artifact names.
    ``endpoint_host`` replaces the scenario's endpoint host: the runner passes
    the address it resolved for a topology role label such as ``cloud``."""
    scenario = load_scenario(scenario_path)
    if endpoint_host is not None:
        scenario = scenario.with_connect_host(endpoint_host)
    run_dir.mkdir(parents=True, exist_ok=True)
    # The analyzer needs the scenario inside the run dir. The runner puts it
    # there itself; in runner-less flows (docker compose) the agents do.
    scenario_copy = run_dir / "scenario.yaml"
    if not scenario_copy.exists():
        shutil.copyfile(scenario_path, scenario_copy)
    scenario_dir = scenario_path.parent  # TLS file paths resolve against this
    if role == "receiver":
        await _run_receiver(scenario, scenario_dir, run_dir)
    else:
        await _run_sender(scenario, scenario_dir, run_dir, peer_control, instance)


async def _run_receiver(scenario: ScenarioConfig, scenario_dir: Path, run_dir: Path) -> None:
    host = scenario.topology.receiver
    write_environment_file(host, run_dir)
    recorder = EventRecorder(host, run_dir / f"{host}-events.jsonl")
    adapter = create_adapter(scenario.protocol, recorder, scenario_dir)
    agent = ReceiverAgent(scenario, adapter, recorder)
    control = ControlServer(scenario.receiver.control_bind_host, scenario.receiver.control_port)
    sampler = _make_sampler(scenario, host, run_dir)
    try:
        await adapter.start()
        await agent.start()
        await control.start()
        if sampler is not None:
            await sampler.start()
            await sampler.measure_baseline(scenario.instrumentation.baseline_s)
        _request_shutdown_on_signals(control.shutdown_requested)
        logger.info(
            "receiver ready (protocol=%s, control port %d)",
            scenario.protocol.type,
            scenario.receiver.control_port,
        )
        await control.shutdown_requested.wait()
        logger.info("receiver shutting down (%d events recorded)", len(recorder))
    finally:
        if sampler is not None:
            await sampler.stop()
        await adapter.stop()
        await control.stop()
        recorder.close()


async def _run_sender(
    scenario: ScenarioConfig,
    scenario_dir: Path,
    run_dir: Path,
    peer_control: str | None,
    instance: int = 0,
) -> None:
    host = scenario.topology.sender
    # Per-instance artifacts carry the instance in their name (edge.0-...);
    # per-host ones (environment, clock probe) are written by instance 0 only.
    label = instance_label(host, instance, scenario.load.senders)
    if instance == 0:
        write_environment_file(host, run_dir)
    recorder = EventRecorder(host, run_dir / f"{label}-events.jsonl")
    adapter = create_adapter(scenario.protocol, recorder, scenario_dir)
    agent = SenderAgent(scenario, adapter, recorder, instance=instance)
    sampler = _make_sampler(scenario, host, run_dir, label)
    try:
        await adapter.start()
        if sampler is not None:
            await sampler.start()
        clock_before = await _probe_clock(peer_control)
        if sampler is not None:
            # after the probe, before any traffic: a genuinely quiet window
            await sampler.measure_baseline(scenario.instrumentation.baseline_s)
        report = await agent.run()
        report_path = run_dir / f"{label}-load-report.json"
        report_path.write_text(json.dumps(asdict(report), indent=2), encoding="utf-8")
        if scenario.experiment.cooldown_s > 0:
            await asyncio.sleep(scenario.experiment.cooldown_s)
        if clock_before is not None:
            clock_sync = combine_clock_probes(clock_before, await _probe_clock(peer_control))
            (run_dir / f"{host}-clock-sync.json").write_text(
                json.dumps(clock_sync, indent=2), encoding="utf-8"
            )
            logger.info(
                "clock offset vs receiver: %.3f ms (± %.3f ms)",
                clock_sync["offset_ns"] / 1e6,
                clock_sync["uncertainty_ns"] / 1e6,
            )
    finally:
        if sampler is not None:
            await sampler.stop()
        await adapter.stop()
        recorder.close()


async def _probe_clock(peer_control: str | None) -> dict | None:
    """Measure the clock offset against the receiver's control endpoint."""
    if peer_control is None:
        return None
    try:
        return await probe_clock_offset(f"http://{peer_control}")
    except Exception as exc:
        logger.warning("clock offset probe against %s failed: %r", peer_control, exc)
        return None


def _make_sampler(
    scenario: ScenarioConfig, host: str, run_dir: Path, label: str | None = None
) -> SystemMetricsSampler | None:
    if not scenario.instrumentation.system_metrics:
        return None
    return SystemMetricsSampler(
        host=host,
        interval_s=scenario.instrumentation.system_metrics_interval_s,
        output_path=run_dir / f"{label or host}-system-metrics.jsonl",
    )


def _request_shutdown_on_signals(shutdown_requested: asyncio.Event) -> None:
    """Flush events on SIGTERM/SIGINT (e.g. `docker compose stop`).

    Not supported by the Windows Proactor loop; there the control endpoint is
    the only graceful shutdown path.
    """
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, shutdown_requested.set)
        except (NotImplementedError, RuntimeError):
            return
