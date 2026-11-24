"""Test scenario runner: drives the full experiment lifecycle.

load configs -> create run dir -> record metadata -> start receiver ->
health check -> run sender (warmup, load, drain, cooldown happen inside the
sender agent) -> graceful receiver shutdown -> collect artifacts -> analyze.

Which infrastructure each topology role runs on comes from the hosts
configuration (local subprocesses by default), so scenarios and hosts mix
and match freely.
"""

from __future__ import annotations

import asyncio
import json
import logging
import shutil
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiohttp

from eccom.agents.control import HEALTH_PATH, SHUTDOWN_PATH
from eccom.analysis.run import analyze_run
from eccom.core.config import ScenarioConfig, load_scenario
from eccom.core.hosts import HostConfig, HostsConfig, load_hosts, local_hosts
from eccom.instrumentation.clock_sync import CHRONY_METHOD
from eccom.orchestration.broker import MqttBrokerManager
from eccom.orchestration.deployment import AgentProcess, Deployment
from eccom.orchestration.link import LinkCapacityProbe
from eccom.orchestration.local import LocalProcessDeployment

logger = logging.getLogger(__name__)

_HEALTH_TIMEOUT_S = 20.0


def _make_deployment(host: HostConfig) -> Deployment:
    if host.deployment == "ssh":
        # Imported lazily so local-only use never needs asyncssh at import time.
        from eccom.orchestration.ssh import SshDeployment

        assert host.ssh is not None  # enforced by config validation
        return SshDeployment(host.ssh)
    return LocalProcessDeployment()


class TestRunner:
    def __init__(
        self,
        scenario_path: Path,
        runs_root: Path = Path("runs"),
        hosts_path: Path | None = None,
        analyze: bool = True,
    ) -> None:
        """``analyze=False`` leaves the run dir unanalysed (``eccom analyze``
        later, e.g. in parallel with the next run of a suite)."""
        self._scenario_path = scenario_path
        self._runs_root = runs_root
        self._hosts_path = hosts_path
        self._analyze = analyze

    async def run(self) -> Path:
        """Execute the scenario and return the run directory."""
        scenario = load_scenario(self._scenario_path)
        hosts = (
            load_hosts(self._hosts_path)
            if self._hosts_path is not None
            else local_hosts(scenario.topology.sender, scenario.topology.receiver)
        )
        deployments = {
            label: _make_deployment(hosts.for_role(label))
            for label in (scenario.topology.sender, scenario.topology.receiver)
        }
        # A scenario may name a topology role as its endpoint host ("cloud");
        # only the hosts file knows that role's address, so it is resolved
        # here and handed to every agent as --endpoint-host.
        scenario = scenario.with_connect_host(
            hosts.resolve_address(scenario.protocol.connect_host)
        )
        endpoint = ("--endpoint-host", scenario.protocol.connect_host)

        run_dir = self._create_run_dir(scenario)
        logger.info("run directory: %s", run_dir)
        shutil.copyfile(self._scenario_path, run_dir / "scenario.yaml")
        await self._write_run_metadata(scenario, hosts, deployments, run_dir)

        receiver_deployment = deployments[scenario.topology.receiver]
        sender_deployment = deployments[scenario.topology.sender]
        # The receiver's control endpoint as seen from this machine.
        control_host = hosts.for_role(scenario.topology.receiver).address
        broker = self._make_broker_manager(scenario, hosts, deployments)
        collected = False
        try:
            # Sync code / build the agent images each target needs (parallel;
            # each host gets the program the scenario picked for its role)
            # before anything starts.
            implementations = {
                scenario.topology.sender: frozenset({scenario.sender.implementation}),
                scenario.topology.receiver: frozenset({scenario.receiver.implementation}),
            }
            await asyncio.gather(
                *(
                    deployment.prepare(implementations[label])
                    for label, deployment in deployments.items()
                )
            )
            if scenario.instrumentation.link_capacity:
                await self._probe_link_capacity(scenario, deployments, run_dir)
            if broker is not None:
                await broker.start(run_dir)
            receiver = await receiver_deployment.start_agent(
                "receiver",
                self._scenario_path,
                run_dir,
                extra_args=endpoint,
                implementation=scenario.receiver.implementation,
            )
            senders: list[AgentProcess] = []
            try:
                await self._wait_until_receiver_ready(scenario, control_host, receiver, run_dir)
                instances = scenario.load.senders
                logger.info(
                    "receiver healthy, starting %s",
                    "sender" if instances == 1 else f"{instances} sender processes",
                )
                # Only instance 0 probes the clock offset (one probe per host is
                # enough); the instances start together so their schedules are
                # aligned to within process start-up jitter.
                peer_control = ("--peer-control", f"{control_host}:{scenario.receiver.control_port}")
                senders = list(
                    await asyncio.gather(
                        *(
                            sender_deployment.start_agent(
                                "sender",
                                self._scenario_path,
                                run_dir,
                                extra_args=(*endpoint, *peer_control) if index == 0 else endpoint,
                                instance=None if instances == 1 else index,
                                implementation=scenario.sender.implementation,
                                cpus=scenario.sender.container_cpus,
                            )
                            for index in range(instances)
                        )
                    )
                )
                exit_codes = await asyncio.gather(*(sender.wait() for sender in senders))
                failed = [(index, code) for index, code in enumerate(exit_codes) if code != 0]
                if failed:
                    raise RuntimeError(
                        "sender agent exited with a non-zero code "
                        f"(instance, code: {failed}), see {run_dir / 'logs'}"
                    )
            finally:
                for sender in senders:
                    if sender.returncode is None:
                        await sender.terminate()
                await self._stop_receiver(scenario, control_host, receiver)
            for deployment in deployments.values():
                await deployment.collect_artifacts(run_dir)
            collected = True
        finally:
            if broker is not None:
                try:
                    await broker.stop()
                except Exception:
                    logger.warning("stopping the managed broker failed", exc_info=True)
            if collected:
                # The local run dir is now the record; the remote copies would
                # otherwise fill a small host's disk within a few suites.
                for deployment in deployments.values():
                    try:
                        await deployment.discard_run(run_dir)
                    except Exception:
                        logger.warning("discarding the remote run dir failed", exc_info=True)
            for deployment in deployments.values():
                await deployment.close()
        if self._analyze:
            analyze_run(run_dir)
        return run_dir

    def _make_broker_manager(
        self, scenario: ScenarioConfig, hosts: HostsConfig, deployments: dict[str, Deployment]
    ) -> MqttBrokerManager | None:
        if not scenario.protocol.requires_broker or not hosts.broker.managed:
            return None
        label = hosts.broker.run_on or scenario.topology.receiver
        if label not in deployments:
            raise ValueError(
                f"broker.run_on={label!r} must be one of the topology roles "
                f"({sorted(deployments)})"
            )
        return MqttBrokerManager(
            hosts.broker,
            deployments[label],
            scenario.protocol,
            self._scenario_path.parent,
        )

    async def _probe_link_capacity(
        self, scenario: ScenarioConfig, deployments: dict[str, Deployment], run_dir: Path
    ) -> None:
        """iperf3 between the sender's and the receiver's host, both
        directions, while nothing listens on the scenario's port yet; the
        record is written to ``<sender>-link-capacity.json`` in the run dir."""
        sender, receiver = scenario.topology.sender, scenario.topology.receiver
        port = scenario.protocol.connect_port
        logger.info("probing link capacity %s -> %s with iperf3 on port %d", sender, receiver, port)
        probe = LinkCapacityProbe(
            server=deployments[receiver],
            client=deployments[sender],
            server_address=scenario.protocol.connect_host,
            port=port,
            duration_s=scenario.instrumentation.link_capacity_duration_s,
            run_id=run_dir.name,
        )
        record = await probe.measure()
        record.update({"client_host": sender, "server_host": receiver})
        (run_dir / f"{sender}-link-capacity.json").write_text(
            json.dumps(record, indent=2), encoding="utf-8"
        )
        for name, result in record["directions"].items():
            if "error" in result:
                logger.warning("link capacity %s: %s", name, result["error"])
            else:
                logger.info("link capacity %s: %.1f Mbit/s", name, result["mbit_s"])

    def _create_run_dir(self, scenario: ScenarioConfig) -> Path:
        run_id = f"{time.strftime('%Y%m%dT%H%M%S')}-{scenario.name}"
        run_dir = self._runs_root / run_id
        suffix = 1
        while run_dir.exists():
            suffix += 1
            run_dir = self._runs_root / f"{run_id}-{suffix}"
        run_dir.mkdir(parents=True)
        return run_dir

    async def _write_run_metadata(
        self,
        scenario: ScenarioConfig,
        hosts: HostsConfig,
        deployments: dict[str, Deployment],
        run_dir: Path,
    ) -> None:
        labels = list(deployments)
        infos = await asyncio.gather(
            *(dep.clock_sync_info() for dep in deployments.values())
        )
        clock_infos = dict(zip(labels, infos))
        if all(info["method"] == "same_host" for info in clock_infos.values()):
            clock_sync: dict[str, Any] = next(iter(clock_infos.values()))
        else:
            # When every host is chrony-disciplined, the true inter-host offset
            # is bounded by the sum of the per-host errors — usually far tighter
            # than the WAN probe's RTT/2, and independent of link asymmetry.
            # The analyzer applies whichever bound is smaller.
            combined_max_error_s = None
            if all(
                info["method"] == CHRONY_METHOD and info.get("max_error_s") is not None
                for info in clock_infos.values()
            ):
                combined_max_error_s = sum(
                    info["max_error_s"] for info in clock_infos.values()
                )
            clock_sync = {
                "method": None,
                "synchronized": True if combined_max_error_s is not None else None,
                "combined_max_error_s": combined_max_error_s,
                "note": (
                    "every host is chrony-disciplined; true inter-host offset is "
                    "bounded by combined_max_error_s (sum of per-host bounds)"
                    if combined_max_error_s is not None
                    else "hosts are not verified as synchronized; see per-host details"
                ),
                "per_host": clock_infos,
            }
        metadata: dict[str, Any] = {
            "run_id": run_dir.name,
            "scenario_name": scenario.name,
            "scenario_path": str(self._scenario_path),
            "created_at": datetime.now(UTC).isoformat(),
            "deployments": {label: type(dep).__name__ for label, dep in deployments.items()},
            "clock_sync": clock_sync,
        }
        (run_dir / "run.json").write_text(json.dumps(metadata, indent=2), encoding="utf-8")
        (run_dir / "hosts.json").write_text(
            json.dumps(hosts.redacted_dump(), indent=2), encoding="utf-8"
        )

    async def _wait_until_receiver_ready(
        self, scenario: ScenarioConfig, control_host: str, receiver: AgentProcess, run_dir: Path
    ) -> None:
        url = self._control_url(control_host, scenario, HEALTH_PATH)
        deadline = time.monotonic() + _HEALTH_TIMEOUT_S
        async with aiohttp.ClientSession() as session:
            while time.monotonic() < deadline:
                if receiver.returncode is not None:
                    raise RuntimeError(
                        f"receiver agent exited early with code {receiver.returncode}, "
                        f"see {run_dir / 'logs' / 'receiver.log'}"
                    )
                try:
                    async with session.get(
                        url, timeout=aiohttp.ClientTimeout(total=1)
                    ) as response:
                        if response.status == 200:
                            return
                except (aiohttp.ClientError, TimeoutError, asyncio.TimeoutError):
                    pass
                await asyncio.sleep(0.2)
        raise TimeoutError(f"receiver did not become healthy within {_HEALTH_TIMEOUT_S}s")

    async def _stop_receiver(
        self, scenario: ScenarioConfig, control_host: str, receiver: AgentProcess
    ) -> None:
        if receiver.returncode is not None:
            return
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    self._control_url(control_host, scenario, SHUTDOWN_PATH),
                    timeout=aiohttp.ClientTimeout(total=5),
                ):
                    pass
            await asyncio.wait_for(receiver.wait(), timeout=10)
        except (aiohttp.ClientError, TimeoutError, asyncio.TimeoutError):
            logger.warning("graceful receiver shutdown failed, terminating process")
            await receiver.terminate()

    @staticmethod
    def _control_url(control_host: str, scenario: ScenarioConfig, path: str) -> str:
        return f"http://{control_host}:{scenario.receiver.control_port}{path}"
