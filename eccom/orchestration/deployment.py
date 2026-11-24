"""Deployment abstraction.

One Deployment instance manages one target host. Implementations (local
subprocess, SSH to a remote host) provide the same
operations: start an agent on the host, collect its artifacts into the run
directory, describe how its clock relates to others, and release resources.
The runner resolves which implementation serves which topology role from the
hosts configuration (see eccom.core.hosts).

An agent *program* is chosen per role by the scenario
(``sender.implementation`` / ``receiver.implementation``): the Python package
or the Go agent under ``go/``. Both take the same command line and write the
same artifacts, so a deployment only has to pick the image or binary.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import eccom
from eccom.agents.runtime import Role
from eccom.core.config import Implementation

PYTHON_ONLY: frozenset[Implementation] = frozenset({"python"})


def local_source_root() -> Path:
    """The local eccom checkout: the build context synced to SSH targets and
    the location of the Go module for local runs."""
    root = Path(eccom.__file__).resolve().parents[1]
    if not (root / "pyproject.toml").exists():
        raise RuntimeError(
            "deployments must run from an eccom source checkout "
            f"(no pyproject.toml found at {root})"
        )
    return root


class AgentProcess(ABC):
    """Handle to one running agent."""

    @property
    @abstractmethod
    def returncode(self) -> int | None:
        """Exit code, or None while still running."""

    @abstractmethod
    async def wait(self) -> int:
        """Wait for the agent to exit and return its exit code."""

    @abstractmethod
    async def terminate(self) -> None:
        """Forcefully stop the agent (used when graceful shutdown fails)."""


class Deployment(ABC):
    async def prepare(self, implementations: frozenset[Implementation] = PYTHON_ONLY) -> None:
        """One-time per-run setup on the target host for the agent programs
        that will run there (e.g. sync the source tree and build their
        images). No-op by default."""

    @abstractmethod
    async def start_agent(
        self,
        role: Role,
        scenario_path: Path,
        run_dir: Path,
        extra_args: Sequence[str] = (),
        instance: int | None = None,
        implementation: Implementation = "python",
        cpus: float | None = None,
    ) -> AgentProcess:
        """Start one agent process for ``role`` on its target host.

        ``extra_args`` are appended to the ``agent`` command line (e.g.
        --peer-control for the sender's clock offset probes). ``instance``
        numbers one of several processes of the same role
        (``load.senders > 1``): it is passed to the agent as ``--instance`` and
        keeps log and container names apart; None means the only process.
        ``implementation`` selects the agent program. ``cpus`` caps the
        agent's container at that many cores (``docker --cpus``, replacing
        the host's ``resources.cpus``); ignored where agents are bare
        processes."""

    async def collect_artifacts(self, run_dir: Path) -> None:
        """Copy remote artifacts into ``run_dir``. No-op when hosts share a
        filesystem (local deployment)."""

    async def discard_run(self, run_dir: Path) -> None:
        """Remove the run's remote working copy once ``collect_artifacts`` has
        made ``run_dir`` the record. No-op when nothing is remote."""

    def container_flags(self) -> list[str]:
        """Extra ``docker run`` flags for every container started on this host
        (resource caps from the hosts file). None by default."""
        return []

    def container_image(self) -> str | None:
        """An agent image on this host, when auxiliary tools (the link
        probe's iperf3) run inside it; None runs them as bare host commands."""
        return None

    async def run_command(self, args: list[str]) -> tuple[int, str]:
        """Run one auxiliary command on the target host (e.g. managing a
        broker container). Returns (exit code, combined output)."""
        raise NotImplementedError(f"{type(self).__name__} cannot run commands")

    async def push_run_dir(self, run_dir: Path, relative: str) -> str:
        """Make the local directory ``run_dir/relative`` available on the
        target host and return its host-absolute path (e.g. for Docker bind
        mounts). No-op path translation for local deployments."""
        raise NotImplementedError(f"{type(self).__name__} cannot push files")

    async def close(self) -> None:
        """Release connections/resources held by this deployment."""

    async def clock_sync_info(self) -> dict[str, Any]:
        """Describe this host's clock discipline (or the hosts' clock
        relationship); stored in the run metadata. Async so implementations
        can query the target host."""
        return {
            "method": None,
            "synchronized": None,
            "note": "clock relationship between hosts is unknown",
        }
