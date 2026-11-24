"""Local deployment: both agents run as subprocesses on this machine.

The Python agent runs as ``python -m eccom agent``; the Go agent runs as the
binary ``go/bin/eccom-agent`` of this checkout, which ``prepare()`` builds
when a Go toolchain is on PATH and otherwise expects to exist already
(e.g. cross-compiled elsewhere).
"""

from __future__ import annotations

import asyncio
import shutil
import sys
from collections.abc import Sequence
from pathlib import Path
from typing import IO, Any

from eccom.agents.runtime import Role
from eccom.core.config import Implementation
from eccom.orchestration.deployment import (
    PYTHON_ONLY,
    AgentProcess,
    Deployment,
    local_source_root,
)

GO_MODULE_DIR = "go"
GO_AGENT_PACKAGE = "./cmd/eccom-agent"


def go_agent_binary(root: Path | None = None) -> Path:
    """Where the locally built Go agent lives (platform-specific suffix)."""
    root = root or local_source_root()
    suffix = ".exe" if sys.platform == "win32" else ""
    return root / GO_MODULE_DIR / "bin" / f"eccom-agent{suffix}"


def agent_command(
    implementation: Implementation,
    role: Role,
    scenario_path: Path,
    run_dir: Path,
    extra_args: Sequence[str] = (),
) -> list[str]:
    """The local command line that starts one agent process."""
    program = (
        [sys.executable, "-m", "eccom"]
        if implementation == "python"
        else [str(go_agent_binary())]
    )
    return [
        *program,
        "agent",
        "--role",
        role,
        "--scenario",
        str(scenario_path),
        "--run-dir",
        str(run_dir),
        *extra_args,
    ]


class LocalAgentProcess(AgentProcess):
    def __init__(self, process: asyncio.subprocess.Process, log_handle: IO[bytes]) -> None:
        self._process = process
        self._log_handle = log_handle

    @property
    def returncode(self) -> int | None:
        return self._process.returncode

    async def wait(self) -> int:
        returncode = await self._process.wait()
        self._close_log()
        return returncode

    async def terminate(self) -> None:
        if self._process.returncode is None:
            self._process.terminate()
            try:
                await asyncio.wait_for(self._process.wait(), timeout=5)
            except TimeoutError:
                self._process.kill()
                await self._process.wait()
        self._close_log()

    def _close_log(self) -> None:
        if not self._log_handle.closed:
            self._log_handle.close()


class LocalProcessDeployment(Deployment):
    async def prepare(self, implementations: frozenset[Implementation] = PYTHON_ONLY) -> None:
        if "go" in implementations:
            await self._build_go_agent()

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
        logs_dir = run_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_name = f"{role}.log" if instance is None else f"{role}.{instance}.log"
        log_handle = (logs_dir / log_name).open("wb")
        if instance is not None:
            extra_args = (*extra_args, "--instance", str(instance))
        process = await asyncio.create_subprocess_exec(
            *agent_command(implementation, role, scenario_path, run_dir, extra_args),
            stdout=log_handle,
            stderr=asyncio.subprocess.STDOUT,
        )
        return LocalAgentProcess(process, log_handle)

    async def push_run_dir(self, run_dir: Path, relative: str) -> str:
        return str((run_dir / relative).resolve())

    async def run_command(self, args: list[str]) -> tuple[int, str]:
        process = await asyncio.create_subprocess_exec(
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        try:
            output, _ = await process.communicate()
        except asyncio.CancelledError:
            process.kill()
            raise
        assert process.returncode is not None
        return process.returncode, output.decode(errors="replace")

    async def clock_sync_info(self) -> dict[str, Any]:
        return {
            "method": "same_host",
            "synchronized": True,
            "note": "sender and receiver ran on the same machine and clock",
        }

    async def _build_go_agent(self) -> None:
        """``go build`` the agent binary; without a toolchain a prebuilt
        binary is used as is."""
        root = local_source_root()
        binary = go_agent_binary(root)
        if shutil.which("go") is None:
            if binary.is_file():
                return
            raise RuntimeError(
                f"implementation: go needs the Go agent at {binary}: install a Go "
                f"toolchain (go build runs automatically) or build the binary there"
            )
        process = await asyncio.create_subprocess_exec(
            "go", "build", "-o", str(binary), GO_AGENT_PACKAGE,
            cwd=root / GO_MODULE_DIR,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
        output, _ = await process.communicate()
        if process.returncode != 0:
            raise RuntimeError(f"building the Go agent failed:\n{output.decode(errors='replace')}")
