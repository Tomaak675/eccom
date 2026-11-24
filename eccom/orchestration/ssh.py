"""SSH deployment: run agents as Docker containers on a remote host.

Resilience: the connection sends keepalives and is reopened
when lost; an agent container runs without ``--rm`` and, if the channel
that started it dies, is followed through ``docker inspect`` over a fresh
connection until it exits, its log then taken from ``docker logs``.

The target needs only Docker and SSH — no repo clone and
no Python or Go environment. ``prepare()`` syncs the local source tree to
``<working_dir>/src`` and builds the agent images the host needs *natively
on the host* (so ARM64 and x86-64 hosts each get a matching image;
Docker layer caching makes code-only rebuilds take seconds): the Python
agent from ``docker/Dockerfile`` as ``<image>``, the Go agent from
``docker/Dockerfile.go`` as ``<image>-go``. Agents run with
``--network host`` so no Docker NAT sits inside the measured path. Run
artifacts land in ``<working_dir>/runs/<run-id>`` via a bind mount, agent
logs stream back live, and everything is collected over SFTP after the run.

Host key verification is disabled (freshly provisioned test hosts); tighten
this before using untrusted networks.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import shlex
import time
from collections.abc import Awaitable, Callable, Sequence
from pathlib import Path
from typing import IO, Any

import asyncssh

from eccom.agents.runtime import Role
from eccom.core.config import Implementation
from eccom.core.hosts import SshConfig
from eccom.instrumentation.clock_sync import (
    CHRONY_METHOD,
    chrony_max_error_s,
    parse_chrony_tracking,
)
from eccom.instrumentation.environment import git_commit
from eccom.orchestration.deployment import (
    PYTHON_ONLY,
    AgentProcess,
    Deployment,
    local_source_root,
)

logger = logging.getLogger(__name__)

# Open-file limit for agent containers; see the docker run flags below.
NOFILE_LIMIT = 65536
# Upper bound on `docker stop` when a graceful agent shutdown has already failed.
STOP_TIMEOUT_S = 30
# SSH keepalives: a session whose NAT mapping died silently is detected
# after KEEPALIVE_INTERVAL_S * KEEPALIVE_COUNT_MAX instead of never.
KEEPALIVE_INTERVAL_S = 15
KEEPALIVE_COUNT_MAX = 3
# When the channel that started a container dies, its state is polled through
# a fresh connection instead; the poll gives up after RECOVERY_TIMEOUT_S.
POLL_INTERVAL_S = 5
RECOVERY_TIMEOUT_S = 1800
# A run writes up to a few GB of artifacts on each host; below this much free
# space prepare() warns (a full disk truncates the events mid-line).
MIN_FREE_GIB = 5

_SOURCE_FILES = (
    Path("pyproject.toml"),
    Path("README.md"),
    Path("docker/Dockerfile"),
    Path("docker/Dockerfile.go"),
)
# Source trees shipped as the build context: the Python package and the Go
# module (its locally built binary excluded; the image compiles its own).
_SOURCE_TREES = (("eccom", ("__pycache__",)), ("go", ("bin",)))
_DOCKERFILES: dict[Implementation, str] = {
    "python": "docker/Dockerfile",
    "go": "docker/Dockerfile.go",
}


def iter_source_files(root: Path) -> list[Path]:
    """Repo-relative files that make up the agent images' build context."""
    files = list(_SOURCE_FILES)
    for tree, excluded in _SOURCE_TREES:
        for path in sorted((root / tree).rglob("*")):
            relative = path.relative_to(root)
            if path.is_file() and not set(relative.parts[1:]) & set(excluded):
                files.append(relative)
    return files


def _without_flag(flags: list[str], name: str) -> list[str]:
    """``flags`` minus every ``name <value>`` pair."""
    kept: list[str] = []
    skip = False
    for flag in flags:
        if skip:
            skip = False
        elif flag == name:
            skip = True
        else:
            kept.append(flag)
    return kept


def _container_name(role: str, run_id: str, instance: int | None = None) -> str:
    suffix = "" if instance is None else f".{instance}"
    return "eccom-" + re.sub(r"[^a-zA-Z0-9_.-]", "-", f"{role}-{run_id}{suffix}")


RunCommand = Callable[[list[str]], Awaitable[tuple[int, str]]]


class SshAgentProcess(AgentProcess):
    """One agent container, followed through the SSH channel that started it
    and, if that channel dies, through Docker over a fresh connection.

    The container runs without ``--rm`` so its exit code and logs survive the
    session; it is removed after ``wait`` or ``terminate``.
    """

    def __init__(
        self,
        process: asyncssh.SSHClientProcess,
        log_task: asyncio.Task[None],
        log_handle: IO[str],
        container: str,
        run_command: RunCommand,
    ) -> None:
        self._process = process
        self._log_task = log_task
        self._log_handle = log_handle
        self._container = container
        self._run_command = run_command
        self._returncode: int | None = None

    @property
    def returncode(self) -> int | None:
        return self._returncode if self._returncode is not None else self._process.exit_status

    async def wait(self) -> int:
        """Block until the container exits.

        Returns its exit code, from the channel or, after a dead session, from Docker.
        Raises RuntimeError when the container cannot be reached for RECOVERY_TIMEOUT_S.
        """
        code = await self._channel_exit()
        if code is None:
            code = await self._exit_from_docker()
        self._returncode = code
        await self._finish_log()
        await self._remove_container()
        return code

    async def terminate(self) -> None:
        if self.returncode is None:
            # docker stop sends SIGTERM first, so the agent can flush events.
            # Bounded: on a starved host `docker stop` itself can hang, and an
            # unbounded wait here stalls the whole suite.
            with contextlib.suppress(OSError, asyncssh.Error, TimeoutError):
                await asyncio.wait_for(
                    self._run_command(["docker", "stop", "-t", "5", self._container]),
                    timeout=STOP_TIMEOUT_S,
                )
            try:
                await asyncio.wait_for(self._process.wait(), timeout=10)
            except (TimeoutError, OSError, asyncssh.Error):
                self._process.close()
                with contextlib.suppress(TimeoutError, OSError, asyncssh.Error):
                    await asyncio.wait_for(self._process.wait_closed(), timeout=10)
            self._returncode = self._process.exit_status
        await self._finish_log()
        await self._remove_container()

    async def _channel_exit(self) -> int | None:
        """The exit status from the channel; None when the session died first."""
        try:
            await self._process.wait()
        except (OSError, asyncssh.Error):
            return None
        return self._process.exit_status

    async def _exit_from_docker(self) -> int:
        """Poll the container's state through the deployment's (reconnecting)
        command path until it has exited; then replace the streamed log,
        which stopped when the session died, with Docker's complete one."""
        logger.warning(
            "SSH channel of %s closed without an exit status; following it through Docker",
            self._container,
        )
        deadline = time.monotonic() + RECOVERY_TIMEOUT_S
        inspect = ["docker", "inspect", "-f", "{{.State.Running}} {{.State.ExitCode}}", self._container]
        while time.monotonic() < deadline:
            try:
                code, output = await self._run_command(inspect)
            except (OSError, asyncssh.Error) as exc:
                logger.warning("host unreachable while following %s: %r", self._container, exc)
            else:
                running, _, exit_code = output.strip().partition(" ")
                if code == 0 and running == "false":
                    await self._replace_log_with_docker_logs()
                    return int(exit_code)
                if code != 0:
                    logger.warning("docker inspect %s failed: %s", self._container, output.strip()[:200])
            await asyncio.sleep(POLL_INTERVAL_S)
        raise RuntimeError(
            f"container {self._container} could not be followed to its exit within "
            f"{RECOVERY_TIMEOUT_S}s after the SSH session died"
        )

    async def _replace_log_with_docker_logs(self) -> None:
        code, output = await self._run_command(["docker", "logs", self._container])
        if code != 0:
            logger.warning("docker logs %s failed: %s", self._container, output.strip()[:200])
            return
        if not self._log_handle.closed:
            self._log_handle.seek(0)
            self._log_handle.truncate()
            self._log_handle.write(output)
            self._log_handle.flush()

    async def _remove_container(self) -> None:
        with contextlib.suppress(OSError, asyncssh.Error):
            await self._run_command(["docker", "rm", self._container])

    async def _finish_log(self) -> None:
        if not self._log_task.done():
            self._log_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await self._log_task
        if not self._log_handle.closed:
            self._log_handle.close()


class SshDeployment(Deployment):
    def __init__(self, config: SshConfig) -> None:
        self._config = config
        self._conn: asyncssh.SSHClientConnection | None = None
        self._synced = False
        self._image_ids: dict[Implementation, str | None] = {}
        self._uid_gid: str | None = None
        # OpenSSH admits MaxSessions (default 10) concurrent channels per
        # connection and every running agent holds one for its lifetime, so
        # the short-lived channels of agents starting concurrently
        # (load.senders > 1) are serialized: at most one of them is open
        # while up to nine agent channels accumulate.
        self._setup = asyncio.Lock()
        self._staged_runs: set[str] = set()

    def image_name(self, implementation: Implementation) -> str:
        """Tag of one agent program's image on this host."""
        return self._config.image if implementation == "python" else f"{self._config.image}-go"

    async def prepare(self, implementations: frozenset[Implementation] = PYTHON_ONLY) -> None:
        """Sync the source tree (once) and build the requested agent images
        on the target."""
        conn = await self._ensure_connected()
        if not self._synced:
            await self._warn_if_low_on_space(conn)
            await self._sync_sources(conn)
            # Containers run without --rm; an orchestrator that died leaves
            # them behind as exited, so clear those before building.
            await conn.run(
                "docker ps -aq --filter name=eccom- --filter status=exited | xargs -r docker rm",
                check=False,
            )
            self._synced = True
        for implementation in sorted(implementations):
            if implementation not in self._image_ids:
                self._image_ids[implementation] = await self._build_image(conn, implementation)

    async def _warn_if_low_on_space(self, conn: asyncssh.SSHClientConnection) -> None:
        result = await conn.run(
            f"mkdir -p {shlex.quote(self._config.working_dir)} && "
            f"df -Pk {shlex.quote(self._config.working_dir)} | tail -1",
            check=False,
        )
        fields = str(result.stdout or "").split()
        if len(fields) < 4 or not fields[3].isdigit():
            return
        free_gib = int(fields[3]) / 2**20
        if free_gib < MIN_FREE_GIB:
            logger.warning(
                "%s has only %.1f GiB free under %s; a run can need several GB",
                self._config.host, free_gib, self._config.working_dir,
            )

    async def _sync_sources(self, conn: asyncssh.SSHClientConnection) -> None:
        working_dir = self._config.working_dir
        source_dir = f"{working_dir}/src"
        await conn.run(
            f"rm -rf {shlex.quote(source_dir)} && "
            f"mkdir -p {shlex.quote(source_dir)} {shlex.quote(working_dir + '/runs')}",
            check=True,
        )
        root = local_source_root()
        files = iter_source_files(root)
        remote_dirs = sorted(
            {f"{source_dir}/{f.parent.as_posix()}" for f in files if f.parent != Path(".")}
        )
        if remote_dirs:
            await conn.run(
                "mkdir -p " + " ".join(shlex.quote(d) for d in remote_dirs), check=True
            )
        logger.info("syncing %d source files to %s:%s", len(files), self._config.host, source_dir)
        async with conn.start_sftp_client() as sftp:
            for relative in files:
                await sftp.put(str(root / relative), f"{source_dir}/{relative.as_posix()}")

    async def _build_image(
        self, conn: asyncssh.SSHClientConnection, implementation: Implementation
    ) -> str | None:
        source_dir = f"{self._config.working_dir}/src"
        image = self.image_name(implementation)
        logger.info(
            "building image %s on %s (first build may take several minutes)",
            image,
            self._config.host,
        )
        build = await conn.run(
            f"cd {shlex.quote(source_dir)} && "
            f"docker build -t {shlex.quote(image)} -f {_DOCKERFILES[implementation]} . 2>&1",
            check=False,
        )
        if build.exit_status != 0:
            tail = "\n".join(str(build.stdout or "").splitlines()[-30:])
            raise RuntimeError(
                f"building the agent image {image} on {self._config.host} failed "
                f"(is Docker installed and usable by {self._config.user}?):\n{tail}"
            )
        inspect = await conn.run(f"docker images -q {shlex.quote(image)}", check=True)
        image_id = str(inspect.stdout or "").strip() or None
        logger.info("image %s ready on %s (id %s)", image, self._config.host, image_id)
        return image_id

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
        conn = await self._ensure_connected()
        if instance is not None:
            extra_args = (*extra_args, "--instance", str(instance))
        if implementation not in self._image_ids:
            await self.prepare(frozenset({implementation}))
        image, image_id = self.image_name(implementation), self._image_ids[implementation]
        runs_dir = f"{self._config.working_dir}/runs"
        remote_run_dir = f"{runs_dir}/{run_dir.name}"
        container = _container_name(role, run_dir.name, instance)
        async with self._setup:
            await self._stage_run(conn, scenario_path, remote_run_dir)
            await conn.run(f"docker rm -f {shlex.quote(container)}", check=False)
            uid_gid = await self._resolve_uid_gid(conn)

        env_flags = f"-e HOME=/tmp -e ECCOM_IMAGE_DIGEST={shlex.quote(image_id or '')}"
        if commit := git_commit():
            env_flags += f" -e ECCOM_GIT_COMMIT={shlex.quote(commit)}"
        agent_args = (
            f"agent --role {role} "
            f"--scenario /runs/{run_dir.name}/scenario.yaml "
            f"--run-dir /runs/{run_dir.name}"
            + "".join(f" {shlex.quote(arg)}" for arg in extra_args)
        )
        # --network host keeps Docker NAT out of the measured path; --user
        # makes run artifacts owned by the SSH user instead of root; --ulimit
        # lifts the container's default 1024 open-file cap, which an HTTP
        # receiver at concurrency 1000 exhausts (one socket per in-flight
        # request) and which would then also swallow the events file at exit;
        # the optional resource caps keep a runaway agent from starving the
        # host's sshd and dockerd (see ContainerResources); the scenario's
        # per-agent CPU cap replaces the host's.
        flags = self.container_flags()
        if cpus is not None:
            flags = _without_flag(flags, "--cpus") + ["--cpus", f"{cpus:g}"]
        resource_flags = "".join(f"{shlex.quote(flag)} " for flag in flags)
        # No --rm: the container's exit code and logs must outlive the SSH
        # session that started it (SshAgentProcess removes it afterwards).
        command = (
            f"docker run --network host --name {shlex.quote(container)} "
            f"--ulimit nofile={NOFILE_LIMIT}:{NOFILE_LIMIT} {resource_flags}"
            f"--user {uid_gid} {env_flags} "
            f"-v {shlex.quote(runs_dir)}:/runs {shlex.quote(image)} "
            f"{agent_args} 2>&1"
        )
        logger.info("starting %s on %s: %s", role, self._config.host, command)
        process = await conn.create_process(command)

        logs_dir = run_dir / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_name = f"{role}.log" if instance is None else f"{role}.{instance}.log"
        log_handle = (logs_dir / log_name).open("w", encoding="utf-8", newline="\n")
        log_task = asyncio.create_task(_stream_log(process, log_handle))
        return SshAgentProcess(process, log_task, log_handle, container, self.run_command)

    def container_flags(self) -> list[str]:
        resources = self._config.resources
        return resources.docker_flags() if resources is not None else []

    def container_image(self) -> str | None:
        """Whichever agent image this host has built (both carry iperf3)."""
        built = sorted(self._image_ids)
        return self.image_name(built[0] if built else "python")

    async def collect_artifacts(self, run_dir: Path) -> None:
        conn = await self._ensure_connected()
        remote_run_dir = f"{self._config.working_dir}/runs/{run_dir.name}"
        # The JSONL artifacts (gigabytes for a multi-million-message run)
        # shrink ~8x under gzip -1; the analysis reads .jsonl.gz.
        await conn.run(
            f"cd {shlex.quote(remote_run_dir)} && ls *.jsonl >/dev/null 2>&1 && gzip -1 -f *.jsonl",
            check=False,
        )
        async with conn.start_sftp_client() as sftp:
            for remote_path in await sftp.glob(f"{remote_run_dir}/*"):
                if await sftp.isfile(remote_path):
                    await sftp.get(remote_path, str(run_dir / Path(remote_path).name))
        logger.info("collected artifacts from %s:%s", self._config.host, remote_run_dir)

    async def discard_run(self, run_dir: Path) -> None:
        """``rm -rf`` the run's remote directory, so a suite of large runs
        does not fill the host's disk.

        run_dir: the local run directory; its name selects the remote one.
        """
        conn = await self._ensure_connected()
        remote_run_dir = f"{self._config.working_dir}/runs/{run_dir.name}"
        await conn.run(f"rm -rf {shlex.quote(remote_run_dir)}", check=False)
        logger.info("discarded %s:%s", self._config.host, remote_run_dir)

    async def run_command(self, args: list[str]) -> tuple[int, str]:
        conn = await self._ensure_connected()
        result = await conn.run(shlex.join(args), check=False)
        output = f"{result.stdout or ''}{result.stderr or ''}"
        return result.exit_status if result.exit_status is not None else -1, output

    async def push_run_dir(self, run_dir: Path, relative: str) -> str:
        conn = await self._ensure_connected()
        remote_dir = f"{self._config.working_dir}/runs/{run_dir.name}/{relative}"
        await conn.run(f"mkdir -p {shlex.quote(remote_dir)}", check=True)
        async with conn.start_sftp_client() as sftp:
            for path in sorted((run_dir / relative).iterdir()):
                if path.is_file():
                    remote_path = f"{remote_dir}/{path.name}"
                    await sftp.put(str(path), remote_path)
                    # containers (e.g. the broker) run as other users
                    await conn.run(f"chmod 644 {shlex.quote(remote_path)}", check=False)
        return remote_dir

    async def _stage_run(
        self, conn: asyncssh.SSHClientConnection, scenario_path: Path, remote_run_dir: str
    ) -> None:
        """Create the remote run dir and ship the scenario with its TLS files,
        once per run: every agent instance of a run shares them.

        conn: the host's connection.
        scenario_path: the local scenario file.
        remote_run_dir: the run's directory on the host.
        """
        if remote_run_dir in self._staged_runs:
            return
        await conn.run(f"mkdir -p {shlex.quote(remote_run_dir)}", check=True)
        async with conn.start_sftp_client() as sftp:
            await sftp.put(str(scenario_path), f"{remote_run_dir}/scenario.yaml")
        # TLS files referenced by the scenario must exist next to it on the
        # target; ship them into the run dir preserving their relative paths.
        await self._push_tls_files(conn, scenario_path, remote_run_dir)
        self._staged_runs.add(remote_run_dir)

    async def _push_tls_files(
        self,
        conn: asyncssh.SSHClientConnection,
        scenario_path: Path,
        remote_run_dir: str,
    ) -> None:
        from eccom.core.config import load_scenario

        tls = load_scenario(scenario_path).protocol.tls
        if not tls.enabled:
            return
        relatives = {p for p in (tls.cert_file, tls.key_file, tls.ca_file) if p is not None}
        dirs = sorted(
            {f"{remote_run_dir}/{p.parent.as_posix()}" for p in relatives if p.parent != Path(".")}
        )
        if dirs:
            await conn.run("mkdir -p " + " ".join(shlex.quote(d) for d in dirs), check=True)
        async with conn.start_sftp_client() as sftp:
            for relative in sorted(relatives):
                local = scenario_path.parent / relative
                if not local.is_file():
                    raise FileNotFoundError(f"TLS file {local} referenced by the scenario")
                await sftp.put(str(local), f"{remote_run_dir}/{relative.as_posix()}")

    async def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            await self._conn.wait_closed()
            self._conn = None

    async def clock_sync_info(self) -> dict[str, Any]:
        """Snapshot of the host's own time discipline via ``chronyc tracking``.

        Runs on the host itself, not in the agent container — the containers
        use --network host and share the host kernel clock, so the host's
        chronyd is authoritative for every timestamp the agents record. Gives
        the analyzer a per-host error bound that is independent of the WAN
        probe (see eccom.instrumentation.clock_sync).
        """
        try:
            conn = await self._ensure_connected()
            result = await conn.run("chronyc tracking", check=True)
            tracking = parse_chrony_tracking(str(result.stdout or ""))
        except Exception as exc:  # chrony absent, command refused, transport error
            logger.debug("chronyc tracking failed on %s: %r", self._config.host, exc)
            tracking = None
        if tracking is None:
            return {
                "method": None,
                "synchronized": None,
                "note": (
                    "chronyc tracking unavailable on this host; the sender's "
                    "clock offset probe (<sender>-clock-sync.json) alone bounds "
                    "the error"
                ),
            }
        max_error_s = chrony_max_error_s(tracking)
        return {
            "method": CHRONY_METHOD,
            "synchronized": max_error_s is not None,
            # |host clock − true time| ≤ |system time offset| + root_delay/2
            # + root_dispersion, against this host's own chrony reference.
            "max_error_s": max_error_s,
            "tracking": tracking,
        }

    async def _resolve_uid_gid(self, conn: asyncssh.SSHClientConnection) -> str:
        if self._uid_gid is None:
            result = await conn.run('echo "$(id -u):$(id -g)"', check=True)
            self._uid_gid = str(result.stdout or "").strip()
        return self._uid_gid

    async def _ensure_connected(self) -> asyncssh.SSHClientConnection:
        """The host's connection, opened on first use and reopened after a loss
        (keepalives turn a silently dead session into a closed one)."""
        if self._conn is None or self._conn.is_closed():
            if self._conn is not None:
                logger.warning("SSH connection to %s was lost; reconnecting", self._config.host)
            self._conn = await asyncssh.connect(
                self._config.host,
                port=self._config.port,
                username=self._config.user,
                client_keys=[str(self._config.key_file)] if self._config.key_file else None,
                password=self._config.password,
                known_hosts=None,
                keepalive_interval=KEEPALIVE_INTERVAL_S,
                keepalive_count_max=KEEPALIVE_COUNT_MAX,
            )
        return self._conn


async def _stream_log(process: asyncssh.SSHClientProcess, log_handle: IO[str]) -> None:
    try:
        async for line in process.stdout:
            log_handle.write(line)
            log_handle.flush()
    except (OSError, asyncssh.Error) as exc:
        logger.warning("log stream ended early: %r", exc)
