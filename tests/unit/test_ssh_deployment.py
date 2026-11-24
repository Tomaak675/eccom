from pathlib import Path

from eccom.orchestration.deployment import local_source_root
from eccom.orchestration.ssh import iter_source_files


def test_source_file_list_covers_both_agent_build_contexts() -> None:
    root = local_source_root()
    files = iter_source_files(root)

    as_posix = {f.as_posix() for f in files}
    assert "pyproject.toml" in as_posix
    assert "README.md" in as_posix  # pyproject references it; pip needs it
    assert "docker/Dockerfile" in as_posix
    assert "eccom/cli.py" in as_posix
    assert "eccom/protocols/http.py" in as_posix
    # the Go agent: module, sources and its Dockerfile, never a local binary
    assert "docker/Dockerfile.go" in as_posix
    assert "go/go.mod" in as_posix
    assert "go/cmd/eccom-agent/main.go" in as_posix
    assert not any(f.parts[:2] == ("go", "bin") for f in files)

    # nothing outside the build context, no caches, everything exists
    for relative in files:
        assert not relative.is_absolute()
        assert "__pycache__" not in relative.parts
        assert (root / relative).is_file()
    assert not any(f.parts[0] in (".venv", "runs", "tests") for f in files)


def test_each_implementation_has_its_own_image_tag() -> None:
    from eccom.core.hosts import SshConfig
    from eccom.orchestration.ssh import SshDeployment

    deployment = SshDeployment(
        SshConfig(host="h", user="u", password="p", working_dir="/tmp/e", image="agent")
    )
    assert deployment.image_name("python") == "agent"
    assert deployment.image_name("go") == "agent-go"
    assert deployment.container_image() == "agent"  # nothing built yet: the default


def test_container_name_is_sanitized() -> None:
    from eccom.orchestration.ssh import _container_name

    name = _container_name("receiver", "20260830T150951-mqtt one:way")
    assert name == "eccom-receiver-20260830T150951-mqtt-one-way"


async def test_terminate_returns_even_when_docker_stop_hangs(monkeypatch) -> None:
    """A starved host can leave `docker stop` hanging; terminate() must bound it
    instead of stalling the whole suite behind one dead receiver."""
    import asyncio
    import io

    from eccom.orchestration import ssh as ssh_module
    from eccom.orchestration.ssh import SshAgentProcess

    class _Process:
        exit_status = None

        async def wait(self) -> None:  # the container died on its own
            self.exit_status = 137

        def close(self) -> None:
            pass

        async def wait_closed(self) -> None:
            pass

    commands: list[list[str]] = []

    async def run_command(args: list[str]) -> tuple[int, str]:
        commands.append(args)
        if args[:2] == ["docker", "stop"]:
            await asyncio.sleep(3600)
        return 0, ""

    monkeypatch.setattr(ssh_module, "STOP_TIMEOUT_S", 0.05)
    log_task = asyncio.create_task(asyncio.sleep(3600))
    agent = SshAgentProcess(_Process(), log_task, io.StringIO(), "eccom-x", run_command)

    await asyncio.wait_for(agent.terminate(), timeout=2)

    assert [c[:2] for c in commands] == [["docker", "stop"], ["docker", "rm"]]
    assert agent.returncode == 137
    assert log_task.cancelled()


async def test_wait_follows_the_container_through_docker_when_the_session_dies(
    monkeypatch, tmp_path
) -> None:
    """An SSH session can die silently mid-run. The channel closing
    without an exit status makes wait() poll Docker over the (reconnecting)
    command path, take the exit code from there, and replace the truncated
    streamed log with Docker's complete one."""
    import asyncio

    from eccom.orchestration import ssh as ssh_module
    from eccom.orchestration.ssh import SshAgentProcess

    class _Process:
        exit_status = None  # the channel closed before docker run reported

        async def wait(self) -> None:
            pass

    inspections = iter(["true 0\n", "true 0\n", "false 3\n"])
    commands: list[list[str]] = []

    async def run_command(args: list[str]) -> tuple[int, str]:
        commands.append(args)
        if args[:2] == ["docker", "inspect"]:
            return 0, next(inspections)
        if args[:2] == ["docker", "logs"]:
            return 0, "complete line 1\ncomplete line 2\n"
        return 0, ""

    monkeypatch.setattr(ssh_module, "POLL_INTERVAL_S", 0.01)
    log_path = tmp_path / "sender.log"
    handle = log_path.open("w", encoding="utf-8", newline="\n")
    handle.write("streamed line 1\n")
    log_task = asyncio.create_task(asyncio.sleep(0))
    agent = SshAgentProcess(_Process(), log_task, handle, "eccom-sender-x", run_command)

    code = await asyncio.wait_for(agent.wait(), timeout=5)

    assert code == 3 and agent.returncode == 3
    assert [c[:2] for c in commands].count(["docker", "inspect"]) == 3
    assert ["docker", "rm", "eccom-sender-x"] in commands
    assert log_path.read_text(encoding="utf-8") == "complete line 1\ncomplete line 2\n"


async def test_ensure_connected_reopens_a_lost_connection_with_keepalives(monkeypatch) -> None:
    from eccom.core.hosts import SshConfig
    from eccom.orchestration import ssh as ssh_module
    from eccom.orchestration.ssh import SshDeployment

    class _Conn:
        def __init__(self, closed: bool) -> None:
            self._closed = closed

        def is_closed(self) -> bool:
            return self._closed

    connections = [_Conn(closed=True), _Conn(closed=False)]
    kwargs_seen: list[dict] = []

    async def fake_connect(host, **kwargs):
        kwargs_seen.append(kwargs)
        return connections.pop(0)

    monkeypatch.setattr(ssh_module.asyncssh, "connect", fake_connect)
    deployment = SshDeployment(SshConfig(host="h", user="u", working_dir="/w", password="p"))

    first = await deployment._ensure_connected()
    second = await deployment._ensure_connected()  # the first one is closed: reopened
    third = await deployment._ensure_connected()  # still open: reused

    assert first.is_closed() and not second.is_closed() and third is second
    assert len(kwargs_seen) == 2
    assert kwargs_seen[0]["keepalive_interval"] == ssh_module.KEEPALIVE_INTERVAL_S
    assert kwargs_seen[0]["keepalive_count_max"] == ssh_module.KEEPALIVE_COUNT_MAX


async def test_concurrent_agent_starts_keep_one_short_lived_channel_open(
    monkeypatch, tmp_path
) -> None:
    """sshd admits MaxSessions (10) channels per connection and each running
    agent keeps one: starting eight senders at once must not open more than
    one command/SFTP channel at a time, and must ship the scenario once."""
    import asyncio
    import contextlib

    from eccom.core.hosts import SshConfig
    from eccom.orchestration import ssh as ssh_module
    from eccom.orchestration.ssh import SshDeployment

    class _Result:
        stdout, stderr, exit_status = "1000:1000", "", 0

    class _Process:
        exit_status = None
        stdout = None

    class _Conn:
        def __init__(self) -> None:
            self.open = 0  # channels open right now
            self.max_open = 0
            self.puts = 0

        async def _hold(self, seconds: float = 0.01):
            self.open += 1
            self.max_open = max(self.max_open, self.open)
            await asyncio.sleep(seconds)

        async def run(self, command: str, check: bool = False) -> _Result:
            await self._hold()
            self.open -= 1
            return _Result()

        @contextlib.asynccontextmanager
        async def start_sftp_client(self):
            await self._hold()
            conn = self

            class _Sftp:
                async def put(self, local: str, remote: str) -> None:
                    conn.puts += 1

            try:
                yield _Sftp()
            finally:
                self.open -= 1

        async def create_process(self, command: str) -> _Process:
            await self._hold()  # the agent's channel stays open
            return _Process()

    conn = _Conn()
    deployment = SshDeployment(
        SshConfig(host="h", user="u", working_dir="/w", password="p")
    )
    deployment._image_ids = {"python": "sha"}

    async def fake_connect() -> _Conn:
        return conn

    async def no_log(process, handle) -> None:
        pass

    monkeypatch.setattr(deployment, "_ensure_connected", fake_connect)
    monkeypatch.setattr(ssh_module, "_stream_log", no_log)
    scenario = tmp_path / "s.yaml"
    scenario.write_text(
        "name: t\nprotocol: {type: http}\npayload: {size_bytes: 1}\n"
        "load: {messages_per_second: 1, total_messages: 1}\n",
        encoding="utf-8",
    )
    run_dir = tmp_path / "runs" / "r1"

    agents = await asyncio.gather(
        *(deployment.start_agent("sender", scenario, run_dir, instance=i) for i in range(8))
    )

    assert len(agents) == 8
    assert conn.puts == 1  # the scenario is staged once per run
    assert conn.max_open <= 1 + 8  # one short-lived channel beside the agents'


async def test_collect_artifacts_gzips_the_jsonl_files_on_the_host_first(monkeypatch, tmp_path) -> None:
    """A run's events can be gigabytes; they are gzip-compressed on the
    host before the SFTP download."""
    import contextlib

    from eccom.core.hosts import SshConfig
    from eccom.orchestration.ssh import SshDeployment

    order: list[str] = []

    class _Sftp:
        async def glob(self, pattern: str):
            return ["/w/runs/r1/edge-events.jsonl.gz", "/w/runs/r1/summary.json"]

        async def isfile(self, path: str) -> bool:
            return True

        async def get(self, remote: str, local: str) -> None:
            order.append(f"get {remote.rsplit('/', 1)[1]}")

    class _Conn:
        async def run(self, command: str, check: bool = False):
            order.append(command)
            return type("R", (), {"stdout": "", "stderr": "", "exit_status": 0})()

        @contextlib.asynccontextmanager
        async def start_sftp_client(self):
            yield _Sftp()

    deployment = SshDeployment(SshConfig(host="h", user="u", working_dir="/w", password="p"))

    async def fake_connect():
        return _Conn()

    monkeypatch.setattr(deployment, "_ensure_connected", fake_connect)
    await deployment.collect_artifacts(tmp_path / "r1")
    assert order[0].startswith("cd /w/runs/r1 && ") and "gzip -1 -f *.jsonl" in order[0]
    assert order[1:] == ["get edge-events.jsonl.gz", "get summary.json"]


async def test_discard_run_removes_the_remote_run_dir(monkeypatch) -> None:
    from eccom.core.hosts import SshConfig
    from eccom.orchestration.ssh import SshDeployment

    commands: list[str] = []

    class _Conn:
        async def run(self, command: str, check: bool = False):
            commands.append(command)
            return type("R", (), {"stdout": "", "stderr": "", "exit_status": 0})()

    deployment = SshDeployment(SshConfig(host="h", user="u", working_dir="/w", password="p"))

    async def fake_connect():
        return _Conn()

    monkeypatch.setattr(deployment, "_ensure_connected", fake_connect)
    await deployment.discard_run(Path("runs/20261019T172105-s5_http"))
    assert commands == ["rm -rf /w/runs/20261019T172105-s5_http"]


async def test_sender_cpu_cap_replaces_the_hosts_cpus_flag(monkeypatch, tmp_path) -> None:
    from eccom.core.hosts import ContainerResources, SshConfig
    from eccom.orchestration import ssh as ssh_module
    from eccom.orchestration.ssh import SshDeployment

    class _Result:
        stdout, stderr, exit_status = "1000:1000", "", 0

    class _Process:
        exit_status = None
        stdout = None

    class _Conn:
        commands: list[str] = []

        async def run(self, command: str, check: bool = False) -> _Result:
            return _Result()

        async def create_process(self, command: str) -> _Process:
            self.commands.append(command)
            return _Process()

    conn = _Conn()
    capped = SshDeployment(SshConfig(
        host="h", user="u", working_dir="/w", password="p",
        resources=ContainerResources(cpus=1.5, memory="750m"),
    ))
    capped._image_ids = {"go": "sha"}

    async def fake_connect() -> _Conn:
        return conn

    async def staged(conn, scenario_path, remote_run_dir) -> None:
        pass

    async def no_log(process, handle) -> None:
        pass

    monkeypatch.setattr(capped, "_ensure_connected", fake_connect)
    monkeypatch.setattr(capped, "_stage_run", staged)
    monkeypatch.setattr(ssh_module, "_stream_log", no_log)
    run_dir = tmp_path / "runs" / "r1"

    await capped.start_agent("sender", tmp_path / "s.yaml", run_dir, implementation="go", cpus=6)
    await capped.start_agent("receiver", tmp_path / "s.yaml", run_dir, implementation="go")

    sender, receiver = conn.commands
    assert "--memory 750m --memory-swap 750m --cpus 6 " in sender and "--cpus 1.5" not in sender
    assert "--cpus 1.5 --memory 750m --memory-swap 750m" in receiver and sender.count("--cpus") == 1


def test_container_flags_come_from_the_hosts_resources_block() -> None:
    from eccom.core.hosts import SshConfig
    from eccom.orchestration.ssh import SshDeployment

    base = {"host": "h", "user": "u", "password": "p", "working_dir": "/tmp/e"}
    assert SshDeployment(SshConfig(**base)).container_flags() == []
    capped = SshDeployment(SshConfig(**base, resources={"cpus": 1.5, "memory": "750m"}))
    assert capped.container_flags() == ["--cpus", "1.5", "--memory", "750m", "--memory-swap", "750m"]
