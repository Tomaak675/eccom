"""Local deployment: which program starts an agent, per implementation."""

import sys
from pathlib import Path

import pytest

from eccom.orchestration import local as local_module
from eccom.orchestration.local import LocalProcessDeployment, agent_command, go_agent_binary


def test_python_agent_runs_the_package_with_the_current_interpreter() -> None:
    command = agent_command("python", "receiver", Path("s.yaml"), Path("runs/x"), ("--instance", "1"))
    assert command[:4] == [sys.executable, "-m", "eccom", "agent"]
    assert command[4:] == [
        "--role", "receiver", "--scenario", "s.yaml", "--run-dir", str(Path("runs/x")), "--instance", "1",
    ]


def test_go_agent_runs_the_binary_from_the_checkout() -> None:
    command = agent_command("go", "receiver", Path("s.yaml"), Path("runs/x"))
    binary = Path(command[0])
    assert binary == go_agent_binary()
    assert binary.parts[-3:-1] == ("go", "bin")
    assert binary.name == ("eccom-agent.exe" if sys.platform == "win32" else "eccom-agent")
    assert command[1:] == ["agent", "--role", "receiver", "--scenario", "s.yaml", "--run-dir", str(Path("runs/x"))]


async def test_prepare_without_toolchain_or_binary_explains_what_is_missing(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(local_module.shutil, "which", lambda name: None)
    monkeypatch.setattr(local_module, "go_agent_binary", lambda root=None: tmp_path / "eccom-agent")
    deployment = LocalProcessDeployment()
    await deployment.prepare(frozenset({"python"}))  # nothing to build
    with pytest.raises(RuntimeError, match="Go toolchain"):
        await deployment.prepare(frozenset({"go"}))
    (tmp_path / "eccom-agent").write_bytes(b"")
    await deployment.prepare(frozenset({"go"}))  # a prebuilt binary is enough
