from pathlib import Path
from types import SimpleNamespace

import pytest

from eccom import cli
from eccom.cli import scenario_paths


def test_suite_pauses_between_scenarios_but_not_before_the_first(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    started: list[str] = []
    sleeps: list[float] = []

    class FakeRunner:
        def __init__(self, scenario_path: Path, runs_dir: Path, hosts_path=None, analyze=True) -> None:
            self._path = scenario_path

        async def run(self) -> Path:
            started.append(self._path.name)
            return tmp_path / self._path.stem

    monkeypatch.setattr(cli, "TestRunner", FakeRunner)
    monkeypatch.setattr(cli, "time", SimpleNamespace(sleep=sleeps.append))
    monkeypatch.setattr(cli, "analyze_summary_of", lambda run_dir: {})
    monkeypatch.setattr(cli, "format_summary", lambda summary: "ok")

    paths = [tmp_path / name for name in ("01_a.yaml", "02_b.yaml", "03_c.yaml")]
    cli._run_scenarios(paths, tmp_path, None, pause_s=30)

    assert started == ["01_a.yaml", "02_b.yaml", "03_c.yaml"]
    assert sleeps == [30, 30]

    sleeps.clear()
    cli._run_scenarios(paths[:1], tmp_path, None, pause_s=30)
    assert sleeps == []


def test_single_file_passes_through(tmp_path: Path) -> None:
    scenario = tmp_path / "one.yaml"
    scenario.touch()
    assert scenario_paths(scenario) == [scenario]


def test_suite_directory_yields_sorted_yaml_files(tmp_path: Path) -> None:
    for name in ("02_b.yaml", "01_a.yaml", "03_c.yml", "notes.txt", "certs"):
        if name == "certs":
            (tmp_path / name).mkdir()
        else:
            (tmp_path / name).touch()
    assert [p.name for p in scenario_paths(tmp_path)] == ["01_a.yaml", "02_b.yaml", "03_c.yml"]


def test_shuffle_reorders_the_suite_without_losing_files(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    names = ["01_a.yaml", "02_b.yaml", "03_c.yaml"]
    for name in names:
        (tmp_path / name).touch()
    monkeypatch.setattr(cli.random, "shuffle", lambda files: files.reverse())
    assert [p.name for p in scenario_paths(tmp_path, shuffle=True)] == names[::-1]


def test_empty_suite_directory_is_an_error(tmp_path: Path) -> None:
    with pytest.raises(SystemExit, match="no scenario YAML files"):
        scenario_paths(tmp_path)
