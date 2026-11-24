"""Environment record: the container's CPU cap is read back from its cgroup."""

from pathlib import Path

from eccom.instrumentation.environment import collect_environment, container_cpu_limit


def test_cgroup_v2_cpu_max_gives_cores(tmp_path: Path) -> None:
    (tmp_path / "cpu.max").write_text("600000 100000\n", encoding="utf-8")
    assert container_cpu_limit(tmp_path) == 6.0
    (tmp_path / "cpu.max").write_text("max 100000\n", encoding="utf-8")
    assert container_cpu_limit(tmp_path) is None


def test_cgroup_v1_quota_and_period_give_cores(tmp_path: Path) -> None:
    (tmp_path / "cpu").mkdir()
    (tmp_path / "cpu" / "cpu.cfs_quota_us").write_text("350000\n", encoding="utf-8")
    (tmp_path / "cpu" / "cpu.cfs_period_us").write_text("100000\n", encoding="utf-8")
    assert container_cpu_limit(tmp_path) == 3.5
    (tmp_path / "cpu" / "cpu.cfs_quota_us").write_text("-1\n", encoding="utf-8")
    assert container_cpu_limit(tmp_path) is None


def test_no_cgroup_means_no_limit(tmp_path: Path) -> None:
    assert container_cpu_limit(tmp_path / "missing") is None
    assert "container_limit_cores" in collect_environment("edge")["cpu"]
