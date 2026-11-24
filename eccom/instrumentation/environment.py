"""Environment metadata collection for experiment reproducibility.

Collected per host at agent startup. Fields that cannot be determined are
recorded as null rather than omitted, so the schema stays stable.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import platform
import socket
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

_TRACKED_PACKAGES = ("aiohttp", "pydantic", "PyYAML", "psutil", "numpy", "pandas", "matplotlib")


def collect_environment(host: str) -> dict[str, Any]:
    return {
        "host_role": host,
        "implementation": "python",
        "hostname": socket.gethostname(),
        "collected_at": datetime.now(UTC).isoformat(),
        "python_version": platform.python_version(),
        "platform": platform.platform(),
        "os": platform.system(),
        "kernel_version": platform.release(),
        "cpu": {
            "model": platform.processor() or None,
            "architecture": platform.machine(),
            "logical_cores": os.cpu_count(),
            # The container's docker --cpus cap, read back from its cgroup;
            # None when uncapped or not in a cgroup.
            "container_limit_cores": container_cpu_limit(),
        },
        # Soft/hard RLIMIT_NOFILE of the agent process: one socket per open
        # connection, so this caps how many peers a receiver can hold.
        "open_files_limit": _open_files_limit(),
        "git_commit": git_commit(),
        # Set by the deployment when running from an immutable image.
        "docker_image_digest": os.environ.get("ECCOM_IMAGE_DIGEST"),
        "dependencies": {name: _package_version(name) for name in _TRACKED_PACKAGES},
        # One-way (edge->cloud) latencies are only valid when host clocks are
        # synchronized; until an NTP/PTP/chrony probe exists this records that
        # nothing is known. The deployment may override it (e.g. same_host).
        "clock_sync": {
            "method": None,
            "synchronized": None,
            "note": "no clock synchronization information collected",
        },
    }


def write_environment_file(host: str, run_dir: Path) -> Path:
    path = run_dir / f"{host}-environment.json"
    path.write_text(json.dumps(collect_environment(host), indent=2), encoding="utf-8")
    return path


def container_cpu_limit(root: Path = Path("/sys/fs/cgroup")) -> float | None:
    """Cores allowed by the cgroup's CFS quota (cgroup v2 ``cpu.max`` or v1
    ``cpu.cfs_quota_us`` / ``cpu.cfs_period_us``); None when unlimited or
    unreadable.

    root: the cgroup mount, as the container sees it.
    """
    try:
        quota, period = (root / "cpu.max").read_text(encoding="utf-8").split()
        return None if quota == "max" else int(quota) / int(period)
    except (OSError, ValueError):
        pass
    for subdir in ("cpu", "cpu,cpuacct"):
        try:
            quota = int((root / subdir / "cpu.cfs_quota_us").read_text(encoding="utf-8"))
            period = int((root / subdir / "cpu.cfs_period_us").read_text(encoding="utf-8"))
            return None if quota < 0 else quota / period
        except (OSError, ValueError):
            continue
    return None


def _open_files_limit() -> dict[str, int | None]:
    try:
        import resource
    except ImportError:  # Windows has no RLIMIT_NOFILE
        return {"soft": None, "hard": None}
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    return {"soft": soft, "hard": hard}


def _package_version(name: str) -> str | None:
    try:
        return importlib.metadata.version(name)
    except importlib.metadata.PackageNotFoundError:
        return None


def git_commit() -> str | None:
    # Inside containers there is no .git directory; deployments pass the
    # commit through the environment instead.
    if commit := os.environ.get("ECCOM_GIT_COMMIT"):
        return commit
    try:
        result = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except OSError:
        return None
    return result.stdout.strip() or None
