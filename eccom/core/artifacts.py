"""Naming of per-host run artifacts when a role runs as several processes.

A single agent per role writes ``<host>-events.jsonl``, ``<host>-load-report.json``
and ``<host>-system-metrics.jsonl``. With ``load.senders > 1`` every sender
instance writes its own set under ``<host>.<i>-...`` (``edge.0``, ``edge.1``,
...) and the analysis merges them back into one ``<host>`` view. Per-host
artifacts that do not depend on the instance (environment, clock-sync probe)
keep the bare host name and are written by instance 0 only.
"""

from __future__ import annotations

import re
from pathlib import Path


def instance_label(host: str, instance: int, instances: int) -> str:
    """File-name label of one agent instance: the bare host label for a single
    process (artifact names unchanged), ``host.N`` when the role runs as
    several processes."""
    return host if instances == 1 else f"{host}.{instance}"


def instance_labels(run_dir: Path, host: str, suffix: str) -> list[str]:
    """Labels of every instance of ``host`` that wrote ``<label>-<suffix>``:
    ``[host]`` for a single process, ``[host.0, host.1, ...]`` (in instance
    order) for several, ``[]`` when nothing was written."""
    if (run_dir / f"{host}-{suffix}").exists():
        return [host]
    pattern = re.compile(rf"^{re.escape(host)}\.(\d+)-{re.escape(suffix)}$")
    found = []
    for path in run_dir.iterdir():
        if match := pattern.match(path.name):
            found.append((int(match.group(1)), path.name[: -len(suffix) - 1]))
    return [label for _, label in sorted(found)]


def split_label(label: str) -> tuple[str, int | None]:
    """``edge.3`` -> (``edge``, 3); ``edge`` -> (``edge``, None)."""
    if match := re.fullmatch(r"(.+)\.(\d+)", label):
        return match.group(1), int(match.group(2))
    return label, None
