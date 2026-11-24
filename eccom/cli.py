"""Command-line interface.

eccom run <scenario.yaml>     run a full experiment locally (both agents)
eccom agent --role ...        run one agent process (used by deployments)
eccom analyze <run-dir>       recompute statistics and plots from raw events
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import random
import sys
import time
from pathlib import Path

from eccom.agents.runtime import run_agent
from eccom.analysis import (
    analyze_run,
    compare_runs,
    format_comparison,
    format_summary,
    generate_comparison_plots,
    write_csv,
)
from eccom.orchestration.runner import TestRunner


def _use_selector_loop_on_windows() -> None:
    """Agent processes need a selector loop on Windows: paho-mqtt requires
    loop.add_reader/add_writer, which the default Proactor loop lacks. The
    runner keeps Proactor (it needs subprocess support); agents never spawn
    subprocesses. No-op on other platforms."""
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="eccom", description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_parser = subparsers.add_parser("run", help="run a scenario end to end")
    run_parser.add_argument(
        "scenario",
        type=Path,
        help="a scenario YAML file, or a directory of scenario files to run in sequence",
    )
    run_parser.add_argument(
        "--runs-dir", type=Path, default=Path("runs"), help="root directory for run artifacts"
    )
    run_parser.add_argument(
        "--hosts",
        type=Path,
        default=None,
        help="hosts YAML mapping topology roles to machines (default: local subprocesses)",
    )
    run_parser.add_argument(
        "--no-analyze",
        action="store_true",
        help=(
            "skip the analysis after each run (run `eccom analyze <run dir>` later, e.g. "
            "while the next scenario measures; a 4 M-message run takes minutes to analyse)"
        ),
    )
    run_parser.add_argument(
        "--pause-s",
        type=float,
        default=0.0,
        help=(
            "quiet gap between the scenarios of a suite, so consecutive runs show up "
            "as separate bursts in network monitoring (default: none)"
        ),
    )

    run_parser.add_argument(
        "--shuffle",
        action="store_true",
        help=(
            "run the scenarios of a suite in random order instead of by filename, so "
            "time-dependent effects do not always hit the same variants (the order is logged)"
        ),
    )

    agent_parser = subparsers.add_parser("agent", help="run a single agent process")
    agent_parser.add_argument("--role", choices=["sender", "receiver"], required=True)
    agent_parser.add_argument("--scenario", type=Path, required=True)
    agent_parser.add_argument("--run-dir", type=Path, required=True)
    agent_parser.add_argument(
        "--peer-control",
        default=None,
        help="receiver control endpoint (host:port) for sender-side clock offset probes",
    )
    agent_parser.add_argument(
        "--instance",
        type=int,
        default=0,
        help="0-based index of this process among load.senders sender processes",
    )
    agent_parser.add_argument(
        "--endpoint-host",
        default=None,
        help=(
            "connect to this address instead of the scenario's endpoint host "
            "(the runner passes the address it resolved for a role label such as cloud)"
        ),
    )

    analyze_parser = subparsers.add_parser("analyze", help="(re)analyze a run directory")
    analyze_parser.add_argument("run_dir", type=Path)

    compare_parser = subparsers.add_parser(
        "compare", help="tabulate per-stage latency and resource usage across runs"
    )
    compare_parser.add_argument("run_dir", type=Path, nargs="+")
    compare_parser.add_argument(
        "--csv", type=Path, default=None, help="also write long-format per-stage statistics here"
    )
    compare_parser.add_argument(
        "--plots",
        type=Path,
        default=None,
        help="also render comparison charts (statistics per scenario) into this directory",
    )
    compare_parser.add_argument(
        "--output", type=Path, default=None, help="write the markdown tables here instead of stdout"
    )

    args = parser.parse_args(argv)
    setup_logging()

    match args.command:
        case "run":
            _run_scenarios(
                scenario_paths(args.scenario, shuffle=args.shuffle),
                args.runs_dir,
                args.hosts,
                args.pause_s,
                analyze=not args.no_analyze,
            )
        case "agent":
            _use_selector_loop_on_windows()
            asyncio.run(
                run_agent(
                    args.role,
                    args.scenario,
                    args.run_dir,
                    args.peer_control,
                    args.instance,
                    args.endpoint_host,
                )
            )
        case "analyze":
            print(format_summary(analyze_run(args.run_dir)))
        case "compare":
            _compare_runs(args.run_dir, args.csv, args.output, args.plots)


def _compare_runs(
    run_dirs: list[Path],
    csv_path: Path | None,
    output: Path | None,
    plots_dir: Path | None = None,
) -> None:
    rows = compare_runs(run_dirs)
    if not rows:
        raise SystemExit("no run directories could be read")
    table = format_comparison(rows)
    if output is not None:
        output.write_text(table + "\n", encoding="utf-8")
        print(f"comparison written to {output}")
    else:
        print(table)
    if csv_path is not None:
        write_csv(rows, csv_path)
        print(f"per-stage statistics written to {csv_path}")
    if plots_dir is not None:
        for path in generate_comparison_plots(plots_dir, rows):
            print(f"comparison chart written to {path}")


def scenario_paths(path: Path, shuffle: bool = False) -> list[Path]:
    """A single scenario file, or every YAML in a suite directory (sorted, so
    number-prefixed filenames control execution order).

    path: scenario file or suite directory.
    shuffle: put a suite's files in random order instead and log that order.
    Returns the scenario files in execution order.
    Raises SystemExit when a suite directory holds no YAML file."""
    if path.is_dir():
        files = sorted(p for p in path.iterdir() if p.suffix in (".yaml", ".yml"))
        if not files:
            raise SystemExit(f"no scenario YAML files found in {path}")
        if shuffle:
            random.shuffle(files)
            logging.getLogger("eccom").info(
                "shuffled suite order: %s", ", ".join(p.name for p in files)
            )
        return files
    return [path]


def _run_scenarios(
    paths: list[Path],
    runs_dir: Path,
    hosts: Path | None,
    pause_s: float = 0.0,
    analyze: bool = True,
) -> None:
    """Run scenarios in sequence; one failing variant never aborts the suite.

    ``pause_s`` idles between consecutive scenarios (never before the first)
    so back-to-back runs separate into distinct bursts on the hosts' NICs.
    The gap sits between one run's artifact download and the next run's
    source sync, so only the small SSH transfers fall next to it."""
    results: list[tuple[str, Path | None]] = []
    for index, scenario_path in enumerate(paths):
        if index > 0 and pause_s > 0:
            logging.getLogger("eccom").info(
                "pausing %.0f s before %s", pause_s, scenario_path.name
            )
            time.sleep(pause_s)
        try:
            run_dir = asyncio.run(
                TestRunner(scenario_path, runs_dir, hosts_path=hosts, analyze=analyze).run()
            )
        except Exception:
            logging.getLogger("eccom").exception("scenario %s failed", scenario_path.name)
            results.append((scenario_path.name, None))
            continue
        print()
        if analyze:
            print(format_summary(analyze_summary_of(run_dir)))
        print(f"artifacts: {run_dir}{'' if analyze else ' (not analysed: eccom analyze)'}\n")
        results.append((scenario_path.name, run_dir))
    if len(results) > 1:
        print("suite results:")
        for name, run_dir in results:
            print(f"  {name}: {run_dir if run_dir is not None else 'FAILED (see log above)'}")
    if any(run_dir is None for _, run_dir in results):
        raise SystemExit(1)


def analyze_summary_of(run_dir: Path) -> dict:
    """Read the summary the runner just wrote (avoids re-analyzing)."""
    import json

    return json.loads((run_dir / "summary.json").read_text(encoding="utf-8"))


if __name__ == "__main__":
    main()
