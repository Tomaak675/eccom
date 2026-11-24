"""Offline plot generation (matplotlib, headless).

Kept strictly separate from test execution: plots are rendered from the RTT
table derived from raw events, never from live protocol code.
"""

from __future__ import annotations

import logging
import os.path
import re
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from matplotlib.ticker import FuncFormatter, LogLocator, NullFormatter

from eccom.analysis.statistics import RAMP_AXES, ramp_dimension

logger = logging.getLogger(__name__)

# Single-series charts on a light surface; values from the validated
# reference palette (categorical slot 1 + chrome/ink tokens).
_SERIES = "#2a78d6"
_SCATTER_POINTS = 200_000  # per-message scatter plots are sampled down to this many
_SURFACE = "#fcfcfb"
_INK_PRIMARY = "#0b0b0b"
_INK_MUTED = "#898781"
_GRIDLINE = "#e1e0d9"
_BASELINE = "#c3c2b7"


def generate_plots(
    plots_dir: Path, latency: pd.DataFrame, metric_label: str = "Round-trip time"
) -> list[Path]:
    """Render the standard latency/throughput plots for one run.

    ``latency`` is a measurement-phase table with ``send_wall_time_ns`` and
    ``latency_ms`` columns (RTT for request/response runs, offset-corrected
    one-way latency for one-way runs); ``metric_label`` names the metric in
    titles and axis labels.
    """
    if latency.empty:
        logger.warning("no completed messages with latency; skipping plots")
        return []
    plots_dir.mkdir(parents=True, exist_ok=True)

    elapsed_s = (latency["send_wall_time_ns"] - latency["send_wall_time_ns"].min()) / 1e9
    latency_ms = latency["latency_ms"]

    written = [
        _plot_histogram(plots_dir / "latency_histogram.png", latency_ms, metric_label),
        _plot_latency_over_time(
            plots_dir / "latency_over_time.png", elapsed_s, latency_ms, metric_label
        ),
        _plot_throughput_over_time(
            plots_dir / "throughput_over_time.png",
            arrival_elapsed_s=elapsed_s + latency_ms / 1000.0,
        ),
    ]
    return written


def _new_axes(title: str, xlabel: str, ylabel: str) -> tuple[plt.Figure, plt.Axes]:
    fig, ax = plt.subplots(figsize=(8, 4.5), dpi=120)
    fig.patch.set_facecolor(_SURFACE)
    _style_axes(ax, title, xlabel, ylabel)
    return fig, ax


def _style_axes(ax: plt.Axes, title: str, xlabel: str, ylabel: str, title_size: int = 12) -> None:
    ax.set_facecolor(_SURFACE)
    ax.set_title(title, color=_INK_PRIMARY, fontsize=title_size, loc="left", pad=12)
    ax.set_xlabel(xlabel, color=_INK_MUTED, fontsize=9)
    ax.set_ylabel(ylabel, color=_INK_MUTED, fontsize=9)
    ax.tick_params(colors=_INK_MUTED, labelsize=8)
    for spine in ("top", "right"):
        ax.spines[spine].set_visible(False)
    for spine in ("left", "bottom"):
        ax.spines[spine].set_color(_BASELINE)
    ax.grid(True, axis="y", color=_GRIDLINE, linewidth=0.75)
    ax.set_axisbelow(True)


def _log_axis(ax: plt.Axes) -> None:
    """Log y axis labelled in plain numbers (10, 20, 50, 100, ...) instead of
    powers of ten with scientific minor labels."""
    ax.set_yscale("log")
    ax.yaxis.set_major_locator(LogLocator(base=10, subs=(1.0, 2.0, 5.0)))
    ax.yaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}"))
    ax.yaxis.set_minor_formatter(NullFormatter())


def _second_buckets(elapsed_s: "np.ndarray | pd.Series") -> tuple[np.ndarray, np.ndarray]:
    """Counts per fully covered 1 s bucket: (edges, counts). A trailing
    partial bucket is dropped, since it would show a bogus collapse at the
    end of the run."""
    edges = np.arange(0.0, np.floor(float(np.max(elapsed_s))) + 1.0)
    if len(edges) < 2:
        edges = np.array([0.0, float(np.max(elapsed_s))])
    counts, _ = np.histogram(elapsed_s, bins=edges)
    return edges, counts


def _save(fig: plt.Figure, path: Path, rect: tuple[float, float, float, float] | None = None) -> Path:
    fig.tight_layout(rect=rect) if rect is not None else fig.tight_layout()
    fig.savefig(path, facecolor=fig.get_facecolor())
    plt.close(fig)
    return path


def _plot_histogram(path: Path, latency_ms: pd.Series, metric_label: str) -> Path:
    fig, ax = _new_axes(
        f"{metric_label} distribution", f"{metric_label} (ms)", "Messages"
    )
    bins = min(100, max(10, int(np.sqrt(len(latency_ms)))))
    ax.hist(latency_ms, bins=bins, color=_SERIES, edgecolor=_SURFACE, linewidth=0.5)
    return _save(fig, path)


def _plot_latency_over_time(
    path: Path, elapsed_s: pd.Series, latency_ms: pd.Series, metric_label: str
) -> Path:
    fig, ax = _new_axes(
        f"{metric_label} over the run", "Time since first send (s)", f"{metric_label} (ms)"
    )
    # Millions of markers cost minutes to rasterize and show nothing more
    # than an even sample of them.
    if len(elapsed_s) > _SCATTER_POINTS:
        keep = np.linspace(0, len(elapsed_s) - 1, _SCATTER_POINTS).astype(int)
        elapsed_s, latency_ms = elapsed_s.iloc[keep], latency_ms.iloc[keep]
    ax.scatter(elapsed_s, latency_ms, s=8, color=_SERIES, alpha=0.6, linewidths=0)
    return _save(fig, path)


def _plot_throughput_over_time(path: Path, arrival_elapsed_s: pd.Series) -> Path:
    fig, ax = _new_axes(
        "Achieved throughput (delivered messages)", "Time since first send (s)", "Messages/s"
    )
    edges, counts = _second_buckets(arrival_elapsed_s)
    ax.plot(edges[:-1] + np.diff(edges) / 2, counts, color=_SERIES, linewidth=2)
    ax.set_ylim(bottom=0)
    return _save(fig, path)


# --- ramp figure (load.mode: ramp) ---------------------------------------------

# The two agents keep one hue each on every panel: sender = categorical slot 1
# (also the single-series blue above), receiver = slot 2. The offered rate is a
# reference, not a measurement, so it wears muted ink.
_SENDER = "#2a78d6"
_RECEIVER = "#eb6834"
_OFFERED = "#898781"
_RAMP_PERCENTILES = (  # (label, quantile, color) — the ordinal blue ramp
    ("p50", 0.5, "#86b6ef"),
    ("p95", 0.95, "#256abf"),
    ("p99", 0.99, "#104281"),
)


def generate_ramp_plot(
    path: Path,
    cost: pd.DataFrame,
    cost_label: str,
    arrivals_wall_ns: "np.ndarray | None",
    metrics: dict[str, list[dict[str, Any]]],
    sender_host: str,
    receiver_host: str,
) -> Path | None:
    """One time-aligned figure for a ramp run, five panels on a shared axis:
    offered vs achieved vs delivered rate, the sender-observed latency
    percentiles per second, and both agents' CPU, NIC throughput and
    event-loop lag from their resource samples. Step boundaries run through
    every panel.

    ``cost`` is the measurement-phase per-message table with
    ``send_wall_time_ns``, ``latency_ms``, ``step`` and ``target_rate``
    (send-ack for one-way runs, RTT for request/response); ``arrivals_wall_ns``
    are delivery times on the sender's timeline, None without a clock offset.
    Returns None when there is nothing to draw.
    """
    if cost.empty:
        logger.warning("no completed messages in the ramp; skipping the ramp plot")
        return None
    path.parent.mkdir(parents=True, exist_ok=True)
    t0 = int(cost["send_wall_time_ns"].min())

    def seconds(wall_ns: Any) -> np.ndarray:
        return (np.asarray(wall_ns, dtype=np.int64) - t0) / 1e9

    send_s = seconds(cost["send_wall_time_ns"])
    end_s = max(float(send_s.max()), 1.0)
    edges, sent_counts = _second_buckets(send_s)
    centers = edges[:-1] + np.diff(edges) / 2

    fig, axes = plt.subplots(5, 1, figsize=(9, 12.5), dpi=120, sharex=True)
    fig.patch.set_facecolor(_SURFACE)
    titles = (
        ("Offered vs achieved rate", "Messages/s"),
        (f"{cost_label} per second", f"{cost_label} (ms)"),
        ("Agent CPU", "% of one core"),
        ("NIC throughput", "Mbit/s"),
        ("Event-loop lag", "max per sample (ms)"),
    )
    for ax, (title, ylabel) in zip(axes, titles):
        _style_axes(ax, title, "", ylabel, title_size=10)
    axes[-1].set_xlabel("Time since first measurement send (s)", color=_INK_MUTED, fontsize=9)

    # Step boundaries: the first send of every step but the first.
    steps = (
        cost.groupby("step")
        .agg(start=("send_wall_time_ns", "min"), target=("target_rate", "first"))
        .sort_index()
    )
    step_starts = seconds(steps["start"])
    for ax in axes:
        for x in step_starts[1:]:
            ax.axvline(x, color=_BASELINE, linewidth=0.75, zorder=0)

    def positive(values: np.ndarray) -> np.ndarray:
        # log axes: zero buckets are gaps, not points clipped to the floor
        values = np.asarray(values, dtype=float)
        return np.where(values > 0, values, np.nan)

    ax = axes[0]
    targets = steps["target"].to_numpy(dtype=float)
    ax.step(
        np.append(step_starts, end_s),
        np.append(targets, targets[-1]),
        where="post",
        color=_OFFERED,
        linewidth=1.5,
        linestyle="--",
        label="offered",
    )
    ax.plot(centers, positive(sent_counts), color=_SENDER, linewidth=2, label=f"sent ({sender_host})")
    if arrivals_wall_ns is not None and len(arrivals_wall_ns):
        delivered, _ = np.histogram(seconds(arrivals_wall_ns), bins=edges)
        ax.plot(
            centers,
            positive(delivered),
            color=_RECEIVER,
            linewidth=2,
            label=f"delivered ({receiver_host})",
        )
    _log_axis(ax)

    ax = axes[1]
    bucket = np.digitize(send_s, edges) - 1
    per_bucket = pd.DataFrame({"bucket": bucket, "latency_ms": cost["latency_ms"].to_numpy()})
    per_bucket = per_bucket[(per_bucket["bucket"] >= 0) & (per_bucket["bucket"] < len(centers))]
    quantiles = per_bucket.groupby("bucket")["latency_ms"].quantile(
        [q for _, q, _ in _RAMP_PERCENTILES]
    ).unstack()
    for label, quantile, color in _RAMP_PERCENTILES:
        ax.plot(
            centers[quantiles.index.to_numpy()],
            positive(quantiles[quantile].to_numpy()),
            color=color,
            linewidth=2,
            label=label,
        )
    _log_axis(ax)

    for host, color, net_key, net_label in (
        (sender_host, _SENDER, "net_bytes_sent", "sent"),
        (receiver_host, _RECEIVER, "net_bytes_recv", "received"),
    ):
        samples = metrics.get(host) or []
        if len(samples) < 2:
            continue
        frame = pd.DataFrame(samples).sort_values("wall_time_ns")
        t = seconds(frame["wall_time_ns"])
        dt = np.diff(t)
        valid = dt > 0
        if "process_cpu_seconds" in frame:
            cpu = 100.0 * np.diff(frame["process_cpu_seconds"].to_numpy(dtype=float)) / dt
            axes[2].plot(t[1:][valid], cpu[valid], color=color, linewidth=2, label=host)
        if net_key in frame:
            mbit = 8e-6 * np.diff(frame[net_key].to_numpy(dtype=float)) / dt
            axes[3].plot(
                t[1:][valid], mbit[valid], color=color, linewidth=2, label=f"{host} {net_label}"
            )
        if "loop_lag_ms_max" in frame:
            axes[4].plot(
                t, frame["loop_lag_ms_max"].to_numpy(dtype=float), color=color, linewidth=2,
                label=host,
            )
    for ax in axes[2:]:
        ax.set_ylim(bottom=0)

    axes[0].set_xlim(0, end_s)
    for ax in axes:
        if ax.get_legend_handles_labels()[0]:
            ax.legend(
                loc="upper left", frameon=False, fontsize=8, labelcolor=_INK_PRIMARY, ncols=3,
                columnspacing=1.2, handletextpad=0.4,
            )
    return _save(fig, path)


# --- cross-run comparison charts (eccom compare --plots) -----------------------

# Ordinal ramp for the percentile ladder (one hue, light -> dark = deeper into
# the tail) plus mean as an ink dash; median/p90/p95/p99 also differ by marker
# shape so the chart survives grayscale printing.
_PERCENTILE_STYLE = (  # (stat, color, marker)
    ("median", "#86b6ef", "o"),
    ("p90", "#5598e7", "^"),
    ("p95", "#256abf", "D"),
    ("p99", "#104281", "v"),
)
_MEAN_MARKER_SIZE = 260
_UNCERTAINTY_BAND = "#dedcd3"

_LABEL_TOKENS = {"http": "HTTP", "https": "HTTPS", "mqtt": "MQTT", "mqtts": "MQTTS"}
_LABEL_PAIRS = {("req", "rsp"): "req/rsp", ("no", "keepalive"): "no keep-alive"}


def variant_labels(names: list[str]) -> list[str]:
    """Short axis labels from scenario names: the common prefix (e.g. ``s1_``)
    is dropped and the remaining tokens are prettified (``mqtt_qos1`` ->
    ``MQTT QoS 1``)."""
    stripped = names
    if len(names) > 1:
        prefix = os.path.commonprefix(names)
        cut = prefix.rfind("_") + 1
        stripped = [name[cut:] or name for name in names]
    return [_pretty_variant(name) for name in stripped]


def _variant_name(name: str) -> str:
    """A homes-ramp scenario name without its aggregation-factor token
    (``s6_http_x4`` -> ``s6_http``), so the same variant matches across factors."""
    return re.sub(r"_x\d+$", "", name)


def _pretty_variant(name: str) -> str:
    tokens = name.split("_")
    parts: list[str] = []
    index = 0
    while index < len(tokens):
        pair = tuple(tokens[index : index + 2])
        if pair in _LABEL_PAIRS:
            parts.append(_LABEL_PAIRS[pair])
            index += 2
            continue
        token = tokens[index]
        if token.startswith("qos") and token[3:].isdigit():
            parts.append(f"QoS {token[3:]}")
        else:
            parts.append(_LABEL_TOKENS.get(token, token))
        index += 1
    return " ".join(parts)


def sender_cost_series(
    rows: list[dict[str, Any]], labels: list[str]
) -> tuple[list[dict[str, float] | None], list[str]]:
    """The sender-observed cost of each ``compare_runs()`` row and its axis label.

    Request/response runs contribute ``rtt`` (marked "(RTT)" on the label) even
    when the adapter also recorded a transport ack, as the MQTT one does;
    one-way runs contribute ``send_ack``.

    rows: ``compare_runs()`` rows.
    labels: one axis label per row, from ``variant_labels()``.
    Returns: the statistics dict (or None) per row and the labels to draw.
    """
    series: list[dict[str, float] | None] = []
    axis_labels: list[str] = []
    for row, label in zip(rows, labels):
        if row["interaction"] == "request_response":
            series.append(row["stages"].get("rtt"))
            axis_labels.append(label + "\n(RTT)")
        else:
            series.append(row["stages"].get("send_ack"))
            axis_labels.append(label)
    return series, axis_labels


def generate_comparison_plots(plots_dir: Path, rows: list[dict[str, Any]]) -> list[Path]:
    """Cross-run charts from ``compare_runs()`` rows: the five distribution
    statistics (mean, median, p90, p95, p99) per scenario, one chart for the
    sender-observed cost (send_ack; rtt for request/response runs, marked on
    the axis) and one for offset-corrected delivery, which also draws each
    run's clock-offset bound behind the markers."""
    if not rows:
        return []
    plots_dir.mkdir(parents=True, exist_ok=True)
    labels = variant_labels([row["scenario"] for row in rows])
    written: list[Path] = []

    sender, sender_labels = sender_cost_series(rows, labels)
    if any(sender):
        written.append(
            _plot_statistics_ladder(
                plots_dir / "compare_sender_cost.png",
                "Sender-observed cost per configuration",
                sender_labels,
                sender,
            )
        )

    delivery = [row["stages"].get("delivery") for row in rows]
    if any(delivery):
        written.append(
            _plot_statistics_ladder(
                plots_dir / "compare_delivery.png",
                "One-way delivery per configuration",
                labels,
                delivery,
                uncertainty_ms=[row["clock"]["uncertainty_ms"] for row in rows],
            )
        )

    ramp_rows = [
        (label, row)
        for label, row in zip(labels, rows)
        if row.get("ramp_steps") and row.get("aggregation_factor") is None
    ]
    if ramp_rows:
        written.extend(generate_ramp_sweep_plots(plots_dir, ramp_rows))
    # homes ramps: one sweep per aggregation factor (the same number of homes
    # offers a different rate under every factor), then every factor's
    # ceilings side by side
    homes_rows = [row for row in rows if row.get("ramp_steps") and row.get("aggregation_factor") is not None]
    for factor in sorted({row["aggregation_factor"] for row in homes_rows}):
        group = [row for row in homes_rows if row["aggregation_factor"] == factor]
        group_labels = variant_labels([_variant_name(row["scenario"]) for row in group])
        written.extend(generate_ramp_sweep_plots(plots_dir, list(zip(group_labels, group)), factor))
    ceiling = generate_homes_ceiling_plot(plots_dir, rows)
    if ceiling is not None:
        written.append(ceiling)
    return written


# --- rate-sweep charts for ramp runs (eccom compare --plots) --------------------

# The reference palette's eight categorical slots in their validated order
# (adjacent pairs clear the colour-vision-deficiency floor only in this order)
# plus a ninth, unvalidated brown for a nine-variant comparison; one marker
# shape per slot so identity never rests on colour alone.
_CATEGORICAL = (
    "#2a78d6", "#eb6834", "#1baf7a", "#eda100", "#e87ba4", "#008300", "#4a3aa7", "#e34948",
    "#8a6d3b",  # ninth slot (brown), outside the validated eight
)
_MARKERS = ("o", "s", "^", "D", "v", "P", "X", "h", "*")
_SWEEP_SAMPLES = 200
_SWEEP_NAMES = {"rate": "offered rate", "size": "payload size", "homes": "homes"}


def generate_ramp_sweep_plots(
    plots_dir: Path,
    labelled_rows: list[tuple[str, dict[str, Any]]],
    factor: int | None = None,
) -> list[Path]:
    """Two cross-variant charts over the dimension the ramp sweeps (x: the
    offered rate or the payload size on a log axis, the number of simulated
    homes on a linear one): the received rate, each step's distinct receipts
    over its scheduled sending time
    (``compare_ramp_delivered_rate.png``), and the offset-corrected delivery
    latency, median and p95 side by side
    (``compare_ramp_delivery_latency.png``). One smoothed line per variant
    through its per-step points (monotone cubic interpolation in log-log
    space, so the curve never overshoots the data); steps without receiver
    data are omitted.

    plots_dir: where the charts are written.
    labelled_rows: ``(label, compare row)`` pairs with ``ramp_steps``.
    factor: the aggregation factor of a group of homes runs; suffixes the
        file names (``_x<factor>``) and the titles.
    Returns: the written paths, empty when no series has data.
    """
    plots_dir.mkdir(parents=True, exist_ok=True)
    dimension = ramp_dimension([row["ramp_steps"] for _, row in labelled_rows])
    x_key, _, x_label = RAMP_AXES[dimension]
    x_name = _SWEEP_NAMES[dimension]
    suffix = "" if factor is None else f"_x{factor}"
    subtitle = "" if factor is None else f", aggregation factor {factor}"
    series = []
    for index, (label, row) in enumerate(labelled_rows):
        steps = row["ramp_steps"]
        x = np.array([s[x_key] for s in steps], dtype=float)
        offered = np.array([s["target_message_rate"] for s in steps], dtype=float)
        delivered = np.array([s["received_rate"] or np.nan for s in steps], dtype=float)
        median = np.array(
            [(s["delivery_ms"] or {}).get("median", np.nan) for s in steps], dtype=float
        )
        p95 = np.array([(s["delivery_ms"] or {}).get("p95", np.nan) for s in steps], dtype=float)
        style = (_CATEGORICAL[index % len(_CATEGORICAL)], _MARKERS[index % len(_MARKERS)])
        series.append((label, style, x, delivered, median, p95, offered))
    if not any(np.isfinite(s[3]).any() or np.isfinite(s[4]).any() for s in series):
        return []

    written = []
    fig, ax = _new_axes(f"Received rate vs {x_name}{subtitle}", x_label, "Received (msg/s, log)")
    fig.set_size_inches(9, 4.8)
    x_all = np.unique(np.concatenate([s[2] for s in series]))
    if dimension == "rate":
        reference, reference_label = x_all, "received = offered"
    elif dimension == "homes":
        # every run of one factor offers the same rate at a given number of homes
        offered_at = {x: o for _, _, xs, _, _, _, offered in series for x, o in zip(xs, offered)}
        reference = np.array([offered_at[x] for x in x_all], dtype=float)
        reference_label = "offered"
    else:
        offered_rate = float(np.nanmedian(np.concatenate([s[6] for s in series])))
        reference, reference_label = np.full_like(x_all, offered_rate), f"offered {offered_rate:g} msg/s"
    ax.plot(
        x_all, reference, color=_BASELINE, linewidth=1, linestyle=(0, (4, 3)),
        label=reference_label, zorder=1,
    )
    for label, style, x, delivered, _, _, _ in series:
        _sweep_line(ax, x, delivered, label, style)
    _log_axis(ax)
    _sweep_x_axis(ax, x_all, dimension)
    # A size sweep keeps the received rate flat along the top until the link
    # saturates, so the legend must sit in the empty lower-left corner; a rate
    # or homes sweep runs diagonally and leaves the upper left free.
    legend_loc = "lower left" if dimension == "size" else "upper left"
    ax.legend(loc=legend_loc, frameon=False, fontsize=8, labelcolor=_INK_PRIMARY, ncol=2)
    _sweep_footnote(fig)
    written.append(
        _save(fig, plots_dir / f"compare_ramp_delivered_rate{suffix}.png", rect=_SWEEP_RECT)
    )

    # median above p95, sharing the x axis, so the eye compares the two
    # statistics for one variant by moving straight down
    fig, axes = plt.subplots(2, 1, figsize=(9, 8.6), dpi=120, sharex=True)
    fig.patch.set_facecolor(_SURFACE)
    for ax, key, title in ((axes[0], 4, "median"), (axes[1], 5, "p95")):
        _style_axes(
            ax, f"Delivery latency vs {x_name} ({title}){subtitle}",
            x_label if key == 5 else "", "Delivery latency (ms, log)",
        )
        for label, style, x, _, median, p95, _ in series:
            _sweep_line(ax, x, median if key == 4 else p95, label, style)
        _log_axis(ax)
        _sweep_x_axis(ax, x_all, dimension)
    axes[0].tick_params(axis="x", labelbottom=False)
    axes[0].legend(loc="upper left", frameon=False, fontsize=8, labelcolor=_INK_PRIMARY, ncol=2)
    _sweep_footnote(fig)
    written.append(
        _save(fig, plots_dir / f"compare_ramp_delivery_latency{suffix}.png", rect=_SWEEP_RECT_TALL)
    )
    return written


def generate_homes_ceiling_plot(plots_dir: Path, rows: list[dict[str, Any]]) -> Path | None:
    """Grouped bars of the homes each variant supported
    (``compare_homes_ceiling.png``): one group per aggregation factor, one
    bar per variant in the palette's fixed order. A bar is the last number
    of homes whose schedule held (nothing lost, rate achieved). A ramp
    whose schedule held to its last step is an outlined bar labelled ">=":
    its ceiling lies above the last step it ran.

    plots_dir: where the chart is written.
    rows: ``compare_runs()`` rows; those with ``aggregation_factor`` and ``ramp`` are drawn.
    Returns: the chart's path, or None without homes runs.
    """
    homes_rows = [r for r in rows if r.get("aggregation_factor") is not None and r.get("ramp")]
    if not homes_rows:
        return None
    plots_dir.mkdir(parents=True, exist_ok=True)
    factors = sorted({r["aggregation_factor"] for r in homes_rows})
    names = list(dict.fromkeys(_variant_name(r["scenario"]) for r in homes_rows))
    ceilings = {
        (_variant_name(r["scenario"]), r["aggregation_factor"]): r["ramp"] for r in homes_rows
    }
    width = 0.8 / len(names)
    fig, ax = _new_axes(
        "Homes supported per aggregation factor", "Aggregation factor (readings per message)",
        "Homes",
    )
    fig.set_size_inches(9, 4.8)
    top = 0.0
    for index, (name, label) in enumerate(zip(names, variant_labels(names))):
        color = _CATEGORICAL[index % len(_CATEGORICAL)]
        offset = (index - (len(names) - 1) / 2) * width
        labelled = False
        for position, factor in enumerate(factors):
            ramp = ceilings.get((name, factor))
            if ramp is None:
                continue
            homes = ramp["exact_homes"] or 0
            open_ended = ramp["miss_step"] is None
            x = position + offset
            ax.bar(
                x, homes, width * 0.9, color=_SURFACE if open_ended else color,
                edgecolor=color, linewidth=1.5, label=None if labelled else label, zorder=3,
            )
            labelled = True
            ax.annotate(
                f"{'>=' if open_ended else ''}{homes:g}", (x, homes), xytext=(0, 2),
                textcoords="offset points", ha="center", va="bottom", fontsize=6.5,
                color=_INK_MUTED,
            )
            top = max(top, homes)
    ax.set_xticks(range(len(factors)))
    ax.set_xticklabels([str(factor) for factor in factors], color=_INK_PRIMARY)
    ax.set_ylim(0, top * 1.35 if top else 1)
    ax.legend(loc="upper left", frameon=False, fontsize=8, labelcolor=_INK_PRIMARY, ncol=3)
    return _save(fig, plots_dir / "compare_homes_ceiling.png")


def _sweep_line(
    ax: plt.Axes, x: np.ndarray, y: np.ndarray, label: str, style: tuple[str, str]
) -> None:
    """A smoothed line through the finite points of one series (contiguous
    runs interpolated separately, so a gap stays a gap) plus the points."""
    color, marker = style
    finite = np.isfinite(x) & np.isfinite(y) & (y > 0)
    if not finite.any():
        return
    first = True
    for run in _contiguous_runs(finite):
        xs, ys = x[run], y[run]
        if len(run) >= 2:
            grid = np.logspace(np.log10(xs[0]), np.log10(xs[-1]), _SWEEP_SAMPLES)
            smooth = 10 ** monotone_cubic(np.log10(xs), np.log10(ys), np.log10(grid))
            ax.plot(grid, smooth, color=color, linewidth=2, solid_joinstyle="round",
                    solid_capstyle="round", label=label if first else None, zorder=3)
            first = False
        ax.plot(xs, ys, linestyle="none", marker=marker, markersize=6, color=color,
                markeredgecolor=_SURFACE, markeredgewidth=1.2,
                label=label if first else None, zorder=4)
        first = False


def _contiguous_runs(mask: np.ndarray) -> list[np.ndarray]:
    indices = np.flatnonzero(mask)
    if indices.size == 0:
        return []
    breaks = np.flatnonzero(np.diff(indices) > 1) + 1
    return [run for run in np.split(indices, breaks)]


def monotone_cubic(x: np.ndarray, y: np.ndarray, xs: np.ndarray) -> np.ndarray:
    """Shape-preserving piecewise cubic interpolation (Fritsch-Carlson, as in
    PCHIP): passes through every point and never overshoots between them, so a
    smoothed line cannot invent a bump the data does not have. ``x`` strictly
    increasing, at least two points."""
    x = np.asarray(x, dtype=float)
    y = np.asarray(y, dtype=float)
    h = np.diff(x)
    delta = np.diff(y) / h
    slopes = np.zeros_like(y)
    if len(x) == 2:
        slopes[:] = delta[0]
    else:
        for k in range(1, len(x) - 1):
            if delta[k - 1] * delta[k] > 0:
                w1 = 2 * h[k] + h[k - 1]
                w2 = h[k] + 2 * h[k - 1]
                slopes[k] = (w1 + w2) / (w1 / delta[k - 1] + w2 / delta[k])
        slopes[0] = _end_slope(h[0], h[1], delta[0], delta[1])
        slopes[-1] = _end_slope(h[-1], h[-2], delta[-1], delta[-2])
    index = np.clip(np.searchsorted(x, xs, side="right") - 1, 0, len(x) - 2)
    t = (xs - x[index]) / h[index]
    t2, t3 = t * t, t * t * t
    return (
        (2 * t3 - 3 * t2 + 1) * y[index]
        + (t3 - 2 * t2 + t) * h[index] * slopes[index]
        + (-2 * t3 + 3 * t2) * y[index + 1]
        + (t3 - t2) * h[index] * slopes[index + 1]
    )


def _end_slope(h0: float, h1: float, d0: float, d1: float) -> float:
    slope = ((2 * h0 + h1) * d0 - h0 * d1) / (h0 + h1)
    if np.sign(slope) != np.sign(d0):
        return 0.0
    if np.sign(d0) != np.sign(d1) and abs(slope) > 3 * abs(d0):
        return 3 * d0
    return float(slope)


def _format_bytes(value: float, _position: int = 0) -> str:
    """Tick label for a byte count: 128 B, 2 KiB, 1 MiB."""
    for unit in ("B", "KiB", "MiB"):
        if value < 1024:
            return f"{value:g} {unit}"
        value /= 1024
    return f"{value:g} GiB"


def _log_x_axis(ax: plt.Axes, ticks: np.ndarray, bytes_axis: bool = False) -> None:
    ax.set_xscale("log")
    ax.set_xticks(ticks)
    formatter = _format_bytes if bytes_axis else (lambda value, _: f"{value:,.0f}")
    ax.xaxis.set_major_formatter(FuncFormatter(formatter))
    ax.xaxis.set_minor_formatter(NullFormatter())
    ax.tick_params(axis="x", labelrotation=45)
    ax.grid(True, axis="x", color=_GRIDLINE, linewidth=0.75)


def _sweep_x_axis(ax: plt.Axes, ticks: np.ndarray, dimension: str) -> None:
    """The sweep's x axis with a tick at every data value: log for rates and
    sizes, linear for a count of homes (a linear ramp of small integers)."""
    if dimension != "homes":
        _log_x_axis(ax, ticks, bytes_axis=dimension == "size")
        return
    ax.set_xticks(ticks)
    ax.xaxis.set_major_formatter(FuncFormatter(lambda value, _: f"{value:g}"))
    ax.grid(True, axis="x", color=_GRIDLINE, linewidth=0.75)


# Bottom margin reserved for the two-line footnote (tight_layout rect).
_SWEEP_RECT = (0.0, 0.075, 1.0, 1.0)
_SWEEP_RECT_TALL = (0.0, 0.045, 1.0, 1.0)


def _sweep_footnote(fig: plt.Figure) -> None:
    fig.text(
        0.01, 0.008,
        "One point per ramp step; lines are shape-preserving interpolation between them, so up "
        "to the knee every variant sits on the same line.\nReceived rate = the step's distinct "
        "receipts over its scheduled sending time, counted from the step's first receipt "
        "for the same time; steps without receiver "
        "data are omitted.",
        fontsize=7, color=_INK_MUTED, linespacing=1.4,
    )


def _plot_statistics_ladder(
    path: Path,
    title: str,
    labels: list[str],
    stats: list[dict[str, Any] | None],
    uncertainty_ms: list[float] | None = None,
) -> Path:
    fig, ax = _new_axes(title, "", "Latency (ms)")
    fig.set_size_inches(9, 4.8)
    xs = np.arange(len(labels))

    def series(key: str) -> np.ndarray:
        return np.array([np.nan if s is None else s.get(key, np.nan) for s in stats])

    if uncertainty_ms is not None:
        # The clock-offset bound applies to the whole distribution's absolute
        # position, so it is drawn once per variant, behind the markers.
        ax.errorbar(
            xs,
            series("median"),
            yerr=np.array(uncertainty_ms, dtype=float),
            fmt="none",
            ecolor=_UNCERTAINTY_BAND,
            elinewidth=7,
            zorder=1,
            label="clock-offset bound",
        )
    ax.vlines(
        xs, series("median"), series("p99"), color=_BASELINE, linewidth=1, zorder=2
    )
    for stat, color, marker in _PERCENTILE_STYLE:
        ax.scatter(
            xs, series(stat), s=34, color=color, marker=marker, label=stat, zorder=3,
            linewidths=0,
        )
    ax.scatter(
        xs,
        series("mean"),
        s=_MEAN_MARKER_SIZE,
        color=_INK_PRIMARY,
        marker="_",
        linewidths=1.6,
        label="mean",
        zorder=4,
    )

    ax.set_xticks(xs)
    ax.set_xticklabels(labels, rotation=30, ha="right", fontsize=8, color=_INK_PRIMARY)
    ax.set_ylim(bottom=0)
    ax.margins(x=0.03)
    ax.legend(
        loc="upper left", frameon=False, fontsize=8, labelcolor=_INK_PRIMARY,
        ncols=6, columnspacing=1.2, handletextpad=0.4,
    )
    return _save(fig, path)
