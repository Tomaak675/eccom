# Measured values reference

Every number this framework reports is derived offline from **raw events**.
Agents and protocol adapters only record timestamps; they never aggregate. That
separation is deliberate: the measurement path stays cheap (one list append and
two clock reads), and any statistic can be recomputed from stored artifacts
without re-running an experiment.

```
agents record events  ->  *-events.jsonl  ->  eccom analyze   ->  summary.json + plots/
                          *-system-metrics.jsonl              ->  eccom compare  ->  comparison tables
```

- `eccom analyze <run-dir>` — statistics for one run.
- `eccom compare <run-dir>...` — several runs side by side, decomposed per stage.

## 1. The event log

One JSONL file per host, `<role>-events.jsonl`. Each line is a
`MeasurementEvent` (`eccom/core/events.py`):

| Field | Meaning |
|---|---|
| `event_type` | One of the types in §1.1. |
| `correlation_id` | Ties the sender's and receiver's view of one message together. This is the join key for every cross-host metric. |
| `sequence_number` | Monotonically increasing per sender, across warmup and measurement. |
| `host` | Topology role label (`edge`, `cloud`) — names the file and identifies the clock domain. |
| `phase` | `warmup` or `measurement`, **sender side only**. Receiver events carry `null`; the analyzer derives their phase by joining on `correlation_id`. |
| `monotonic_ns` | `time.perf_counter_ns()`. Comparable only within one host and process. All duration maths uses this. |
| `wall_time_ns` | `time.time_ns()`. Comparable across hosts only after applying the measured clock offset. |
| `metadata` | Optional per-event detail (see §1.2). |

`perf_counter_ns` is used rather than `monotonic_ns` because the latter has
15.6 ms resolution on Windows; on Linux both map to `CLOCK_MONOTONIC`.

### 1.1 Event types and where they are recorded

| Event | Host | Recorded by | Fires when |
|---|---|---|---|
| `PAYLOAD_CREATED` | sender | `SenderAgent` | The payload bytes exist and the `Message` is built, before any protocol work. |
| `SEND_STARTED` | sender | `SenderAgent` | Immediately before handing the message to the adapter. **Start of every sender-side latency.** |
| `SEND_COMPLETED` | sender | protocol adapter (wire boundary) | The transport considers the message handed off — see §3.2, this is protocol-specific. |
| `MESSAGE_RECEIVED` | receiver | protocol adapter | Bytes arrived and metadata was parsed. |
| `HANDLER_STARTED` | receiver | `ReceiverAgent` | Application handler entered. |
| `HANDLER_COMPLETED` | receiver | `ReceiverAgent` | Handler returned (includes `artificial_processing_delay_ms`). |
| `REPLY_SEND_STARTED` | receiver | protocol adapter | Reply handed to the transport (request/response only). |
| `REPLY_RECEIVED` | sender | `SenderAgent` | Correlated reply arrived. **Stops the round trip.** |
| `TIMEOUT` | sender | `SenderAgent` | No reply within `timeout_s`. |
| `ERROR` | sender | `SenderAgent` | Any other send failure; `metadata.error` holds the exception repr. |
| `REPLY_SEND_COMPLETED` | — | **never recorded** | Defined but unused — see §7. |

Not every event appears in every configuration:

- **`SEND_COMPLETED` is absent for HTTP request/response.** The HTTP adapter
  records it only for one-way sends; in request/response the reply *is* the
  acknowledgement, so `rtt` covers it. MQTT records it in both modes.
- **`REPLY_SEND_STARTED` and `REPLY_RECEIVED` exist only in request/response.**
- **`HANDLER_*` always exist**, in both interaction models.

### 1.2 Event metadata

| Key | On which events | Meaning |
|---|---|---|
| `size_bytes` | `PAYLOAD_CREATED`, `MESSAGE_RECEIVED`, `REPLY_*` | Payload size in bytes; feeds byte throughput. |
| `qos` | `SEND_COMPLETED` (MQTT) | The QoS level that produced this acknowledgement. |
| `device`, `device_type` | `PAYLOAD_CREATED` (devices mode) | Which simulated device fired. |
| `error` | `ERROR` | `repr()` of the exception. |

## 2. Clocks and the clock-offset correction

Two timestamps are captured for every event because two different questions
need different clocks.

**Same-host durations** (round trip, send-ack, handler time) subtract two
`monotonic_ns` values from the same process. These are exact and need no clock
synchronization — they are the trustworthy numbers.

**Cross-host durations** (one-way delivery) must compare the sender's clock to
the receiver's, which are never perfectly synchronized. The framework measures
the relationship rather than assuming it. The sender probes the receiver's
control endpoint 15 times before and after the run, NTP-style
(`eccom/instrumentation/clock_sync.py`):

```
offset = ((t1 - t0) + (t2 - t3)) / 2      # receiver clock minus sender clock
rtt    = (t3 - t0) - (t2 - t1)
```

with `t0`/`t3` the sender's send/receive wall times and `t1`/`t2` the
receiver's. The probe with the smallest RTT wins, since asymmetry is bounded by
the round trip.

`<sender>-clock-sync.json`:

| Field | Meaning |
|---|---|
| `method` | `control-endpoint-ntp-probe`, or `same_host` when both agents share a machine (offset 0, uncertainty 0). |
| `offset_ns` | Mean of the before/after probes. Subtracted from cross-host deltas. |
| `uncertainty_ns` | `max(before, after uncertainty) + abs(drift) / 2`, where each probe's uncertainty is `min_rtt / 2` — the worst case under fully asymmetric routing. |
| `drift_ns` | How far the offset moved during the run. Non-zero drift widens the uncertainty. |
| `before`, `after` | The raw probe records (`offset_ns`, `uncertainty_ns`, `min_rtt_ns`, `samples`, `measured_at_wall_ns`). |

### 2.1 Host clock discipline (chrony) — the tighter bound

The probe can only synchronize over the link it measures, so its bound is
RTT/2 *of that link*. A host disciplined by chronyd carries its own bound over
a much shorter path — to its time reference — and at run start the runner
snapshots `chronyc tracking` on every SSH host (the agent containers run with
`--network host`, so the host clock chronyd disciplines is exactly the clock
the agents stamp). Stored in `run.json` under `clock_sync`:

| Field | Meaning |
|---|---|
| `per_host.<role>.max_error_s` | Worst case for that host's clock against true time: `\|system time offset\| + root_delay/2 + root_dispersion`. The `root_delay/2` term is the same asymmetry bound the probe pays, but over the short path to the host's own reference. |
| `per_host.<role>.tracking` | The parsed tracking fields (reference, stratum, offsets, delays, leap status). |
| `combined_max_error_s` | Sum over hosts — bounds the true inter-host offset independently of the measured link's asymmetry. Present only when every host reports a synchronized chrony. |

The analyzer (`resolve_clock_sync`) always applies the probe's measured offset
and reports whichever bound is tighter: the probe's `uncertainty_ns`, or
`|probe offset| + combined_max_error_s` (the applied offset's own magnitude
joins the chrony bound, since chrony bounds the *true* offset around zero).
`summary.json` records the winner in `clock_sync.uncertainty_source`
(`probe` / `chrony-tracking`) and the printed one-way line appends
"bound from chrony". A run whose probe failed falls back to offset 0 under the
chrony bound alone; a host without chrony simply leaves `combined_max_error_s`
absent and the probe bound stands, so old runs and bare hosts keep working.

> **Interpretation rule.** A one-way latency is only meaningful when it is
> large compared with the reported uncertainty. Over a ~24 ms link the
> probe alone bounds it at ~±12 ms; with both hosts chrony-disciplined against
> nearby references the combined bound is typically a few milliseconds. One-way *differences* smaller than the
> bound are not resolvable and must be reported as "no measurable difference"
> rather than as a ranking. Same-host metrics (RTT, send-ack, handler) are
> unaffected — they never leave one clock.

## 3. Latency metrics

### 3.1 Round trip — `rtt_ms` (request/response only)

```
rtt = REPLY_RECEIVED.monotonic_ns − SEND_STARTED.monotonic_ns      (sender clock)
```

The complete interaction as the sender experiences it. Clock-sync-free.

### 3.2 Send acknowledgement — `send_ack_ms`

```
send_ack = SEND_COMPLETED.monotonic_ns − SEND_STARTED.monotonic_ns  (sender clock)
```

Clock-sync-free, but **what counts as "completed" is protocol-specific**, and
this is the single most important thing to keep straight when comparing
configurations:

| Configuration | `SEND_COMPLETED` fires when |
|---|---|
| HTTP / HTTPS, one-way | The server's `204` arrives — a transport ack costing one round trip. The handler runs asynchronously afterwards. |
| HTTP, request/response | Not recorded; use `rtt_ms`. |
| MQTT QoS 0 | The publish call returns, i.e. bytes handed to the socket. **No broker acknowledgement exists at QoS 0** — this is a purely local measurement. |
| MQTT QoS 1 | Broker `PUBACK` — one round trip. |
| MQTT QoS 2 | Broker `PUBCOMP` — the four-packet handshake, two round trips. |
| MQTT with `reconnect_per_message` | All of the above **plus** the TCP handshake and `CONNECT`/`CONNACK` that preceded the publish. |

A QoS 0 send-ack of ~1 ms and a QoS 1 send-ack of ~24 ms do not mean QoS 0
delivers 20× faster; they mean QoS 0 does not wait. Compare delivery (§3.3) to
answer that question.

### 3.3 One-way delivery — `one_way_latency_ms` (one-way only)

```
delivery = MESSAGE_RECEIVED.wall_time_ns − SEND_STARTED.wall_time_ns − offset_ns
```

Genuine edge-to-cloud delivery time, and the only metric that needs the clock
offset — read it together with `uncertainty_ns` (§2).

### 3.4 Receiver-side

| Metric | Interval | Note |
|---|---|---|
| `receiver_handler_ms` | `HANDLER_STARTED` → `HANDLER_COMPLETED` | Application processing; equals `artificial_processing_delay_ms` plus real work. |
| `receiver_total_ms` | `MESSAGE_RECEIVED` → `REPLY_SEND_STARTED` | Total time in the receiver, request/response only. |

Both use the receiver's own monotonic clock, so both are clock-sync-free.

### 3.5 Stage decomposition (`eccom compare`)

`eccom compare` splits each run into every interval the event log supports:

| Stage | Interval | Clock |
|---|---|---|
| `build` | `PAYLOAD_CREATED` → `SEND_STARTED` | sender monotonic |
| `send_ack` | `SEND_STARTED` → `SEND_COMPLETED` | sender monotonic |
| `delivery` | `SEND_STARTED` → `MESSAGE_RECEIVED` (offset-corrected) | cross-host |
| `rtt` | `SEND_STARTED` → `REPLY_RECEIVED` | sender monotonic |
| `dispatch` | `MESSAGE_RECEIVED` → `HANDLER_STARTED` | receiver monotonic |
| `handler` | `HANDLER_STARTED` → `HANDLER_COMPLETED` | receiver monotonic |
| `reply_send` | `HANDLER_COMPLETED` → `REPLY_SEND_STARTED` | receiver monotonic |

The **headline metric** switches with the interaction model — `rtt` for
request/response, `delivery` for one-way — so runs of different types tabulate
together. Stages a configuration does not record show `-` rather than a
fabricated zero.

Note that `build`, `dispatch` and `handler` are typically 0.01–0.2 ms against
network times of tens of milliseconds. Local processing is normally noise; if
it is not, that itself is the finding.

## 4. Counts and rates

All counts cover the **measurement phase only** — warmup is excluded.

| Field | Meaning |
|---|---|
| `counts.messages_attempted` | Measurement-phase `SEND_STARTED` events. |
| `counts.messages_completed` | One-way: `SEND_COMPLETED` count. Request/response: number of completed round trips. |
| `counts.messages_failed` | `attempted − completed`. |
| `counts.timeouts` / `counts.errors` | Sender-side `TIMEOUT` / `ERROR` events. |
| `counts.received_by_receiver` | `MESSAGE_RECEIVED` events whose correlation id belongs to the measurement phase. Compare with `messages_completed` to detect silent loss — at QoS 0 an acknowledged send can still never arrive. |

Rates, from `<sender>-load-report.json` plus the events:

| Field | Meaning |
|---|---|
| `rates.requested_message_rate` | What the scenario asked for. |
| `rates.actual_message_rate` | `attempted / send_window_s`, as achieved. **Always check this**: the load generator is open loop and Python may be the bottleneck, so a shortfall here invalidates any throughput conclusion. |
| `send_window_s` | First send start to last send start. |
| `total_duration_s` | Including draining in-flight requests. |
| `per_device` | Devices mode only: attempted/completed per simulated device. |
| `steps` | Ramp and homes modes only: one entry per step — `target_message_rate`, `actual_message_rate` (attempted / `duration_s`), attempted/completed/failed, and `duration_s`, how long that step was the active schedule. A step the schedule never reached appears with zero sends. |

`throughput` is measured on a single host to avoid clock-sync error:

- **One-way**: from the receiver's `MESSAGE_RECEIVED` wall times — count and
  window both live on the receiver.
- **Request/response**: from the sender, using send wall time plus RTT.

Both report `duration_s`, `messages_per_second` and `payload_bytes_per_second`
(payload bytes only — protocol headers are not counted).

### 4.1 Ramp decomposition — `ramp` (load.mode: ramp and homes)

A ramp run multiplies the offered rate every `step_duration_s` (a homes run
adds simulated homes instead), and the question it answers is *where*
latency leaves its floor and *what* runs out first. `summary.json` therefore
carries `ramp`: one row per step over the measurement phase. Messages belong
to the step whose tag the load generator put in their `PAYLOAD_CREATED`
metadata (`step`, `target_rate`, plus `target_homes` in homes mode), never
to a timestamp window. Two windows are then laid over a step's own messages:
the resource `window` scopes the CPU and NIC samples, and the receive window
decides which of the step's receipts count toward its
received rate. A message never moves to another step, however late it
arrives.

| Field | Meaning |
|---|---|
| `step`, `target_message_rate`, `payload_size_bytes`, `homes` | Step index, the offered rate, the step's payload size (constant unless `load.size_multiplier` grows it; `null` in homes mode, whose steps mix sizes) and the step's number of simulated homes (`null` outside homes mode). The offered rate is the whole schedule's: with `load.senders: n` each process tags its own share, and the analysis multiplies the tag by `n`. |
| `messages_attempted`, `messages_completed`, `messages_failed`, `timeouts`, `errors` | The §4 sender counts, restricted to this step's messages. `timeouts_delivered` are the timeouts whose message reached the receiver anyway. |
| `received_by_receiver`, `duplicate_receipts`, `lost` | Distinct messages of this step the receiver saw, **whenever** they arrived; the extra arrivals of a message already seen; and `messages_attempted − received_by_receiver`, the messages that never arrived. |
| `step_sending_time_s` | The step's scheduled sending time: `load.step_duration_s`, shorter for a truncated last step. `load.step_pause_s` is not part of it. |
| `received_in_step`, `received_after_step` | `received_by_receiver` split at the end of the step's receive window: `step_sending_time_s` long, starting at the first receipt of one of the step's messages (receiver clock only, no offset needed; with `load.senders: n` per sender process, since the processes start apart). The sender sends for one sending time, the receiver is given the same time from the moment the step reaches it. `received_after_step` arrived during the step's pause, a later step or after the run. `received_in_step + received_after_step + lost = messages_attempted`. |
| `sent_rate`, `completed_rate`, `received_rate` | `messages_attempted`, `messages_completed` and `received_in_step`, each divided by `step_sending_time_s`: three rates on one denominator, directly comparable with `target_message_rate`. A message that arrives during the pause is drained into `received_by_receiver` but not into `received_rate`: the rate is what the receiver got while the step was being sent, so it falls below the sent rate as soon as a backlog builds. |
| `sent_fraction`, `completed_fraction`, `received_fraction` | `sent_rate / target_message_rate`, `messages_completed / messages_attempted` and `received_by_receiver / messages_attempted` (late receipts included). |
| `actual_send_rate` | `(n − 1) / span` of the step's `SEND_STARTED` wall times — the cadence the sender actually achieved, and the rate the schedule-held check below tests. Falls below the target once the sender saturates (or the concurrency cap binds). With `load.senders: n` it is formed per process over that process's own span and summed: the processes start up to a few seconds apart, and the pooled span would understate every step by that skew. |
| `delivered_rate` | Same estimator over the receiver's `MESSAGE_RECEIVED` wall times for this step's messages (per sender process, summed) — receiver clock only, no offset needed. A single straggler delivered tens of seconds late stretches the span and deflates it, and a backlog draining in a burst inflates it: kept for the record, the tables and charts read `received_rate`. |
| `send_ack_ms`, `rtt_ms`, `delivery_ms` | §6 statistics of this step alone (`delivery_ms` needs the clock offset). |
| `window` | `[start_wall_ns, end_wall_ns)`: first send of the step up to the first send of the next step; the last step ends at its last send, and so does every step when `load.step_pause_s` separates them (the pause belongs to no step). |
| `resources.<host>` | That agent's samples inside the window: `process_cpu_percent` (of one core, from the cumulative counter — §5.2), `system_cpu_percent` (host-wide, % of all cores, mean of the window's 1 Hz samples), `net_sent_mbit_s` / `net_recv_mbit_s` (from the NIC counters, so host-wide and summed over every interface, loopback included: with the broker on the receiver's host its hop to the receiver is counted too, so the sender host's figure is the one to read for the link), `loop_lag_ms_max` (§5), `samples`. Counter figures are `-` with fewer than two samples in the window. |

`plots/ramp_over_time.png` draws the run on one time axis — offered vs
achieved vs delivered rate per second of wall time, the sender-observed latency percentiles per
second, and both agents' CPU, NIC throughput and event-loop lag — with step
boundaries through every panel. `eccom compare` reduces a ramp run to two
numbers. `exact_message_rate` is the achieved rate of the last step that
**held**: the receiver got at least 99 % of the attempted messages (late
arrivals included, `received_fraction`) and received at least 95 % of the
offered rate within the step's scheduled sending time (`received_rate`);
sender timeouts alone do not end the walk (`miss_step` is the first step that
did not hold; when every step held, `exact_*` is the last step).
`sustained_message_rate` is the achieved rate of the last step before the
**knee**, the first step that lost a message or whose sender-observed p95
(send-ack for one-way, RTT for request/response) exceeded twice the first
step's p95 — an earlier, latency-based warning that trips on a transient
p95 spike or, for a variant whose send-ack is a local operation (MQTT
QoS 0), on a floor of a millisecond. For a ramp that grows the payload
(`load.size_multiplier`) the same rules apply with the payload size as the
swept dimension: `exact_payload_size_bytes`, `knee_payload_size_bytes` and
`sustained_payload_size_bytes`, and the tables and sweep charts below use
payload size as their axis; for a homes ramp likewise `exact_homes`,
`knee_homes` and `sustained_homes`, with homes as the axis — one sweep chart
per aggregation factor (`compare_ramp_*_x<k>.png`) and
`compare_homes_ceiling.png`, one bar per variant and factor for the homes
held. It also tabulates CPU per step across the compared runs — agent
process and host-wide, one table per host, offered rate down and runs across
— with each run's busiest step in bold, so the step where a host maxed out
is read off directly.

Reading the rows across steps:

| Observation | Meaning |
|---|---|
| `actual_send_rate` falls below the target, sender CPU near one full core, `loop_lag_ms_max` climbing | The sender is the ceiling. Its late timestamps also inflate every latency it records — see the note on loop lag in §5. |
| Send rate below target, sender CPU low | In-flight hit `load.concurrency`; raise it. |
| Send rate holds, `delivery_ms` grows, receiver process CPU near one core | The receiver is the ceiling. |
| Send rate holds, receiver *host* CPU high, receiver *process* CPU low | The broker is the ceiling (MQTT). |
| Every CPU has headroom, latency grows, sender NIC Mbit/s flat | The uplink is saturated (queueing in the network). Check against the link's measured capacity. |
| `timeouts` appear, or `received_by_receiver` < `messages_completed` | Loss: sender timeouts, or a broker queue limit dropping messages. |

Two levers sharpen the last two rows: the payload size (rerun with a payload
several times larger — if the knee in msg/s moves down in proportion it is
bandwidth, if it stays it is per-message overhead) and a one-off `iperf3`
measurement of the edge → cloud capacity so the NIC figures have a ceiling to
compare against.

### 4.2 Several sender processes — `load.senders > 1`

Each sender instance is an ordinary sender agent running its share of the
schedule (rate / n, counts and in-flight cap split evenly), with its own
`<sender>.<i>-events.jsonl`, `<sender>.<i>-load-report.json` and
`<sender>.<i>-system-metrics.jsonl`. The analysis presents them as one
sender:

- **Events** carry `host = <sender>` regardless of instance, so every latency
  table and the per-step attribution work unchanged; correlation ids are
  UUIDs and never collide, sequence numbers restart per instance.
- **Load reports** are summed: requested and achieved rates, attempted /
  completed / failed, per-step counters and target rates step by step;
  windows are the longest instance's. The merged report carries
  `instances: n`.
- **Resource samples** are folded onto instance 0's timeline (as-of join: for
  each of its samples, every other instance contributes its latest sample not
  older than ~2 intervals). Process CPU seconds, CPU % and RSS **add up**
  (the sender's total cost on the host, which can exceed 100 % of a core);
  `loop_lag_ms_max` is the maximum across instances; host-wide series (system
  CPU, memory, NIC counters) and the phase tag are instance 0's. So `edge`
  process CPU in §5 and in the `ramp` table is the sum over instances.
- Per-host artifacts that do not depend on the instance — the environment
  file and the clock-offset probe — are written by instance 0 under the bare
  host name.
- **The Go sender (`sender.implementation: go`) is always one process**, so
  none of this applies to it: it paces the whole schedule over
  `sender.threads` goroutines, writes the bare `<sender>-*` files, tags every
  send with the whole offered rate, and its single `process_cpu_percent` can
  exceed 100 % of a core by itself. `go_max_procs` in
  `<sender>-environment.json` records how many cores it was allowed.

### 4.3 Link capacity — `link_capacity` (instrumentation.link_capacity)

What the path between the sender's and the receiver's host can carry,
independent of any protocol under test, so NIC throughput and large-message
serialization delays have a ceiling to compare against. With
`instrumentation.link_capacity: true` the runner starts a one-off iperf3
server on the receiver's host and a client on the sender's host before any
agent starts, on the scenario's own endpoint port (open between the hosts by
construction, and still free at that point), for `link_capacity_duration_s`
seconds in each direction. The raw iperf3 JSON documents land in
`<sender>-link-capacity.json`; `summary.json` carries the digest:

| Field | Meaning |
|---|---|
| `sender_to_receiver_mbit_s` | Goodput at the receiving end, sender's host → receiver's host — the direction the scenario's traffic takes. |
| `receiver_to_sender_mbit_s` | The reverse direction (`iperf3 -R`). |
| `retransmits` | TCP retransmissions during the forward run — loss on the path. |
| `mean_rtt_ms` | The kernel's mean RTT on the forward connection, when iperf3 reports it. |
| `errors` | Per direction, when the probe failed; that direction's figures are then absent. |

Caveats: a single TCP flow for a few seconds measures what one connection
gets through slow start and the path's bottleneck, not a guaranteed rate, and
run-to-run variation on a consumer uplink is normal — which is why the probe
runs per run rather than once per suite. It is off by default and never
fatal.

## 5. Resource metrics

`<role>-system-metrics.jsonl`, sampled every
`instrumentation.system_metrics_interval_s` (default 1.0 s):

| Field | Meaning |
|---|---|
| `phase` | `startup`, `baseline` or `traffic` — see below. |
| `wall_time_ns`, `monotonic_ns` | Sample timestamps. |
| `process_cpu_seconds` | **Cumulative** CPU seconds for the agent process (`cpu_times().user + .system`). The basis for all CPU figures. |
| `process_cpu_percent` | psutil's sampled percentage. Retained for continuity, but see the warning below. |
| `system_cpu_percent` | Host-wide CPU. |
| `process_rss_bytes` | Agent resident set size. |
| `system_memory_percent` | Host-wide memory. |
| `net_bytes_sent`, `net_bytes_recv` | Cumulative host-wide NIC counters, summed over all interfaces including loopback (all traffic on the machine, not just this agent). |
| `loop_lag_ms_max` | Worst event-loop stall since the previous sample: a 10 ms asyncio timer in the agent measures how late it actually fired. Every eccom timestamp is taken in userspace, so when the agent's loop is saturated its `SEND_*` stamps are late by exactly this kind of delay and the inflation is indistinguishable from network latency in the events alone — this field is what separates "my process is late" from "the network is slow". Also catches GC pauses. Floor: the OS timer resolution (~0.1 ms on Linux, ~15 ms on Windows). For a Go agent (`implementation: go` in either role) it is the overshoot of a 10 ms sleep in a goroutine, i.e. scheduler plus GC latency; same key, same reading. |

> **Do not average `process_cpu_percent`.** At 1 Hz it quantizes to whole
> percent, which is coarser than a lightly loaded agent's real usage — samples
> read `0.0`, `1.0`, `2.0` and nothing between. Averaging them produced
> *negative* adjusted CPU in practice. Use `process_cpu_seconds`.

### 5.1 The idle baseline

Each agent sits quiet for `instrumentation.baseline_s` (default 3 s) before any
traffic and tags those samples `phase: "baseline"`. On the sender this window
sits *after* the clock probe, so the probe's own traffic is not counted as
idle; on the receiver it runs before the sender has even been started. This
gives an idle reference for that exact process on that exact host, so resource
use can be reported as the cost of the traffic rather than in absolute terms.
Runs recorded before this existed fall back to inferring quiet windows from the
traffic boundaries.

### 5.2 Adjusted CPU

CPU over a window comes from the counter delta, never from averaged
percentages:

```
cpu%(W) = 100 × (cpu_seconds[last] − cpu_seconds[first])
              / ((wall_ns[last] − wall_ns[first]) / 1e9)
```

For several disjoint windows, numerators and denominators are summed
separately. Then:

```
cpu_adjusted% = cpu%(measurement window) − cpu%(baseline window)
```

Worked example from a real run:

```
BASELINE     n=3   cpu_seconds 0.7188 → 0.7188   Δ=0.0000 s over  2.031 s → 0.000 %
MEASUREMENT  n=60  cpu_seconds 1.2031 → 3.1719   Δ=1.9688 s over 59.310 s → 3.319 %
cpu_adjusted = 3.319 − 0.000 = 3.319 %
```

Percentages are **of one core** (psutil convention): a saturated 4-core host
reads 400 %. Precision does not depend on the sample count — the counter delta
is exact whether the window holds 3 samples or 300.

### 5.3 Adjusted memory

RSS is a gauge, not a counter, so it uses arithmetic means:

```
rss_adjusted = mean(rss over measurement) − mean(rss over baseline)
```

The memory baseline uses **only the pre-traffic window**, never post-run
samples. The rule dates from when the agents buffered every event in memory
until close (RSS never returned to its resting value, and post-run samples
made the delta negative); both agents now stream their events to disk as
they go, but the post-run window still carries the drain and the
cooldown, so the rule stands.

## 6. Statistics produced

Every latency metric is summarized by `latency_stats_ms`
(`eccom/analysis/statistics.py`):

`count`, `min`, `mean`, `median`, `std` (sample standard deviation, `ddof=1`),
`p90`, `p95`, `p99`, `max` — percentiles via `numpy.percentile` (linear
interpolation).

Read the tail, not just the mean. Two configurations with the same mean can
behave very differently: MQTT QoS 0 shows a p90 of ~13 ms and a p99 of ~24 ms
because nothing absorbs jitter, while QoS 1 moves under 2 ms over the same
interval.

## 7. What is deliberately *not* measured

- **Serialization / deserialization.** There are no `SERIALIZATION_*` event
  types. Encoding happens inside aiohttp and paho and is never timestamped, so
  it is folded invisibly into `send_ack` and `delivery`. Capturing it means
  emitting new events inside the adapters around the encode/decode calls.
  Treat any serialization figure quoted elsewhere as unmeasured.
- **`REPLY_SEND_COMPLETED`.** The type exists but is never recorded: aiohttp
  does not cheaply expose "response fully written". Client tracing hooks are
  the extension point.
- **Network-level timing.** `instrumentation.network_capture` is reserved and
  rejected at config load; there is no pcap / `SO_TIMESTAMPING` / eBPF probe
  yet. Everything here is measured in userspace, so kernel queueing and NIC
  time are included in the transport intervals but not separable from them.
- **Per-agent network bytes.** `net_bytes_*` are host-wide NIC counters, so on
  a busy machine they include unrelated traffic.

## 8. Where each value lands

| Artifact | Contents |
|---|---|
| `<role>-events.jsonl` | Raw events — the source of truth for every latency figure. |
| `<role>-system-metrics.jsonl` | Resource samples. |
| `<sender>-load-report.json` | Requested vs. achieved send rate, attempted/completed/failed, windows; per-step `steps` for ramp runs. |
| `<sender>-clock-sync.json` | Offset, uncertainty, drift, raw probes. |
| `<sender>-link-capacity.json` | The iperf3 link probe (§4.3), both directions with the raw documents — only with `instrumentation.link_capacity`. |
| `<role>-environment.json` | Interpreter or toolchain (`implementation`, `python_version` / `go_version`, `go_max_procs` for the Go agent), OS, CPU, dependency and git metadata per host. |
| `broker/mosquitto.conf`, `broker/nanomq.conf` | MQTT runs with a managed broker: the exact config the broker ran from (listener, flow-control limits, packet-size limit, TLS material), named after `protocol.mqtt.broker.implementation`; the NanoMQ one also records the image it was generated for. |
| `broker/mosquitto.log`, `broker/nanomq.log` | The managed broker's own log, captured at shutdown. mosquitto: its version (the `eclipse-mosquitto:2` tag floats between releases) and every client disconnect with its reason. NanoMQ: every client's `online!` / `offline!` line and every dropped message (`msg lost due to reach the limit of lmq`). |
| `summary.json` | Everything in §3–§5 for one run, written by `eccom analyze`; ramp runs add the §4.1 `ramp` table. |
| `plots/` | Latency histogram, latency over time, throughput over time; ramp runs add `ramp_over_time.png`. |
| `run.json`, `hosts.json` | Run id, deployment type, and which infrastructure each role used (secrets redacted). |

## Related

- `docs/scenario-parameters.md` — every configurable input, including
  `instrumentation.baseline_s`.
- `docs/architecture.md` — the timestamp capture points drawn on a sequence
  diagram.
- `README.md` — the `eccom compare` workflow.
