# eccom — Edge-to-Cloud Communication Measurement Framework

An experimental framework for comparing Edge-to-Cloud communication
technologies. HTTP and MQTT v5 are implemented, each with or without TLS;
further protocols plug in as adapters. Edge and Cloud run the **same application code**; behavior is selected by configuration
(`--role sender|receiver` plus a YAML scenario). All measurements are recorded
as **raw events** on each host and merged/analyzed offline.

## Quick start (local, no Docker)

Requires Python 3.12+.

```bash
python -m venv .venv
.venv/Scripts/activate          # Windows; on Linux: source .venv/bin/activate
pip install -e ".[dev]"

# run a short end-to-end experiment on this machine
eccom run scenarios/http_smoke.yaml
```

Or with simulated IoT devices instead of a uniform stream:

```bash
eccom run scenarios/iot_devices_http.yaml
```

Or over MQTT (needs Docker: the runner starts and removes a mosquitto broker
container automatically):

```bash
eccom run scenarios/mqtt_smoke.yaml
```

This starts the receiver ("cloud") and sender ("edge") as local subprocesses,
runs warmup + the measured load, drains in-flight requests, shuts the receiver
down gracefully, and then analyzes the run. You get a terminal digest plus a
run directory:

```
runs/<timestamp>-<scenario-name>/
  scenario.yaml               # the exact configuration that ran
  run.json                    # run id, deployment type, clock sync info
  edge-environment.json       # per-host metadata (python, os, cpu, deps, git commit)
  cloud-environment.json
  hosts.json                  # which infrastructure each role ran on (secrets redacted)
  edge-events.jsonl           # RAW measurement events (one JSON object per line;
  cloud-events.jsonl          #   .jsonl.gz when collected from an SSH host)
  edge-load-report.json       # requested vs. actually achieved send rate
  edge-clock-sync.json        # measured clock offset vs. receiver + uncertainty
  edge-system-metrics.jsonl   # periodic cpu/memory/network samples (psutil)
  cloud-system-metrics.jsonl
  summary.json                # counts, latency stats, throughput, per-host CPU/memory
  plots/                      # latency_histogram, latency_over_time, throughput_over_time
  logs/                       # per-agent stdout/stderr
```

Re-analyze any run later without re-running the experiment:

```bash
eccom analyze runs/<run-id>
```

The longer baseline (60 s at 100 msg/s, per the scenario) is
`scenarios/http_baseline.yaml`.

## Commands

| Command | Purpose |
|---|---|
| `eccom run <scenario.yaml or suite-dir> [--hosts <hosts.yaml>] [--pause-s <s>] [--shuffle] [--no-analyze]` | full lifecycle (orchestrator); a directory runs every YAML inside in sequence, `--pause-s` idles between them so runs separate in network monitoring, `--shuffle` runs them in random order instead of by filename (the order is logged), `--no-analyze` leaves the analysis to a later `eccom analyze` |
| `eccom agent --role sender\|receiver --scenario <yaml> --run-dir <dir>` | one agent process; what deployments launch on each host |
| `eccom analyze <run-dir>` | recompute `summary.json` + `plots/` from raw events |
| `eccom compare <run-dir>... [--csv <f>] [--output <f>] [--plots <dir>]` | tabulate several runs side by side: headline latency, per-stage means, resource usage |

### Comparing runs

`eccom analyze` summarizes one run; `eccom compare` puts several next to each
other and is what you want after running a suite:

```bash
eccom compare runs/<run-id>... --output results.md --csv stages.csv
```

It prints three markdown tables: the headline latency (round-trip for
request/response, one-way delivery for one-way) with mean/median/p90/p95/p99,
the mean of every measurable stage, and CPU/memory measured against each
agent's own idle window. `--csv` writes long-format per-stage statistics for
further analysis. Stages that a configuration does not record show `-` rather
than a fabricated zero — for example `send_ack` is absent for HTTP
request/response, which has no separate transport ack.

The stages come straight from the event log:

| Stage | Interval | Clock |
|---|---|---|
| `build` | `PAYLOAD_CREATED` → `SEND_STARTED` | sender monotonic |
| `send_ack` | `SEND_STARTED` → `SEND_COMPLETED` | sender monotonic |
| `delivery` | `SEND_STARTED` → `MESSAGE_RECEIVED` (offset-corrected) | cross-host |
| `rtt` | `SEND_STARTED` → `REPLY_RECEIVED` | sender monotonic |
| `dispatch` | `MESSAGE_RECEIVED` → `HANDLER_STARTED` | receiver monotonic |
| `handler` | `HANDLER_STARTED` → `HANDLER_COMPLETED` | receiver monotonic |
| `reply_send` | `HANDLER_COMPLETED` → `REPLY_SEND_STARTED` | receiver monotonic |

Serialization is deliberately absent: it happens inside aiohttp/paho and is
never timestamped, so it is folded into `send_ack` and `delivery`.

CPU is computed from the cumulative `process_cpu_seconds` counter rather than
sampled percentages, which quantize to whole percent at 1 Hz and cannot
resolve a lightly loaded agent.

The idle reference is real, not inferred: every agent sits quiet for
`instrumentation.baseline_s` (default 3 s) before any traffic and tags those
samples `phase: baseline`. The adjusted columns are therefore the cost of the
traffic itself, with the agent's own resting cost subtracted.

With `instrumentation.link_capacity: true` the runner also measures the link
itself before every run: an iperf3 server on the receiver's host and a client
on the sender's host, both directions, on the scenario's own endpoint port
(so no extra firewall rule), from the agent image. The result lands in
`<sender>-link-capacity.json`, the summary and the comparison table, giving
NIC figures and large-message latencies a ceiling to compare against.

## Scenario suites (one experiment, many protocol configurations)

A directory of scenario files is a suite: `eccom run scenarios/scenario1
--hosts hosts/remote.yaml` runs every YAML inside in filename order
(number-prefix them to control the sequence), keeps going when one variant
fails, prints a per-variant summary plus a final overview, and produces one
run directory per variant. `scenarios/scenario1/` is a worked example: the
same 128 B / 1 msg/s / 60 s / concurrency 1 traffic across http, https, mqtt
QoS 0/1/2 and mqtts, plus a connection-setup variant per protocol (HTTP
without keep-alive, MQTT reconnecting per message) and a request/response
variant per protocol against its one-way baseline. Each file changes
exactly one thing, so measured differences are attributable to that one
configuration choice. TLS variants need certs in
`scenarios/scenario1/certs/` (gitignored; see the TLS section) and their
ports opened on the receiver (8443, 8883 in the example).
`scenarios/scenario2/` reuses that matrix (minus the connection-setup
variants) under a ramped load — `load.mode: ramp` doubles the rate every 5 s
until the sender saturates — to find each configuration's throughput ceiling;
its `summary.json` carries a per-step `ramp` table and `plots/` gains
`ramp_over_time.png`, and `eccom compare` reports the rate the schedule held
(nothing lost, rate achieved) and the rate sustained before the latency knee
and, with `--plots`, draws every variant's received rate and delivery
latency against the offered rate. `scenarios/scenario3/` keeps the rate at
10 msg/s and quadruples the payload every 10 s instead (`load.size_multiplier`),
128 B to 2 MiB, to find where message size rather than message rate sets the
latency. `load.senders: n` runs the sender as `n` processes (containers on
SSH hosts) that share the schedule, for when one Python sender is the
ceiling; their artifacts are merged back into one sender view by the
analysis. `scenarios/scenario4/` uses that — six sender processes, the Go
receiver and a linear ramp (`load.rate_increment`: 1 800 msg/s + 600 per
10 s step up to 15 600 msg/s, with a 10 s `load.step_pause_s` after every
step so each step starts from a drained queue) — to find the ceilings of
the protocol stacks once neither side is one Python core.
`scenarios/scenario5/` runs the same matrix with the Go sender and a
steeper ramp, plus NanoMQ variants of the MQTT files.
`scenarios/scenario6/` ramps the number of simulated smart
homes instead of a uniform rate (`load.mode: homes`, below) across scenario
2's matrix, four times over for aggregation factors 1, 2, 4 and 8, to find
how many homes each stack carries and how batching moves that ceiling.
`scenarios/scenario7/` repeats that ramp with TLS on every variant (HTTPS,
HTTPS request/response, MQTTS QoS 0, 1, 2 and MQTTS request/response), each
file running a four-step window instead of the full ramp.
`docs/scenario-parameters.md` describes every suite.

## Mix and match: scenarios × hosts

A scenario describes the *experiment* (protocol, traffic, payloads); a hosts
file describes the *infrastructure* (where each topology role runs). Any
scenario runs on any host mapping:

```bash
eccom run scenarios/http_baseline.yaml                                # local
eccom run scenarios/http_baseline.yaml --hosts hosts/remote.yaml       # remote
```

A hosts file maps topology labels to deployments (see
`hosts/remote.example.yaml`):

```yaml
hosts:
  edge:
    deployment: ssh
    ssh:
      host: 192.168.1.50
      user: edge
      key_file: ~/.ssh/edge_cloud_key   # or password: (discouraged; redacted in artifacts)
      working_dir: /opt/eccom           # scratch dir for the synced source and run artifacts
  cloud:
    deployment: ssh
    ssh:
      host: 20.0.0.1
      user: cloud
      key_file: ~/.ssh/edge_cloud_key
      working_dir: /home/cloud/eccom-runtime
      resources:                        # optional docker caps, see below
        cpus: 1.5
        memory: 512m
```

**SSH deployment = agents in Docker, no repo clone.** Targets need only
Docker and SSH. At run start the framework, per remote host and in parallel:

1. syncs the local source tree (a few hundred kB) to `<working_dir>/src`
   over SFTP — `working_dir` is a scratch directory, created automatically;
2. builds the agent image **natively on the host** (`docker build`), so the
   an ARM64 edge device and an x86-64 VM each get a matching image; the dependency
   layer is cached, so only the first build per host takes minutes and
   code-only changes rebuild in seconds. The image id is recorded in the
   run's environment metadata;
3. runs the agent as a container with `--network host` (no Docker NAT inside
   the measured path) and `--ulimit nofile=65536` (the container default of
   1024 is one socket per in-flight request away from choking a receiver at
   concurrency 1000), streams its logs back live into `runs/<id>/logs/`,
   and bind-mounts `<working_dir>/runs` for artifacts, which are collected
   over SFTP afterwards — gzip-compressed on the host first (about 8×
   smaller), so the local artifacts are `*.jsonl.gz` and the analysis reads
   either form; the remote copy is then removed, and `prepare()` warns when a host has less
   than 5 GiB free. `eccom run --no-analyze` skips the per-run analysis
   (minutes for a multi-million-message run) so a suite can keep measuring
   while `eccom analyze <run dir>` runs alongside. Graceful stop goes through the control endpoint
   (fallback: `docker stop`, which SIGTERMs so events still flush). The SSH
   connection sends keepalives and is reopened when lost, and a container
   runs without `--rm` so that, if the channel that started it dies, the
   runner follows it through `docker inspect` on a fresh connection, takes
   its log from `docker logs`, and removes it afterwards — a silently dead
   session does not hang a suite. An
   optional per-host `resources` block (`cpus`, `memory`) adds `--cpus` /
   `--memory` (swap disabled) to every container started on that host,
   agent and managed broker alike — on a small VM a runaway receiver can
   otherwise starve sshd and dockerd and hang the whole suite. The
   scenario's `sender.cpus` (or a Go sender's `threads`) replaces the
   host's `--cpus` for the sender containers, since the sender's cores are
   part of the experiment.

Configuration requirements: the scenario's protocol endpoint host is the
receiver's address *as reachable from the sender* (and from the orchestrator
machine, for health checks) — either a literal, or the receiver's topology
role label (`cloud`), which the runner resolves from the hosts file and
passes to every agent as `--endpoint-host` (the shipped scenarios do this,
so a new receiver host is a hosts-file change only) — and remote receivers bind `0.0.0.0`
(`server.bind_host`, `receiver.control_bind_host`). Host key verification is
disabled, which suits freshly provisioned test hosts; tighten it before using
untrusted networks. Filled-in hosts files are gitignored (only `local.yaml`
and `*.example.yaml` are tracked). Per-host native builds avoid
cross-architecture image shipping entirely.

HTTP settings are split per side, so sender and receiver behavior are
configured independently:

```yaml
protocol:
  type: http
  http:
    endpoint: { host: cloud, port: 8080 }      # the receiver: a role label (hosts file) or an address
    client:   { keep_alive: true, timeout_s: 5 } # sender-side
    server:   { bind_host: 0.0.0.0 }             # receiver-side
```

### Go agent

Either role can run as the Go agent under `go/` instead of the Python
package:

```yaml
sender:
  implementation: go        # default: python
  threads: 6                # Go only: cores the one sender process uses (0 = all)
receiver:
  implementation: go        # default: python
```

Same command line, same artifacts, same control API, so the runner and the
analysis do not change; only the deployment picks a different program. SSH
hosts build `docker/Dockerfile.go` natively (image `<image>-go`, a static
binary on a slim base), so remote runs need no Go toolchain anywhere. Local
runs use `go/bin/eccom-agent`, built automatically when `go` is on PATH.
A Python agent is a single event loop on one core, so its ceiling is a few
thousand messages per second per process. The Go
receiver uses every core, and the Go sender paces the whole schedule from
one process over `sender.threads` goroutines (so `load.senders` stays 1 and
`load.concurrency` / `http.client.max_connections` apply to that one
process); on SSH hosts its container is also capped at `threads` cores
(`docker --cpus`, or `sender.cpus` to choose), because `GOMAXPROCS` bounds
the goroutines but not the kernel work their syscalls do. Libraries, layout
and the few semantic differences are documented in `go/README.md`.

## Interaction models: request/response and one-way

`interaction: request_response` (default) sends every message as a request
and waits for the correlated reply; the primary metric is **RTT**, computed
purely from sender-side monotonic timestamps (clock-sync-free).

`interaction: one_way` (see `scenarios/http_one_way.yaml`,
`scenarios/mqtt_one_way.yaml`) sends fire-and-forget telemetry: MQTT
publishes without a response topic; HTTP posts and the server acknowledges
with 204 *before* processing (handlers run asynchronously, mirroring MQTT's
broker-side decoupling). Two latency metrics replace RTT:

- **send-ack** (`SEND_STARTED -> SEND_COMPLETED`, one clock, always valid):
  HTTP = the 204 arrived; MQTT QoS 1/2 = the broker's PUBACK/PUBCOMP; MQTT
  QoS 0 = only the local publish call (no network meaning).
- **one-way latency** (`SEND_STARTED` on edge -> `MESSAGE_RECEIVED` on
  cloud): crosses hosts, so it is corrected by a **measured clock offset**.

### Clock synchronization: measured, not assumed

Perfectly synchronized clocks don't exist; the framework instead *measures*
the offset with an explicit error bound. Before and after each run the
sender probes the receiver's control endpoint NTP-style
(`offset = ((t1-t0)+(t2-t3))/2`, best-of-N by lowest RTT); the worst-case
error is half that probe's RTT, widened by any drift observed between the
two snapshots. The result lands in `edge-clock-sync.json` and in
`summary.json` next to the one-way statistics — **a one-way latency is only
meaningful when it is large against the reported uncertainty**. Practical
guidance: on a LAN the bound is sub-millisecond; across the internet the
probe bound is roughly RTT/2, so also run chrony/NTP on both hosts — the
runner then reports the tighter of the two bounds (`docs/metrics.md` §2.1). Windows-only caveat: the *wall* clock ticks coarsely
(~0.5–15.6 ms), so sub-millisecond one-way values quantize on a Windows dev
machine; Linux hosts are unaffected.

### Latency phases

With both interaction models, the recorded events decompose a message's life
into: send/ack (`SEND_STARTED->SEND_COMPLETED`, sender clock), transport
(`SEND_STARTED->MESSAGE_RECEIVED`, offset-corrected), receiver dispatch and
processing (`MESSAGE_RECEIVED->HANDLER_STARTED->HANDLER_COMPLETED`, receiver
clock). Application (de)serialization is deliberately **not** a phase:
adapters receive prebuilt payload bytes, so there is nothing to measure
until serialization formats (JSON/MessagePack/protobuf) become their own
experiment dimension.

## Simulated IoT devices

Instead of a uniform constant-rate stream, a scenario can define simulated
devices, each firing on its own interval (`scenarios/iot_devices_http.yaml`):

```yaml
load:
  mode: devices
  duration_s: 60

devices:
  - { name: meter-1, type: smart_meter, interval_s: 1.0 }        # smart electricity meter
  - { name: temp-1, type: temperature_sensor, interval_s: 5.0 }
  - { name: cam-1, type: camera, interval_s: 10.0, size_bytes: 65536 }
  - { name: blob-1, type: generic, interval_s: 0.5, size_bytes: 4096 }
```

`smart_meter` and `temperature_sensor` carry hardcoded, realistic JSON
readings; `camera` and `generic` carry fixed seeded-random bytes of the given
size (camera defaults to 100 kB). Payloads are built once at startup, so
firing is free at send time and runs are reproducible. Devices fire open loop
(a slow network never delays the next fire), same-interval devices are
deterministically staggered, and every `PAYLOAD_CREATED` event is tagged with
`device`/`device_type` so results can be analyzed per device; the load report
contains per-device attempted/completed counts.

## Simulated smart homes

`load.mode: homes` replicates one *referential home* — a fixed mix of sensor
classes — and ramps the number of homes step by step
(`scenarios/scenario6/`):

```yaml
home:
  seed: 42
  sensors:
    - { type: smart_plug, count: 40, rate_hz: 1,  size_bytes: 16 }
    - { type: microphone, count: 4,  rate_hz: 50, size_bytes: 640 }

load:
  mode: homes
  homes: 2                 # first step
  homes_increment: 2       # added per step
  aggregation_factor: 2    # readings of one sensor slot across 2 homes -> one message
  step_duration_s: 10
  step_pause_s: 10
  duration_s: 120
  concurrency: 10000
```

The schedule is the ramp generator's (open loop, concurrency cap, step tags,
drain pauses) at the mix's aggregate rate, and the messages come out in the
order the sensors' staggered timers would fire them, so every second carries
the exact mix. The aggregation factor batches each sensor slot's readings
across that many homes into one message: the message rate drops by the
factor, the byte rate stays. Every message is tagged with its step, number
of homes and sensor class; the per-step table and `eccom compare` key the
held schedule and the knee on homes, and `--plots` draws one sweep per
factor plus the homes ceiling of every variant grouped by factor.

## MQTT

MQTT v5 is a second `ProtocolAdapter` (aiomqtt/paho) behind the same logical
API, so every traffic model (constant rate, ramp, devices, homes) and every scenario knob
works unchanged — only the `protocol` block differs:

```yaml
protocol:
  type: mqtt
  mqtt:
    endpoint: { host: 127.0.0.1, port: 1883 }   # the broker, reachable from both agents
    client:   { qos: 1, keepalive_s: 60, clean_start: true, timeout_s: 5,
                max_inflight_messages: 20 }  # raise the QoS 1/2 window for throughput tests
    base_topic: eccom
```

Correlation uses native MQTT v5 mechanisms: **Correlation Data** carries the
correlation id, **Response Topic** routes replies (each sender owns
`<base_topic>/replies/<client-id>`; requests flow on
`<base_topic>/requests`), and a user property carries the sequence number —
payload bytes are never touched. The adapter additionally records
`SEND_COMPLETED` when the broker acknowledges a publish (PUBACK/PUBCOMP for
QoS 1/2), so broker-ack latency can be separated from full RTT offline.

**Broker**: MQTT needs a broker; where it runs is infrastructure, so that
lives in the hosts file. By default the runner starts a Docker container on
the receiver's host (locally or over SSH) before the agents and removes it
afterwards. Customize or disable via:

```yaml
broker:
  run_on: cloud          # topology role whose machine hosts the broker
  managed: true          # false = you run the broker yourself
  # docker_image: eclipse-mosquitto:2   # pins an image; unset = the implementation's default
  container_name: eccom-mqtt-broker
```

(The key is `run_on`, not `on` — bare `on` is a boolean in YAML.) The broker
listens on the port from the scenario's `mqtt.endpoint`. *Which* broker is
part of the experiment and so a scenario setting,
`protocol.mqtt.broker.implementation`: `mosquitto` (`eclipse-mosquitto:2`,
one event loop on one core) or `nanomq` (`emqx/nanomq:0.24.14-slim`, multi-threaded;
0.25.x disconnects every MQTT 5 publisher after 1024 QoS 1/2 messages, see
`docs/scenario-parameters.md`). Either runs from a config the runner
generates into the run's `broker/` directory (`mosquitto.conf` /
`nanomq.conf`) with the scenario's limits; the broker's own log is kept next
to it. NanoMQ takes no in-flight setting (a fixed 1024 unacknowledged
publishes per connection, disconnect beyond) and always runs with
`TCP_NODELAY`, so `broker.max_inflight_messages` and `tcp_nodelay: false`
are rejected with it.

## TLS (HTTPS / MQTT over TLS)

TLS is configuration composed over the existing adapters — never a separate
protocol implementation (label runs via the scenario name: `https_...`,
`mqtts_...`). Generate a self-signed cert once (paths are relative to the scenario
file; `scenarios/certs/` is gitignored):

```bash
openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout scenarios/certs/server-key.pem -out scenarios/certs/server.pem \
  -days 365 -subj "//CN=eccom" -addext "subjectAltName=IP:<receiver-or-broker-ip>"
```

```yaml
protocol:
  type: http            # or mqtt (conventionally port 8883)
  tls:
    enabled: true
    cert_file: certs/server.pem       # server side (HTTP receiver / MQTT broker)
    key_file: certs/server-key.pem
    ca_file: certs/server.pem         # client verifies the chain against this;
                                      # omit for encrypt-without-verify
```

Hostname verification is off (self-signed certs rarely carry correct SANs); the
handshake being measured is identical either way. For remote runs the SSH
deployment ships the referenced files into the run directory automatically,
and for MQTT the managed broker is started with a TLS listener
built from the same files. Measurement tip: `keep_alive: false` pays the
full TLS handshake per message, `keep_alive: true` + warmup amortizes it —
that difference *is* the TLS cost story. See `scenarios/https_smoke.yaml`
and `scenarios/mqtts_smoke.yaml`.

Windows note: `eccom agent` switches to the selector event loop (paho needs
`add_reader`, which the default Proactor loop lacks); the runner keeps the
Proactor loop for subprocess support. Linux is unaffected.

## Docker Compose

Two containers ("cloud" receiver, "edge" sender) sharing `./runs`; the
scenario is selected via `SCENARIO` (defaults to the HTTP baseline):

```bash
SCENARIO=http_smoke_compose.yaml RUN_ID=smoke \
  docker compose up --build --exit-code-from edge   # quick containerized check
RUN_ID=run1 docker compose up --build --exit-code-from edge
# MQTT: the --profile mqtt flag adds the mosquitto broker service
SCENARIO=mqtt_baseline_compose.yaml RUN_ID=run2 \
  docker compose --profile mqtt up --build --exit-code-from edge
eccom analyze runs/<RUN_ID>
```

`--exit-code-from edge` stops the receiver with SIGTERM when the sender
finishes; the receiver flushes its events on that signal. The compose scenario
(`scenarios/http_baseline_compose.yaml`) differs from the local one only in
networking (`host: cloud`, `bind_host: 0.0.0.0`).

Compose is a single-host containerized test rig; multi-host runs use
`eccom run --hosts` (see above), which builds the same image on each target.

## Architecture

Diagrams (message sequence with timestamps, deployment process, UML) live in
[`docs/architecture.md`](docs/architecture.md).

```
eccom/
  core/            message model, ProtocolAdapter ABC, event model/recorder,
                   typed scenario + hosts configs (Pydantic, strict)
  protocols/       adapters; http.py (aiohttp client+server), mqtt.py
                   (aiomqtt, v5 properties), registry in __init__
  agents/          sender (traffic + interaction events), receiver (handler events),
                   control server (health/shutdown), runtime (process wiring)
  generators/      payload generation, open-loop load generator (constant
                   rate, ramp), IoT device simulator, smart-home mix
  instrumentation/ system metrics sampler (psutil), environment metadata,
                   clock-offset probe
  orchestration/   Deployment abstraction, local subprocess deployment,
                   SSH+Docker deployment, managed MQTT broker, link probe,
                   TestRunner
  analysis/        offline: event loading, statistics, run comparison,
                   plots (matplotlib)
go/                the Go agent (both roles), see go/README.md
scenarios/         example YAML scenarios and suites
hosts/             example host mappings (local, remote.example)
docker/            Dockerfile (Python agent image), Dockerfile.go (Go agent)
tests/             unit + integration (pytest, pytest-asyncio)
```

Key decisions:

- **One codebase, two roles.** There is no separate edge/cloud implementation;
  `eccom agent --role ...` selects behavior. `TestRunner` orchestrates:
  start receiver → health check (control endpoint) → run sender (warmup →
  measured load → drain → cooldown) → graceful receiver shutdown → analyze.
- **Protocol adapters expose one logical API** (`send`,
  `send_and_wait_reply`, `listen`, `listen_with_reply`), implemented per
  protocol. Operations a protocol cannot express raise
  `UnsupportedOperationError` instead of being faked. Interaction models
  (request-response, one-way, ...) are therefore not hardwired to protocols.
- **aiohttp instead of FastAPI + httpx.** One dependency covers async client
  and server with lower per-request overhead (no ASGI/validation layer);
  relevant because the Python sender is the expected throughput bottleneck.
- **Correlation via protocol-native metadata.** HTTP carries
  `X-Correlation-ID` / `X-Sequence-Number` headers; payload bytes are never
  modified. MQTT v5 uses Correlation Data / Response Topic / a user property.
- **Events, not statistics, cross the host boundary.** Each host appends
  events (`PAYLOAD_CREATED`, `SEND_STARTED`, `MESSAGE_RECEIVED`,
  `HANDLER_STARTED/COMPLETED`, `REPLY_SEND_STARTED`, `REPLY_RECEIVED`,
  `TIMEOUT`, `ERROR`, ...) with `monotonic_ns` + `wall_time_ns`; the analyzer
  joins edge/cloud logs on `correlation_id` after the run. Nothing is sent
  between hosts per message.
- **RTT is clock-safe; one-way latency is offset-corrected.** RTT comes from
  sender-side *monotonic* timestamps only. One-way latency uses cross-host
  wall clocks corrected by the NTP-style offset probe (see the interaction
  models section) and is always reported together with the measured
  uncertainty; without any clock information it is omitted, never guessed.
- **The load generator is open-loop** (fixed send cadence, concurrency cap)
  and always reports the *achieved* rate next to the requested one, so a
  sender-side bottleneck is visible in the data rather than hidden. The Go
  sender implements the same schedule for rates beyond a Python process.
- **Warmup handling:** warmup messages are tagged `phase=warmup` by the sender
  and excluded offline; receiver events get their phase via the correlation
  join.

## Measurement semantics

- The framework hands *already-serialized* payload bytes to the adapter, so
  what is measured are protocol/application boundaries — not application
  serialization cost. Serialization formats (JSON/MessagePack/protobuf) are a
  future, independent experiment dimension.
- `SEND_COMPLETED` is the transport acknowledgement (HTTP one-way: the
  server's 204; MQTT QoS 1/2: PUBACK/PUBCOMP), not "bytes fully written";
  `REPLY_SEND_COMPLETED` is defined but never recorded, since aiohttp does
  not cheaply expose "response fully written".
- System metrics sampling runs as a separate task and stays off the
  request path; event recording is an in-memory append that a writer thread
  (Python) or goroutine (Go) streams to disk within a second, so memory
  stays flat over a run of millions of events.

## Testing

```bash
pytest            # unit + integration (integration runs HTTP loopback in-process)
cd go && go test ./...   # the Go agent (see go/README.md for the Docker variant)
```

`tests/integration/test_go_receiver.py` runs the Python sender against the
Go receiver and `test_go_sender.py` the Go sender against both receivers
(plus a ramp with pauses) through the real runner, checking the artifacts
against the analysis; both skip without `go/bin/eccom-agent` or a Go
toolchain. The Go MQTT adapter tests need a broker in
`ECCOM_TEST_MQTT_BROKER` (e.g. `docker run -d -p 18830:1883
eclipse-mosquitto:2 mosquitto -c /mosquitto-no-auth.conf`, then
`localhost:18830`) and skip otherwise.

