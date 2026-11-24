# Scenario parameters reference

Every scenario YAML is validated against the Pydantic models in
`eccom/core/config.py`. Validation is strict: unknown keys are rejected, so a
typo fails loudly at load time instead of silently falling back to a default.
Host/infrastructure mapping (where `edge` and `cloud` physically run) lives in
a separate hosts file (`eccom/core/hosts.py`) so the same scenario runs on any
host mapping without edits.

## Top level

| Parameter | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `name` | non-empty string | required | Run label; names the run's artifact files. |
| `interaction` | `request_response` \| `one_way` | `request_response` | `request_response`: every message waits for a correlated reply, RTT is measured on the sender's monotonic clock. `one_way`: fire-and-forget telemetry; latency comes from clock-offset-corrected edge/cloud wall timestamps plus the sender-side transport ack. |

## `topology`

| Parameter | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `sender` | string | `edge` | Role label of the sending host. Must match an entry in the hosts file; names the per-host artifacts (events, logs). |
| `receiver` | string | `cloud` | Role label of the receiving host. Must differ from `sender`. |

## `payload` — used only when `load.mode: constant_rate`

| Parameter | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `size_bytes` | int > 0 | required | Size of every message's payload. |
| `content` | `zeros` \| `random` | `zeros` | `zeros`: one shared immutable buffer, zero per-message generation cost. `random`: seeded PRNG bytes, different per message but reproducible across runs. |
| `seed` | int | `0` | PRNG seed making `random` payload sequences reproducible. |

## `devices` — list, used only when `load.mode: devices`

Each entry is one simulated IoT device firing on a fixed interval. Payloads
are built once at startup, so firing costs nothing at send time.

| Parameter | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `name` | non-empty string | required | Device label (must be unique); attached to each fire's events and reported per device. |
| `type` | `smart_meter` \| `temperature_sensor` \| `camera` \| `generic` | required | `smart_meter` / `temperature_sensor` carry hardcoded, realistic JSON readings; `camera` / `generic` carry fixed seeded-random bytes. |
| `interval_s` | float > 0 | required | Firing period. Devices fire open loop — a slow network never delays the next fire. |
| `size_bytes` | int > 0 | see meaning | Required for `generic`; optional for `camera` (default 100 000 B); forbidden for the two sensor types (hardcoded payloads). |
| `seed` | int | `0` | Derives the device's stable random byte blob (seeded per device name). |

## `home` — used only when `load.mode: homes`

The referential smart home that `load.mode: homes` replicates: a fixed mix of
sensor classes. Readings are seeded random bytes of the class's size (nothing
on the path compresses, so content is not a variable). One home sends
Σ `count` × `rate_hz` messages per second; see `load.aggregation_factor` for
what batching does to that.

| Parameter | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `sensors[].type` | non-empty string, unique within the home | required | Sensor class label; attached to every message as `sensor` in its `PAYLOAD_CREATED` metadata. |
| `sensors[].count` | int ≥ 1 | required | Sensors of this class per home. |
| `sensors[].rate_hz` | float > 0 | required | Readings per second per sensor. |
| `sensors[].size_bytes` | int > 0 | required | Bytes per reading; a message carries `aggregation_factor` × this (fewer in the last batch when the number of homes is not a multiple of the factor). |
| `seed` | int | `0` | Derives the reading bytes (seeded per sensor class and batch size). |

## `load` — the traffic model

| Parameter | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `mode` | `constant_rate` \| `ramp` \| `devices` \| `homes` | `constant_rate` | `constant_rate`: uniform paced stream of `payload` messages. `ramp`: the same stream, but every `step_duration_s` the rate is multiplied by `rate_multiplier` or raised by `rate_increment`, and/or the payload size multiplied by `size_multiplier` — one continuous open-loop schedule with nothing drained between steps (unless `step_pause_s` inserts a quiet gap after each), for locating the rate (or the message size) at which latency leaves its floor. `devices`: each device in `devices` fires independently on its own interval. `homes`: the `home` sensor mix replicated `homes` times, `homes_increment` more every `step_duration_s`, paced like a ramp at the mix's aggregate rate with the messages handed out in the order the sensors' staggered timers would fire them; `aggregation_factor` batches each sensor slot's readings across that many homes into one message. |
| `messages_per_second` | float > 0 | required for constant_rate / ramp | Target send rate (ramp: the first step's rate). The schedule is open loop; the *actual* achieved rate is always measured and reported alongside it (Python may be the bottleneck). Forbidden in devices and homes modes (rates derive from the device intervals / the home's sensor mix). |
| `duration_s` | float > 0 | — | Length of the send window. constant_rate: set exactly one of `duration_s` / `total_messages`. ramp, homes: required — the whole ramp's *sending* time (pauses excluded); the number of steps is `duration_s / step_duration_s`, rounded up. devices: required. |
| `total_messages` | int > 0 | — | Alternative stop condition for constant_rate: stop after this many messages. Not available for ramp. |
| `concurrency` | int ≥ 1 | `1` | Cap on in-flight sends (an asyncio semaphore) — the only backpressure on the open-loop schedule. Max achievable rate ≈ `concurrency` / average completion latency. Ignored in devices mode (no cap — real sensors do not wait for each other); applies in homes mode, whose schedule is a ramp's. |
| `senders` | int ≥ 1 | `1` | Number of sender processes (containers on SSH hosts) that generate this schedule together, started at the same time on the sender host. `messages_per_second`, `rate_increment`, `total_messages` and `concurrency` are split evenly between them (remainders to the lowest instances); durations and the ramp shape are unchanged, so the instances' schedules add up to the values above. Instance `i` writes `<sender>.<i>-events.jsonl`, `-load-report.json` and `-system-metrics.jsonl`, uses payload seed `seed + i`, and only instance 0 probes the clock offset; the analysis merges the files back into one `<sender>` view (`docs/metrics.md` §4.2). Lifts the one-core ceiling of a single Python sender. Not supported with `devices` or `homes` mode. |
| `rate_multiplier` | float > 1 | ramp: at least one of `rate_multiplier` / `rate_increment` / `size_multiplier` | Factor applied to the rate at every step boundary (`2` doubles it): geometric steps, for locating a knee whose position is unknown to within orders of magnitude. Not combinable with `rate_increment`. Rejected in any other mode. |
| `rate_increment` | float > 0 | ramp: at least one of `rate_multiplier` / `rate_increment` / `size_multiplier` | Added to the rate at every step boundary (`800` turns 2400 into 3200, 4000, …): linear steps of equal resolution, for a ramp that starts near a known operating point and walks through the ceiling. With `senders: n` each process adds `rate_increment / n`, like the rate. Not combinable with `rate_multiplier`. Rejected in any other mode. |
| `size_multiplier` | float > 1 | ramp: at least one of `rate_multiplier` / `rate_increment` / `size_multiplier` | Factor applied to the payload size at every step boundary, starting from `payload.size_bytes` (`4` quadruples it; sizes are rounded to whole bytes). Each send is also tagged with its step's `target_size`, the per-step tables carry `payload_size_bytes`, and when the sizes vary the comparison tables and sweep charts use payload size as their axis instead of the offered rate. Rejected in any other mode. |
| `step_pause_s` | float > 0 | — (ramp and homes) | Quiet gap after every step but the last: the generator schedules nothing, the in-flight backlog drains, and the next step starts from an empty queue instead of inheriting the previous step's queue. Adds `(steps − 1) × step_pause_s` to the run; each step's resource window then ends at its last send, so the pause dilutes no per-step CPU or throughput figure. Rejected in constant_rate and devices modes. |
| `step_duration_s` | float > 0 | required for ramp and homes | Length of one step. Every send is tagged with its step index and target rate (`PAYLOAD_CREATED` metadata), so the analysis attributes messages to steps exactly rather than by timestamp; a step the schedule never reached (blocked on the concurrency cap for its whole duration) still appears in the load report with zero sends. Rejected in constant_rate and devices modes. |
| `homes` | int ≥ 1 | required for homes | Copies of `home` in the first step (the warmup runs that step too). Rejected in any other mode. |
| `homes_increment` | int ≥ 1 | required for homes | Homes added at every step boundary. Equal to `aggregation_factor` every step adds one batch per sensor slot, so every factor walks the same msg/s steps. Rejected in any other mode. |
| `aggregation_factor` | int ≥ 1 | `1` | Readings of one sensor slot across this many homes travel as one message of `aggregation_factor` × `size_bytes`: the message rate is Σ `count` × `rate_hz` × ⌈homes / factor⌉, the byte rate unchanged. Every send is tagged with `step`, `target_rate`, `target_homes` and `sensor`; the per-step tables carry `homes`, `eccom compare` reports `exact_homes` (schedule held) and `sustained_homes` / `knee_homes` (latency knee), and `--plots` draws one sweep per factor (`compare_ramp_*_x<k>.png`, homes on a linear axis) plus `compare_homes_ceiling.png` across factors. Must stay `1` outside homes mode. |

## `sender`

| Parameter | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `implementation` | `python` \| `go` | `python` | Which agent program generates the load: the Python package (`eccom agent`, one asyncio loop per process, so `load.senders` runs several) or the Go agent under `go/` (one process pacing the whole schedule over `threads` goroutines; `load.senders` must be 1). Same command line and artifacts, so the runner and the analysis are unaffected; the deployments build `docker/Dockerfile.go` on the sender's host or run `go/bin/eccom-agent` locally. The Go sender is for ramps beyond what several Python processes can pace. Per-process knobs apply to the one Go process: `load.concurrency` is the total in flight and `http.client.max_connections` the total pool, and all MQTT traffic rides one connection. Not implemented for Go and rejected at load time: `load.mode: devices`, `mqtt.client.reconnect_per_message`. `mqtt.client.max_inflight_messages` is inert for Go (paho.golang takes the broker's Receive Maximum, `broker.max_inflight_messages`). Semantic differences are listed in `go/README.md`. |
| `threads` | int ≥ 0 | `0` | Go sender only (rejected with `python`): `GOMAXPROCS` of the sender process and the number of pacing goroutines, i.e. how many of the host's cores the load generator may use; `0` = every core the runtime sees (cgroup-aware). The Go analogue of `load.senders` for Python. Recorded as `go_max_procs` in `<sender>-environment.json`. |
| `cpus` | float > 0 | — | `docker --cpus` for each sender container on an SSH host (a CFS quota over all cores, not a pin), replacing the host's `resources.cpus` for that container. Unset, the Go sender is capped at `threads` cores — `GOMAXPROCS` bounds its goroutines but not the kernel work done in its syscalls — and the Python sender runs uncapped. Ignored by the local deployment (bare processes). Every agent reads the cap back from its cgroup into `cpu.container_limit_cores` of `<host>-environment.json`, so a run documents what bound it. |

## `receiver`

| Parameter | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `artificial_processing_delay_ms` | float ≥ 0 | `0` | Simulated server-side work per message, between HANDLER_STARTED and HANDLER_COMPLETED. |
| `control_port` | 1–65535 | `8081` | Out-of-band health/clock/shutdown HTTP endpoint — separate from the measured path and independent of the protocol under test. |
| `control_bind_host` | string | `127.0.0.1` | Bind address of the control endpoint. Use `0.0.0.0` on remote hosts so the orchestrator can reach it. |
| `implementation` | `python` \| `go` | `python` | Which agent program serves the receiver: the Python package (`eccom agent`, aiohttp / aiomqtt) or the Go agent under `go/` (`net/http` / paho.golang, see `go/README.md`). Same command line, same artifacts, same control API, so the runner and the analysis are unaffected; the deployments build `docker/Dockerfile.go` on the receiver's host (image `<image>-go`) or run `go/bin/eccom-agent` locally. The Python receiver is one process on one core; the Go receiver uses every core, so in throughput experiments the receiver is less likely to be the ceiling. Part of the experiment, so it lives here and is recorded in every run's `scenario.yaml`. Known semantic differences are listed in `go/README.md`. |

## `experiment`

| Parameter | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `warmup_s` | float ≥ 0 | `0` | Traffic sent before the measurement window (connection setup, JIT, caches); events tagged `phase=warmup` and excluded from the analysis stats. |
| `cooldown_s` | float ≥ 0 | `0` | Wait after the load finishes, before shutdown. |

## `instrumentation`

| Parameter | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `system_metrics` | bool | `true` | Sample CPU/memory/network via psutil on both agents. |
| `system_metrics_interval_s` | float > 0 | `1.0` | Sampling period. |
| `baseline_s` | float ≥ 0 | `3.0` | Quiet window each agent sits idle before any traffic, with its samples tagged `phase: baseline`. Gives `eccom compare` an idle reference for that exact process on that exact host, so CPU/memory can be reported as the cost of the traffic rather than in absolute terms. Adds this many seconds to every run; `0` disables it. |
| `link_capacity` | bool | `false` | Before the agents start, run iperf3 between the sender's host and the receiver's host, both directions, on the scenario's endpoint port — the one port known to be open between them, still free at that point: what the path can carry independent of the protocol under test. SSH hosts run it from the agent image; a local deployment needs `iperf3` on PATH. The record is written to `<sender>-link-capacity.json`, digested into `summary.json` (`link_capacity`) and tabulated by `eccom compare`. A failed probe is recorded, not fatal. Adds roughly 2 × `link_capacity_duration_s` + 5 s to every run. |
| `link_capacity_duration_s` | float > 0 | `5.0` | Seconds per direction. |
| `network_capture` | bool | `false` | Reserved (NetworkProbe not implemented yet); must stay `false`. |

## `protocol`

| Parameter | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `type` | `http` \| `mqtt` | required | Selects the protocol adapter. Only the matching sub-block below is used. |
| `tcp_nodelay` | bool | `true` | Nagle's algorithm off (`TCP_NODELAY`) on every socket the scenario opens: the aiohttp client and server, the MQTT client in both roles, and the managed broker (`set_tcp_nodelay` in its generated `mosquitto.conf`; an external broker keeps its own setting; NanoMQ sets `TCP_NODELAY` on every accepted connection and has no switch, so `false` is rejected with `broker.implementation: nanomq`). `true` is how asyncio and most HTTP stacks run anyway; paho and mosquitto default to Nagle **on**, and `false` reproduces that for either protocol: a small write made while the previous one is still unacknowledged waits for that ACK, up to one round trip, as soon as the message spacing drops below the acknowledgement time. For HTTP, `false` is a configuration aiohttp never runs in by default. |

### `protocol.http`

| Parameter | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `endpoint.host` | string | `127.0.0.1` | Where the receiver's HTTP server is reachable **from the sender**: a hostname / IP, or a topology role label (`cloud`) that the runner resolves to that role's address from the hosts file and passes to the agents as `--endpoint-host`. The shipped scenarios name the role, so a new receiver host is a hosts-file change only. |
| `endpoint.port` | 1–65535 | `8080` | Receiver's HTTP port. |
| `client.keep_alive` | bool | `true` | Reuse TCP connections across requests vs. reconnect per request. |
| `client.timeout_s` | float > 0 | `5.0` | Per-request timeout (request_response: reply deadline; one_way: transport-ack deadline). |
| `client.max_connections` | int ≥ 0 | `0` | Connection pool cap **per sender process**. `0` = unbounded: `load.concurrency` is the only limit, and any backlog (a stalled event loop, a slow reply) opens new TCP connections, because HTTP/1.1 carries one request per connection at a time. A cap makes requests beyond it wait for a free connection instead — client-side queueing (visible in send-ack) rather than connection churn, structurally like MQTT's single connection with its in-flight window. The Go client also keeps its idle connections up to the cap (unbounded pools without limit) rather than Go's default of two. |
| `server.bind_host` | string | `127.0.0.1` | Receiver bind address; `0.0.0.0` in containers / on real hosts. |

### `protocol.mqtt`

| Parameter | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `endpoint.host` | string | `127.0.0.1` | Where the broker is reachable **from both agents**: a hostname / IP, or a topology role label (`cloud`) resolved from the hosts file like the HTTP one. |
| `endpoint.port` | 1–65535 | `1883` | Broker port (convention: 8883 with TLS). |
| `client.qos` | 0 \| 1 \| 2 | `1` | MQTT delivery guarantee: 0 at-most-once (no ack), 1 at-least-once (PUBACK), 2 exactly-once (PUBREC/PUBREL/PUBCOMP). |
| `client.keepalive_s` | int ≥ 1 | `60` | MQTT keepalive interval. |
| `client.clean_start` | bool | `true` | `true` = fresh session per connect; `false` = the broker resumes the session the previous connect left behind. **Only observable together with `reconnect_per_message`** — with one connection per run there is never a prior session to resume. |
| `client.timeout_s` | float > 0 | `5.0` | Request/reply timeout. Also bounds the per-message connect when `reconnect_per_message` is on. |
| `client.max_inflight_messages` | int ≥ 1 | `20` | QoS 1/2 unacknowledged-publish window (paho default 20). Caps outstanding publishes regardless of `load.concurrency` — raise it for max-throughput experiments so the client window isn't the bottleneck. |
| `client.reconnect_per_message` | bool | `false` | Sender only: connect, publish and disconnect for every message instead of holding one connection for the whole run — the MQTT counterpart of `http.client.keep_alive: false`, and what a duty-cycled sensor that sleeps between readings pays. The connect sits inside the measured `SEND_STARTED` → `SEND_COMPLETED` interval, so send-ack prices session setup (TCP + CONNECT/CONNACK + PUBLISH/PUBACK); the disconnect follows `SEND_COMPLETED` and is excluded. Receivers keep their persistent connection either way. |
| `broker.implementation` | `mosquitto` \| `nanomq` | `mosquitto` | Which broker the runner starts when the hosts file says `broker.managed: true` (an external broker is whatever you run): [mosquitto](https://mosquitto.org/) from `eclipse-mosquitto:2` — one event loop on one core — or [NanoMQ](https://nanomq.io/) from `emqx/nanomq:0.24.14-slim` (the base image has no TLS) — C, multi-threaded, two task-queue threads and contexts per core by default. Part of the experiment, so it lives here; the hosts file's `broker.docker_image` only pins a different image. Both run from a config generated into the run's `broker/` directory (`mosquitto.conf` / `nanomq.conf`, see `docs/metrics.md`). NanoMQ differences that the other `broker.*` rows spell out: a fixed inbound window of 1024 unacknowledged QoS 1/2 publishes per connection (compile-time, not announced as Receive Maximum) that it enforces by **disconnecting** the publisher with reason 0x93 instead of queueing — so a broker backlog past 1024 shows up as a reconnect of the Go sender (`logs/sender.log`, a few failed sends) where mosquitto's showed as ack latency; no unlimited queue; `TCP_NODELAY` always on. It also resends an unacknowledged QoS 1/2 message after 10 s (`retry_interval`, its default) where mosquitto only resends on reconnect. **Why 0.24.14:** 0.25.x (checked 0.25.6) never refills that window, so every MQTT 5 publisher is disconnected after exactly 1024 QoS 1/2 messages per connection, even with one message in flight. |
| `broker.max_inflight_messages` | int ≥ 0 | unset (mosquitto's own `20`) | **mosquitto only** — rejected with `implementation: nanomq`, which takes no setting for it (1024 unacknowledged publishes per connection at compile time, enforced by disconnect, not announced as Receive Maximum), so the client's own window applies there (65535 for paho.golang; Python paho's `client.max_inflight_messages` either way). Written into the **managed** broker's `mosquitto.conf` (an external broker keeps its own settings): QoS 1/2 messages mosquitto keeps in flight per client, in both directions, and the Receive Maximum it announces in CONNACK. **mosquitto 2.1 quirk:** for `0` and `65535` it omits the CONNACK property and then applies a quota of 20, logging `exceeded its receive-maximum quota` for every publish beyond it — it still accepts and acknowledges them, but a run at thousands in flight produces one log line per message. Set it to the client's window instead (e.g. `10000`), which is announced and honoured silently. The default is mosquitto's own. |
| `broker.max_queued_messages` | int ≥ 0 | `1000` | Managed broker only: QoS 1/2 messages mosquitto queues per client beyond the in-flight window. A subscriber that falls further behind **silently loses the excess** — the only symptom is `received_by_receiver` below `messages_completed`. Set `0` (unlimited) for saturation experiments so the backlog shows up as latency instead of loss. The default is mosquitto's own. NanoMQ (`max_mqueue_len`, its shipped default 2048) has no unlimited setting: `0` becomes 2²⁰ = 1 048 576 entries per subscriber (8 MiB of pointers each), and what no longer fits is dropped with `msg lost due to reach the limit of lmq` in `broker/nanomq.log`. |
| `broker.max_packet_size` | int ≥ 20 | `268435455` | Managed broker only: the largest MQTT packet the broker accepts, in bytes. mosquitto 2.1 defaults to 2 000 000 and disconnects a client that exceeds it (`oversize packet` in its log; the `eclipse-mosquitto:2` tag floats between the two); 2.0 had no limit, and 2.1 rejects `0`. The default here is the MQTT maximum, i.e. no limit on either version. NanoMQ takes the same value (`max_packet_size`, its own default 10 MiB) and announces it as Maximum Packet Size in CONNACK. |
| `base_topic` | string, no `#` `+` whitespace | `eccom` | Requests flow on `<base_topic>/requests`, replies on `<base_topic>/replies/<client-id>` via MQTT v5 Response Topic. |

### `protocol.tls`

TLS composed over either adapter (HTTPS / MQTT over TLS). All paths are
relative to the scenario file's directory (never absolute, no `..`), so
deployments can ship them into the remote run directory.

| Parameter | Type / allowed values | Default | Meaning |
|---|---|---|---|
| `enabled` | bool | `false` | Turn TLS on; requires `cert_file` and `key_file`. |
| `cert_file` | relative path | — | Server certificate (HTTP receiver / MQTT broker). |
| `key_file` | relative path | — | Server private key. |
| `ca_file` | relative path | — | Client-side trust anchor for verifying the server chain (hostname verification stays off — self-signed certs rarely carry correct SANs). Without it the connection is encrypted but unverified — measurement-equivalent (identical handshake). |

## Cross-field validation rules

- `load.senders > 1` requires `mode: constant_rate` or `ramp`, and
  `total_messages` (if set) ≥ `senders`.
- `sender.implementation: go` requires `load.senders: 1` (use
  `sender.threads`), `load.mode` other than `devices` or `homes` and
  `mqtt.client.reconnect_per_message: false`; `sender.threads` is only
  accepted with the Go sender.

- `topology.sender` ≠ `topology.receiver`.
- `constant_rate` requires `payload` and `messages_per_second`, plus exactly
  one of `duration_s` / `total_messages`; `devices` entries are forbidden.
- `ramp` requires `payload`, `messages_per_second` (the first step),
  `step_duration_s`, `duration_s` and at least one of `rate_multiplier` /
  `rate_increment` / `size_multiplier`; `rate_multiplier` and `rate_increment`
  exclude each other; `total_messages` is not supported. The multipliers and
  the increment are rejected in any other mode; `step_duration_s` and
  `step_pause_s` are shared with `homes` and rejected elsewhere.
- `devices` mode requires a non-empty `devices` list with unique names and
  `duration_s`; `payload`, `messages_per_second`, `total_messages` are
  forbidden.
- `homes` mode requires a `home` block (unique sensor types), `homes`,
  `homes_increment`, `step_duration_s` and `duration_s`; `payload`,
  `devices`, `messages_per_second` and `total_messages` are forbidden;
  `load.senders` must be 1. `home`, `homes`, `homes_increment` and an
  `aggregation_factor` other than 1 are rejected in any other mode.
- `tls.enabled: true` requires `cert_file` + `key_file`.
- `mqtt.client.reconnect_per_message` requires `interaction: one_way` (the
  per-message connection closes before a reply could arrive) and
  `constant_rate` load with `concurrency: 1` (every message reconnects under
  the same client id, and a broker drops the older session as soon as a second
  connection claims that id, so overlapping sends would disconnect each other).

---

# Shipped scenario suites

`scenarios/scenario1` … `scenario7` are worked examples of suites: every
file of a suite carries the same traffic and varies one thing against its
baseline, so a difference between two runs is attributable to that one
setting. All of them name the receiver's role (`host: cloud`) as the
endpoint, so the hosts file supplies the address, and bind the receiver and
its control endpoint to `0.0.0.0`. TLS variants read a self-signed
certificate from the suite's own `certs/` directory (`server.pem` +
`server-key.pem`, the client verifying against `ca_file: certs/server.pem`;
gitignored — generate one as the README's TLS section shows).

Run a suite and compare its runs:

```bash
eccom run scenarios/scenario2 --hosts hosts/<your-hosts>.yaml --pause-s 30
eccom compare runs/*-s2_* --plots plots/scenario2 --output scenario2.md
```

The rates, sizes and windows below are starting points sized for a small
edge device on a consumer uplink; scale the ramps to the hardware and link
under test.

## scenario1 — protocol comparison under identical light telemetry

128 B random payload (`seed: 42`), 1 msg/s for 60 s, concurrency 1, warmup
5 s, cooldown 2 s. The payload is incompressible, so no layer can shrink it,
and the shared seed makes every variant send the same message sequence. The
suite isolates transport security (HTTP↔HTTPS, MQTT↔MQTTS at QoS 1), MQTT
delivery guarantees (QoS 0/1/2), connection reuse (`01` vs `07`, `04` vs
`08`) and the interaction model (`01` vs `09`, `04` vs `10`).

| File | Name | Protocol | Port | QoS | TLS | Varies vs baseline |
|---|---|---|---|---|---|---|
| `01_http.yaml` | `s1_http` | HTTP | 8080 | — | no | — (HTTP baseline) |
| `02_https.yaml` | `s1_https` | HTTPS | 8443 | — | yes | TLS on |
| `03_mqtt_qos0.yaml` | `s1_mqtt_qos0` | MQTT v5 | 1883 | 0 | no | QoS 0 |
| `04_mqtt_qos1.yaml` | `s1_mqtt_qos1` | MQTT v5 | 1883 | 1 | no | — (MQTT baseline) |
| `05_mqtt_qos2.yaml` | `s1_mqtt_qos2` | MQTT v5 | 1883 | 2 | no | QoS 2 |
| `06_mqtts_qos1.yaml` | `s1_mqtts_qos1` | MQTT v5 + TLS | 8883 | 1 | yes | TLS on |
| `07_http_no_keepalive.yaml` | `s1_http_no_keepalive` | HTTP | 8080 | — | no | `keep_alive: false` |
| `08_mqtt_cold_start.yaml` | `s1_mqtt_cold_start` | MQTT v5 | 1883 | 1 | no | `reconnect_per_message: true` |
| `09_http_req_rsp.yaml` | `s1_http_req_rsp` | HTTP | 8080 | — | no | `interaction: request_response` |
| `10_mqtt_req_rsp.yaml` | `s1_mqtt_req_rsp` | MQTT v5 | 1883 | 1 | no | `interaction: request_response` |

- **`07` / `08` — connection setup per message.** `07` opens a fresh TCP
  connection per request (one handshake round trip before the request's);
  `08` connects, publishes and disconnects per message (TCP handshake,
  CONNECT/CONNACK, PUBLISH/PUBACK). Both price what a duty-cycled sensor
  pays; with `clean_start: true` every MQTT session is cold.
- **`09` — HTTP request/response.** Headline `rtt`, measured on the sender's
  monotonic clock, so it carries no clock-offset uncertainty. One-way HTTP
  already waits for the server's 204, so the gap between `01`'s send-ack and
  `09`'s RTT is the cost of coupling the transport ack to the handler; raise
  `artificial_processing_delay_ms` to see the two diverge by the handler's
  cost. `send_ack` is blank for `09` (recorded on the one-way path only).
- **`10` — MQTT request/response.** The reply is a second QoS 1 publish on
  the sender's reply topic, routed by the MQTT v5 Response Topic and
  Correlation Data properties: four hops (sender → broker → receiver →
  broker → sender) against `09`'s two. With the broker on the receiver's
  host (`broker.run_on`) two of them stay on that host. `send_ack` (the
  PUBACK) is still recorded, so the run reports the transport ack and the
  application reply side by side.

One-way delivery crosses two clocks: read it against the reported
uncertainty (`docs/metrics.md` §2).

## scenario2 — throughput ceiling under a geometric ramp

scenario1's matrix without the two connection-setup variants, under one
ramp: 32 B random payload, 10 msg/s doubling every 5 s step for 60 s of
sending (12 steps, nominally 10 → 20 480 msg/s), a 10 s `step_pause_s`
after every step so each step starts from a drained queue, up to 10 000
sends in flight, warmup 5 s (the first step), cooldown 10 s. The offered
rate keeps doubling past what one Python sender can do, so the top steps
are the "as fast as possible" measurement.

Files `01`–`06` as in scenario1 with the `s2_` prefix, `07_http_req_rsp`
and `08_mqtt_req_rsp` for the interaction model. Every MQTT file sets the
client's `max_inflight_messages` to the concurrency (otherwise paho queues
publishes locally and send-ack measures that queue) and lifts the broker's
limits (`broker.max_inflight_messages: 0`, `max_queued_messages: 0`) so a
slow subscriber builds a backlog instead of the broker dropping messages.

- **Reading the result.** `summary.json` carries the per-step `ramp` table
  and `plots/ramp_over_time.png` draws it on one time axis; `eccom compare`
  reduces each run to the rate whose schedule held and the rate before the
  latency knee. `docs/metrics.md` §4.1 lists the fields and how to attribute
  a ceiling to the sender, the receiver, the broker or the link.
- **Concurrency differs per protocol.** HTTP/1.1 opens one connection per
  in-flight request (and a TLS handshake each for HTTPS) unless
  `client.max_connections` bounds the pool; MQTT pipelines every publish
  over one connection.

## scenario3 — payload-size sweep at a fixed rate

scenario2's matrix (`s3_` prefix) at a fixed 10 msg/s with the payload
quadrupled every 10 s step (`load.size_multiplier: 4`): 128 B, 512 B,
2 KiB, 8 KiB, 32 KiB, 128 KiB, 512 KiB, 2 MiB — eight steps of 100
messages, 10 kbit/s to 168 Mbit/s offered, a 10 s pause after every step,
concurrency 20, `link_capacity: true` so every run records the link's own
capacity. The rate is far below any sender's ceiling, so the suite shows
where message size starts to set the latency: segments per message, TCP
window growth, serialization on the uplink, and finally the uplink's
capacity.

- **Concurrency 20 is the backpressure.** At 10 msg/s it binds only once a
  message takes longer than 2 s, i.e. once the link is saturated; the
  achieved rate then drops instead of the in-process queue growing.
- **MQTT limits.** The client window equals the concurrency, the broker's
  flow-control limits stay at their defaults, and `broker.max_packet_size`
  stays at the framework's default (the MQTT maximum) so the 2 MiB step is
  accepted.
- **The reply does not grow.** `07` and `08` answer with an empty payload.
- **Reading the result.** The per-step table gains a payload-size column
  and the comparison tables and sweep charts are keyed on payload size. A
  sender CPU that rises with size is a per-byte cost in the client stack; a
  flat sender with rising delivery latency is the network.

## scenario4 — throughput ceiling with several sender processes

scenario2's matrix (`s4_` prefix) with the load generator and the receiver
scaled out: six Python sender processes (`load.senders: 6`), the Go
receiver (`receiver.implementation: go`) and a *linear* ramp
(`load.rate_increment`): 1 800 msg/s + 600 per 10 s step, 24 steps (1 800 →
15 600 msg/s; 300 + 100 per process), a 10 s pause after every step,
10 000 sends in flight in total, `link_capacity: true`. Equal steps give
the ceiling a 600 msg/s resolution.

A ninth file, `09_http_pool.yaml` (`s4_http_pool`), is `01` with a bounded
connection pool (`client.max_connections: 250` per process). `01` alone
runs unbounded: a stall then opens a burst of new connections, where a
bounded pool queues the backlog inside the client the way MQTT queues
behind its in-flight window. `02` and `07` use the bounded pool too. The
MQTT files announce the client's window from the broker
(`broker.max_inflight_messages: 10000`, see that parameter for why not
`0`).

Leave the sender's host a core or two beyond `senders` for the kernel's
network stack, dockerd and sshd. The `edge host cpu%` column of
`eccom compare`'s CPU-per-step table shows the kernel's share.

## scenario5 — scenario4's matrix with the Go sender

scenario4's nine files (`s5_` prefix) with the load generator replaced by
one Go process (`sender.implementation: go`, `threads: 6`, its container
capped at six cores) and a steeper ramp meant to reach the link's capacity:
2 400 msg/s + 2 400 per 10 s step, 18 steps (2 400 → 43 200 msg/s). Every
step up to 14 400 msg/s is also a scenario4 step, so the two suites compare
at shared offered rates; the difference there is the load generator's.

Per-process knobs become whole-process knobs: the bounded HTTP pool is
1 500 (6 × 250) in `02`, `07` and `09`, `01` stays unbounded, and one MQTT
connection carries all traffic. `client.max_inflight_messages` is inert
for the Go sender, whose window is the broker's announced Receive Maximum.

Files `10`–`14` repeat the five MQTT variants with
`broker.implementation: nanomq` and nothing else changed (`s5_nanomq_qos0`,
`_qos1`, `_qos2`, `s5_nanomqs_qos1`, `s5_nanomq_req_rsp`): a multi-threaded
broker against mosquitto's single event loop. NanoMQ takes no in-flight
setting, so `load.concurrency` caps the messages in flight; past its fixed
window of 1 024 unacknowledged publishes per connection it disconnects the
publisher, which shows as reconnects in `logs/sender.log` and failed sends
in the step table. `13` against `06` also swaps the TLS library (mbedTLS
for OpenSSL).

A run that holds the schedule to the top writes about 4 M messages; analyze
such runs separately (`eccom run --no-analyze`, then `eccom analyze`).

## scenario6 — simulated smart homes and the aggregation factor

Ramps the number of *referential smart homes* one edge gateway (one Python
sender process) forwards to the Go receiver, under four aggregation
factors. The home:

| Sensor class | Per home | Rate | Reading | Messages/s | kB/s |
|---|---:|---:|---:|---:|---:|
| `light` | 4 | 1 Hz | 4 B | 4 | 0.016 |
| `smart_plug` | 40 | 1 Hz | 16 B | 40 | 0.64 |
| `temperature` | 4 | 1 Hz | 4 B | 4 | 0.016 |
| `motion` | 4 | 1 Hz | 4 B | 4 | 0.016 |
| `microphone` | 4 | 50 Hz | 640 B | 200 | 128 |
| `mmwave` | 4 | 10 Hz | 20 B | 40 | 0.8 |
| **total** | **60** | | | **292** | **129.5** (~1.04 Mbit/s) |

The microphones are 68 % of the messages and 99 % of the bytes. Aggregation
factor k batches the readings of one sensor slot across k homes into one
message of k × the reading size: the message rate falls k-fold, the byte
rate stays. Every file adds k homes per 10 s step (`homes_increment` =
`aggregation_factor`), so every factor walks the same offered rates — 292
msg/s per step, 12 steps to 3 504 msg/s, a 10 s pause after every step —
while the homes and bytes behind them are k times larger: x1 ends at 12
homes (~12 Mbit/s of payload), x8 at 96 (~100 Mbit/s). Low factors
therefore run into the sender's per-message cost, high factors into the
link.

scenario2's eight variants with scenario4's protocol blocks and a bounded
HTTP pool of 250, times the factors 1, 2, 4, 8 — 32 files numbered
factor-major (`01`–`08` x1, `09`–`16` x2, `17`–`24` x4, `25`–`32` x8),
named `s6_<variant>_x<k>`.

Each `PAYLOAD_CREATED` carries `step`, `target_rate`, `target_homes` and
`sensor`; the per-step tables carry `homes`, `eccom compare` reports
`exact_homes` and `sustained_homes` / `knee_homes`, and `--plots` draws one
sweep per factor plus `compare_homes_ceiling.png`.

## scenario7 — scenario6's homes ramp with TLS on every variant

The six variants that can carry TLS — HTTPS, HTTPS request/response, MQTT
over TLS at QoS 0, 1, 2 and request/response — times the four factors: 24
files numbered factor-major, named `s7_<variant>_x<k>`. Every file is its
scenario6 twin with TLS on: the same home, sender, receiver, experiment and
instrumentation blocks, the same client and broker settings, the endpoint
port moved to 8443 / 8883 and the `tls` block added.

| Name | scenario6 twin |
|---|---|
| `s7_https_x<k>` | `s6_https_x<k>` |
| `s7_https_req_rsp_x<k>` | `s6_http_req_rsp_x<k>` + TLS |
| `s7_mqtts_qos0_x<k>` | `s6_mqtt_qos0_x<k>` + TLS |
| `s7_mqtts_qos1_x<k>` | `s6_mqtts_qos1_x<k>` |
| `s7_mqtts_qos2_x<k>` | `s6_mqtt_qos2_x<k>` + TLS |
| `s7_mqtts_req_rsp_x<k>` | `s6_mqtt_req_rsp_x<k>` + TLS |

Instead of scenario6's full ramp each file runs a four-step window
(`load.duration_s: 40`) starting at its own `load.homes`, which keeps the
suite short once the neighbourhood of a ceiling is known. Set `load.homes`
to one batch below the ceiling expected on the hardware under test (or
start from scenario6's `homes: k`, `duration_s: 120`). A ceiling outside
the window reads as "not reached" or as the first step failing; the latency
knee is then relative to a loaded first step, while `exact_homes` does not
depend on the first step.
