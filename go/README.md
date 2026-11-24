# eccom Go agent

The Go implementation of the eccom agent process, both roles. A scenario
selects it per role (`sender.implementation: go`, `receiver.implementation:
go`); the deployments then run this program instead of `eccom agent`, with
the same command line:

```
eccom-agent agent --role sender|receiver --scenario <file> --run-dir <dir>
                  [--endpoint-host <addr>] [--peer-control <host:port>]
```

`--endpoint-host` replaces the scenario's endpoint host: the runner passes the
address it resolved when the scenario names a topology role (`host: cloud`).
`--peer-control` is the receiver's control endpoint for the sender's clock
offset probe. `--instance` is accepted for parity and ignored: the Go sender
is always one process (the Python validator enforces `load.senders: 1`).

Both roles write exactly the artifacts the Python agents write
(`<host>-events.jsonl`, `<host>-system-metrics.jsonl`, `<host>-environment.json`,
the sender's `<host>-load-report.json` and `<host>-clock-sync.json`, the
`scenario.yaml` copy) and the receiver serves the same control API
(`/health`, `POST /shutdown`, `/clock`), so the runner and the analysis do not
know which implementation produced a run. Nothing accumulates in memory over
a run: events stream to disk through a writer goroutine (64 k-event channel,
1 MB buffer, flushed every second) and every resource sample is written as it
is taken, so a 10 M-event run costs the same RSS as a 10-event one.

Why it exists: a Python agent is one event loop on one core, which tops out
at a few thousand msg/s per process. The Go receiver uses every core of its
host, and the Go sender paces the whole schedule from one process over
`sender.threads` cores, so the stacks under test, the path and the cloud side
set the ceiling rather than the load generator.

## Layout

| Package | Python counterpart |
|---|---|
| `cmd/eccom-agent` | `eccom/cli.py` (`agent` subcommand) |
| `internal/agent` | `eccom/agents/sender.py`, `receiver.py`, `runtime.py` |
| `internal/load` | `eccom/generators/load.py` (LoadGenerator, LoadReport) |
| `internal/payload` | `eccom/generators/payload.py` |
| `internal/clocksync` | `eccom/instrumentation/clock_sync.py` (the probe half) |
| `internal/protocol` | `eccom/protocols/http.py`, `mqtt.py`, `eccom/core/tls.py` |
| `internal/events` | `eccom/core/events.py` |
| `internal/control` | `eccom/agents/control.py` |
| `internal/metrics` | `eccom/instrumentation/system_metrics.py` |
| `internal/environment` | `eccom/instrumentation/environment.py` |
| `internal/config` | the agent-relevant subset of `eccom/core/config.py`, same defaults |
| `internal/clock` | `time.perf_counter_ns` / `time.time_ns` |

Not ported: `load.mode: devices`, `load.mode: homes` and
`mqtt.client.reconnect_per_message` (the Python validator rejects them with
the Go sender).

## Libraries

- `net/http`, `crypto/tls` (stdlib): the HTTP/1.1 client and server; h2 is
  disabled on both so the wire protocol matches the Python side.
- `github.com/eclipse/paho.golang`: MQTT v5 client, needed for Correlation
  Data, Response Topic and user properties. The only mature v5 client in Go;
  Eclipse-maintained like the Python paho.
- `github.com/shirou/gopsutil/v4`: the Go port of psutil, so the resource
  samples are computed the same way as the Python sampler's.
- `gopkg.in/yaml.v3`: scenario parsing.
- stdlib `encoding/json`, `flag`, `log/slog`, `math/rand/v2`, `testing` for
  the rest.

## Build and test

SSH deployments build `docker/Dockerfile.go` natively on the host, so no Go
toolchain is needed anywhere for remote runs. Local runs
(`eccom run <scenario>` without `--hosts`) and the contract tests
(`tests/integration/test_go_receiver.py`, `test_go_sender.py`) need
`go/bin/eccom-agent[.exe]`, which the local deployment builds when `go` is
on PATH:

```bash
cd go && go test ./... && go build -o bin/eccom-agent ./cmd/eccom-agent
```

Without a toolchain, Docker does the same (from the repository root; the
MQTT adapter tests need a broker in `ECCOM_TEST_MQTT_BROKER`, otherwise
they skip):

```bash
docker run --rm -v "$PWD:/repo" -w /repo/go golang:1.27-bookworm go test ./...
docker run --rm -v "$PWD:/repo" -w /repo/go -e GOOS=windows golang:1.27-bookworm \
  go build -o bin/eccom-agent.exe ./cmd/eccom-agent
```

## The sender

- **One process, `sender.threads` lanes.** `RunSender` sets `GOMAXPROCS` to
  `sender.threads` (0 keeps the runtime's default, which is cgroup-aware in
  Go 1.25+), the SSH deployment caps the container at the same number of
  cores (`docker --cpus`; `sender.cpus` overrides) because `GOMAXPROCS`
  does not bound the kernel time of the process's syscalls, and the
  environment file records the cap it found in its cgroup
  (`cpu.container_limit_cores`). The load generator paces the schedule with that many
  goroutines: lane *i* fires the sends *k·lanes + i* of one common cadence,
  so the aggregate is exactly the scenario's schedule and each lane sleeps
  and wakes at a lanes-th of the rate. All lanes share the concurrency
  semaphore, the step counters, the recorder and one events file. Step tags
  carry the process's offered rate, which with `senders: 1` is the schedule's,
  so `step_tags` and the ramp table need nothing new. The same semantics as
  the Python loop otherwise: open loop, catch-up bursts when behind, the
  deadline re-checked after the semaphore, skipped steps reported with zero
  sends, `step_pause_s` honoured, `duration_s` counting sending time only.
- **Per-process knobs become whole-process knobs.** `load.concurrency` and
  `http.client.max_connections` apply to the one process (`load.senders: n`
  splits them over n Python processes); the MQTT sender holds one
  connection where n Python processes hold n.
- **HTTP client:** `http.Transport` with `MaxConnsPerHost = max_connections`,
  idle connections kept up to that cap (or without limit when the pool is
  unbounded; Go's default would keep two and churn the rest), idle timeout
  15 s as aiohttp's, compression off, h2 off, `Content-Type:
  application/octet-stream`, the per-request deadline as a context. A
  deadline is a `TIMEOUT` event, every other failure an `ERROR`.
- **MQTT client:** one paho.golang connection, the own reply topic
  subscribed in `Start`, requests published with Correlation Data, Response
  Topic and the sequence-number user property. `Publish` returns once the
  packet is written for QoS 0 and on PUBACK/PUBCOMP for QoS 1/2, which is
  when `SEND_COMPLETED` is stamped (`{"qos": n}`). In request/response the
  publish runs on its own goroutine so the round trip is never floored at
  the PUBACK; a failed publish fails the request at once rather than at the
  timeout. paho.golang honours the broker's Receive Maximum, so
  `broker.max_inflight_messages` is the sender's in-flight window and
  `client.max_inflight_messages` (a Python-paho knob) is inert. The sender
  inherits the receiver's reconnect loop; the Python sender does not
  reconnect.
- **Payload:** `zeros` is one shared buffer; `random` is a ChaCha8 stream
  seeded from `payload.seed`, reproducible per seed but not byte-identical to
  Python's Mersenne Twister. Nothing on the path compresses, so the bytes do
  not matter for the measurement.
- **Correlation ids** are 16 random bytes as 32 hex characters, like
  `uuid4().hex`; sequence numbers count up from 0 across warmup and
  measurement, the phase tag separating them.
- **`go_max_procs`** is recorded in `<host>-environment.json`.

## Semantics worth knowing (both roles)

- `MESSAGE_RECEIVED` for MQTT is stamped in paho.golang's receive callback,
  before the PUBACK goes out and before any queue. The Python adapter stamps
  after aiomqtt's unbounded incoming queue, so under a receiver backlog the
  Python delivery latency includes that queueing and the Go one does not.
- The receiver subscribes only `<base>/requests`, the sender only its reply
  topic (the Python adapter subscribes the reply topic in both roles as a
  side effect of serving both; it carries no traffic either way).
- `loop_lag_ms_max` is the overshoot of a 10 ms sleep in a goroutine:
  scheduler and GC latency, the Go analogue of asyncio event-loop lag. A Go
  process's `process_cpu_percent` can exceed 100.
- `monotonic_ns` counts from process start rather than from boot; the
  analysis only ever differences it within one process's events.
