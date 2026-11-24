# eccom architecture diagrams

Four views of the framework: the life of one round-trip message with every
timestamp capture point, the deployment/run pipeline, and the class-level
structure split in two — the agent/protocol layer running inside the sender
and receiver processes, and the TestRunner orchestration layer that deploys
them and analyzes the results. GitHub and most IDEs render the mermaid blocks
natively.

## 1. One round-trip message (sequence + timestamps)

Every timestamp is captured by `EventRecorder.record()`, which stamps both
clocks at once: `monotonic_ns` (`perf_counter`, valid within one host) and
`wall_time_ns` (`time_ns`, cross-host with the measured offset). Protocol
(de)serialization happens inside the adapter/library and is not separately
timestamped.

```mermaid
sequenceDiagram
    autonumber
    box Edge host - sender process
        participant LG as LoadGenerator /<br/>DeviceSimulator
        participant SA as SenderAgent
        participant CAD as ProtocolAdapter<br/>client side
    end
    participant NET as Network<br/>(+ MQTT broker)
    box Cloud host - receiver process
        participant SAD as ProtocolAdapter<br/>server side
        participant RA as ReceiverAgent
    end

    LG->>SA: send_one() at its paced instant
    SA->>SA: payload = PayloadGenerator.generate()
    SA->>SA: Message(correlation_id, sequence_number, payload)
    Note over SA: PAYLOAD_CREATED<br/>edge mono + wall
    Note over SA: SEND_STARTED - RTT starts<br/>edge mono + wall
    SA->>CAD: send_and_wait_reply(message, timeout_s)
    Note over CAD: protocol serialization - HTTP headers or MQTT v5 properties<br/>carry correlation_id + sequence, payload bytes untouched<br/>(inside the library - serialization_* events reserved)
    CAD->>NET: HTTP POST /messages or PUBLISH eccom/requests
    NET-->>CAD: MQTT QoS 1/2 - broker PUBACK (concurrent)
    Note over CAD: SEND_COMPLETED<br/>edge mono + wall
    NET->>SAD: bytes arrive
    Note over SAD: protocol deserialization -<br/>parse metadata, read payload
    Note over SAD: MESSAGE_RECEIVED<br/>cloud mono + wall
    SAD->>RA: handler(Message)
    Note over RA: HANDLER_STARTED - cloud mono
    RA->>RA: artificial_processing_delay_ms
    Note over RA: HANDLER_COMPLETED - cloud mono
    RA-->>SAD: reply payload bytes
    Note over SAD: REPLY_SEND_STARTED<br/>cloud mono + wall
    SAD-->>NET: HTTP 200 response or PUBLISH to Response Topic<br/>with Correlation Data
    NET-->>CAD: reply bytes arrive
    CAD-->>SA: reply Message
    Note over SA: REPLY_RECEIVED - RTT stops<br/>edge mono + wall
    Note over LG,RA: Analyzer joins both hosts' event logs on correlation_id.<br/>RTT = REPLY_RECEIVED - SEND_STARTED (edge monotonic, clock-sync-free)<br/>send-ack = SEND_COMPLETED - SEND_STARTED (edge monotonic)<br/>one-way = MESSAGE_RECEIVED wall - SEND_STARTED wall - measured clock offset<br/>(offset bound = min of probe RTT/2 and summed per-host chrony errors)<br/>handler = HANDLER_COMPLETED - HANDLER_STARTED (cloud monotonic)
```

## 2. Deployment and test run (process)

The SSH deployment needs only Docker and SSH on the targets: source is
synced (no repo clone), the agent image is built natively per host, agents
run as containers with host networking, and artifacts come back over SFTP.

```mermaid
flowchart TD
    subgraph ORCH["Orchestrator - eccom run scenario.yaml --hosts hosts.yaml"]
        A["Load + validate ScenarioConfig and HostsConfig"] --> B["Create runs/run-id/, copy scenario.yaml<br/>snapshot chronyc tracking on every host - per-host clock error bounds<br/>write run.json + hosts.json (secrets redacted)"]
    end
    B --> PREP
    subgraph PREP["prepare() - per SSH host, in parallel"]
        C["SFTP sync source tree to working_dir/src<br/>(pyproject, Dockerfile, eccom package - no repo clone)"] --> D["docker build eccom-agent<br/>native architecture, dependency layer cached"] --> E["Record image id as ECCOM_IMAGE_DIGEST"]
    end
    PREP --> F{"MQTT scenario with managed broker?"}
    F -- "yes" --> G["Stage mosquitto config under runs/run-id/broker<br/>+ TLS listener certs if tls.enabled"] --> H["docker run mosquitto on the broker host<br/>wait until TCP reachable"] --> I
    F -- "no" --> I["Start receiver agent container<br/>docker run --network host -v runs volume (no --rm: removed after exit)<br/>scenario + TLS files uploaded to the remote run dir"]
    I --> J["Health check GET receiver:8081/health<br/>logs stream back live"]
    J --> K["Start sender agent container<br/>with --peer-control receiver:8081"]
    K --> SEND
    subgraph SEND["Sender agent lifecycle"]
        L["Clock offset probe - before"] --> M["Warmup - events tagged phase=warmup"] --> N["Measurement load - open loop,<br/>events streamed to events.jsonl by a writer thread (Python) or goroutine (Go)"] --> O["Drain in-flight + cooldown"] --> P["Clock offset probe - after<br/>write events.jsonl, load report, clock-sync.json"]
    end
    SEND --> Q["POST receiver:8081/shutdown<br/>receiver flushes events and exits"]
    Q --> R["Collect artifacts over SFTP from every host<br/>into local runs/run-id/"]
    R --> S["Remove broker container, close SSH connections"]
    S --> T["analyze_run: join events on correlation_id<br/>offset uncertainty = min(probe RTT/2, chrony bound)<br/>write summary.json + plots/"]
```

## 3. Class structure — agents and protocol layer (high-level UML)

Everything below runs inside the sender/receiver agent processes. The only
link to the orchestration layer (diagram 4) is that the TestRunner starts
these processes via `Deployment.start_agent()` and later collects the event
logs they write.

The Go agent (`go/`, selected per role with `sender.implementation` /
`receiver.implementation: go`) mirrors this structure package by package —
`internal/agent`, `internal/load`, `internal/payload`, `internal/protocol`,
`internal/events`, `internal/control`, `internal/metrics`,
`internal/clocksync`; the mapping table is in `go/README.md` — and writes
the same events and artifacts. Two differences in shape: the Go
LoadGenerator paces one schedule from several goroutines inside one
process (`sender.threads`) where the Python sender runs one event loop per
process (`load.senders`), and the Go protocol adapters serve both roles from
one struct exactly as the Python classes do. DeviceSimulator has no Go
counterpart.

```mermaid
classDiagram
    direction TB

    class ProtocolAdapter {
        <<abstract>>
        +start()
        +stop()
        +send(message)
        +send_and_wait_reply(message, timeout_s) Message
        +listen(handler)
        +listen_with_reply(handler)
    }
    class HttpAdapter {
        aiohttp client + server
        TLS via ssl contexts
    }
    class MqttAdapter {
        aiomqtt, MQTT v5
        CorrelationData + ResponseTopic
    }
    ProtocolAdapter <|-- HttpAdapter
    ProtocolAdapter <|-- MqttAdapter

    class Message {
        <<dataclass>>
        +correlation_id str
        +sequence_number int
        +payload bytes
    }
    class MeasurementEvent {
        <<dataclass>>
        +event_type EventType
        +correlation_id, sequence_number
        +host, phase
        +monotonic_ns, wall_time_ns
        +metadata
    }
    class EventRecorder {
        +set_phase(phase)
        +record(type, corr_id, seq, metadata)
        +close() writes JSONL
    }
    EventRecorder o-- MeasurementEvent : buffers

    class SenderAgent {
        +run() LoadReport
    }
    class ReceiverAgent {
        +start()
    }
    class LoadGenerator {
        +run(send_one) LoadReport
    }
    class DeviceSimulator {
        +run(send_payload) LoadReport
    }
    class PayloadGenerator {
        +generate() bytes
    }
    class LoadReport {
        <<dataclass>>
        +requested vs actual rate
        +attempted, completed, failed
        +per_device counts
        +steps (ramp mode)
    }
    class ControlServer {
        GET /health and /clock
        POST /shutdown
    }
    SenderAgent --> ProtocolAdapter : sends via
    SenderAgent --> LoadGenerator : constant_rate / ramp / homes mode
    SenderAgent --> HomeTraffic : homes mode (sensor mix, fire order)
    SenderAgent --> DeviceSimulator : devices mode
    SenderAgent --> PayloadGenerator
    SenderAgent --> EventRecorder : interaction events
    ReceiverAgent --> ProtocolAdapter : listens via
    ReceiverAgent --> EventRecorder : handler events
    ProtocolAdapter --> EventRecorder : wire events
    ProtocolAdapter ..> Message : transmits unchanged
    LoadGenerator --> LoadReport
    DeviceSimulator --> LoadReport

    class ClockSync {
        <<module>>
        +probe_clock_offset(url)
        +combine_clock_probes()
    }
    class SystemMetricsSampler {
        psutil sampling task
    }
    SenderAgent ..> ClockSync : offset probes
    ControlServer ..> ClockSync : serves /clock
    SenderAgent --> SystemMetricsSampler
    ReceiverAgent --> SystemMetricsSampler
```

## 4. Class structure — TestRunner orchestration (high-level UML)

The orchestration layer runs on the operator's machine: it loads the configs,
deploys the agent processes from diagram 3 (via `Deployment.start_agent()`),
manages the broker, collects the artifacts, and runs the analysis.

```mermaid
classDiagram
    direction TB

    class TestRunner {
        +run() run_dir
    }
    class ScenarioConfig {
        <<pydantic>>
        +name, interaction
        +topology, protocol incl tls
        +payload or devices
        +load, receiver, experiment, instrumentation
    }
    class HostsConfig {
        <<pydantic>>
        +hosts per role - local or ssh
        +broker BrokerConfig
    }
    class Deployment {
        <<abstract>>
        +prepare()
        +start_agent(role, scenario, run_dir) AgentProcess
        +collect_artifacts(run_dir)
        +run_command(args)
        +push_run_dir(run_dir, relative)
        +clock_sync_info()
    }
    class AgentProcess {
        <<abstract>>
        +returncode
        +wait() int
        +terminate()
    }
    class LocalProcessDeployment {
        subprocess on this machine
    }
    class SshDeployment {
        sync code, build image natively,
        docker run --network host --ulimit nofile
    }
    Deployment <|-- LocalProcessDeployment
    Deployment <|-- SshDeployment
    Deployment ..> AgentProcess : creates
    class MqttBrokerManager {
        +start(run_dir)
        +stop()
    }
    class Analysis {
        <<module>>
        +analyze_run(run_dir)
        +compute_rtt_table()
        +compute_one_way_table()
        +latency_stats_ms()
        +generate_plots()
    }

    TestRunner --> ScenarioConfig : loads
    TestRunner --> HostsConfig : loads
    TestRunner --> Deployment : one per role
    TestRunner --> MqttBrokerManager : mqtt + managed
    TestRunner --> Analysis : after collection
    MqttBrokerManager --> Deployment : docker via run_command
```
