"""Typed scenario configuration (Pydantic) loaded from YAML.

Every model forbids unknown keys so typos in scenario files fail loudly at
load time instead of silently falling back to defaults.

Host/infrastructure configuration (where "edge" and "cloud" physically run)
deliberately lives in a separate file — see :mod:`eccom.core.hosts` — so the
same scenario can run on any host mapping without edits.
"""

from __future__ import annotations

import math
from pathlib import Path
from typing import Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class TopologyConfig(_StrictModel):
    """Host role labels; they name the per-host artifact files (events, logs)
    and must match entries in the hosts configuration."""

    sender: str = "edge"
    receiver: str = "cloud"

    @model_validator(mode="after")
    def _distinct_labels(self) -> Self:
        if self.sender == self.receiver:
            raise ValueError("topology.sender and topology.receiver must differ")
        return self


class HttpEndpointConfig(_StrictModel):
    """Where the receiver's HTTP server is reachable (the sender connects here)."""

    host: str = "127.0.0.1"
    port: int = Field(default=8080, ge=1, le=65535)


class HttpClientConfig(_StrictModel):
    """Sender-side HTTP behavior."""

    keep_alive: bool = True
    timeout_s: float = Field(default=5.0, gt=0)
    # Connection pool cap per sender process; 0 = unbounded, so the in-flight
    # concurrency is the only limit and every backlog opens new connections.
    max_connections: int = Field(default=0, ge=0)


class HttpServerConfig(_StrictModel):
    """Receiver-side HTTP behavior."""

    bind_host: str = "127.0.0.1"  # 0.0.0.0 in containers / on real hosts


class HttpConfig(_StrictModel):
    endpoint: HttpEndpointConfig = Field(default_factory=HttpEndpointConfig)
    client: HttpClientConfig = Field(default_factory=HttpClientConfig)
    server: HttpServerConfig = Field(default_factory=HttpServerConfig)


class MqttEndpointConfig(_StrictModel):
    """Where the MQTT broker is reachable from both agents."""

    host: str = "127.0.0.1"
    port: int = Field(default=1883, ge=1, le=65535)


class MqttClientConfig(_StrictModel):
    qos: Literal[0, 1, 2] = 1
    keepalive_s: int = Field(default=60, ge=1)
    # Only observable together with reconnect_per_message: with one connection
    # per run there is never a previous session to resume, so the flag cannot
    # change anything. true = fresh session per connect, false = the broker
    # resumes the session the previous connect left behind.
    clean_start: bool = True
    timeout_s: float = Field(default=5.0, gt=0)  # request/reply timeout
    # QoS 1/2 in-flight window (paho default 20). This caps outstanding
    # unacknowledged publishes regardless of load.concurrency — raise it for
    # max-throughput experiments so the client window isn't the bottleneck.
    max_inflight_messages: int = Field(default=20, ge=1)
    # Sender only: connect, publish, disconnect for every message instead of
    # holding one connection for the whole run — the MQTT counterpart of
    # http.client.keep_alive: false, and what duty-cycled sensors that sleep
    # between readings actually do. The connect happens inside the measured
    # SEND_STARTED -> SEND_COMPLETED interval, so send-ack prices session
    # setup rather than just the publish.
    reconnect_per_message: bool = False


BrokerImplementation = Literal["mosquitto", "nanomq"]


class MqttBrokerConfig(_StrictModel):
    """The *managed* broker (hosts file ``broker.managed: true``): which one,
    and the limits written into the config the runner generates for it; an
    external broker keeps whatever it has. The flow-control defaults are
    mosquitto's own, so scenarios without this block run exactly as before;
    0 = unlimited for those two.

    implementation: mosquitto (one event loop, one core) or nanomq (NanoMQ,
    multi-threaded over every core). Part of the experiment, so it lives
    here rather than in the hosts file, which may only override the image.
    max_inflight_messages: QoS 1/2 messages the broker keeps in flight per
    client (also announced as Receive Maximum in CONNACK; paho ignores that).
    Unset = mosquitto's own 20. mosquitto only: NanoMQ takes no setting for
    it (a compile-time 1024 unacknowledged publishes per connection, enforced
    by disconnecting the client and not announced as Receive Maximum), so
    the client's own window applies (65535 for paho.golang) and a value is
    rejected with nanomq.
    max_queued_messages: QoS 1/2 messages queued per client beyond the
    in-flight window. A subscriber that falls further behind than this
    silently loses the excess -- which flattens a saturation curve and hides
    the knee, so high-rate experiments lift both limits and let the backlog
    show up as latency instead. NanoMQ has no unlimited queue: 0 becomes its
    largest practical per-subscriber queue (orchestration/broker.py).
    max_packet_size: the largest MQTT packet the broker accepts, in bytes.
    mosquitto 2.1 defaults to 2 000 000 and disconnects a client that exceeds
    it ("oversize packet"); 2.0 had no limit, and 2.1 rejects 0. The default
    here is the MQTT maximum, i.e. no limit on either version (NanoMQ's own
    default is 10 MiB).
    """

    implementation: BrokerImplementation = "mosquitto"
    max_inflight_messages: int | None = Field(default=None, ge=0)
    max_queued_messages: int = Field(default=1000, ge=0)
    max_packet_size: int = Field(default=268_435_455, ge=20)

    @model_validator(mode="after")
    def _inflight_window_is_a_mosquitto_knob(self) -> Self:
        if self.implementation == "nanomq" and self.max_inflight_messages is not None:
            raise ValueError(
                "broker.max_inflight_messages is a mosquitto setting: NanoMQ takes none "
                "(its inbound QoS 1/2 window is a compile-time 1024 per connection, "
                "enforced by disconnecting the client and not announced as Receive "
                "Maximum), so the client's own window applies (65535 for paho.golang, "
                "mqtt.client.max_inflight_messages for Python paho)"
            )
        return self


class MqttConfig(_StrictModel):
    endpoint: MqttEndpointConfig = Field(default_factory=MqttEndpointConfig)
    client: MqttClientConfig = Field(default_factory=MqttClientConfig)
    broker: MqttBrokerConfig = Field(default_factory=MqttBrokerConfig)
    # Requests flow on <base_topic>/requests, replies on
    # <base_topic>/replies/<client-id> via MQTT v5 Response Topic.
    base_topic: str = Field(default="eccom", min_length=1, pattern=r"^[^#+\s]+$")


class TlsConfig(_StrictModel):
    """TLS composed over the existing adapters (HTTPS / MQTT over TLS).

    All paths are relative to the scenario file's directory (never absolute,
    no ".."), so deployments can ship them into the remote run directory.
    The server side (HTTP receiver / MQTT broker) uses cert_file + key_file.
    The client side verifies the server chain against ca_file when given
    (hostname verification stays off — self-signed certs rarely carry correct
    SANs); without ca_file the connection is encrypted but unverified, which
    is measurement-equivalent (identical handshake).
    """

    enabled: bool = False
    cert_file: Path | None = None
    key_file: Path | None = None
    ca_file: Path | None = None

    @model_validator(mode="after")
    def _consistent(self) -> Self:
        if self.enabled and (self.cert_file is None or self.key_file is None):
            raise ValueError("tls.enabled requires cert_file and key_file")
        for name in ("cert_file", "key_file", "ca_file"):
            path: Path | None = getattr(self, name)
            if path is None:
                continue
            # anchor (not is_absolute) also catches "/etc/x" on Windows
            if path.anchor or ".." in path.parts:
                raise ValueError(
                    f"tls.{name} must be relative to the scenario file, without '..' "
                    "(so it can be shipped to remote hosts)"
                )
        return self


class ProtocolConfig(_StrictModel):
    type: Literal["http", "mqtt"]
    http: HttpConfig = Field(default_factory=HttpConfig)
    mqtt: MqttConfig = Field(default_factory=MqttConfig)
    tls: TlsConfig = Field(default_factory=TlsConfig)
    # Nagle's algorithm off (TCP_NODELAY) on every socket the scenario opens:
    # the aiohttp client and server, the MQTT client in both roles and the
    # managed broker. asyncio does this unconditionally; paho and mosquitto
    # default to Nagle on, which holds a small write for up to a round trip
    # whenever the previous one is still unacknowledged. false reproduces that
    # for either protocol. See docs/scenario-parameters.md.
    tcp_nodelay: bool = True

    @model_validator(mode="after")
    def _nanomq_always_runs_without_nagle(self) -> Self:
        if self.type == "mqtt" and self.mqtt.broker.implementation == "nanomq" and not self.tcp_nodelay:
            raise ValueError(
                "tcp_nodelay: false is not available with the NanoMQ broker: it sets "
                "TCP_NODELAY on every accepted connection and has no setting to turn it off"
            )
        return self

    # Per-type switches that keep protocol-specific fields out of the agents
    # and the runner.
    @property
    def connect_host(self) -> str:
        """The address the sender connects to (HTTP endpoint / MQTT broker)."""
        if self.type == "mqtt":
            return self.mqtt.endpoint.host
        return self.http.endpoint.host

    @property
    def connect_port(self) -> int:
        """The port the sender connects to (HTTP endpoint / MQTT broker)."""
        if self.type == "mqtt":
            return self.mqtt.endpoint.port
        return self.http.endpoint.port

    @property
    def request_timeout_s(self) -> float:
        if self.type == "mqtt":
            return self.mqtt.client.timeout_s
        return self.http.client.timeout_s

    @property
    def requires_broker(self) -> bool:
        return self.type == "mqtt"


class PayloadConfig(_StrictModel):
    """Uniform payload for constant_rate load (unused in devices mode)."""

    size_bytes: int = Field(gt=0)
    content: Literal["zeros", "random"] = "zeros"
    seed: int = 0  # makes "random" payload sequences reproducible


class DeviceConfig(_StrictModel):
    """One simulated IoT device firing messages on a fixed interval.

    smart_meter (smart electricity meter) and temperature_sensor carry
    hardcoded, realistic readings; camera and generic carry fixed
    seeded-random bytes of ``size_bytes``.
    """

    name: str = Field(min_length=1)
    type: Literal["smart_meter", "temperature_sensor", "camera", "generic"]
    interval_s: float = Field(gt=0)
    size_bytes: int | None = Field(default=None, gt=0)  # camera (optional) / generic (required)
    seed: int = 0

    @model_validator(mode="after")
    def _size_matches_type(self) -> Self:
        if self.type in ("smart_meter", "temperature_sensor") and self.size_bytes is not None:
            raise ValueError(f"{self.type} has a hardcoded payload; size_bytes is not configurable")
        if self.type == "generic" and self.size_bytes is None:
            raise ValueError("generic devices require size_bytes")
        return self


class SensorConfig(_StrictModel):
    """One class of sensors in the referential home: how many there are, how
    often each one reports and how many bytes one reading carries."""

    type: str = Field(min_length=1)
    count: int = Field(ge=1)
    rate_hz: float = Field(gt=0)
    size_bytes: int = Field(gt=0)


class HomeConfig(_StrictModel):
    """The referential smart home that ``load.mode: homes`` replicates: a fixed
    mix of sensor classes. Readings are seeded random bytes of the class's
    size (nothing on the path compresses, so content is not a variable)."""

    sensors: list[SensorConfig] = Field(min_length=1)
    seed: int = 0

    @model_validator(mode="after")
    def _types_unique(self) -> Self:
        types = [sensor.type for sensor in self.sensors]
        if len(types) != len(set(types)):
            raise ValueError("home.sensors types must be unique")
        return self

    def message_rate(self, homes: int, aggregation_factor: int) -> float:
        """Messages per second that ``homes`` copies of this home produce when
        every sensor slot batches its readings across ``aggregation_factor``
        homes into one message.

        homes: number of simulated homes.
        aggregation_factor: readings per message (homes batched per sensor slot).
        Returns: the aggregate message rate.
        """
        batches = math.ceil(homes / aggregation_factor)
        return sum(sensor.count * sensor.rate_hz * batches for sensor in self.sensors)


class LoadConfig(_StrictModel):
    """Traffic model.

    constant_rate: uniform paced stream of ``payload`` messages;
    ``concurrency`` caps in-flight requests.
    ramp: like constant_rate, but every ``step_duration_s`` the rate is
    multiplied by ``rate_multiplier`` or raised by ``rate_increment``
    (geometric or linear steps) and/or the payload size multiplied by
    ``size_multiplier`` (starting from ``messages_per_second`` and
    ``payload.size_bytes``) until ``duration_s`` of sending has elapsed — one
    continuous open-loop schedule with nothing drained between steps, for
    finding the rate, or the message size, at which latency leaves its floor;
    ``step_pause_s`` instead inserts a quiet gap after every step so the
    backlog drains and each step starts from an empty queue. Every send is
    tagged with its step (PAYLOAD_CREATED metadata).
    devices: the simulated devices in the scenario's ``devices`` list fire
    independently on their own intervals for ``duration_s`` (open loop, no
    concurrency cap — real sensors do not wait for each other).
    homes: the scenario's ``home`` (a sensor mix) replicated ``homes`` times,
    ``homes_increment`` more every ``step_duration_s``, paced like a ramp at
    the mix's aggregate rate with every send tagged with its step and number
    of homes; ``aggregation_factor`` batches each sensor slot's readings
    across that many homes into one message, dividing the message rate by it
    while the byte rate stays.
    """

    mode: Literal["constant_rate", "ramp", "devices", "homes"] = "constant_rate"
    messages_per_second: float | None = Field(default=None, gt=0)  # ramp: first step
    duration_s: float | None = Field(default=None, gt=0)
    total_messages: int | None = Field(default=None, gt=0)
    concurrency: int = Field(default=1, ge=1)
    # Number of sender processes (containers on SSH hosts) that generate this
    # schedule together; the rate, total_messages and concurrency are split
    # evenly between them (see for_instance). Lifts the one-core ceiling of a
    # single Python sender.
    senders: int = Field(default=1, ge=1)
    # ramp only: at least one of these, applied at every step boundary;
    # rate_multiplier and rate_increment are alternatives
    rate_multiplier: float | None = Field(default=None, gt=1)
    rate_increment: float | None = Field(default=None, gt=0)
    size_multiplier: float | None = Field(default=None, gt=1)
    step_duration_s: float | None = Field(default=None, gt=0)
    # ramp and homes: quiet gap after every step (none after the last); not
    # part of duration_s, which counts sending time only
    step_pause_s: float | None = Field(default=None, gt=0)
    # homes only: copies of the home in the first step and added per step;
    # readings of one sensor slot across aggregation_factor homes travel as
    # one message
    homes: int | None = Field(default=None, ge=1)
    homes_increment: int | None = Field(default=None, ge=1)
    aggregation_factor: int = Field(default=1, ge=1)

    @property
    def stepped(self) -> bool:
        """Whether the schedule walks steps (ramp and homes modes): every send
        is tagged with its step and the report carries per-step results."""
        return self.mode in ("ramp", "homes")

    @model_validator(mode="after")
    def _mode_consistent(self) -> Self:
        if self.mode != "ramp" and (
            self.rate_multiplier is not None
            or self.rate_increment is not None
            or self.size_multiplier is not None
        ):
            raise ValueError(
                "rate_multiplier, rate_increment and size_multiplier are only used when load.mode=ramp"
            )
        if not self.stepped and (self.step_duration_s is not None or self.step_pause_s is not None):
            raise ValueError(
                "step_duration_s and step_pause_s are only used when load.mode=ramp or homes"
            )
        if self.mode != "homes" and (
            self.homes is not None or self.homes_increment is not None or self.aggregation_factor != 1
        ):
            raise ValueError(
                "homes, homes_increment and aggregation_factor are only used when load.mode=homes"
            )
        if self.senders > 1:
            if self.mode in ("devices", "homes"):
                raise ValueError(f"load.senders > 1 is not supported with load.mode={self.mode}")
            if self.total_messages is not None and self.total_messages < self.senders:
                raise ValueError("load.total_messages must be at least load.senders")
        if self.mode == "constant_rate":
            if self.messages_per_second is None:
                raise ValueError("constant_rate load requires messages_per_second")
            if (self.duration_s is None) == (self.total_messages is None):
                raise ValueError("set exactly one of load.duration_s or load.total_messages")
        elif self.mode == "ramp":
            if self.messages_per_second is None:
                raise ValueError("ramp load requires messages_per_second (the first step's rate)")
            if self.step_duration_s is None or (
                self.rate_multiplier is None
                and self.rate_increment is None
                and self.size_multiplier is None
            ):
                raise ValueError(
                    "ramp load requires step_duration_s and at least one of "
                    "rate_multiplier / rate_increment / size_multiplier"
                )
            if self.rate_multiplier is not None and self.rate_increment is not None:
                raise ValueError("set rate_multiplier or rate_increment, not both")
            if self.duration_s is None or self.total_messages is not None:
                raise ValueError(
                    "ramp load requires duration_s (total ramp length) and "
                    "does not support total_messages"
                )
        elif self.mode == "homes":
            if (
                self.homes is None
                or self.homes_increment is None
                or self.step_duration_s is None
                or self.duration_s is None
            ):
                raise ValueError(
                    "homes load requires homes (the first step's count), homes_increment, "
                    "step_duration_s and duration_s"
                )
            if self.messages_per_second is not None or self.total_messages is not None:
                raise ValueError(
                    "homes load derives its rate from the home's sensors; "
                    "remove messages_per_second/total_messages"
                )
        else:  # devices
            if self.duration_s is None:
                raise ValueError("devices load requires duration_s")
            if self.messages_per_second is not None or self.total_messages is not None:
                raise ValueError(
                    "devices load derives rates from device intervals; "
                    "remove messages_per_second/total_messages"
                )
        return self

    def for_instance(self, instance: int) -> Self:
        """The share of this schedule that one of ``senders`` processes
        generates: the rate, its per-step increment, the total message count
        and the in-flight cap are split evenly (remainders go to the lowest
        instances); durations and the ramp shape are unchanged, so the
        instances' schedules add up to this one. A single-sender config is
        returned unchanged."""
        n = self.senders
        if n == 1:
            return self
        if not 0 <= instance < n:
            raise ValueError(f"sender instance {instance} out of range for load.senders={n}")

        def share(total: int) -> int:
            return total // n + (1 if instance < total % n else 0)

        return self.model_copy(
            update={
                "senders": 1,
                "messages_per_second": (
                    None if self.messages_per_second is None else self.messages_per_second / n
                ),
                "rate_increment": (
                    None if self.rate_increment is None else self.rate_increment / n
                ),
                "total_messages": (
                    None if self.total_messages is None else share(self.total_messages)
                ),
                "concurrency": max(1, share(self.concurrency)),
            }
        )


# Which agent program runs a role: the Python package (``eccom agent``) or the
# Go agent under go/ (same command line, same artifacts, see go/README.md).
Implementation = Literal["python", "go"]


class SenderConfig(_StrictModel):
    """Which program generates the load and how many cores it gets. The
    Python sender is one event loop on one core, so ``load.senders`` runs
    several of them; the Go sender is one process that paces the whole
    schedule over ``threads`` goroutines and lets the runtime use that many
    cores (GOMAXPROCS), and its container is capped at that many cores too.
    Part of the experiment, hence recorded in the scenario rather than the
    hosts file."""

    implementation: Implementation = "python"
    # Go only: cores the sender process may use and the number of pacing
    # goroutines; 0 = every core the runtime sees.
    threads: int = Field(default=0, ge=0)
    # ``docker --cpus`` for each sender container on an SSH host (a CFS
    # quota, not a pin). Unset, a Go sender is capped at ``threads`` cores --
    # GOMAXPROCS bounds its goroutines, not the kernel work its syscalls do
    # -- and the Python sender runs uncapped. Overrides the host's
    # ``resources.cpus``.
    cpus: float | None = Field(default=None, gt=0)

    @model_validator(mode="after")
    def _threads_are_a_go_knob(self) -> Self:
        if self.implementation == "python" and self.threads:
            raise ValueError("sender.threads applies to the Go sender only (sender.implementation: go)")
        return self

    @property
    def container_cpus(self) -> float | None:
        """The ``--cpus`` cap for each sender container; None = no cap."""
        if self.cpus is not None:
            return self.cpus
        if self.implementation == "go" and self.threads:
            return float(self.threads)
        return None


class ReceiverConfig(_StrictModel):
    artificial_processing_delay_ms: float = Field(default=0, ge=0)
    # Out-of-band health/shutdown endpoint, separate from the measured path
    # and independent of the protocol under test. Bind 0.0.0.0 on remote
    # hosts so the orchestrator can reach it.
    control_port: int = Field(default=8081, ge=1, le=65535)
    control_bind_host: str = "127.0.0.1"
    # The Python receiver is one process on one core and tops out at a few
    # thousand msg/s; the Go receiver uses every core, so it stays below the
    # sender's ceiling in throughput experiments. Part of the experiment, hence
    # recorded in the scenario rather than the hosts file.
    implementation: Implementation = "python"


class ExperimentConfig(_StrictModel):
    warmup_s: float = Field(default=0, ge=0)
    cooldown_s: float = Field(default=0, ge=0)


class InstrumentationConfig(_StrictModel):
    system_metrics: bool = True
    system_metrics_interval_s: float = Field(default=1.0, gt=0)
    # Quiet window sampled before any traffic, tagged phase="baseline", so
    # resource usage can be reported against the agent's own idle cost rather
    # than in absolute terms. 0 disables it (no baseline, no added run time).
    baseline_s: float = Field(default=3.0, ge=0)
    # iperf3 between the sender's and the receiver's host before the agents
    # start, on the scenario's endpoint port: what the path can carry,
    # independent of the protocol. See eccom.orchestration.link.
    link_capacity: bool = False
    link_capacity_duration_s: float = Field(default=5.0, gt=0)
    network_capture: bool = False

    @model_validator(mode="after")
    def _capture_not_implemented(self) -> Self:
        if self.network_capture:
            raise ValueError("network capture (NetworkProbe) is not implemented yet")
        return self


class ScenarioConfig(_StrictModel):
    name: str = Field(min_length=1)
    # request_response: every message waits for a correlated reply (RTT).
    # one_way: fire-and-forget telemetry; latency comes from clock-offset-
    # corrected edge/cloud wall timestamps plus the sender-side ack.
    interaction: Literal["request_response", "one_way"] = "request_response"
    topology: TopologyConfig = Field(default_factory=TopologyConfig)
    protocol: ProtocolConfig
    payload: PayloadConfig | None = None
    devices: list[DeviceConfig] | None = None
    home: HomeConfig | None = None
    load: LoadConfig
    sender: SenderConfig = Field(default_factory=SenderConfig)
    receiver: ReceiverConfig = Field(default_factory=ReceiverConfig)
    experiment: ExperimentConfig = Field(default_factory=ExperimentConfig)
    instrumentation: InstrumentationConfig = Field(default_factory=InstrumentationConfig)

    @model_validator(mode="after")
    def _go_sender_is_one_paced_process(self) -> Self:
        """The Go sender implements the paced stream in one process; the
        rest fails here, at load time, rather than on the sender's host."""
        if self.sender.implementation != "go":
            return self
        if self.load.senders != 1:
            raise ValueError(
                "sender.implementation=go runs as one process: set sender.threads "
                "instead of load.senders"
            )
        if self.load.mode in ("devices", "homes"):
            raise ValueError(f"the Go sender does not implement load.mode={self.load.mode}")
        if self.protocol.type == "mqtt" and self.protocol.mqtt.client.reconnect_per_message:
            raise ValueError("the Go sender does not implement mqtt.client.reconnect_per_message")
        return self

    @model_validator(mode="after")
    def _traffic_model_consistent(self) -> Self:
        mode = self.load.mode
        if mode == "devices":
            if not self.devices:
                raise ValueError("load.mode=devices requires a non-empty devices list")
            names = [device.name for device in self.devices]
            if len(names) != len(set(names)):
                raise ValueError("device names must be unique")
        elif self.devices:
            raise ValueError("devices are only used when load.mode=devices")
        if mode == "homes":
            if self.home is None:
                raise ValueError("load.mode=homes requires a home block")
        elif self.home is not None:
            raise ValueError("home is only used when load.mode=homes")
        if mode in ("devices", "homes"):
            if self.payload is not None:
                raise ValueError(f"payload is not used when load.mode={mode}; remove it")
        elif self.payload is None:
            raise ValueError(f"payload is required when load.mode={mode}")
        return self

    @model_validator(mode="after")
    def _reconnect_per_message_is_sequential_one_way(self) -> Self:
        if not (self.protocol.type == "mqtt" and self.protocol.mqtt.client.reconnect_per_message):
            return self
        if self.interaction != "one_way":
            raise ValueError(
                "mqtt.client.reconnect_per_message requires interaction=one_way: "
                "the per-message connection closes before a reply could arrive"
            )
        # Every message reconnects under the same client id, and a broker drops
        # the older session as soon as a second connection claims that id, so
        # overlapping sends would disconnect each other.
        if self.load.mode != "constant_rate" or self.load.concurrency != 1:
            raise ValueError(
                "mqtt.client.reconnect_per_message requires load.mode=constant_rate "
                "with concurrency=1: overlapping connections share one client id "
                "and would disconnect each other"
            )
        return self

    def with_connect_host(self, host: str) -> ScenarioConfig:
        """A copy whose active endpoint (HTTP endpoint / MQTT broker) is ``host``.

        host: the address to connect to, replacing a topology role label the scenario named.
        Returns a deep copy; the inactive protocol block is untouched.
        """
        copy = self.model_copy(deep=True)
        if copy.protocol.type == "mqtt":
            copy.protocol.mqtt.endpoint.host = host
        else:
            copy.protocol.http.endpoint.host = host
        return copy


def load_scenario(path: Path) -> ScenarioConfig:
    """Load and validate a scenario YAML file."""
    with path.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if not isinstance(raw, dict):
        raise ValueError(f"scenario file {path} must contain a YAML mapping")
    return ScenarioConfig.model_validate(raw)
