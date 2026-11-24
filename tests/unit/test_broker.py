"""Managed broker: the mosquitto.conf it writes and the container it starts."""

from pathlib import Path

import pytest

from eccom.core.config import MqttConfig, ProtocolConfig, TlsConfig
from eccom.core.hosts import BrokerConfig
from eccom.orchestration.broker import MqttBrokerManager
from eccom.orchestration.deployment import Deployment


class FakeDeployment(Deployment):
    def __init__(self, flags: list[str] | None = None) -> None:
        self.commands: list[list[str]] = []
        self.pushed: list[str] = []
        self.flags = flags or []

    def container_flags(self) -> list[str]:
        return self.flags

    async def start_agent(self, role, scenario_path, run_dir, extra_args=()):
        raise NotImplementedError

    async def run_command(self, args: list[str]) -> tuple[int, str]:
        self.commands.append(args)
        if args[:2] == ["docker", "logs"]:
            return 0, "mosquitto version 2.1.2 starting\n"
        return 0, ""

    async def push_run_dir(self, run_dir: Path, relative: str) -> str:
        self.pushed.append(relative)
        return f"/remote/{relative}"


async def _reachable(self) -> None:  # the fake never listens
    return None


@pytest.fixture(autouse=True)
def skip_reachability(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(MqttBrokerManager, "_wait_reachable", _reachable)


async def test_scenario_flow_control_limits_land_in_the_broker_config(tmp_path: Path) -> None:
    protocol = ProtocolConfig.model_validate(
        {
            "type": "mqtt",
            "mqtt": {
                "endpoint": {"port": 1884},
                "broker": {"max_inflight_messages": 0, "max_queued_messages": 0},
            },
        }
    )
    deployment = FakeDeployment()
    manager = MqttBrokerManager(BrokerConfig(), deployment, protocol, tmp_path)
    await manager.start(tmp_path)

    conf = (tmp_path / "broker" / "mosquitto.conf").read_text(encoding="utf-8")
    assert "listener 1884\n" in conf
    assert "allow_anonymous true\n" in conf
    assert "max_inflight_messages 0\n" in conf
    assert "max_queued_messages 0\n" in conf
    assert "max_packet_size 268435455\n" in conf  # the MQTT maximum: mosquitto 2.1 caps at 2 MB
    assert "set_tcp_nodelay true\n" in conf  # the scenario default; mosquitto's own is false
    assert "certfile" not in conf
    assert deployment.pushed == ["broker"]

    run = deployment.commands[-1]
    assert run[:3] == ["docker", "run", "-d"]
    assert "1884:1884" in run  # the generated listener replaces the image's 1883 default
    assert "/remote/broker:/mosquitto/config:ro" in run
    assert "--user" not in run
    assert run[-1] == "eclipse-mosquitto:2"


async def test_defaults_are_mosquittos_own_and_tls_adds_the_key_material(tmp_path: Path) -> None:
    certs = tmp_path / "certs"
    certs.mkdir()
    (certs / "server.pem").write_text("cert", encoding="utf-8")
    (certs / "server-key.pem").write_text("key", encoding="utf-8")
    protocol = ProtocolConfig(
        type="mqtt",
        mqtt=MqttConfig.model_validate({"endpoint": {"port": 8883}}),
        tls=TlsConfig(
            enabled=True, cert_file=Path("certs/server.pem"), key_file=Path("certs/server-key.pem")
        ),
    )
    assert protocol.mqtt.broker.max_inflight_messages is None  # unset: mosquitto's own 20
    assert protocol.mqtt.broker.max_queued_messages == 1000
    assert protocol.mqtt.broker.max_packet_size == 268_435_455

    deployment = FakeDeployment()
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    manager = MqttBrokerManager(BrokerConfig(), deployment, protocol, tmp_path)
    await manager.start(run_dir)
    run = deployment.commands[-1]
    await manager.stop()
    # the broker's own log is kept before the container goes
    assert (run_dir / "broker" / "mosquitto.log").read_text(encoding="utf-8").startswith(
        "mosquitto version 2.1.2"
    )
    assert deployment.commands[-2:] == [
        ["docker", "logs", "eccom-mqtt-broker"], ["docker", "rm", "-f", "eccom-mqtt-broker"],
    ]

    conf = (run_dir / "broker" / "mosquitto.conf").read_text(encoding="utf-8")
    assert "listener 8883\n" in conf
    assert "max_inflight_messages 20\n" in conf
    assert "max_queued_messages 1000\n" in conf
    assert "certfile /mosquitto/config/server.pem\n" in conf
    assert "keyfile /mosquitto/config/server-key.pem\n" in conf
    assert (run_dir / "broker" / "server.pem").read_text(encoding="utf-8") == "cert"
    assert (run_dir / "broker" / "server-key.pem").read_text(encoding="utf-8") == "key"
    assert "--user" in run and run[run.index("--user") + 1] == "root"
    assert "8883:8883" in run


async def test_host_resource_caps_apply_to_the_broker_container(tmp_path: Path) -> None:
    deployment = FakeDeployment(flags=["--cpus", "1.5", "--memory", "750m", "--memory-swap", "750m"])
    manager = MqttBrokerManager(BrokerConfig(), deployment, ProtocolConfig(type="mqtt"), tmp_path)
    await manager.start(tmp_path)

    run = deployment.commands[-1]
    assert run[-1] == "eclipse-mosquitto:2"
    assert run[-7:-1] == ["--cpus", "1.5", "--memory", "750m", "--memory-swap", "750m"]


async def test_tcp_nodelay_off_reaches_the_broker_config(tmp_path: Path) -> None:
    protocol = ProtocolConfig(type="mqtt", tcp_nodelay=False)
    await MqttBrokerManager(BrokerConfig(), FakeDeployment(), protocol, tmp_path).start(tmp_path)

    conf = (tmp_path / "broker" / "mosquitto.conf").read_text(encoding="utf-8")
    assert "set_tcp_nodelay false\n" in conf


def _nanomq(port: int = 1883, **broker) -> ProtocolConfig:
    return ProtocolConfig.model_validate(
        {
            "type": "mqtt",
            "mqtt": {"endpoint": {"port": port}, "broker": {"implementation": "nanomq", **broker}},
        }
    )


async def test_nanomq_runs_from_a_generated_hocon_config(tmp_path: Path) -> None:
    """The scenario's broker limits land in nanomq.conf, the container runs
    the slim image (the base one has no TLS) on the generated config, and
    the broker's log is kept under the implementation's name."""
    deployment = FakeDeployment(flags=["--cpus", "3.5"])
    run_dir = tmp_path / "run"
    run_dir.mkdir()
    manager = MqttBrokerManager(BrokerConfig(), deployment, _nanomq(1884, max_queued_messages=0), tmp_path)
    await manager.start(run_dir)
    run = deployment.commands[-1]
    await manager.stop()

    conf = (run_dir / "broker" / "nanomq.conf").read_text(encoding="utf-8")
    assert conf.startswith("# Generated by eccom for emqx/nanomq:0.24.14-slim")
    assert "    max_mqueue_len = 1048576\n" in conf  # 0 = unlimited has no NanoMQ equivalent
    assert "    max_packet_size = 268435455\n" in conf
    assert 'listeners.tcp {\n    bind = "0.0.0.0:1884"\n}\n' in conf
    assert "listeners.ssl" not in conf and "max_inflight" not in conf.split("#", 1)[0]
    assert "    to = [console]\n" in conf and "    allow_anonymous = true\n" in conf
    assert not (run_dir / "broker" / "mosquitto.conf").exists()
    assert deployment.pushed == ["broker"]

    assert run[:3] == ["docker", "run", "-d"]
    assert "1884:1884" in run
    assert "/remote/broker:/eccom/broker:ro" in run
    assert "--user" not in run
    image = run.index("emqx/nanomq:0.24.14-slim")
    assert run[image - 2:image] == ["--cpus", "3.5"]  # host caps before the image
    assert run[image + 1:] == ["--conf", "/eccom/broker/nanomq.conf"]  # options after it: the entrypoint prepends `nanomq start`
    assert (run_dir / "broker" / "nanomq.log").read_text(encoding="utf-8").startswith("mosquitto version")
    assert not (run_dir / "broker" / "mosquitto.log").exists()


async def test_nanomq_tls_listener_replaces_the_plaintext_one(tmp_path: Path) -> None:
    certs = tmp_path / "certs"
    certs.mkdir()
    (certs / "server.pem").write_text("cert", encoding="utf-8")
    (certs / "server-key.pem").write_text("key", encoding="utf-8")
    protocol = ProtocolConfig(
        type="mqtt",
        mqtt=MqttConfig.model_validate({"endpoint": {"port": 8883}, "broker": {"implementation": "nanomq"}}),
        tls=TlsConfig(
            enabled=True, cert_file=Path("certs/server.pem"), key_file=Path("certs/server-key.pem")
        ),
    )
    deployment = FakeDeployment()
    await MqttBrokerManager(BrokerConfig(), deployment, protocol, tmp_path).start(tmp_path)

    conf = (tmp_path / "broker" / "nanomq.conf").read_text(encoding="utf-8")
    assert "listeners.tcp" not in conf
    assert 'listeners.ssl {\n    bind = "0.0.0.0:8883"\n' in conf
    assert '    keyfile = "/eccom/broker/server-key.pem"\n' in conf
    assert '    certfile = "/eccom/broker/server.pem"\n' in conf
    assert '    cacertfile = "/eccom/broker/server.pem"\n' in conf
    assert "    verify_peer = false\n" in conf
    assert "    max_mqueue_len = 1000\n" in conf  # the scenario default, mosquitto's own
    assert (tmp_path / "broker" / "server.pem").read_text(encoding="utf-8") == "cert"
    assert (tmp_path / "broker" / "server-key.pem").read_text(encoding="utf-8") == "key"
    run = deployment.commands[-1]
    assert "--user" not in run  # the NanoMQ image already runs as root
    assert "8883:8883" in run


async def test_hosts_file_image_overrides_the_implementations_default(tmp_path: Path) -> None:
    deployment = FakeDeployment()
    broker = BrokerConfig(docker_image="emqx/nanomq:0.23.10-slim")
    await MqttBrokerManager(broker, deployment, _nanomq(), tmp_path).start(tmp_path)

    run = deployment.commands[-1]
    assert "emqx/nanomq:0.23.10-slim" in run and "emqx/nanomq:0.24.14-slim" not in run
    conf = (tmp_path / "broker" / "nanomq.conf").read_text(encoding="utf-8")
    assert conf.startswith("# Generated by eccom for emqx/nanomq:0.23.10-slim")
