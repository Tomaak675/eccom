from pathlib import Path

import pytest
from pydantic import ValidationError

from eccom.core.config import ScenarioConfig, load_scenario

SCENARIOS_DIR = Path(__file__).parents[2] / "scenarios"


def minimal_scenario(**overrides) -> dict:
    scenario = {
        "name": "test",
        "protocol": {"type": "http"},
        "payload": {"size_bytes": 64},
        "load": {"messages_per_second": 10, "total_messages": 5},
    }
    scenario.update(overrides)
    return scenario


def test_minimal_scenario_is_valid() -> None:
    config = ScenarioConfig.model_validate(minimal_scenario())
    assert config.protocol.type == "http"
    assert config.protocol.http.endpoint.port == 8080
    assert config.protocol.connect_host == "127.0.0.1"
    assert config.protocol.request_timeout_s == 5.0
    assert config.load.concurrency == 1
    assert config.interaction == "request_response"


@pytest.mark.parametrize(
    "scenario_file",
    [
        "http_baseline.yaml",
        "http_smoke.yaml",
        "http_baseline_compose.yaml",
        "http_smoke_compose.yaml",
        "http_one_way.yaml",
        "mqtt_one_way.yaml",
        "https_smoke.yaml",
        "mqtts_smoke.yaml",
    ],
)
def test_shipped_example_scenarios_are_valid(scenario_file: str) -> None:
    config = load_scenario(SCENARIOS_DIR / scenario_file)
    assert config.payload is not None
    assert config.payload.size_bytes == 1024


SUITES = sorted(path.name for path in SCENARIOS_DIR.iterdir() if path.is_dir() and path.name != "certs")


@pytest.mark.parametrize("suite", SUITES)
def test_shipped_suite_loads_with_distinct_names_and_the_cloud_role_as_endpoint(suite: str) -> None:
    """Every file of a shipped suite validates, labels its run uniquely and
    names the receiver's role, so the hosts file supplies the address."""
    configs = [load_scenario(path) for path in sorted((SCENARIOS_DIR / suite).glob("*.yaml"))]
    assert configs
    assert len({config.name for config in configs}) == len(configs)
    for config in configs:
        assert config.protocol.connect_host == "cloud", config.name


def test_receiver_implementation_defaults_to_python() -> None:
    from eccom.core.config import ReceiverConfig

    assert ReceiverConfig().implementation == "python"
    assert ReceiverConfig(implementation="go").implementation == "go"
    with pytest.raises(ValidationError):
        ReceiverConfig(implementation="rust")


def test_load_senders_split_the_schedule_evenly() -> None:
    """Several sender processes generate shares that add up to the scenario's
    load: rate / n, counts and the in-flight cap split with remainders to the
    lowest instances, durations and the ramp shape untouched."""
    from eccom.core.config import LoadConfig

    ramp = LoadConfig(
        mode="ramp", messages_per_second=10, rate_multiplier=2, step_duration_s=5,
        duration_s=60, concurrency=10, senders=4,
    )
    shares = [ramp.for_instance(i) for i in range(4)]
    assert [s.messages_per_second for s in shares] == [2.5] * 4
    assert [s.concurrency for s in shares] == [3, 3, 2, 2]
    assert all(s.senders == 1 and s.rate_multiplier == 2 and s.duration_s == 60 for s in shares)
    fixed = LoadConfig(messages_per_second=100, total_messages=10, senders=3)
    assert [fixed.for_instance(i).total_messages for i in range(3)] == [4, 3, 3]
    assert [fixed.for_instance(i).concurrency for i in range(3)] == [1, 1, 1]
    linear = LoadConfig(
        mode="ramp", messages_per_second=1800, rate_increment=600, step_duration_s=10,
        step_pause_s=10, duration_s=130, concurrency=10000, senders=6,
    )
    share = linear.for_instance(0)
    assert (share.messages_per_second, share.rate_increment, share.concurrency) == (300, 100, 1667)
    assert share.step_pause_s == 10 and share.step_duration_s == 10
    single = LoadConfig(messages_per_second=1, duration_s=1)
    assert single.for_instance(0) is single
    with pytest.raises(ValueError):
        ramp.for_instance(4)


def test_load_senders_validation() -> None:
    from eccom.core.config import LoadConfig

    with pytest.raises(ValidationError):
        LoadConfig(mode="devices", duration_s=1, senders=2)
    with pytest.raises(ValidationError):
        LoadConfig(messages_per_second=1, total_messages=2, senders=3)
    with pytest.raises(ValidationError):
        LoadConfig(messages_per_second=1, duration_s=1, senders=0)


def test_link_capacity_probe_is_off_by_default_and_uses_the_endpoint_port() -> None:
    config = ScenarioConfig.model_validate(minimal_scenario())
    assert not config.instrumentation.link_capacity
    assert config.instrumentation.link_capacity_duration_s == 5.0
    assert config.protocol.connect_port == 8080
    mqtt = ScenarioConfig.model_validate(minimal_scenario(protocol={"type": "mqtt"}))
    assert mqtt.protocol.connect_port == 1883


def test_shipped_device_scenario_is_valid() -> None:
    config = load_scenario(SCENARIOS_DIR / "iot_devices_http.yaml")
    assert config.load.mode == "devices"
    assert config.devices is not None
    assert {d.type for d in config.devices} == {
        "smart_meter",
        "temperature_sensor",
        "camera",
        "generic",
    }


def test_unknown_keys_are_rejected() -> None:
    with pytest.raises(ValidationError, match="extra_field"):
        ScenarioConfig.model_validate(minimal_scenario(extra_field=1))


def test_payload_size_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        ScenarioConfig.model_validate(minimal_scenario(payload={"size_bytes": 0}))


def test_load_requires_exactly_one_stop_condition() -> None:
    with pytest.raises(ValidationError, match="exactly one"):
        ScenarioConfig.model_validate(
            minimal_scenario(
                load={"messages_per_second": 10, "total_messages": 5, "duration_s": 1}
            )
        )
    with pytest.raises(ValidationError, match="exactly one"):
        ScenarioConfig.model_validate(minimal_scenario(load={"messages_per_second": 10}))


def test_mqtt_scenario_is_valid_with_defaults() -> None:
    config = ScenarioConfig.model_validate(minimal_scenario(protocol={"type": "mqtt"}))
    assert config.protocol.mqtt.endpoint.port == 1883
    assert config.protocol.mqtt.client.qos == 1
    assert config.protocol.mqtt.client.max_inflight_messages == 20  # paho default
    raised = ScenarioConfig.model_validate(
        minimal_scenario(
            protocol={"type": "mqtt", "mqtt": {"client": {"max_inflight_messages": 500}}}
        )
    )
    assert raised.protocol.mqtt.client.max_inflight_messages == 500
    assert config.protocol.connect_host == "127.0.0.1"
    assert config.protocol.request_timeout_s == 5.0
    assert config.protocol.requires_broker
    assert not ScenarioConfig.model_validate(minimal_scenario()).protocol.requires_broker


def test_tcp_nodelay_is_a_protocol_level_switch_that_defaults_on() -> None:
    assert ScenarioConfig.model_validate(minimal_scenario()).protocol.tcp_nodelay
    nagle_on = ScenarioConfig.model_validate(
        minimal_scenario(protocol={"type": "mqtt", "tcp_nodelay": False})
    )
    assert not nagle_on.protocol.tcp_nodelay


def test_ramp_load_requires_its_parameters_and_forbids_them_elsewhere() -> None:
    ramp = {
        "mode": "ramp",
        "messages_per_second": 10,
        "rate_multiplier": 2,
        "step_duration_s": 5,
        "duration_s": 60,
    }
    config = ScenarioConfig.model_validate(minimal_scenario(load=ramp))
    assert config.load.mode == "ramp"
    assert config.load.rate_multiplier == 2
    with pytest.raises(ValidationError, match="at least one of"):
        ScenarioConfig.model_validate(
            minimal_scenario(load={"mode": "ramp", "messages_per_second": 10, "duration_s": 60})
        )
    with pytest.raises(ValidationError, match="does not support total_messages"):
        ScenarioConfig.model_validate(
            minimal_scenario(load=ramp | {"duration_s": None, "total_messages": 100})
        )
    with pytest.raises(ValidationError):  # the multiplier must actually grow the rate
        ScenarioConfig.model_validate(minimal_scenario(load=ramp | {"rate_multiplier": 1}))
    with pytest.raises(ValidationError, match="only used when load.mode=ramp"):
        ScenarioConfig.model_validate(
            minimal_scenario(
                load={"messages_per_second": 10, "total_messages": 5, "rate_multiplier": 2}
            )
        )
    # a linear ramp: the increment is the multiplier's alternative, never its companion
    linear = ramp | {"rate_multiplier": None, "rate_increment": 800}
    config = ScenarioConfig.model_validate(minimal_scenario(load=linear))
    assert config.load.rate_increment == 800 and config.load.rate_multiplier is None
    with pytest.raises(ValidationError, match="not both"):
        ScenarioConfig.model_validate(minimal_scenario(load=ramp | {"rate_increment": 800}))
    with pytest.raises(ValidationError, match="only used when load.mode=ramp"):
        ScenarioConfig.model_validate(
            minimal_scenario(
                load={"messages_per_second": 10, "total_messages": 5, "rate_increment": 800}
            )
        )
    # the drain pause between steps is a ramp knob too
    paused = ScenarioConfig.model_validate(minimal_scenario(load=linear | {"step_pause_s": 10}))
    assert paused.load.step_pause_s == 10
    with pytest.raises(ValidationError, match="only used when load.mode=ramp"):
        ScenarioConfig.model_validate(
            minimal_scenario(
                load={"messages_per_second": 10, "total_messages": 5, "step_pause_s": 10}
            )
        )
    # a payload-size ramp: the rate may stay constant
    size_ramp = ramp | {"rate_multiplier": None, "size_multiplier": 4}
    config = ScenarioConfig.model_validate(minimal_scenario(load=size_ramp))
    assert config.load.rate_multiplier is None and config.load.size_multiplier == 4
    with pytest.raises(ValidationError):  # the multiplier must actually grow the payload
        ScenarioConfig.model_validate(minimal_scenario(load=size_ramp | {"size_multiplier": 1}))
    with pytest.raises(ValidationError, match="only used when load.mode=ramp"):
        ScenarioConfig.model_validate(
            minimal_scenario(
                load={"messages_per_second": 10, "total_messages": 5, "size_multiplier": 4}
            )
        )


def test_mqtt_reconnect_per_message_requires_sequential_one_way() -> None:
    reconnect = {"type": "mqtt", "mqtt": {"client": {"reconnect_per_message": True}}}
    # minimal_scenario defaults to request_response, where the per-message
    # connection would be closed before a reply could arrive
    with pytest.raises(ValidationError, match="requires interaction=one_way"):
        ScenarioConfig.model_validate(minimal_scenario(protocol=reconnect))
    # overlapping sends would share one client id and disconnect each other
    with pytest.raises(ValidationError, match="concurrency=1"):
        ScenarioConfig.model_validate(
            minimal_scenario(
                protocol=reconnect,
                interaction="one_way",
                load={"messages_per_second": 10, "total_messages": 5, "concurrency": 2},
            )
        )
    config = ScenarioConfig.model_validate(
        minimal_scenario(protocol=reconnect, interaction="one_way")
    )
    assert config.protocol.mqtt.client.reconnect_per_message
    assert config.protocol.mqtt.client.clean_start  # cold session per message
    assert not ScenarioConfig.model_validate(
        minimal_scenario(protocol={"type": "mqtt"})
    ).protocol.mqtt.client.reconnect_per_message  # off by default


def test_shipped_mqtt_scenarios_are_valid() -> None:
    for scenario_file in ("mqtt_baseline.yaml", "mqtt_smoke.yaml", "mqtt_baseline_compose.yaml"):
        config = load_scenario(SCENARIOS_DIR / scenario_file)
        assert config.protocol.type == "mqtt"


def test_mqtt_qos_and_topic_are_validated() -> None:
    with pytest.raises(ValidationError):
        ScenarioConfig.model_validate(
            minimal_scenario(protocol={"type": "mqtt", "mqtt": {"client": {"qos": 3}}})
        )
    with pytest.raises(ValidationError):  # wildcards not allowed in the base topic
        ScenarioConfig.model_validate(
            minimal_scenario(protocol={"type": "mqtt", "mqtt": {"base_topic": "eccom/#"}})
        )


def test_tls_scenario_is_valid_with_relative_paths() -> None:
    config = ScenarioConfig.model_validate(
        minimal_scenario(
            protocol={
                "type": "http",
                "tls": {
                    "enabled": True,
                    "cert_file": "certs/server.pem",
                    "key_file": "certs/server-key.pem",
                },
            }
        )
    )
    assert config.protocol.tls.enabled
    assert config.protocol.tls.ca_file is None  # encrypt-without-verify mode


def test_tls_requires_cert_and_key() -> None:
    with pytest.raises(ValidationError, match="cert_file and key_file"):
        ScenarioConfig.model_validate(
            minimal_scenario(protocol={"type": "http", "tls": {"enabled": True}})
        )


@pytest.mark.parametrize("bad_path", ["/etc/ssl/server.pem", "../certs/server.pem"])
def test_tls_paths_must_be_shippable(bad_path: str) -> None:
    with pytest.raises(ValidationError, match="relative to the scenario file"):
        ScenarioConfig.model_validate(
            minimal_scenario(
                protocol={
                    "type": "http",
                    "tls": {
                        "enabled": True,
                        "cert_file": bad_path,
                        "key_file": "certs/server-key.pem",
                    },
                }
            )
        )


def test_network_capture_is_rejected_until_implemented() -> None:
    with pytest.raises(ValidationError, match="network capture"):
        ScenarioConfig.model_validate(
            minimal_scenario(instrumentation={"network_capture": True})
        )


def test_topology_roles_must_differ() -> None:
    with pytest.raises(ValidationError, match="must differ"):
        ScenarioConfig.model_validate(
            minimal_scenario(topology={"sender": "edge", "receiver": "edge"})
        )


def device_scenario(devices: list[dict], **overrides) -> dict:
    scenario = {
        "name": "devices-test",
        "protocol": {"type": "http"},
        "load": {"mode": "devices", "duration_s": 10},
        "devices": devices,
    }
    scenario.update(overrides)
    return scenario


def test_device_scenario_is_valid() -> None:
    config = ScenarioConfig.model_validate(
        device_scenario(
            [
                {"name": "m1", "type": "smart_meter", "interval_s": 1},
                {"name": "g1", "type": "generic", "interval_s": 0.5, "size_bytes": 128},
            ]
        )
    )
    assert config.devices is not None and len(config.devices) == 2


def test_devices_mode_requires_devices_and_forbids_payload() -> None:
    with pytest.raises(ValidationError, match="non-empty devices"):
        ScenarioConfig.model_validate(device_scenario([]) | {"devices": None})
    with pytest.raises(ValidationError, match="payload is not used"):
        ScenarioConfig.model_validate(
            device_scenario(
                [{"name": "m1", "type": "smart_meter", "interval_s": 1}],
                payload={"size_bytes": 64},
            )
        )


def test_constant_rate_forbids_devices() -> None:
    with pytest.raises(ValidationError, match="only used when"):
        ScenarioConfig.model_validate(
            minimal_scenario(devices=[{"name": "m1", "type": "smart_meter", "interval_s": 1}])
        )


def test_device_names_must_be_unique() -> None:
    with pytest.raises(ValidationError, match="unique"):
        ScenarioConfig.model_validate(
            device_scenario(
                [
                    {"name": "m1", "type": "smart_meter", "interval_s": 1},
                    {"name": "m1", "type": "temperature_sensor", "interval_s": 2},
                ]
            )
        )


def test_hardcoded_sensors_reject_size_bytes_and_generic_requires_it() -> None:
    with pytest.raises(ValidationError, match="hardcoded payload"):
        ScenarioConfig.model_validate(
            device_scenario(
                [{"name": "m1", "type": "smart_meter", "interval_s": 1, "size_bytes": 10}]
            )
        )
    with pytest.raises(ValidationError, match="require size_bytes"):
        ScenarioConfig.model_validate(
            device_scenario([{"name": "g1", "type": "generic", "interval_s": 1}])
        )


def test_devices_mode_rejects_constant_rate_fields() -> None:
    with pytest.raises(ValidationError, match="device intervals"):
        ScenarioConfig.model_validate(
            device_scenario(
                [{"name": "m1", "type": "smart_meter", "interval_s": 1}],
                load={"mode": "devices", "duration_s": 10, "messages_per_second": 5},
            )
        )


REFERENCE_HOME = {
    "seed": 42,
    "sensors": [
        {"type": "light", "count": 4, "rate_hz": 1, "size_bytes": 4},
        {"type": "smart_plug", "count": 40, "rate_hz": 1, "size_bytes": 16},
        {"type": "temperature", "count": 4, "rate_hz": 1, "size_bytes": 4},
        {"type": "motion", "count": 4, "rate_hz": 1, "size_bytes": 4},
        {"type": "microphone", "count": 4, "rate_hz": 50, "size_bytes": 640},
        {"type": "mmwave", "count": 4, "rate_hz": 10, "size_bytes": 20},
    ],
}


def home_scenario(factor: int = 1, **overrides) -> dict:
    scenario = {
        "name": "homes-test",
        "interaction": "one_way",
        "protocol": {"type": "http"},
        "home": REFERENCE_HOME,
        "load": {
            "mode": "homes", "homes": factor, "homes_increment": factor,
            "aggregation_factor": factor, "step_duration_s": 10, "step_pause_s": 10,
            "duration_s": 120, "concurrency": 10000,
        },
    }
    scenario.update(overrides)
    return scenario


def test_homes_scenario_is_valid_and_its_rate_follows_the_sensor_mix() -> None:
    config = ScenarioConfig.model_validate(home_scenario())
    assert config.load.stepped and config.home is not None
    assert config.home.message_rate(1, 1) == 292
    assert config.home.message_rate(8, 8) == 292
    assert config.home.message_rate(9, 8) == 584  # a partial second batch
    assert config.home.message_rate(12, 1) == 3504
    assert ScenarioConfig.model_validate(home_scenario(8)).load.aggregation_factor == 8
    assert not ScenarioConfig.model_validate(minimal_scenario()).load.stepped


def test_homes_mode_validation() -> None:
    load = home_scenario()["load"]
    with pytest.raises(ValidationError, match="requires a home block"):
        ScenarioConfig.model_validate(home_scenario(home=None))
    with pytest.raises(ValidationError, match="payload is not used"):
        ScenarioConfig.model_validate(home_scenario(payload={"size_bytes": 64}))
    with pytest.raises(ValidationError, match="only used when load.mode=devices"):
        ScenarioConfig.model_validate(
            home_scenario(devices=[{"name": "d", "type": "generic", "interval_s": 1, "size_bytes": 8}])
        )
    for field in ("homes", "homes_increment", "step_duration_s", "duration_s"):
        with pytest.raises(ValidationError, match="homes load requires"):
            ScenarioConfig.model_validate(home_scenario(load=load | {field: None}))
    with pytest.raises(ValidationError, match="derives its rate"):
        ScenarioConfig.model_validate(home_scenario(load=load | {"messages_per_second": 5}))
    with pytest.raises(ValidationError, match="only used when load.mode=ramp"):
        ScenarioConfig.model_validate(home_scenario(load=load | {"rate_increment": 5}))
    with pytest.raises(ValidationError, match="not supported with load.mode=homes"):
        ScenarioConfig.model_validate(home_scenario(load=load | {"senders": 2}))
    with pytest.raises(ValidationError, match="only used when load.mode=homes"):
        ScenarioConfig.model_validate(
            minimal_scenario(load={"messages_per_second": 10, "total_messages": 5, "aggregation_factor": 2})
        )
    with pytest.raises(ValidationError, match="only used when load.mode=homes"):
        ScenarioConfig.model_validate(minimal_scenario(home=REFERENCE_HOME))
    with pytest.raises(ValidationError, match="unique"):
        ScenarioConfig.model_validate(
            home_scenario(home={"sensors": [{"type": "a", "count": 1, "rate_hz": 1, "size_bytes": 1}] * 2})
        )


def test_with_connect_host_replaces_only_the_active_endpoint() -> None:
    """The runner swaps a role label for the role's address; the original
    config and the inactive protocol block stay untouched."""
    http = ScenarioConfig.model_validate(
        minimal_scenario(protocol={"type": "http", "http": {"endpoint": {"host": "cloud"}}})
    )
    resolved = http.with_connect_host("20.0.0.1")
    assert resolved.protocol.connect_host == "20.0.0.1"
    assert resolved.protocol.mqtt.endpoint.host == "127.0.0.1"
    assert http.protocol.connect_host == "cloud"

    mqtt = ScenarioConfig.model_validate(
        minimal_scenario(protocol={"type": "mqtt", "mqtt": {"endpoint": {"host": "cloud"}}})
    )
    resolved = mqtt.with_connect_host("20.0.0.1")
    assert resolved.protocol.connect_host == "20.0.0.1"
    assert resolved.protocol.http.endpoint.host == "127.0.0.1"


def test_http_pool_cap_defaults_to_unbounded() -> None:
    config = ScenarioConfig.model_validate(minimal_scenario())
    assert config.protocol.http.client.max_connections == 0
    capped = ScenarioConfig.model_validate(
        minimal_scenario(protocol={"type": "http", "http": {"client": {"max_connections": 100}}})
    )
    assert capped.protocol.http.client.max_connections == 100
    with pytest.raises(ValidationError):
        ScenarioConfig.model_validate(
            minimal_scenario(protocol={"type": "http", "http": {"client": {"max_connections": -1}}})
        )


def test_sender_implementation_defaults_to_python_and_threads_are_a_go_knob() -> None:
    from eccom.core.config import SenderConfig

    assert SenderConfig().implementation == "python" and SenderConfig().threads == 0
    assert SenderConfig(implementation="go", threads=6).threads == 6
    assert SenderConfig(implementation="go").threads == 0  # the runtime's default
    with pytest.raises(ValidationError, match="Go sender only"):
        SenderConfig(threads=4)
    with pytest.raises(ValidationError):
        SenderConfig(implementation="rust")


def test_sender_container_cpus_default_to_the_go_senders_threads() -> None:
    """threads: 6 means six cores for the whole container, not only for the
    goroutines; the Python sender stays uncapped unless cpus says so."""
    from eccom.core.config import SenderConfig

    assert SenderConfig(implementation="go", threads=6).container_cpus == 6.0
    assert SenderConfig(implementation="go").container_cpus is None
    assert SenderConfig().container_cpus is None
    assert SenderConfig(cpus=1.5).container_cpus == 1.5
    assert SenderConfig(implementation="go", threads=6, cpus=4).container_cpus == 4.0
    with pytest.raises(ValidationError):
        SenderConfig(cpus=0)


def test_go_sender_is_one_paced_process() -> None:
    """What the Go sender does not implement fails at load time."""
    go = {"implementation": "go", "threads": 2}
    config = ScenarioConfig.model_validate(minimal_scenario(sender=go))
    assert config.sender.implementation == "go" and config.load.senders == 1
    with pytest.raises(ValidationError, match="sender.threads instead of load.senders"):
        ScenarioConfig.model_validate(
            minimal_scenario(sender=go, load={"messages_per_second": 10, "total_messages": 5, "senders": 2})
        )
    with pytest.raises(ValidationError, match="devices"):
        ScenarioConfig.model_validate(
            minimal_scenario(
                sender=go,
                payload=None,
                load={"mode": "devices", "duration_s": 1},
                devices=[{"name": "d", "type": "generic", "interval_s": 1, "size_bytes": 8}],
            )
        )
    with pytest.raises(ValidationError, match="load.mode=homes"):
        ScenarioConfig.model_validate(home_scenario(sender=go))
    with pytest.raises(ValidationError, match="reconnect_per_message"):
        ScenarioConfig.model_validate(
            minimal_scenario(
                sender=go,
                interaction="one_way",
                protocol={"type": "mqtt", "mqtt": {"client": {"reconnect_per_message": True}}},
            )
        )


def test_nanomq_broker_rejects_the_mosquitto_only_knobs() -> None:
    """NanoMQ keeps no inbound in-flight window and always runs with
    TCP_NODELAY, so the scenario cannot ask for either."""
    nanomq = {"implementation": "nanomq", "max_queued_messages": 0}
    config = ScenarioConfig.model_validate(
        minimal_scenario(protocol={"type": "mqtt", "mqtt": {"broker": nanomq}})
    )
    assert config.protocol.mqtt.broker.implementation == "nanomq"
    assert config.protocol.mqtt.broker.max_inflight_messages is None  # unset: the broker's own
    assert ScenarioConfig.model_validate(minimal_scenario()).protocol.mqtt.broker.implementation == "mosquitto"
    with pytest.raises(ValidationError, match="max_inflight_messages is a mosquitto setting"):
        ScenarioConfig.model_validate(
            minimal_scenario(protocol={"type": "mqtt", "mqtt": {"broker": {**nanomq, "max_inflight_messages": 10000}}})
        )
    with pytest.raises(ValidationError, match="tcp_nodelay: false is not available"):
        ScenarioConfig.model_validate(
            minimal_scenario(protocol={"type": "mqtt", "tcp_nodelay": False, "mqtt": {"broker": nanomq}})
        )
    # an HTTP scenario carries the inactive MQTT block and is not judged by it
    ScenarioConfig.model_validate(
        minimal_scenario(protocol={"type": "http", "tcp_nodelay": False, "mqtt": {"broker": nanomq}})
    )

