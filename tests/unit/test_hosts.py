from pathlib import Path

import pytest
from pydantic import ValidationError

from eccom.core.hosts import HostsConfig, load_hosts, local_hosts

HOSTS_DIR = Path(__file__).parents[2] / "hosts"


def test_shipped_hosts_examples_are_valid() -> None:
    local = load_hosts(HOSTS_DIR / "local.yaml")
    assert local.for_role("edge").deployment == "local"

    remote = load_hosts(HOSTS_DIR / "remote.example.yaml")
    edge = remote.for_role("edge")
    assert edge.deployment == "ssh"
    assert edge.ssh is not None
    assert edge.ssh.user == "edge"
    assert edge.ssh.working_dir == "/home/edge/eccom-runtime"
    assert edge.ssh.image == "eccom-agent"


def test_local_hosts_default() -> None:
    hosts = local_hosts("edge", "cloud")
    assert hosts.for_role("cloud").deployment == "local"


def test_unknown_role_raises_helpful_error() -> None:
    with pytest.raises(ValueError, match="no entry in the hosts configuration"):
        local_hosts("edge").for_role("cloud")


def test_ssh_deployment_requires_ssh_block() -> None:
    with pytest.raises(ValidationError, match="requires an ssh block"):
        HostsConfig.model_validate({"hosts": {"edge": {"deployment": "ssh"}}})


def test_local_deployment_rejects_ssh_block() -> None:
    with pytest.raises(ValidationError, match="must not have an ssh block"):
        HostsConfig.model_validate(
            {
                "hosts": {
                    "edge": {
                        "deployment": "local",
                        "ssh": {"host": "h", "user": "u", "working_dir": "/w", "password": "x"},
                    }
                }
            }
        )


def test_ssh_requires_key_or_password() -> None:
    with pytest.raises(ValidationError, match="key_file or password"):
        HostsConfig.model_validate(
            {
                "hosts": {
                    "edge": {
                        "deployment": "ssh",
                        "ssh": {"host": "h", "user": "u", "working_dir": "/w"},
                    }
                }
            }
        )


def test_broker_defaults_to_managed_mosquitto() -> None:
    hosts = local_hosts("edge", "cloud")
    assert hosts.broker.managed
    assert hosts.broker.run_on is None  # runner defaults to the receiver's host
    assert hosts.broker.docker_image is None  # the implementation's default image


def test_broker_section_is_parsed() -> None:
    hosts = HostsConfig.model_validate(
        {
            "hosts": {"edge": {}, "cloud": {}},
            "broker": {"run_on": "edge", "managed": False},
        }
    )
    assert hosts.broker.run_on == "edge"
    assert not hosts.broker.managed


def test_host_address_for_control_endpoint() -> None:
    hosts = HostsConfig.model_validate(
        {
            "hosts": {
                "edge": {"deployment": "local"},
                "cloud": {
                    "deployment": "ssh",
                    "ssh": {"host": "20.0.0.1", "user": "u", "working_dir": "/w", "password": "x"},
                },
            }
        }
    )
    assert hosts.for_role("edge").address == "127.0.0.1"
    assert hosts.for_role("cloud").address == "20.0.0.1"


def test_password_is_redacted_in_dump() -> None:
    hosts = HostsConfig.model_validate(
        {
            "hosts": {
                "edge": {
                    "deployment": "ssh",
                    "ssh": {"host": "h", "user": "u", "working_dir": "/w", "password": "secret"},
                },
                "cloud": {"deployment": "local"},
            }
        }
    )
    dump = hosts.redacted_dump()
    assert dump["hosts"]["edge"]["ssh"]["password"] == "***"
    assert "secret" not in str(dump)


def test_container_resources_become_docker_caps() -> None:
    """Per-host caps protect a small host's sshd/dockerd from a runaway agent:
    memory caps disable swap for the container so it OOMs instead of thrashing."""
    from eccom.core.hosts import ContainerResources, HostsConfig

    hosts = HostsConfig.model_validate(
        {
            "hosts": {
                "cloud": {
                    "deployment": "ssh",
                    "ssh": {
                        "host": "h",
                        "user": "u",
                        "password": "p",
                        "working_dir": "/tmp/e",
                        "resources": {"cpus": 1.5, "memory": "750m"},
                    },
                },
                "edge": {
                    "deployment": "ssh",
                    "ssh": {"host": "h2", "user": "u", "password": "p", "working_dir": "/tmp/e"},
                },
            }
        }
    )
    assert hosts.hosts["cloud"].ssh.resources.docker_flags() == [
        "--cpus", "1.5", "--memory", "750m", "--memory-swap", "750m",
    ]
    assert hosts.hosts["edge"].ssh.resources is None  # no block, no flags
    assert ContainerResources(cpus=2).docker_flags() == ["--cpus", "2"]
    assert ContainerResources().docker_flags() == []
    assert hosts.redacted_dump()["hosts"]["cloud"]["ssh"]["resources"] == {"cpus": 1.5, "memory": "750m"}


@pytest.mark.parametrize("bad", [{"memory": "lots"}, {"memory": "0m"}, {"cpus": 0}, {"cpus": -1}])
def test_container_resources_reject_nonsense(bad: dict) -> None:
    from eccom.core.hosts import ContainerResources

    with pytest.raises(ValidationError):
        ContainerResources.model_validate(bad)


def test_resolve_address_maps_role_labels_and_keeps_literals() -> None:
    """A scenario may name a topology role as its endpoint host; only the
    hosts mapping knows that role's address, and literals pass through."""
    hosts = HostsConfig.model_validate(
        {
            "hosts": {
                "edge": {"deployment": "local"},
                "cloud": {
                    "deployment": "ssh",
                    "ssh": {"host": "20.0.0.1", "user": "u", "working_dir": "/w", "password": "x"},
                },
            }
        }
    )
    assert hosts.resolve_address("cloud") == "20.0.0.1"
    assert hosts.resolve_address("edge") == "127.0.0.1"
    assert hosts.resolve_address("broker.example.org") == "broker.example.org"
    assert local_hosts("edge", "cloud").resolve_address("cloud") == "127.0.0.1"
