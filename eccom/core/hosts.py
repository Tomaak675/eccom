"""Host/infrastructure configuration: where each topology role runs.

Kept separate from the scenario so experiment definitions and infrastructure
mix and match freely::

    eccom run scenarios/http_baseline.yaml --hosts hosts/remote.yaml

Each entry maps a topology label ("edge", "cloud", ...) to a deployment.
Omitting --hosts runs every role as a local subprocess.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal, Self

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class ContainerResources(_StrictModel):
    """Docker resource caps for every container the framework starts on a host
    (the agent and, if it runs there, the managed broker).

    Meant for small receivers: without a cap a runaway agent can take every
    core and all memory of a small host, and once sshd and dockerd starve the
    orchestrator can neither stop the container nor collect artifacts — the
    whole suite hangs behind one dead host. With a cap the container is
    throttled or OOM-killed instead. Kernel socket buffers are charged to the
    container's cgroup too, so tens of thousands of TCP connections count
    against ``memory``. Swap is disabled for the container (``--memory-swap``
    equals ``--memory``) so it fails fast instead of thrashing.
    """

    cpus: float | None = Field(default=None, gt=0)  # docker --cpus (fractional cores)
    memory: str | None = Field(default=None, pattern=r"^[1-9]\d*(\.\d+)?[bkmgBKMG]?$")  # e.g. 750m

    def docker_flags(self) -> list[str]:
        flags: list[str] = []
        if self.cpus is not None:
            flags += ["--cpus", f"{self.cpus:g}"]
        if self.memory is not None:
            flags += ["--memory", self.memory, "--memory-swap", self.memory]
        return flags


class SshConfig(_StrictModel):
    """A host reached over SSH; agents run there as Docker containers.

    The target needs only Docker and SSH — no repo clone, no Python setup:
    the deployment syncs the source tree to <working_dir>/src, builds the
    agent image natively on the host, and bind-mounts <working_dir>/runs for
    artifacts.
    """

    host: str
    port: int = Field(default=22, ge=1, le=65535)
    user: str
    # Prefer key_file; password auth is supported for convenience but the
    # value is redacted in run artifacts and should not live in committed YAML.
    key_file: Path | None = None
    password: str | None = None
    # Remote scratch directory (created automatically) holding src/ and runs/.
    working_dir: str
    # Tag for the agent image built on this host.
    image: str = "eccom-agent"
    # Optional caps applied to every container started on this host.
    resources: ContainerResources | None = None

    @model_validator(mode="after")
    def _credentials_present(self) -> Self:
        if self.key_file is None and self.password is None:
            raise ValueError("ssh hosts need key_file or password (key_file preferred)")
        return self


class HostConfig(_StrictModel):
    deployment: Literal["local", "ssh"] = "local"
    ssh: SshConfig | None = None

    @property
    def address(self) -> str:
        """The host's address as seen from the orchestrator machine."""
        return self.ssh.host if self.ssh is not None else "127.0.0.1"

    @model_validator(mode="after")
    def _ssh_block_matches_type(self) -> Self:
        if self.deployment == "ssh" and self.ssh is None:
            raise ValueError("deployment=ssh requires an ssh block")
        if self.deployment == "local" and self.ssh is not None:
            raise ValueError("deployment=local must not have an ssh block")
        return self


class BrokerConfig(_StrictModel):
    """How the message broker (needed by e.g. MQTT scenarios) is provided.

    managed=True: the runner starts a Docker container on the machine of the
    ``run_on`` role (default: the receiver's host) before the run and removes
    it afterwards. managed=False: an already-running external broker is used
    and the framework does not touch it. The broker listens on the port from
    the scenario's protocol endpoint. Which broker runs (mosquitto or NanoMQ)
    is the scenario's choice, ``protocol.mqtt.broker.implementation``; the
    hosts file may only pin a different image for it. (Named run_on, not
    "on": bare ``on`` is a boolean literal in YAML 1.1.)
    """

    run_on: str | None = None  # topology role label; None = the receiver's host
    managed: bool = True
    # Unset = the implementation's default (eccom.orchestration.broker.DEFAULT_IMAGES).
    docker_image: str | None = None
    container_name: str = "eccom-mqtt-broker"


class HostsConfig(_StrictModel):
    hosts: dict[str, HostConfig]
    broker: BrokerConfig = Field(default_factory=BrokerConfig)

    def for_role(self, label: str) -> HostConfig:
        try:
            return self.hosts[label]
        except KeyError:
            raise ValueError(
                f"topology role {label!r} has no entry in the hosts configuration "
                f"(defined hosts: {sorted(self.hosts)})"
            ) from None

    def resolve_address(self, host: str) -> str:
        """Resolve a scenario's endpoint host against this mapping.

        host: a topology role label ("cloud") or a literal hostname / IP.
        Returns the role's address as seen from the orchestrator, or ``host`` unchanged.
        """
        if host in self.hosts:
            return self.hosts[host].address
        return host

    def redacted_dump(self) -> dict[str, Any]:
        """model_dump with secrets masked, for storing in run artifacts."""
        data = self.model_dump(mode="json")
        for host in data["hosts"].values():
            if host.get("ssh") and host["ssh"].get("password"):
                host["ssh"]["password"] = "***"
        return data


def load_hosts(path: Path) -> HostsConfig:
    with path.open(encoding="utf-8") as fh:
        raw = yaml.safe_load(fh)
    if not isinstance(raw, dict):
        raise ValueError(f"hosts file {path} must contain a YAML mapping")
    return HostsConfig.model_validate(raw)


def local_hosts(*labels: str) -> HostsConfig:
    """Default mapping: every role runs as a local subprocess."""
    return HostsConfig(hosts={label: HostConfig() for label in labels})
