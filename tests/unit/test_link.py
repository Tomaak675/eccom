"""Link-capacity probe: command wrapping, retries, parsing and the summary line."""

import json

import pytest

from eccom.analysis.run import format_summary
from eccom.orchestration.deployment import Deployment
from eccom.orchestration.link import LinkCapacityProbe, parse_iperf3


def iperf3_json(mbit_s: float, retransmits: int = 3, rtt_us: int = 24_000) -> str:
    """A minimal iperf3 -J client document reporting ``mbit_s`` of goodput."""
    bits = mbit_s * 1e6
    return json.dumps(
        {
            "end": {
                "sum_sent": {
                    "bytes": int(bits / 8 * 5), "seconds": 5.0,
                    "bits_per_second": bits * 1.02, "retransmits": retransmits,
                },
                "sum_received": {"bytes": int(bits / 8 * 5), "seconds": 5.0, "bits_per_second": bits},
                "streams": [{"sender": {"mean_rtt": rtt_us}}],
            }
        }
    )


class FakeDeployment(Deployment):
    """Records commands; answers each with the next canned (exit code, output)."""

    def __init__(self, image: str | None, replies: list[tuple[int, str]] | None = None) -> None:
        self.image = image
        self.commands: list[list[str]] = []
        self.replies = list(replies or [])

    def container_image(self) -> str | None:
        return self.image

    async def start_agent(self, role, scenario_path, run_dir, extra_args=(), instance=None):
        raise NotImplementedError

    async def run_command(self, args: list[str]) -> tuple[int, str]:
        self.commands.append(args)
        return self.replies.pop(0) if self.replies else (0, "")


def test_parse_iperf3_extracts_goodput_retransmits_and_rtt() -> None:
    parsed = parse_iperf3("docker noise before the document\n" + iperf3_json(23.5))
    assert parsed["mbit_s"] == 23.5
    assert parsed["mbit_s_sent"] == pytest.approx(23.5 * 1.02)
    assert parsed["retransmits"] == 3
    assert parsed["mean_rtt_ms"] == 24.0
    with pytest.raises(RuntimeError, match="unable to connect"):
        parse_iperf3('{"error": "unable to connect to server: Connection refused"}')
    with pytest.raises(RuntimeError, match="no iperf3 JSON"):
        parse_iperf3("iperf3: command not found")


async def test_probe_runs_server_then_client_both_ways_inside_the_image() -> None:
    server = FakeDeployment("eccom-agent")
    client = FakeDeployment("eccom-agent", [(0, iperf3_json(23.5)), (0, iperf3_json(87.0))])
    probe = LinkCapacityProbe(server, client, "203.0.113.10", 8080, 5, "run-1")
    probe.retry_delay_s = 0
    record = await probe.measure()

    assert record["method"] == "iperf3" and record["port"] == 8080
    assert record["directions"]["client_to_server"]["mbit_s"] == 23.5
    assert record["directions"]["server_to_client"]["mbit_s"] == 87.0
    assert record["directions"]["server_to_client"]["reverse"] is True
    # a one-off iperf3 server from the agent image on the host network, stale one removed first
    assert server.commands[0] == ["docker", "rm", "-f", "eccom-iperf-server-run-1"]
    assert server.commands[1][:9] == [
        "docker", "run", "--rm", "--network", "host", "--name", "eccom-iperf-server-run-1",
        "--entrypoint", "iperf3",
    ]
    assert server.commands[1][-5:] == ["eccom-agent", "-s", "-1", "-p", "8080"]
    forward, reverse = client.commands
    assert forward[-7:] == ["-c", "203.0.113.10", "-p", "8080", "-t", "5", "-J"]
    assert reverse[-1] == "-R"


async def test_probe_retries_until_the_server_listens_and_records_failures() -> None:
    server = FakeDeployment(None)
    client = FakeDeployment(
        None,
        [
            (1, '{"error": "unable to connect to server: Connection refused"}'),
            (0, iperf3_json(10.0)),
            (127, "iperf3: command not found"),
        ],
    )
    probe = LinkCapacityProbe(server, client, "127.0.0.1", 5201, 2, "run-2")
    probe.retry_delay_s = 0
    record = await probe.measure()

    assert record["directions"]["client_to_server"]["mbit_s"] == 10.0
    assert "command not found" in record["directions"]["server_to_client"]["error"]
    # bare host commands on a deployment without a container image
    assert server.commands[0] == ["iperf3", "-s", "-1", "-p", "5201"]
    assert len(client.commands) == 3


def test_summary_prints_the_link_line() -> None:
    summary = {
        "scenario": "s3_http",
        "counts": {
            "messages_attempted": 1, "messages_completed": 1, "messages_failed": 0,
            "timeouts": 0, "errors": 0, "received_by_receiver": 1,
        },
        "rates": {"requested_message_rate": 10.0, "actual_send_rate_from_events": 10.0},
        "link_capacity": {
            "method": "iperf3", "port": 8080, "duration_s": 5.0,
            "sender_to_receiver_mbit_s": 23.5, "receiver_to_sender_mbit_s": 87.0,
            "retransmits": 3, "mean_rtt_ms": 24.0, "errors": None,
        },
    }
    assert (
        "link: 23.5 Mbit/s sender->receiver, 87.0 Mbit/s receiver->sender "
        "(iperf3 5.0 s on port 8080, 3 retransmits, rtt 24.0 ms)"
    ) in format_summary(summary)
