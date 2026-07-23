# runner: python

from __future__ import annotations

import json
import os
import socket
import subprocess
import tempfile
import textwrap
import time
import uuid
from pathlib import Path

NUM_MESSAGES = 10
GROUP_TIMEOUT = 20


def free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def wait_for_node(
    process: subprocess.Popen[bytes], endpoint: tuple[str, int]
) -> None:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise RuntimeError(
                "node exited before accepting connections\n"
                f"stdout:\n{stdout.decode()}\n"
                f"stderr:\n{stderr.decode()}"
            )
        try:
            with socket.create_connection(endpoint, timeout=0.1):
                return
        except OSError:
            time.sleep(0.05)
    raise RuntimeError("node did not accept connections")


def stop(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=20)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait()


def kafka_command(*args: str, input: bytes | None = None) -> str:
    result = subprocess.run(
        [
            container_runtime,
            "exec",
            "--interactive",
            kafka_container,
            *args,
        ],
        input=input,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=10,
        check=True,
    )
    return result.stdout.decode()


def group_state(group: str) -> str:
    output = kafka_command(
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--describe",
        "--group",
        group,
        "--state",
    )
    for line in output.splitlines():
        fields = line.split()
        if fields and fields[0] == group and len(fields) >= 4:
            return fields[-2]
    return ""


def wait_for_stable_group(group: str) -> None:
    deadline = time.monotonic() + GROUP_TIMEOUT
    state = ""
    while time.monotonic() < deadline:
        state = group_state(group)
        if state == "Stable":
            return
        time.sleep(0.2)
    raise RuntimeError(f"consumer group {group!r} did not become Stable: {state}")


def committed_offset(group: str, topic: str) -> int | None:
    output = kafka_command(
        "/opt/kafka/bin/kafka-consumer-groups.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--describe",
        "--group",
        group,
        "--offsets",
    )
    for line in output.splitlines():
        fields = line.split()
        if len(fields) < 6 or fields[0] != group or fields[1] != topic:
            continue
        if fields[3] == "-":
            return None
        return int(fields[3])
    return None


def wait_for_offset(group: str, topic: str, expected: int) -> int | None:
    deadline = time.monotonic() + GROUP_TIMEOUT
    offset = None
    while time.monotonic() < deadline:
        offset = committed_offset(group, topic)
        if offset == expected:
            return offset
        time.sleep(0.2)
    return offset


if not hasattr(os, "sched_getaffinity"):
    print("skip: sched_getaffinity is unavailable")
    raise SystemExit(0)

available_cpus = os.sched_getaffinity(0)
if not available_cpus:
    print("skip: process has no available CPUs")
    raise SystemExit(0)
cpu = min(available_cpus)

tenzir = Path(os.environ["TENZIR_PYTHON_FIXTURE_BINARY"])
tenzir_node = tenzir.with_name("tenzir-node")
bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
topic = os.environ["KAFKA_TOPIC"]
kafka_container = os.environ["KAFKA_CONTAINER_ID"]
container_runtime = os.environ["KAFKA_CONTAINER_RUNTIME"]
group_suffix = uuid.uuid4().hex
blocker_group = f"configured-fairness-blocker-{group_suffix}"
target_group = f"configured-fairness-target-{group_suffix}"
cli_group = f"configured-fairness-cli-{group_suffix}"
port = free_port()

target_pipeline = textwrap.dedent(
    f"""\
    from_kafka {json.dumps(topic)},
      options={{
        "bootstrap.servers": {json.dumps(bootstrap_servers)},
        "group.id": {{group_id}}
      }}
    where message != null
    this = message.parse_json()
    publish "configured-fairness-output"
    """
)

with tempfile.TemporaryDirectory(
    prefix="tenzir-from-kafka-configured-fairness-"
) as tmp:
    root = Path(tmp)
    package_dir = root / "packages" / "configured_pipeline_fairness"
    package_dir.mkdir(parents=True)
    package = textwrap.dedent(
        f"""\
        id: configured_pipeline_fairness
        name: Configured Kafka pipeline fairness

        pipelines:
          blocker:
            definition: |
              from_kafka {json.dumps(topic)},
                options={{
                  "bootstrap.servers": {json.dumps(bootstrap_servers)},
                  "group.id": {json.dumps(blocker_group)}
                }}
              discard

          target:
            definition: |
        """
    )
    package += textwrap.indent(
        target_pipeline.format(group_id=json.dumps(target_group)), "      "
    )
    (package_dir / "package.yaml").write_text(package)
    config = textwrap.dedent(
        f"""\
        tenzir:
          endpoint: 127.0.0.1:{port}
          package-dirs:
            - {root / "packages"}
        """
    )
    config_path = root / "tenzir-node.yaml"
    config_path.write_text(config)

    def pin_to_one_cpu() -> None:
        os.sched_setaffinity(0, {cpu})

    node = subprocess.Popen(
        [
            tenzir_node,
            f"--config={config_path}",
            f"--state-directory={root / 'state'}",
            f"--cache-directory={root / 'cache'}",
            "--console-verbosity=warning",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        preexec_fn=pin_to_one_cpu,
    )
    cli = None
    try:
        wait_for_node(node, ("127.0.0.1", port))
        cli = subprocess.Popen(
            [
                tenzir,
                "--console-verbosity=warning",
                target_pipeline.format(group_id=json.dumps(cli_group)),
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            preexec_fn=pin_to_one_cpu,
        )
        for group in (blocker_group, target_group, cli_group):
            wait_for_stable_group(group)

        messages = "".join(
            f'{{"strict_ab":true,"record":{record}}}\n'
            for record in range(NUM_MESSAGES)
        ).encode()
        kafka_command(
            "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--topic",
            topic,
            input=messages,
        )

        offsets = {
            group: wait_for_offset(group, topic, NUM_MESSAGES)
            for group in (blocker_group, target_group, cli_group)
        }
        assert offsets == {
            blocker_group: NUM_MESSAGES,
            target_group: NUM_MESSAGES,
            cli_group: NUM_MESSAGES,
        }, (
            "the configured and CLI Kafka pipelines did not all consume the "
            f"same records: {offsets}"
        )
    finally:
        if cli is not None:
            stop(cli)
        stop(node)
