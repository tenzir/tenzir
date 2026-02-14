"""Kafka fixture for integration testing.

Provides a Kafka broker instance for testing Kafka operators.

Environment variables yielded:
- KAFKA_HOST: Broker hostname (127.0.0.1)
- KAFKA_PORT: Broker port exposed on host (dynamically allocated)
- KAFKA_BOOTSTRAP_SERVERS: Host:port bootstrap endpoint
- KAFKA_TOPIC: Default topic created for tests
- KAFKA_CONTAINER_ID: Container ID for in-fixture helpers/scripts
- KAFKA_CONTAINER_RUNTIME: Container runtime used (docker/podman)
"""

from __future__ import annotations

import logging
import subprocess
import time
import uuid
from dataclasses import dataclass
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options

from ._utils import find_container_runtime, find_free_port

logger = logging.getLogger(__name__)

KAFKA_IMAGE = "apache/kafka:latest"
STARTUP_TIMEOUT = 120  # seconds
HEALTH_CHECK_INTERVAL = 2  # seconds


@dataclass(frozen=True)
class KafkaOptions:
    topic: str = "tenzir_test"
    partitions: int = 1
    seed_messages: int = 0
    image: str = KAFKA_IMAGE


def _start_kafka(runtime: str, port: int, image: str) -> str:
    """Start Kafka container and return container ID."""
    container_name = f"tenzir-test-kafka-{uuid.uuid4().hex[:8]}"
    cmd = [
        runtime,
        "run",
        "-d",
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{port}:9094",
        "-e",
        "KAFKA_NODE_ID=1",
        "-e",
        "KAFKA_PROCESS_ROLES=broker,controller",
        "-e",
        "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
        "-e",
        "KAFKA_LISTENERS=PLAINTEXT://:9092,EXTERNAL://:9094,CONTROLLER://:9093",
        "-e",
        f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,EXTERNAL://127.0.0.1:{port}",
        "-e",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "-e",
        "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e",
        "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
        "-e",
        "KAFKA_AUTO_CREATE_TOPICS_ENABLE=true",
        "-e",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
        image,
    ]
    logger.info("Starting Kafka container with %s: %s", runtime, " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    container_id = result.stdout.strip()
    logger.info("Kafka container started: %s", container_id[:12])
    return container_id


def _stop_kafka(runtime: str, container_id: str) -> None:
    """Stop and remove Kafka container."""
    logger.info("Stopping Kafka container: %s", container_id[:12])
    subprocess.run(
        [runtime, "stop", container_id],
        capture_output=True,
        check=False,
    )


def _wait_for_kafka(runtime: str, container_id: str, timeout: float) -> bool:
    """Wait for Kafka to become ready."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        running = subprocess.run(
            [runtime, "inspect", "-f", "{{.State.Running}}", container_id],
            capture_output=True,
            text=True,
            check=False,
        )
        if running.returncode != 0 or running.stdout.strip() != "true":
            logger.debug("Kafka container not running yet")
            time.sleep(HEALTH_CHECK_INTERVAL)
            continue
        ready = subprocess.run(
            [
                runtime,
                "exec",
                container_id,
                "/opt/kafka/bin/kafka-topics.sh",
                "--bootstrap-server",
                "localhost:9092",
                "--list",
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        if ready.returncode == 0:
            logger.info("Kafka is ready")
            return True
        logger.debug("Kafka not ready yet: %s", ready.stderr.strip())
        time.sleep(HEALTH_CHECK_INTERVAL)
    return False


def _create_topic(
    runtime: str,
    container_id: str,
    topic: str,
    partitions: int,
) -> None:
    """Create topic for tests."""
    result = subprocess.run(
        [
            runtime,
            "exec",
            container_id,
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            str(partitions),
            "--replication-factor",
            "1",
        ],
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to create topic: {result.stderr}")


def _seed_topic(runtime: str, container_id: str, topic: str, count: int) -> None:
    """Seed a topic with deterministic messages."""
    if count <= 0:
        return
    payload = "".join(f"message-{index:04d}\n" for index in range(1, count + 1))
    result = subprocess.run(
        [
            runtime,
            "exec",
            "-i",
            container_id,
            "/opt/kafka/bin/kafka-console-producer.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--topic",
            topic,
        ],
        input=payload,
        capture_output=True,
        text=True,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to seed topic: {result.stderr}")


@fixture(options=KafkaOptions)
def kafka() -> Iterator[dict[str, str]]:
    """Start Kafka and yield environment variables for broker access."""
    opts = current_options("kafka")
    runtime = find_container_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )
    if opts.partitions < 1:
        raise RuntimeError("kafka fixture option `partitions` must be >= 1")
    if opts.seed_messages < 0:
        raise RuntimeError("kafka fixture option `seed_messages` must be >= 0")
    port = find_free_port()
    container_id = None
    try:
        container_id = _start_kafka(runtime, port, opts.image)
        if not _wait_for_kafka(runtime, container_id, STARTUP_TIMEOUT):
            raise RuntimeError(
                f"Kafka failed to start within {STARTUP_TIMEOUT} seconds"
            )
        _create_topic(runtime, container_id, opts.topic, opts.partitions)
        _seed_topic(runtime, container_id, opts.topic, opts.seed_messages)
        bootstrap = f"127.0.0.1:{port}"
        env: dict[str, str] = {
            "KAFKA_HOST": "127.0.0.1",
            "KAFKA_PORT": str(port),
            "KAFKA_BOOTSTRAP_SERVERS": bootstrap,
            "KAFKA_TOPIC": opts.topic,
            "KAFKA_CONTAINER_ID": container_id,
            "KAFKA_CONTAINER_RUNTIME": runtime,
        }
        yield env
    finally:
        if container_id:
            _stop_kafka(runtime, container_id)
