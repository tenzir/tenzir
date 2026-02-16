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
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options
from tenzir_test.fixtures.container_runtime import (
    ContainerCommandError,
    ContainerReadinessTimeout,
    ManagedContainer,
    RuntimeSpec,
    detect_runtime,
    start_detached,
    wait_until_ready,
)

from ._utils import find_free_port

logger = logging.getLogger(__name__)

KAFKA_IMAGE = "apache/kafka:latest"
STARTUP_TIMEOUT = 120  # seconds
HEALTH_CHECK_INTERVAL = 2  # seconds
ALLOWED_COMPRESSION_TYPES = {"none", "gzip", "snappy", "lz4", "zstd"}


@dataclass(frozen=True)
class KafkaOptions:
    topic: str = "tenzir_test"
    partitions: int = 1
    messages: int = 0
    compression: str = "none"
    payload_file: str = ""
    image: str = KAFKA_IMAGE


def _start_kafka(
    runtime: RuntimeSpec,
    port: int,
    image: str,
    payload_file: str,
) -> ManagedContainer:
    """Start Kafka container and return a managed container."""
    container_name = f"tenzir-test-kafka-{uuid.uuid4().hex[:8]}"
    run_args = [
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
    ]
    if payload_file:
        run_args.extend(
            [
                "-v",
                f"{payload_file}:/tmp/tenzir-payload.log:ro",
            ]
        )
    run_args.append(image)
    logger.info("Starting Kafka container with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info("Kafka container started: %s", container.container_id[:12])
    return container


def _stop_kafka(container: ManagedContainer) -> None:
    """Stop and remove Kafka container."""
    logger.info("Stopping Kafka container: %s", container.container_id[:12])
    result = container.stop()
    if result.returncode != 0:
        logger.warning(
            "Failed to stop Kafka container %s: %s",
            container.container_id[:12],
            (result.stderr or result.stdout or "").strip() or "no output",
        )


def _wait_for_kafka(container: ManagedContainer, timeout: float) -> None:
    """Wait for Kafka to become ready."""
    def _probe() -> tuple[bool, dict[str, str]]:
        if not container.is_running():
            logger.debug("Kafka container not running yet")
            return False, {"running": "false"}
        try:
            result = container.exec(
                [
                    "/opt/kafka/bin/kafka-topics.sh",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--list",
                ]
            )
        except ContainerCommandError as exc:
            logger.debug("Kafka readiness probe command failed: %s", exc)
            return False, {"running": "true", "probe_error": str(exc)}
        if result.returncode == 0:
            return True, {"running": "true", "probe_returncode": "0"}
        stderr = result.stderr.strip()
        logger.debug("Kafka not ready yet: %s", stderr)
        return False, {
            "running": "true",
            "probe_returncode": str(result.returncode),
            "probe_stderr": stderr,
        }

    try:
        wait_until_ready(
            _probe,
            timeout_seconds=timeout,
            poll_interval_seconds=HEALTH_CHECK_INTERVAL,
            timeout_context="Kafka startup",
        )
    except ContainerReadinessTimeout as exc:
        raise RuntimeError(str(exc)) from exc
    logger.info("Kafka is ready")


def _create_topic(container: ManagedContainer, topic: str, partitions: int) -> None:
    """Create topic for tests."""
    result = container.exec(
        [
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
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to create topic: {result.stderr}")


def _seed_topic_with_console_producer(
    container: ManagedContainer,
    topic: str,
    count: int,
    compression: str,
) -> None:
    """Seed a topic with deterministic messages via kafka-console-producer."""
    cmd = [
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        topic,
    ]
    if compression != "none":
        cmd.extend(["--producer-property", f"compression.type={compression}"])
    payload = "".join(f"message-{index:04d}\n" for index in range(1, count + 1))
    result = container.exec(cmd, input=payload)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to seed topic: {result.stderr}")


def _seed_topic_with_perf_producer(
    container: ManagedContainer,
    topic: str,
    count: int,
    compression: str,
) -> None:
    """Seed a topic quickly from a payload sample via producer-perf-test."""
    cmd = [
        "/opt/kafka/bin/kafka-producer-perf-test.sh",
        "--topic",
        topic,
        "--num-records",
        str(count),
        "--payload-file",
        "/tmp/tenzir-payload.log",
    ]
    cmd.extend(
        [
            "--throughput",
            "-1",
            "--producer-props",
            "bootstrap.servers=localhost:9092",
            "acks=1",
            "linger.ms=20",
            "batch.size=262144",
            f"compression.type={compression}",
        ]
    )
    result = container.exec(cmd)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to seed topic: {result.stderr}")


def _seed_topic(
    container: ManagedContainer,
    topic: str,
    messages: int,
    compression: str,
    payload_file: str,
) -> None:
    """Seed a topic from a sample file or deterministic fallback messages."""
    if messages <= 0:
        return
    if payload_file:
        _seed_topic_with_perf_producer(
            container,
            topic,
            messages,
            compression,
        )
    else:
        _seed_topic_with_console_producer(container, topic, messages, compression)


@fixture(options=KafkaOptions)
def kafka() -> Iterator[dict[str, str]]:
    """Start Kafka and yield environment variables for broker access."""
    opts = current_options("kafka")
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )
    if opts.partitions < 1:
        raise RuntimeError("kafka fixture option `partitions` must be >= 1")
    if opts.messages < 0:
        raise RuntimeError("kafka fixture option `messages` must be >= 0")
    compression = opts.compression.lower()
    if compression not in ALLOWED_COMPRESSION_TYPES:
        allowed = ", ".join(sorted(ALLOWED_COMPRESSION_TYPES))
        raise RuntimeError(
            f"kafka fixture option `compression` must be one of: {allowed}"
        )
    if opts.payload_file:
        payload_path = Path(opts.payload_file)
        if not payload_path.is_file():
            raise RuntimeError(
                "kafka fixture option `payload_file` must reference a file"
            )
        payload_file = str(payload_path.resolve())
    else:
        payload_file = ""
    port = find_free_port()
    container: ManagedContainer | None = None
    try:
        container = _start_kafka(runtime, port, opts.image, payload_file)
        _wait_for_kafka(container, STARTUP_TIMEOUT)
        _create_topic(container, opts.topic, opts.partitions)
        _seed_topic(
            container,
            opts.topic,
            opts.messages,
            compression,
            payload_file,
        )
        bootstrap = f"127.0.0.1:{port}"
        env: dict[str, str] = {
            "KAFKA_HOST": "127.0.0.1",
            "KAFKA_PORT": str(port),
            "KAFKA_BOOTSTRAP_SERVERS": bootstrap,
            "KAFKA_TOPIC": opts.topic,
            "KAFKA_CONTAINER_ID": container.container_id,
            "KAFKA_CONTAINER_RUNTIME": runtime.binary,
        }
        yield env
    finally:
        if container is not None:
            _stop_kafka(container)
