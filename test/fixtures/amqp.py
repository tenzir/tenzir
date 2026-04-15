"""AMQP (RabbitMQ) fixture for integration testing.

Provides a containerized RabbitMQ broker.

Environment variables yielded:
- AMQP_URL: Full AMQP connection URL (amqp://guest:guest@127.0.0.1:PORT/)
- AMQP_HOST: Broker hostname (127.0.0.1)
- AMQP_PORT: Broker port exposed on host (dynamically allocated)
"""

from __future__ import annotations

import logging
import shutil
import subprocess
import time
import uuid
from dataclasses import dataclass
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options

from ._utils import find_free_port

logger = logging.getLogger(__name__)

RABBITMQ_IMAGE = "rabbitmq:4-management"
RABBITMQ_STARTUP_TIMEOUT = 90  # seconds
RABBITMQ_HEALTH_CHECK_INTERVAL = 2  # seconds


@dataclass(frozen=True)
class AmqpOptions:
    image: str = RABBITMQ_IMAGE
    queues: tuple[str, ...] = ()


def _find_runtime() -> str:
    """Return the first available container runtime binary."""
    for candidate in ("podman", "docker"):
        if shutil.which(candidate) is not None:
            return candidate
    raise FixtureUnavailable("container runtime (docker/podman) required but not found")


def _run(cmd: list[str], **kwargs: object) -> subprocess.CompletedProcess[str]:
    logger.debug("exec: %s", " ".join(cmd))
    return subprocess.run(cmd, capture_output=True, text=True, **kwargs)


@fixture(options=AmqpOptions)
def amqp() -> Iterator[dict[str, str]]:
    """Start RabbitMQ and yield environment variables for broker access."""
    opts = current_options("amqp")
    runtime = _find_runtime()
    port = find_free_port()
    container_name = f"tenzir-test-amqp-{uuid.uuid4().hex[:8]}"
    container_id: str | None = None

    try:
        # Start the container.
        result = _run(
            [
                runtime,
                "run",
                "-d",
                "--rm",
                "--name",
                container_name,
                "-p",
                f"{port}:5672",
                opts.image,
            ]
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"failed to start RabbitMQ container: {result.stderr.strip()}"
            )
        container_id = result.stdout.strip()
        logger.info("RabbitMQ container started: %s", container_id[:12])

        # Wait for readiness.
        deadline = time.monotonic() + RABBITMQ_STARTUP_TIMEOUT
        ready = False
        while time.monotonic() < deadline:
            time.sleep(RABBITMQ_HEALTH_CHECK_INTERVAL)
            probe = _run(
                [
                    runtime,
                    "exec",
                    container_id,
                    "rabbitmqctl",
                    "await_startup",
                ]
            )
            if probe.returncode == 0:
                ready = True
                break
            # Check if the container is still alive.
            inspect = _run(
                [
                    runtime,
                    "inspect",
                    "-f",
                    "{{.State.Running}}",
                    container_id,
                ]
            )
            if inspect.stdout.strip().lower() != "true":
                logs = _run([runtime, "logs", "--tail", "30", container_id])
                raise RuntimeError(
                    f"RabbitMQ container exited prematurely:\n"
                    f"{logs.stderr or logs.stdout or '(no logs)'}"
                )
        if not ready:
            raise RuntimeError(
                f"RabbitMQ did not become ready within {RABBITMQ_STARTUP_TIMEOUT}s"
            )
        logger.info("RabbitMQ is ready")

        # Pre-create durable queues with bindings to the default direct
        # exchange so that messages published before a consumer starts are
        # retained.
        for queue_name in opts.queues:
            declare = _run(
                [
                    runtime,
                    "exec",
                    container_id,
                    "rabbitmqadmin",
                    "declare",
                    "queue",
                    "--name",
                    queue_name,
                    "--durable",
                    "true",
                ]
            )
            if declare.returncode != 0:
                raise RuntimeError(
                    f"failed to declare queue '{queue_name}': {declare.stderr.strip()}"
                )
            bind = _run(
                [
                    runtime,
                    "exec",
                    container_id,
                    "rabbitmqadmin",
                    "declare",
                    "binding",
                    "--source",
                    "amq.direct",
                    "--destination-type",
                    "queue",
                    "--destination",
                    queue_name,
                    "--routing-key",
                    queue_name,
                ]
            )
            if bind.returncode != 0:
                raise RuntimeError(
                    f"failed to bind queue '{queue_name}': {bind.stderr.strip()}"
                )
            logger.info("Declared and bound queue: %s", queue_name)

        url = f"amqp://guest:guest@127.0.0.1:{port}"
        yield {
            "AMQP_URL": url,
            "AMQP_HOST": "127.0.0.1",
            "AMQP_PORT": str(port),
        }

    finally:
        if container_id is not None:
            logger.info("Stopping RabbitMQ container: %s", container_id[:12])
            _run([runtime, "stop", container_id])
