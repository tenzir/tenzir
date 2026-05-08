"""RabbitMQ fixture for AMQP integration testing.

Starts a containerized RabbitMQ broker with the management plugin enabled.

Environment variables yielded:
- AMQP_URL: AMQP URL for the test user and vhost.
- AMQP_HOST: Broker hostname (127.0.0.1).
- AMQP_PORT: AMQP port exposed on the host (dynamically allocated).
- AMQP_MANAGEMENT_URL: Management API base URL.
- AMQP_USER: Test user.
- AMQP_PASSWORD: Test password.
- AMQP_VHOST: Test virtual host.
- AMQP_CONTAINER_ID: Container ID for in-fixture helpers/scripts.
- AMQP_CONTAINER_RUNTIME: Container runtime used (docker/podman).
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass
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

RABBITMQ_IMAGE = "rabbitmq:4-management"
RABBITMQ_USER = "tenzir"
RABBITMQ_PASSWORD = "tenzir"
RABBITMQ_VHOST = "/"
STARTUP_TIMEOUT = 90
HEALTH_CHECK_INTERVAL = 1


@dataclass(frozen=True)
class AmqpOptions:
    image: str = RABBITMQ_IMAGE


def _start_rabbitmq(
    runtime: RuntimeSpec,
    amqp_port: int,
    management_port: int,
    image: str,
) -> ManagedContainer:
    container_name = f"tenzir-test-amqp-{uuid.uuid4().hex[:8]}"
    run_args = [
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{amqp_port}:5672",
        "-p",
        f"{management_port}:15672",
        "-e",
        f"RABBITMQ_DEFAULT_USER={RABBITMQ_USER}",
        "-e",
        f"RABBITMQ_DEFAULT_PASS={RABBITMQ_PASSWORD}",
        "-e",
        f"RABBITMQ_DEFAULT_VHOST={RABBITMQ_VHOST}",
        image,
    ]
    logger.info("Starting RabbitMQ container with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info("RabbitMQ container started: %s", container.container_id[:12])
    return container


def _stop_rabbitmq(container: ManagedContainer) -> None:
    logger.info("Stopping RabbitMQ container: %s", container.container_id[:12])
    result = container.stop()
    if result.returncode != 0:
        logger.warning(
            "Failed to stop RabbitMQ container %s: %s",
            container.container_id[:12],
            (result.stderr or result.stdout or "").strip() or "no output",
        )


def _management_request(url: str) -> dict[str, object]:
    password_manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    password_manager.add_password(None, url, RABBITMQ_USER, RABBITMQ_PASSWORD)
    opener = urllib.request.build_opener(
        urllib.request.HTTPBasicAuthHandler(password_manager)
    )
    with opener.open(url, timeout=2) as response:
        return json.loads(response.read().decode("utf-8"))


def _wait_for_rabbitmq(management_url: str, timeout: float) -> None:
    def _probe() -> tuple[bool, dict[str, str]]:
        try:
            overview = _management_request(f"{management_url}/api/overview")
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            return False, {"error": str(exc)}
        listeners = overview.get("listeners", [])
        return bool(listeners), {"listeners": str(len(listeners))}

    try:
        wait_until_ready(
            _probe,
            timeout_seconds=timeout,
            poll_interval_seconds=HEALTH_CHECK_INTERVAL,
            timeout_context="RabbitMQ startup",
        )
    except ContainerReadinessTimeout as exc:
        raise RuntimeError(str(exc)) from exc
    logger.info("RabbitMQ is ready")


@fixture(options=AmqpOptions)
def amqp() -> Iterator[dict[str, str]]:
    """Start RabbitMQ and yield environment variables for broker access."""
    opts = current_options("amqp")
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )
    amqp_port = find_free_port()
    management_port = find_free_port()
    management_url = f"http://127.0.0.1:{management_port}"
    container: ManagedContainer | None = None
    try:
        try:
            container = _start_rabbitmq(
                runtime,
                amqp_port,
                management_port,
                opts.image,
            )
        except ContainerCommandError as exc:
            raise FixtureUnavailable(
                f"failed to start RabbitMQ container: {exc}"
            ) from exc
        _wait_for_rabbitmq(management_url, STARTUP_TIMEOUT)
        quoted_vhost = urllib.parse.quote(RABBITMQ_VHOST, safe="")
        env: dict[str, str] = {
            "AMQP_URL": (
                f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}"
                f"@127.0.0.1:{amqp_port}/{quoted_vhost}"
            ),
            "AMQP_HOST": "127.0.0.1",
            "AMQP_PORT": str(amqp_port),
            "AMQP_MANAGEMENT_URL": management_url,
            "AMQP_USER": RABBITMQ_USER,
            "AMQP_PASSWORD": RABBITMQ_PASSWORD,
            "AMQP_VHOST": RABBITMQ_VHOST,
            "AMQP_CONTAINER_ID": container.container_id,
            "AMQP_CONTAINER_RUNTIME": runtime.binary,
        }
        yield env
    finally:
        if container is not None:
            _stop_rabbitmq(container)
