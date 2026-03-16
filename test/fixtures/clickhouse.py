"""ClickHouse fixture for to_clickhouse integration testing.

Provides a ClickHouse server instance with password authentication.

Environment variables yielded:
- CLICKHOUSE_HOST: Server hostname (127.0.0.1)
- CLICKHOUSE_PORT: Native protocol port (dynamically allocated)
- CLICKHOUSE_PASSWORD: Password for the default user
- CLICKHOUSE_CONTAINER_ID: Container ID for exec operations
- CLICKHOUSE_CONTAINER_RUNTIME: Container runtime binary
"""

from __future__ import annotations

import logging
import uuid
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable
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

CLICKHOUSE_IMAGE = "docker.io/clickhouse/clickhouse-server:latest"
CLICKHOUSE_PASSWORD = "tenzir"
STARTUP_TIMEOUT = 60
HEALTH_CHECK_INTERVAL = 2


def _start_clickhouse(runtime: RuntimeSpec, port: int) -> ManagedContainer:
    """Start ClickHouse container and return a managed container."""
    container_name = f"tenzir-test-clickhouse-{uuid.uuid4().hex[:8]}"
    run_args = [
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{port}:9000",
        "-e",
        f"CLICKHOUSE_PASSWORD={CLICKHOUSE_PASSWORD}",
        CLICKHOUSE_IMAGE,
    ]
    logger.info("Starting ClickHouse container with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info("ClickHouse container started: %s", container.container_id[:12])
    return container


def _stop_clickhouse(container: ManagedContainer) -> None:
    """Stop and remove ClickHouse container."""
    logger.info("Stopping ClickHouse container: %s", container.container_id[:12])
    result = container.stop()
    if result.returncode != 0:
        logger.warning(
            "Failed to stop ClickHouse container %s: %s",
            container.container_id[:12],
            (result.stderr or result.stdout or "").strip() or "no output",
        )


def _wait_for_clickhouse(container: ManagedContainer, timeout: float) -> None:
    """Wait for ClickHouse to become ready."""

    def _probe() -> tuple[bool, dict[str, str]]:
        running = container.is_running()
        if not running:
            logger.debug("Container not running yet")
            return False, {"running": "false"}
        try:
            result = container.exec(
                [
                    "clickhouse-client",
                    f"--password={CLICKHOUSE_PASSWORD}",
                    "--query=SELECT 1",
                ]
            )
        except ContainerCommandError as exc:
            logger.debug("ClickHouse readiness probe failed: %s", exc)
            return False, {"running": "true", "probe_error": str(exc)}
        if result.returncode == 0:
            return True, {"running": "true", "probe_returncode": "0"}
        stderr = result.stderr.strip()
        logger.debug("ClickHouse not ready yet: %s", stderr)
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
            timeout_context="ClickHouse startup",
        )
    except ContainerReadinessTimeout as exc:
        raise RuntimeError(str(exc)) from exc
    logger.info("ClickHouse is ready")


@fixture()
def clickhouse() -> Iterator[dict[str, str]]:
    """Start ClickHouse and yield environment variables for access."""
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )
    port = find_free_port()
    container: ManagedContainer | None = None
    try:
        container = _start_clickhouse(runtime, port)
        _wait_for_clickhouse(container, STARTUP_TIMEOUT)
        env: dict[str, str] = {
            "CLICKHOUSE_HOST": "127.0.0.1",
            "CLICKHOUSE_PORT": str(port),
            "CLICKHOUSE_PASSWORD": CLICKHOUSE_PASSWORD,
            "CLICKHOUSE_CONTAINER_ID": container.container_id,
            "CLICKHOUSE_CONTAINER_RUNTIME": runtime.binary,
        }
        yield env
    finally:
        if container is not None:
            _stop_clickhouse(container)
