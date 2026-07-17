"""Iceberg REST catalog fixture for to_iceberg integration testing.

Starts the official apache/iceberg-rest-fixture with a file-based warehouse
that lives in a host directory bind-mounted into the container at the same
absolute path. Because Iceberg table metadata records absolute file:// paths,
the identical mount point lets both the catalog (in the container) and
clients on the host (tenzir, PyIceberg) resolve the same locations.

Environment variables yielded:
- ICEBERG_REST_URI: REST catalog endpoint (http://127.0.0.1:<port>)
- ICEBERG_WAREHOUSE_DIR: Host path of the warehouse directory
- ICEBERG_REST_CONTAINER_ID: Container ID for exec operations
- ICEBERG_REST_CONTAINER_RUNTIME: Container runtime binary
"""

from __future__ import annotations

import logging
import os
import shutil
import tempfile
import urllib.error
import urllib.request
import uuid
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable
from tenzir_test.fixtures.container_runtime import (
    ContainerReadinessTimeout,
    ManagedContainer,
    RuntimeSpec,
    detect_runtime,
    start_detached,
    wait_until_ready,
)

from ._utils import find_free_port

logger = logging.getLogger(__name__)

ICEBERG_REST_IMAGE = "docker.io/apache/iceberg-rest-fixture:latest"
STARTUP_TIMEOUT = 60
HEALTH_CHECK_INTERVAL = 2


def _start_catalog(
    runtime: RuntimeSpec, port: int, warehouse_dir: str
) -> ManagedContainer:
    """Start the REST catalog container and return a managed container."""
    container_name = f"tenzir-test-iceberg-rest-{uuid.uuid4().hex[:8]}"
    run_args = [
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{port}:8181",
        "-e",
        f"CATALOG_WAREHOUSE=file://{warehouse_dir}",
        "-v",
        f"{warehouse_dir}:{warehouse_dir}",
        ICEBERG_REST_IMAGE,
    ]
    logger.info("Starting Iceberg REST catalog container with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info(
        "Iceberg REST catalog container started: %s", container.container_id[:12]
    )
    return container


def _stop_catalog(container: ManagedContainer) -> None:
    """Stop and remove the REST catalog container."""
    logger.info(
        "Stopping Iceberg REST catalog container: %s", container.container_id[:12]
    )
    result = container.stop()
    if result.returncode != 0:
        logger.warning(
            "Failed to stop Iceberg REST catalog container %s: %s",
            container.container_id[:12],
            (result.stderr or result.stdout or "").strip() or "no output",
        )


def _wait_for_catalog(container: ManagedContainer, port: int, timeout: float) -> None:
    """Wait for the REST catalog to answer its config endpoint."""

    def _probe() -> tuple[bool, dict[str, str]]:
        if not container.is_running():
            logger.debug("Container not running yet")
            return False, {"running": "false"}
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/v1/config", timeout=2
            ) as response:
                return response.status == 200, {
                    "running": "true",
                    "http_status": str(response.status),
                }
        except (urllib.error.URLError, OSError) as exc:
            logger.debug("Iceberg REST catalog readiness probe failed: %s", exc)
            return False, {"running": "true", "probe_error": str(exc)}

    try:
        wait_until_ready(
            _probe,
            timeout_seconds=timeout,
            poll_interval_seconds=HEALTH_CHECK_INTERVAL,
            timeout_context="Iceberg REST catalog startup",
        )
    except ContainerReadinessTimeout as exc:
        raise RuntimeError(str(exc)) from exc
    logger.info("Iceberg REST catalog is ready")


@fixture()
def iceberg_rest() -> Iterator[dict[str, str]]:
    """Start an Iceberg REST catalog and yield environment variables."""
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )
    port = find_free_port()
    # Resolve symlinks (macOS /tmp -> /private/tmp) so the path recorded in
    # the table metadata is identical inside and outside the container.
    warehouse_dir = os.path.realpath(
        tempfile.mkdtemp(prefix="tenzir-iceberg-warehouse-", dir="/tmp")
    )
    container: ManagedContainer | None = None
    try:
        container = _start_catalog(runtime, port, warehouse_dir)
        _wait_for_catalog(container, port, STARTUP_TIMEOUT)
        env: dict[str, str] = {
            "ICEBERG_REST_URI": f"http://127.0.0.1:{port}",
            "ICEBERG_WAREHOUSE_DIR": warehouse_dir,
            "ICEBERG_TABLE_LOCATION": (
                f"file://{warehouse_dir}/custom/createns/events"
            ),
            "ICEBERG_UNUSED_TABLE_LOCATION": (
                f"file://{warehouse_dir}/unused/createns/events"
            ),
            "ICEBERG_REST_CONTAINER_ID": container.container_id,
            "ICEBERG_REST_CONTAINER_RUNTIME": runtime.binary,
        }
        # Tenzir's debug build is ASan-instrumented, while the packaged Avro
        # and Iceberg libraries are not. Disable the resulting false-positive
        # std::vector container annotations for Iceberg fixture processes.
        asan_options = os.environ.get("ASAN_OPTIONS", "")
        env["ASAN_OPTIONS"] = ":".join(
            option for option in (asan_options, "detect_container_overflow=0") if option
        )
        yield env
    finally:
        if container is not None:
            _stop_catalog(container)
        shutil.rmtree(warehouse_dir, ignore_errors=True)
