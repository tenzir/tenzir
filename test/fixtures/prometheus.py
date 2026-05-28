"""Prometheus fixture for Remote Write integration testing.

Starts a Prometheus server with the built-in Remote Write receiver enabled.

Environment variables yielded:
- PROMETHEUS_URL: Prometheus HTTP API base URL.
- PROMETHEUS_REMOTE_WRITE_URL: Remote Write endpoint.
- PROMETHEUS_CONTAINER_ID: Container ID for in-fixture helpers/scripts.
- PROMETHEUS_CONTAINER_RUNTIME: Container runtime used (docker/podman).
"""

from __future__ import annotations

import logging
import shutil
import tempfile
import urllib.error
import urllib.request
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

PROMETHEUS_IMAGE = "docker.io/prom/prometheus:latest"
STARTUP_TIMEOUT = 60
HEALTH_CHECK_INTERVAL = 1


@dataclass(frozen=True)
class PrometheusOptions:
    image: str = PROMETHEUS_IMAGE


def _write_config(temp_dir: Path) -> Path:
    config = temp_dir / "prometheus.yml"
    config.write_text(
        "global:\n  scrape_interval: 1h\nscrape_configs: []\n",
        encoding="utf-8",
    )
    return config


def _start_prometheus(
    runtime: RuntimeSpec, port: int, image: str, config: Path
) -> ManagedContainer:
    container_name = f"tenzir-test-prometheus-{uuid.uuid4().hex[:8]}"
    run_args = [
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{port}:9090",
        "-v",
        f"{config}:/etc/prometheus/prometheus.yml:ro",
        image,
        "--config.file=/etc/prometheus/prometheus.yml",
        "--storage.tsdb.path=/prometheus",
        "--storage.tsdb.retention.time=100y",
        "--web.enable-remote-write-receiver",
        (
            "--web.remote-write-receiver.accepted-protobuf-messages="
            "prometheus.WriteRequest"
        ),
        (
            "--web.remote-write-receiver.accepted-protobuf-messages="
            "io.prometheus.write.v2.Request"
        ),
        "--web.listen-address=0.0.0.0:9090",
    ]
    logger.info("Starting Prometheus container with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info("Prometheus container started: %s", container.container_id[:12])
    return container


def _stop_prometheus(container: ManagedContainer) -> None:
    logger.info("Stopping Prometheus container: %s", container.container_id[:12])
    result = container.stop()
    if result.returncode != 0:
        logger.warning(
            "Failed to stop Prometheus container %s: %s",
            container.container_id[:12],
            (result.stderr or result.stdout or "").strip() or "no output",
        )


def _wait_for_prometheus(base_url: str, timeout: float) -> None:
    def _probe() -> tuple[bool, dict[str, str]]:
        try:
            with urllib.request.urlopen(f"{base_url}/-/ready", timeout=2) as response:
                return response.status == 200, {"status": str(response.status)}
        except urllib.error.HTTPError as exc:
            return False, {"status": str(exc.code)}
        except OSError as exc:
            return False, {"error": str(exc)}

    try:
        wait_until_ready(
            _probe,
            timeout_seconds=timeout,
            poll_interval_seconds=HEALTH_CHECK_INTERVAL,
            timeout_context="Prometheus startup",
        )
    except ContainerReadinessTimeout as exc:
        raise RuntimeError(str(exc)) from exc
    logger.info("Prometheus is ready")


@fixture(options=PrometheusOptions)
def prometheus() -> Iterator[dict[str, str]]:
    """Start Prometheus and yield environment variables for access."""
    opts = current_options("prometheus")
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )
    port = find_free_port()
    temp_dir = Path(tempfile.mkdtemp(prefix="prometheus-fixture-"))
    config = _write_config(temp_dir)
    base_url = f"http://127.0.0.1:{port}"
    container: ManagedContainer | None = None
    try:
        try:
            container = _start_prometheus(runtime, port, opts.image, config)
        except ContainerCommandError as exc:
            raise FixtureUnavailable(
                f"failed to start Prometheus container: {exc}"
            ) from exc
        try:
            _wait_for_prometheus(base_url, STARTUP_TIMEOUT)
        except RuntimeError as exc:
            raise FixtureUnavailable(str(exc)) from exc
        yield {
            "PROMETHEUS_URL": base_url,
            "PROMETHEUS_REMOTE_WRITE_URL": f"{base_url}/api/v1/write",
            "PROMETHEUS_CONTAINER_ID": container.container_id,
            "PROMETHEUS_CONTAINER_RUNTIME": runtime.binary,
        }
    finally:
        if container is not None:
            _stop_prometheus(container)
        shutil.rmtree(temp_dir, ignore_errors=True)
