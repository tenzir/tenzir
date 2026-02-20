"""GCS fixture for from_gcs operator integration testing.

Provides a fake-gcs-server instance for testing the from_gcs operator.

Environment variables yielded:
- GCS_ENDPOINT: fake-gcs-server endpoint (127.0.0.1:PORT)
- GCS_BUCKET: Main test bucket name (tenzir-test)
- GCS_PUBLIC_BUCKET: Public bucket name (tenzir-test-public)

Assertions payload accepted under ``assertions.fixtures.gcs``:
- state: unchanged | removed | renamed
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable
from tenzir_test.fixtures.container_runtime import (
    ContainerReadinessTimeout,
    ManagedContainer,
    RuntimeSpec,
    detect_runtime,
    start_detached,
    wait_until_ready,
)

from ._cloud_storage import (
    BUCKET,
    PUBLIC_BUCKET,
    TEST_FILES,
    CloudStorageAssertions,
    extract_assertions,
    verify_post_test,
)
from ._utils import find_free_port

logger = logging.getLogger(__name__)

# fake-gcs-server configuration
GCS_IMAGE = "docker.io/fsouza/fake-gcs-server"
CONTAINER_PORT = 4443
STARTUP_TIMEOUT = 60  # seconds
HEALTH_CHECK_INTERVAL = 1  # seconds


def _start_fake_gcs(runtime: RuntimeSpec, port: int) -> ManagedContainer:
    """Start fake-gcs-server container and return a managed container."""
    container_name = f"tenzir-test-fake-gcs-{uuid.uuid4().hex[:8]}"
    run_args = [
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{port}:{CONTAINER_PORT}",
        GCS_IMAGE,
        "-scheme",
        "http",
    ]
    logger.info("Starting fake-gcs-server with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info("fake-gcs-server started: %s", container.container_id[:12])
    return container


def _stop_container(container: ManagedContainer, label: str) -> None:
    """Stop a container with logging."""
    logger.info("Stopping %s container: %s", label, container.container_id[:12])
    result = container.stop()
    if result.returncode != 0:
        logger.warning(
            "Failed to stop %s container %s: %s",
            label,
            container.container_id[:12],
            (result.stderr or result.stdout or "").strip() or "no output",
        )


def _wait_for_gcs(port: int, timeout: float) -> None:
    """Wait for fake-gcs-server to become ready via bucket list endpoint."""
    url = f"http://127.0.0.1:{port}/storage/v1/b"

    def _probe() -> tuple[bool, dict[str, str]]:
        try:
            resp = urllib.request.urlopen(url, timeout=2)
            status = resp.status
            resp.close()
            return status == 200, {"status": str(status)}
        except (urllib.error.URLError, OSError) as exc:
            return False, {"error": str(exc)}

    try:
        wait_until_ready(
            _probe,
            timeout_seconds=timeout,
            poll_interval_seconds=HEALTH_CHECK_INTERVAL,
            timeout_context="fake-gcs-server startup",
        )
    except ContainerReadinessTimeout as exc:
        raise RuntimeError(str(exc)) from exc
    logger.info("fake-gcs-server is ready")


def _gcs_create_bucket(port: int, bucket_name: str) -> None:
    """Create a bucket in fake-gcs-server."""
    url = f"http://127.0.0.1:{port}/storage/v1/b"
    data = json.dumps({"name": bucket_name}).encode()
    req = urllib.request.Request(
        url, data=data, headers={"Content-Type": "application/json"}, method="POST"
    )
    resp = urllib.request.urlopen(req, timeout=10)
    resp.close()
    logger.info("Created bucket: %s", bucket_name)


def _gcs_upload_object(port: int, bucket: str, key: str, content: str) -> None:
    """Upload an object to fake-gcs-server."""
    encoded_name = urllib.parse.quote(key, safe="")
    url = (
        f"http://127.0.0.1:{port}/upload/storage/v1/b/{bucket}/o"
        f"?uploadType=media&name={encoded_name}"
    )
    data = content.encode()
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/octet-stream"},
        method="POST",
    )
    resp = urllib.request.urlopen(req, timeout=10)
    resp.close()


def _setup_gcs_data(port: int) -> None:
    """Create buckets and upload test data."""
    _gcs_create_bucket(port, BUCKET)
    _gcs_create_bucket(port, PUBLIC_BUCKET)

    for full_key, content in TEST_FILES.items():
        bucket, key = full_key.split("/", 1)
        _gcs_upload_object(port, bucket, key, json.dumps(content) + "\n")

    logger.info("fake-gcs-server test data setup complete")


def _gcs_file_exists(port: int, key: str) -> bool:
    """Check whether a key exists in the main bucket."""
    encoded_key = urllib.parse.quote(key, safe="")
    url = f"http://127.0.0.1:{port}/storage/v1/b/{BUCKET}/o/{encoded_key}"
    try:
        req = urllib.request.Request(url, method="GET")
        resp = urllib.request.urlopen(req, timeout=10)
        resp.close()
        return True
    except urllib.error.HTTPError as exc:
        if exc.code == 404:
            return False
        raise


@fixture(assertions=CloudStorageAssertions)
def gcs() -> FixtureHandle:
    """Start fake-gcs-server and return fixture handle with assertions."""
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )

    port = find_free_port()
    container: ManagedContainer | None = None

    try:
        container = _start_fake_gcs(runtime, port)
        _wait_for_gcs(port, STARTUP_TIMEOUT)
        _setup_gcs_data(port)
    except Exception:
        if container is not None:
            _stop_container(container, "fake-gcs-server")
        raise
    assert container is not None

    def _assert_test(
        *,
        test: Path,
        assertions: CloudStorageAssertions | dict[str, Any],
        **_: Any,
    ) -> None:
        assertion_config = extract_assertions(assertions)
        try:
            verify_post_test(
                file_exists=lambda key: _gcs_file_exists(port, key),
                assertions=assertion_config,
            )
        except RuntimeError as exc:
            raise AssertionError(f"{test.name}: {exc}") from exc

    return FixtureHandle(
        env={
            "GCS_ENDPOINT": f"127.0.0.1:{port}",
            "GCS_BUCKET": BUCKET,
            "GCS_PUBLIC_BUCKET": PUBLIC_BUCKET,
        },
        teardown=lambda: _stop_container(container, "fake-gcs-server"),
        hooks={"assert_test": _assert_test},
    )
