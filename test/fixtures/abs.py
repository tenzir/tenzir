"""Azure Blob Storage fixture for from_abs operator integration testing.

Provides an Azurite instance for testing the from_abs operator.

Environment variables yielded:
- ABS_ENDPOINT: Azurite blob endpoint (127.0.0.1:PORT)
- ABS_ACCOUNT_NAME: Storage account name (devstoreaccount1)
- ABS_ACCOUNT_KEY: Storage account key
- ABS_CONTAINER: Main test container name (tenzir-test)
- ABS_PUBLIC_CONTAINER: Public container name (tenzir-test-public)

Assertions payload accepted under ``assertions.fixtures.abs``:
- state: unchanged | removed | renamed
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import http.client
import json
import logging
import urllib.error
import urllib.parse
import urllib.request
import uuid
from datetime import datetime, timezone
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

# Azurite configuration
AZURITE_IMAGE = "mcr.microsoft.com/azure-storage/azurite"
BLOB_PORT = 10000
ACCOUNT_NAME = "devstoreaccount1"
ACCOUNT_KEY = (
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq"
    "/K1SZFPTOtr/KBHBeksoGMGw=="
)
STARTUP_TIMEOUT = 60  # seconds
HEALTH_CHECK_INTERVAL = 1  # seconds


def _shared_key_headers(
    method: str,
    path: str,
    query: str = "",
    content_length: int = 0,
    content_type: str = "",
    extra_headers: dict[str, str] | None = None,
) -> dict[str, str]:
    """Compute Azure SharedKey Authorization headers."""
    now = datetime.now(timezone.utc).strftime("%a, %d %b %Y %H:%M:%S GMT")
    hdrs: dict[str, str] = {
        "x-ms-date": now,
        "x-ms-version": "2021-08-06",
    }
    if extra_headers:
        hdrs.update(extra_headers)

    # Canonicalized headers (sorted x-ms-* keys)
    canon_headers = ""
    for key in sorted(hdrs):
        if key.startswith("x-ms-"):
            canon_headers += f"{key}:{hdrs[key]}\n"

    # Canonicalized resource: /{account}/{url-path}
    # For Azurite (path-style), the URL path is /{account}{path}, so the
    # canonical resource includes the account name twice.
    canon_resource = f"/{ACCOUNT_NAME}/{ACCOUNT_NAME}{path}"
    if query:
        params = urllib.parse.parse_qs(query)
        for param_name in sorted(params):
            canon_resource += f"\n{param_name}:{','.join(sorted(params[param_name]))}"

    cl = str(content_length) if content_length > 0 else ""
    string_to_sign = (
        f"{method}\n"
        f"\n"  # Content-Encoding
        f"\n"  # Content-Language
        f"{cl}\n"  # Content-Length
        f"\n"  # Content-MD5
        f"{content_type}\n"  # Content-Type
        f"\n"  # Date (empty, using x-ms-date)
        f"\n"  # If-Modified-Since
        f"\n"  # If-Match
        f"\n"  # If-None-Match
        f"\n"  # If-Unmodified-Since
        f"\n"  # Range
        f"{canon_headers}"
        f"{canon_resource}"
    )

    key_bytes = base64.b64decode(ACCOUNT_KEY)
    sig = base64.b64encode(
        hmac.new(key_bytes, string_to_sign.encode("utf-8"), hashlib.sha256).digest()
    ).decode("utf-8")

    hdrs["Authorization"] = f"SharedKey {ACCOUNT_NAME}:{sig}"
    if content_type:
        hdrs["Content-Type"] = content_type
    return hdrs


def _azurite_request(
    port: int,
    method: str,
    path: str,
    query: str = "",
    data: bytes | None = None,
    content_type: str = "",
    extra_headers: dict[str, str] | None = None,
) -> int:
    """Execute an authenticated request against Azurite. Returns HTTP status.

    Uses http.client directly to preserve header case (urllib capitalizes
    header names which breaks Azurite's SharedKey signature verification).
    """
    request_path = f"/{ACCOUNT_NAME}{path}"
    if query:
        request_path += f"?{query}"
    content_length = len(data) if data else 0

    hdrs = _shared_key_headers(
        method=method,
        path=path,
        query=query,
        content_length=content_length,
        content_type=content_type,
        extra_headers=extra_headers,
    )
    hdrs["Content-Length"] = str(content_length)

    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request(method, request_path, body=data or b"", headers=hdrs)
    resp = conn.getresponse()
    status = resp.status
    body = resp.read()
    conn.close()
    if status >= 400:
        raise RuntimeError(
            f"Azurite {method} {request_path} returned {status}: "
            f"{body.decode(errors='replace')[:200]}"
        )
    return status


def _azurite_check(
    port: int,
    method: str,
    path: str,
    query: str = "",
) -> int | None:
    """Execute a request, returning the HTTP status code."""
    request_path = f"/{ACCOUNT_NAME}{path}"
    if query:
        request_path += f"?{query}"

    hdrs = _shared_key_headers(method=method, path=path, query=query)
    hdrs["Content-Length"] = "0"

    conn = http.client.HTTPConnection("127.0.0.1", port, timeout=10)
    conn.request(method, request_path, body=b"", headers=hdrs)
    resp = conn.getresponse()
    status = resp.status
    resp.read()
    conn.close()
    return status


def _start_azurite(runtime: RuntimeSpec, port: int) -> ManagedContainer:
    """Start Azurite container and return a managed container."""
    container_name = f"tenzir-test-azurite-{uuid.uuid4().hex[:8]}"
    run_args = [
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{port}:{BLOB_PORT}",
        AZURITE_IMAGE,
    ]
    logger.info("Starting Azurite with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info("Azurite started: %s", container.container_id[:12])
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


def _wait_for_azurite(port: int, timeout: float) -> None:
    """Wait for Azurite to become ready."""
    url = f"http://127.0.0.1:{port}/{ACCOUNT_NAME}"

    def _probe() -> tuple[bool, dict[str, str]]:
        try:
            urllib.request.urlopen(url, timeout=2)
            return True, {"status": "200"}
        except urllib.error.HTTPError:
            # Any HTTP error means Azurite is responding.
            return True, {"status": "reachable"}
        except (urllib.error.URLError, OSError) as exc:
            return False, {"error": str(exc)}

    try:
        wait_until_ready(
            _probe,
            timeout_seconds=timeout,
            poll_interval_seconds=HEALTH_CHECK_INTERVAL,
            timeout_context="Azurite startup",
        )
    except ContainerReadinessTimeout as exc:
        raise RuntimeError(str(exc)) from exc
    logger.info("Azurite is ready")


def _create_container(port: int, container_name: str) -> None:
    """Create a blob container in Azurite."""
    _azurite_request(port, "PUT", f"/{container_name}", query="restype=container")
    logger.info("Created container: %s", container_name)


def _upload_blob(port: int, container: str, blob: str, content: str) -> None:
    """Upload a block blob to Azurite."""
    data = content.encode()
    _azurite_request(
        port,
        "PUT",
        f"/{container}/{blob}",
        data=data,
        content_type="application/octet-stream",
        extra_headers={"x-ms-blob-type": "BlockBlob"},
    )


def _setup_azurite_data(port: int) -> None:
    """Create containers and upload test data."""
    _create_container(port, BUCKET)
    _create_container(port, PUBLIC_BUCKET)

    for full_key, content in TEST_FILES.items():
        container, key = full_key.split("/", 1)
        _upload_blob(port, container, key, json.dumps(content) + "\n")

    logger.info("Azurite test data setup complete")


def _blob_exists(port: int, key: str) -> bool:
    """Check whether a blob exists in the main container."""
    status = _azurite_check(port, "HEAD", f"/{BUCKET}/{key}")
    if status is not None and 200 <= status < 300:
        return True
    if status == 404:
        return False
    raise RuntimeError(f"unexpected status {status} checking blob {key}")


@fixture(assertions=CloudStorageAssertions)
def abs() -> FixtureHandle:
    """Start Azurite and return fixture handle with assertions."""
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )

    port = find_free_port()
    container: ManagedContainer | None = None

    try:
        container = _start_azurite(runtime, port)
        _wait_for_azurite(port, STARTUP_TIMEOUT)
        _setup_azurite_data(port)
    except Exception:
        if container is not None:
            _stop_container(container, "Azurite")
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
                file_exists=lambda key: _blob_exists(port, key),
                assertions=assertion_config,
            )
        except RuntimeError as exc:
            raise AssertionError(f"{test.name}: {exc}") from exc

    return FixtureHandle(
        env={
            "ABS_ENDPOINT": f"127.0.0.1:{port}",
            "ABS_ACCOUNT_NAME": ACCOUNT_NAME,
            "ABS_ACCOUNT_KEY": ACCOUNT_KEY,
            "ABS_CONTAINER": BUCKET,
            "ABS_PUBLIC_CONTAINER": PUBLIC_BUCKET,
        },
        teardown=lambda: _stop_container(container, "Azurite"),
        hooks={"assert_test": _assert_test},
    )
