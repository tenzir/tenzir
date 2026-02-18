"""S3 fixture for from_s3 operator integration testing.

Provides a LocalStack S3-compatible server instance for testing the from_s3
operator.

Environment variables yielded:
- S3_ENDPOINT: LocalStack S3 endpoint (127.0.0.1:PORT)
- S3_ACCESS_KEY: Access key (test)
- S3_SECRET_KEY: Secret key (test)
- S3_BUCKET: Main test bucket name (tenzir-test)
- S3_PUBLIC_BUCKET: Anonymous access bucket name (tenzir-test-public)

Options:
- verify_remove (bool): After tests, assert that lifecycle/remove-target.json
  was deleted from the bucket.
- verify_rename (bool): After tests, assert that lifecycle/rename-target.json
  was moved to lifecycle/rename-target.json.done.

When neither option is set, the fixture verifies that all originally uploaded
test files are still present after tests complete.
"""

from __future__ import annotations

import json
import logging
import urllib.error
import urllib.request
import uuid
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options
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
    CloudStorageOptions,
    verify_post_test,
)
from ._utils import find_free_port

logger = logging.getLogger(__name__)

# LocalStack configuration
LOCALSTACK_IMAGE = "docker.io/localstack/localstack"
ACCESS_KEY = "test"
SECRET_KEY = "test"
STARTUP_TIMEOUT = 60  # seconds
HEALTH_CHECK_INTERVAL = 1  # seconds


def _start_localstack(runtime: RuntimeSpec, port: int) -> ManagedContainer:
    """Start LocalStack container and return a managed container."""
    container_name = f"tenzir-test-localstack-{uuid.uuid4().hex[:8]}"
    run_args = [
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{port}:4566",
        "-e",
        "SERVICES=s3,sts,iam",
        "-e",
        f"AWS_ACCESS_KEY_ID={ACCESS_KEY}",
        "-e",
        f"AWS_SECRET_ACCESS_KEY={SECRET_KEY}",
        LOCALSTACK_IMAGE,
    ]
    logger.info("Starting LocalStack container with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info("LocalStack container started: %s", container.container_id[:12])
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


def _wait_for_localstack(port: int, timeout: float) -> None:
    """Wait for LocalStack to become ready via health endpoint."""
    url = f"http://127.0.0.1:{port}/_localstack/health"

    def _probe() -> tuple[bool, dict[str, str]]:
        try:
            resp = urllib.request.urlopen(url, timeout=2)
            data = json.loads(resp.read())
            resp.close()
            s3_status = data.get("services", {}).get("s3", "")
            ready = s3_status in ("available", "running")
            return ready, {"s3": s3_status}
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as exc:
            return False, {"error": str(exc)}

    try:
        wait_until_ready(
            _probe,
            timeout_seconds=timeout,
            poll_interval_seconds=HEALTH_CHECK_INTERVAL,
            timeout_context="LocalStack startup",
        )
    except ContainerReadinessTimeout as exc:
        raise RuntimeError(str(exc)) from exc
    logger.info("LocalStack is ready")


def _awslocal(container: ManagedContainer, args: list[str], **kwargs) -> str:
    """Execute an awslocal CLI command inside the LocalStack container."""
    cmd = ["awslocal"] + args
    result = container.exec(cmd, **kwargs)
    if result.returncode != 0:
        stderr = (result.stderr or "").strip()
        raise RuntimeError(f"awslocal command failed: {' '.join(cmd)}: {stderr}")
    return (result.stdout or "").strip()


def _setup_localstack_data(container: ManagedContainer) -> None:
    """Create buckets, upload test data in LocalStack."""
    # Create buckets
    _awslocal(container, ["s3", "mb", f"s3://{BUCKET}"])
    _awslocal(container, ["s3", "mb", f"s3://{PUBLIC_BUCKET}"])

    # Upload test files via stdin
    for key, content in TEST_FILES.items():
        _awslocal(
            container,
            ["s3", "cp", "-", f"s3://{key}"],
            input=json.dumps(content) + "\n",
        )

    # Set public read policy on the public bucket
    policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Sid": "PublicRead",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:GetObject"],
                    "Resource": [f"arn:aws:s3:::{PUBLIC_BUCKET}/*"],
                },
                {
                    "Sid": "PublicList",
                    "Effect": "Allow",
                    "Principal": "*",
                    "Action": ["s3:ListBucket"],
                    "Resource": [f"arn:aws:s3:::{PUBLIC_BUCKET}"],
                },
            ],
        }
    )
    _awslocal(
        container,
        ["s3api", "put-bucket-policy", "--bucket", PUBLIC_BUCKET, "--policy", policy],
    )

    # Create IAM role for STS assume-role tests
    assume_role_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "arn:aws:iam::000000000000:root"},
                    "Action": "sts:AssumeRole",
                }
            ],
        }
    )
    _awslocal(
        container,
        [
            "iam",
            "create-role",
            "--role-name",
            "test-role",
            "--assume-role-policy-document",
            assume_role_policy,
        ],
    )
    _awslocal(
        container,
        [
            "iam",
            "attach-role-policy",
            "--role-name",
            "test-role",
            "--policy-arn",
            "arn:aws:iam::aws:policy/AmazonS3FullAccess",
        ],
    )

    logger.info("LocalStack test data setup complete")


def _file_exists(container: ManagedContainer, key: str) -> bool:
    """Check whether a key exists in the main bucket."""
    cmd = ["awslocal", "s3api", "head-object", "--bucket", BUCKET, "--key", key]
    result = container.exec(cmd)
    return result.returncode == 0


@fixture(options=CloudStorageOptions)
def s3() -> Iterator[dict[str, str]]:
    """Start LocalStack and yield environment variables for S3 access."""
    opts = current_options("s3")
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )

    port = find_free_port()
    container: ManagedContainer | None = None

    try:
        container = _start_localstack(runtime, port)
        _wait_for_localstack(port, STARTUP_TIMEOUT)
        _setup_localstack_data(container)

        yield {
            "S3_ENDPOINT": f"127.0.0.1:{port}",
            "S3_ACCESS_KEY": ACCESS_KEY,
            "S3_SECRET_KEY": SECRET_KEY,
            "S3_BUCKET": BUCKET,
            "S3_PUBLIC_BUCKET": PUBLIC_BUCKET,
            "AWS_ENDPOINT_URL_STS": f"http://127.0.0.1:{port}",
        }

        verify_post_test(
            file_exists=lambda key: _file_exists(container, key),
            opts=opts,
        )
    finally:
        if container is not None:
            _stop_container(container, "LocalStack")
