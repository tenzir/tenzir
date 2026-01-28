"""LocalStack fixture for AWS service emulation.

Usage overview:

- Tests declare ``fixtures: [localstack]`` in their frontmatter to opt in.
- Importing this module registers the ``@fixture`` definition below.
- **AWS_ENDPOINT_URL** – Base endpoint URL for all AWS services.
- **AWS_ENDPOINT_URL_S3** – S3-specific endpoint URL.
- **AWS_ENDPOINT_URL_SQS** – SQS-specific endpoint URL.
- **AWS_ENDPOINT_URL_STS** – STS-specific endpoint URL (for role assumption).
- **AWS_ACCESS_KEY_ID** – Test AWS access key (always "test").
- **AWS_SECRET_ACCESS_KEY** – Test AWS secret key (always "test").
- **AWS_REGION** – Test AWS region (always "us-east-1").
- **LOCALSTACK_S3_BUCKET** – Pre-created test S3 bucket name.
- **LOCALSTACK_SQS_QUEUE_BASIC** – SQS queue name for load_sqs basic credential tests.
- **LOCALSTACK_SQS_QUEUE_BASIC_URL** – Full URL of the basic credentials queue.
- **LOCALSTACK_SQS_QUEUE_ROLE** – SQS queue name for load_sqs role assumption tests.
- **LOCALSTACK_SQS_QUEUE_ROLE_URL** – Full URL of the role assumption queue.
- **LOCALSTACK_SQS_QUEUE_SAVE** – SQS queue name for save_sqs tests.
- **LOCALSTACK_SQS_QUEUE_SAVE_URL** – Full URL of the save_sqs queue.
- **LOCALSTACK_ROLE_ARN** – ARN of a test IAM role for assume role tests.
- **LOCALSTACK_EXTERNAL_ID** – External ID for assume role tests.

The fixture uses Podman or Docker to run LocalStack and creates test resources
automatically. It requires either Podman or Docker to be available in the
environment (Podman is preferred if both are present).
"""

from __future__ import annotations

import json
import logging
import shutil
import socket
import subprocess
import time
import uuid
from typing import Iterator

from tenzir_test import fixture

logger = logging.getLogger(__name__)

# LocalStack configuration
LOCALSTACK_IMAGE = "localstack/localstack:latest"
SERVICES = "s3,sqs,iam,sts"
TEST_REGION = "us-east-1"
TEST_ACCESS_KEY = "test"
TEST_SECRET_KEY = "test"
TEST_EXTERNAL_ID = "tenzir-test-external-id"
STARTUP_TIMEOUT = 60  # seconds
HEALTH_CHECK_INTERVAL = 1  # seconds


def _find_free_port() -> int:
    """Find an available port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _find_container_runtime() -> str | None:
    """Find an available container runtime (podman or docker)."""
    for runtime in ("podman", "docker"):
        if shutil.which(runtime) is not None:
            return runtime
    return None


def _start_localstack(runtime: str, port: int) -> str:
    """Start LocalStack container and return container ID."""
    container_name = f"tenzir-test-localstack-{uuid.uuid4().hex[:8]}"

    cmd = [
        runtime,
        "run",
        "-d",
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{port}:4566",
        "-e",
        f"SERVICES={SERVICES}",
        "-e",
        "DEBUG=0",
        "-e",
        "EAGER_SERVICE_LOADING=1",
        LOCALSTACK_IMAGE,
    ]

    logger.info("Starting LocalStack container with %s: %s", runtime, " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    container_id = result.stdout.strip()
    logger.info("LocalStack container started: %s", container_id[:12])
    return container_id


def _stop_localstack(runtime: str, container_id: str) -> None:
    """Stop and remove LocalStack container."""
    logger.info("Stopping LocalStack container: %s", container_id[:12])
    subprocess.run(
        [runtime, "stop", container_id],
        capture_output=True,
        check=False,
    )


def _wait_for_localstack(endpoint: str, timeout: float) -> bool:
    """Wait for LocalStack to become healthy."""
    import urllib.request
    import urllib.error

    health_url = f"{endpoint}/_localstack/health"
    deadline = time.time() + timeout

    while time.time() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=5) as response:
                data = json.loads(response.read().decode())
                services = data.get("services", {})
                # Check if required services are running
                s3_ready = services.get("s3") in ("available", "running")
                sqs_ready = services.get("sqs") in ("available", "running")
                iam_ready = services.get("iam") in ("available", "running")
                sts_ready = services.get("sts") in ("available", "running")
                if s3_ready and sqs_ready and iam_ready and sts_ready:
                    logger.info(
                        "LocalStack is ready (S3: %s, SQS: %s, IAM: %s, STS: %s)",
                        services.get("s3"),
                        services.get("sqs"),
                        services.get("iam"),
                        services.get("sts"),
                    )
                    return True
                logger.debug(
                    "Waiting for services: S3=%s, SQS=%s, IAM=%s, STS=%s",
                    services.get("s3"),
                    services.get("sqs"),
                    services.get("iam"),
                    services.get("sts"),
                )
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
            logger.debug("Health check failed: %s", e)
        time.sleep(HEALTH_CHECK_INTERVAL)

    return False


def _create_test_resources(endpoint: str, region: str) -> dict[str, str]:
    """Create test resources, return dict of resource identifiers."""
    try:
        import boto3
        from botocore.config import Config
    except ImportError as e:
        raise RuntimeError(
            "boto3 is required for LocalStack fixture. Install with: pip install boto3"
        ) from e

    config = Config(
        region_name=region,
        signature_version="v4",
        retries={"max_attempts": 3, "mode": "standard"},
    )

    # Generate unique resource names
    suffix = uuid.uuid4().hex[:8]
    bucket_name = f"tenzir-test-bucket-{suffix}"
    role_name = f"tenzir-test-role-{suffix}"

    # Create S3 bucket
    s3 = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=TEST_ACCESS_KEY,
        aws_secret_access_key=TEST_SECRET_KEY,
        config=config,
    )
    logger.info("Creating S3 bucket: %s", bucket_name)
    # LocalStack doesn't enforce location constraints strictly
    s3.create_bucket(Bucket=bucket_name)

    # Upload a test file
    test_content = b'{"message": "Hello from LocalStack test!"}\n'
    s3.put_object(Bucket=bucket_name, Key="test.json", Body=test_content)
    logger.info("Uploaded test file to s3://%s/test.json", bucket_name)

    # Create SQS client
    sqs = boto3.client(
        "sqs",
        endpoint_url=endpoint,
        aws_access_key_id=TEST_ACCESS_KEY,
        aws_secret_access_key=TEST_SECRET_KEY,
        config=config,
    )

    # Create separate queues for each test scenario to avoid message consumption
    # conflicts (load_sqs consumes all messages regardless of head limit).
    test_messages = [
        '{"id": 1, "message": "First test message"}',
        '{"id": 2, "message": "Second test message"}',
        '{"id": 3, "message": "Third test message"}',
    ]

    queue_basic_name = f"tenzir-test-queue-basic-{suffix}"
    queue_role_name = f"tenzir-test-queue-role-{suffix}"
    queue_save_name = f"tenzir-test-queue-save-{suffix}"

    queues = {}
    for name, key in [(queue_basic_name, "basic"), (queue_role_name, "role"),
                      (queue_save_name, "save")]:
        logger.info("Creating SQS queue: %s", name)
        response = sqs.create_queue(QueueName=name)
        queue_url = response["QueueUrl"]
        logger.info("Created SQS queue: %s", queue_url)
        queues[key] = {"name": name, "url": queue_url}
        # Send test messages to this queue
        for msg in test_messages:
            sqs.send_message(QueueUrl=queue_url, MessageBody=msg)
        logger.info("Sent %d test messages to queue %s", len(test_messages), name)

    # Create IAM role for assume role tests
    iam = boto3.client(
        "iam",
        endpoint_url=endpoint,
        aws_access_key_id=TEST_ACCESS_KEY,
        aws_secret_access_key=TEST_SECRET_KEY,
        config=config,
    )
    assume_role_policy = json.dumps({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::000000000000:root"},
            "Action": "sts:AssumeRole",
            "Condition": {
                "StringEquals": {
                    "sts:ExternalId": TEST_EXTERNAL_ID
                }
            }
        }]
    })
    logger.info("Creating IAM role: %s", role_name)
    response = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=assume_role_policy,
        Description="Test role for Tenzir LocalStack tests",
    )
    role_arn = response["Role"]["Arn"]
    logger.info("Created IAM role: %s", role_arn)

    # Attach policies to the role (full access for testing)
    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/AmazonS3FullAccess",
    )
    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/AmazonSQSFullAccess",
    )
    logger.info("Attached S3 and SQS policies to role")

    return {
        "bucket_name": bucket_name,
        "queues": queues,
        "role_arn": role_arn,
    }


@fixture(name="localstack", log_teardown=True)
def run() -> Iterator[dict[str, str]]:
    """Start LocalStack and yield environment variables for AWS access."""
    runtime = _find_container_runtime()
    if runtime is None:
        raise RuntimeError(
            "A container runtime (podman or docker) is required for LocalStack "
            "fixture but none was found. Please install podman or docker and "
            "ensure it's in your PATH."
        )

    port = _find_free_port()
    endpoint = f"http://127.0.0.1:{port}"
    container_id = None

    try:
        container_id = _start_localstack(runtime, port)

        if not _wait_for_localstack(endpoint, STARTUP_TIMEOUT):
            raise RuntimeError(
                f"LocalStack failed to start within {STARTUP_TIMEOUT} seconds"
            )

        resources = _create_test_resources(endpoint, TEST_REGION)

        yield {
            # Standard AWS environment variables
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_ENDPOINT_URL_S3": endpoint,
            "AWS_ENDPOINT_URL_SQS": endpoint,
            "AWS_ENDPOINT_URL_STS": endpoint,
            "AWS_ACCESS_KEY_ID": TEST_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": TEST_SECRET_KEY,
            "AWS_REGION": TEST_REGION,
            "AWS_DEFAULT_REGION": TEST_REGION,
            # Disable EC2 instance metadata lookup (causes timeouts when not on EC2)
            "AWS_EC2_METADATA_DISABLED": "true",
            # Test resource identifiers
            "LOCALSTACK_S3_BUCKET": resources["bucket_name"],
            "LOCALSTACK_S3_TEST_URI": f"s3://{resources['bucket_name']}/test.json",
            # Separate SQS queues for each test scenario
            "LOCALSTACK_SQS_QUEUE_BASIC": resources["queues"]["basic"]["name"],
            "LOCALSTACK_SQS_QUEUE_BASIC_URL": resources["queues"]["basic"]["url"],
            "LOCALSTACK_SQS_QUEUE_ROLE": resources["queues"]["role"]["name"],
            "LOCALSTACK_SQS_QUEUE_ROLE_URL": resources["queues"]["role"]["url"],
            "LOCALSTACK_SQS_QUEUE_SAVE": resources["queues"]["save"]["name"],
            "LOCALSTACK_SQS_QUEUE_SAVE_URL": resources["queues"]["save"]["url"],
            # IAM role for assume role tests
            "LOCALSTACK_ROLE_ARN": resources["role_arn"],
            "LOCALSTACK_EXTERNAL_ID": TEST_EXTERNAL_ID,
        }
    finally:
        if container_id:
            _stop_localstack(runtime, container_id)
