"""LocalStack fixture for AWS service emulation.

Usage overview:

- Tests declare ``fixtures: [localstack]`` in their frontmatter to opt in.
- Importing this module registers the ``@fixture`` definition below.
- **AWS_ENDPOINT_URL** – Base endpoint URL for all AWS services.
- **AWS_ENDPOINT_URL_S3** – S3-specific endpoint URL.
- **AWS_ENDPOINT_URL_SQS** – SQS-specific endpoint URL.
- **AWS_ENDPOINT_URL_KINESIS** – Kinesis-specific endpoint URL.
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
- **LOCALSTACK_SQS_QUEUE_NO_DELETE** – SQS queue name for from_sqs delete=false tests.
- **LOCALSTACK_SQS_QUEUE_NO_DELETE_URL** – Full URL of the from_sqs delete=false queue.
- **LOCALSTACK_ROLE_ARN** – ARN of a test IAM role for assume role tests.
- **LOCALSTACK_EXTERNAL_ID** – External ID for assume role tests.
- **LOCALSTACK_SQS_QUEUE_WEB_IDENTITY_FILE** – SQS queue name for web identity token_file tests.
- **LOCALSTACK_SQS_QUEUE_WEB_IDENTITY_FILE_URL** – Full URL of the web identity token_file queue.
- **LOCALSTACK_SQS_QUEUE_WEB_IDENTITY_TOKEN** – SQS queue name for web identity direct token tests.
- **LOCALSTACK_SQS_QUEUE_WEB_IDENTITY_TOKEN_URL** – Full URL of the web identity direct token queue.
- **LOCALSTACK_WEB_IDENTITY_ROLE_ARN** – ARN of a test IAM role for web identity tests.
- **LOCALSTACK_WEB_IDENTITY_TOKEN_FILE** – Path to a file containing a test JWT token.
- **LOCALSTACK_WEB_IDENTITY_TOKEN** – The test JWT token value (for direct token tests).
- **LOCALSTACK_WEB_IDENTITY_TOKEN_ENDPOINT** – Local HTTP endpoint returning a JSON token payload.
- **LOCALSTACK_WEB_IDENTITY_TOKEN_ENDPOINT_HEADER** – Required header value for the token endpoint.

The fixture uses Podman or Docker to run LocalStack and creates test resources
automatically. It requires either Podman or Docker to be available in the
environment (Podman is preferred if both are present).
"""

# /// script
# dependencies = ["boto3"]
# ///

from __future__ import annotations

import json
import logging
import shutil
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable

logger = logging.getLogger(__name__)

# LocalStack configuration
LOCALSTACK_IMAGE = "localstack/localstack:4.4"
SERVICES = "s3,sqs,kinesis,iam,sts"
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


def _start_token_endpoint_server(
    token: str,
) -> tuple[ThreadingHTTPServer, threading.Thread, str, str]:
    """Start a tiny local HTTP server that serves the test web identity token."""
    required_header_name = "X-Test-Authorization"
    required_header_value = "allow-token"

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            if self.path != "/token":
                self.send_response(404)
                self.end_headers()
                return
            if self.headers.get(required_header_name) != required_header_value:
                self.send_response(403)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(b'{"error":"missing required header"}')
                return
            payload = json.dumps({"access_token": token}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, fmt: str, *args: object) -> None:
            logger.debug("token endpoint: %s", fmt % args if args else fmt)

    port = _find_free_port()
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return (
        server,
        thread,
        f"http://127.0.0.1:{port}/token",
        required_header_value,
    )


def _stop_token_endpoint_server(
    server: ThreadingHTTPServer | None,
    thread: threading.Thread | None,
) -> None:
    """Stop the local HTTP token endpoint."""
    if server is None:
        return
    server.shutdown()
    server.server_close()
    if thread is not None:
        thread.join(timeout=5)


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
                kinesis_ready = services.get("kinesis") in ("available", "running")
                iam_ready = services.get("iam") in ("available", "running")
                sts_ready = services.get("sts") in ("available", "running")
                if s3_ready and sqs_ready and kinesis_ready and iam_ready and sts_ready:
                    logger.info(
                        "LocalStack is ready (S3: %s, SQS: %s, Kinesis: %s, IAM: %s, STS: %s)",
                        services.get("s3"),
                        services.get("sqs"),
                        services.get("kinesis"),
                        services.get("iam"),
                        services.get("sts"),
                    )
                    return True
                logger.debug(
                    "Waiting for services: S3=%s, SQS=%s, Kinesis=%s, IAM=%s, STS=%s",
                    services.get("s3"),
                    services.get("sqs"),
                    services.get("kinesis"),
                    services.get("iam"),
                    services.get("sts"),
                )
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
            logger.debug("Health check failed: %s", e)
        time.sleep(HEALTH_CHECK_INTERVAL)

    return False


def _aws_client(endpoint: str, service: str, region: str) -> Any:
    try:
        import boto3
    except ModuleNotFoundError as e:
        raise FixtureUnavailable(
            "The localstack fixture requires boto3. Run tenzir-test from a uv "
            "environment that includes boto3, for example with "
            "`uvx --with boto3 tenzir-test ...`."
        ) from e
    return boto3.client(
        service,
        endpoint_url=endpoint,
        region_name=region,
        aws_access_key_id=TEST_ACCESS_KEY,
        aws_secret_access_key=TEST_SECRET_KEY,
    )


def _s3_create_bucket(s3: Any, bucket: str) -> None:
    s3.create_bucket(Bucket=bucket)


def _s3_put_object(s3: Any, bucket: str, key: str, body: bytes) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=body)


def _sqs_create_queue(
    sqs: Any,
    name: str,
    *,
    attributes: dict[str, str] | None = None,
) -> str:
    response = sqs.create_queue(QueueName=name, Attributes=attributes or {})
    return response["QueueUrl"]


def _sqs_send_message(sqs: Any, queue_url: str, body: str) -> None:
    sqs.send_message(QueueUrl=queue_url, MessageBody=body)


def _kinesis_create_stream(kinesis: Any, name: str) -> None:
    kinesis.create_stream(StreamName=name, ShardCount=1)
    waiter = kinesis.get_waiter("stream_exists")
    waiter.wait(StreamName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 30})


def _kinesis_put_record(
    kinesis: Any,
    stream_name: str,
    data: bytes,
    partition_key: str,
) -> None:
    kinesis.put_record(
        StreamName=stream_name,
        Data=data,
        PartitionKey=partition_key,
    )


def _iam_create_role(
    iam: Any,
    role_name: str,
    assume_role_policy: str,
    description: str,
) -> str:
    response = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=assume_role_policy,
        Description=description,
    )
    return response["Role"]["Arn"]


def _iam_attach_role_policy(iam: Any, role_name: str, policy_arn: str) -> None:
    iam.attach_role_policy(RoleName=role_name, PolicyArn=policy_arn)


def _create_test_resources(endpoint: str, region: str) -> dict[str, str]:
    """Create test resources, return dict of resource identifiers."""
    # Generate unique resource names
    suffix = uuid.uuid4().hex[:8]
    bucket_name = f"tenzir-test-bucket-{suffix}"
    role_name = f"tenzir-test-role-{suffix}"
    s3 = _aws_client(endpoint, "s3", region)
    sqs = _aws_client(endpoint, "sqs", region)
    kinesis = _aws_client(endpoint, "kinesis", region)
    iam = _aws_client(endpoint, "iam", region)

    # Create S3 bucket
    logger.info("Creating S3 bucket: %s", bucket_name)
    _s3_create_bucket(s3, bucket_name)

    # Upload a test file
    test_content = b'{"message": "Hello from LocalStack test!"}\n'
    _s3_put_object(s3, bucket_name, "test.json", test_content)
    logger.info("Uploaded test file to s3://%s/test.json", bucket_name)

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
    queue_no_delete_name = f"tenzir-test-queue-no-delete-{suffix}"
    queue_wi_file_name = f"tenzir-test-queue-wi-file-{suffix}"
    queue_wi_token_name = f"tenzir-test-queue-wi-token-{suffix}"

    queues = {}
    for name, key in [
        (queue_basic_name, "basic"),
        (queue_role_name, "role"),
        (queue_save_name, "save"),
        (queue_wi_file_name, "wi_file"),
        (queue_wi_token_name, "wi_token"),
    ]:
        logger.info("Creating SQS queue: %s", name)
        queue_url = _sqs_create_queue(sqs, name)
        logger.info("Created SQS queue: %s", queue_url)
        queues[key] = {"name": name, "url": queue_url}
        # Send test messages to this queue
        for msg in test_messages:
            _sqs_send_message(sqs, queue_url, msg)
        logger.info("Sent %d test messages to queue %s", len(test_messages), name)
    logger.info("Creating SQS queue: %s", queue_no_delete_name)
    queue_no_delete_url = _sqs_create_queue(
        sqs,
        queue_no_delete_name,
        attributes={"VisibilityTimeout": "30"},
    )
    logger.info("Created SQS queue: %s", queue_no_delete_url)
    queues["no_delete"] = {
        "name": queue_no_delete_name,
        "url": queue_no_delete_url,
    }
    _sqs_send_message(
        sqs,
        queue_no_delete_url,
        '{"id": 100, "message": "No delete test message"}',
    )
    _sqs_send_message(
        sqs,
        queue_no_delete_url,
        '{"id": 101, "message": "No delete test message 2"}',
    )
    logger.info("Sent no-delete test message to queue %s", queue_no_delete_name)

    kinesis_input_stream = f"tenzir-test-kinesis-input-{suffix}"
    kinesis_output_stream = f"tenzir-test-kinesis-output-{suffix}"
    logger.info("Creating Kinesis stream: %s", kinesis_input_stream)
    _kinesis_create_stream(kinesis, kinesis_input_stream)
    logger.info("Creating Kinesis stream: %s", kinesis_output_stream)
    _kinesis_create_stream(kinesis, kinesis_output_stream)
    for index, msg in enumerate(test_messages, start=1):
        _kinesis_put_record(
            kinesis,
            kinesis_input_stream,
            msg.encode(),
            f"input-{index}",
        )
    logger.info(
        "Sent %d test records to stream %s", len(test_messages), kinesis_input_stream
    )

    assume_role_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {"AWS": "arn:aws:iam::000000000000:root"},
                    "Action": "sts:AssumeRole",
                    "Condition": {"StringEquals": {"sts:ExternalId": TEST_EXTERNAL_ID}},
                }
            ],
        }
    )
    logger.info("Creating IAM role: %s", role_name)
    role_arn = _iam_create_role(
        iam,
        role_name,
        assume_role_policy,
        "Test role for Tenzir LocalStack tests",
    )
    logger.info("Created IAM role: %s", role_arn)

    # Attach policies to the role (full access for testing)
    _iam_attach_role_policy(
        iam,
        role_name,
        "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    )
    _iam_attach_role_policy(
        iam,
        role_name,
        "arn:aws:iam::aws:policy/AmazonSQSFullAccess",
    )
    _iam_attach_role_policy(
        iam,
        role_name,
        "arn:aws:iam::aws:policy/AmazonKinesisFullAccess",
    )
    logger.info("Attached S3 and SQS policies to role")

    # Create a role for web identity (AssumeRoleWithWebIdentity) tests.
    # LocalStack is permissive and accepts any token, so we create a role
    # that trusts a fictional OIDC provider.
    web_identity_role_name = f"tenzir-test-web-identity-role-{suffix}"
    web_identity_assume_role_policy = json.dumps(
        {
            "Version": "2012-10-17",
            "Statement": [
                {
                    "Effect": "Allow",
                    "Principal": {
                        "Federated": "arn:aws:iam::000000000000:oidc-provider/test.example.com"
                    },
                    "Action": "sts:AssumeRoleWithWebIdentity",
                }
            ],
        }
    )
    logger.info("Creating web identity IAM role: %s", web_identity_role_name)
    web_identity_role_arn = _iam_create_role(
        iam,
        web_identity_role_name,
        web_identity_assume_role_policy,
        "Test role for Tenzir LocalStack web identity tests",
    )
    logger.info("Created web identity IAM role: %s", web_identity_role_arn)

    # Attach policies to the web identity role
    _iam_attach_role_policy(
        iam,
        web_identity_role_name,
        "arn:aws:iam::aws:policy/AmazonS3FullAccess",
    )
    _iam_attach_role_policy(
        iam,
        web_identity_role_name,
        "arn:aws:iam::aws:policy/AmazonSQSFullAccess",
    )
    _iam_attach_role_policy(
        iam,
        web_identity_role_name,
        "arn:aws:iam::aws:policy/AmazonKinesisFullAccess",
    )
    logger.info("Attached S3 and SQS policies to web identity role")

    return {
        "bucket_name": bucket_name,
        "queues": queues,
        "kinesis_input_stream": kinesis_input_stream,
        "kinesis_output_stream": kinesis_output_stream,
        "role_arn": role_arn,
        "web_identity_role_arn": web_identity_role_arn,
    }


@fixture(name="localstack", log_teardown=True)
def run() -> Iterator[dict[str, str]]:
    """Start LocalStack and yield environment variables for AWS access."""
    runtime = _find_container_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "A container runtime (podman or docker) is required for LocalStack "
            "fixture but none was found. Please install podman or docker and "
            "ensure it's in your PATH."
        )

    port = _find_free_port()
    endpoint = f"http://127.0.0.1:{port}"
    container_id = None
    temp_dir = None
    token_endpoint_server = None
    token_endpoint_thread = None
    token_endpoint_url = None
    token_endpoint_header = None

    try:
        container_id = _start_localstack(runtime, port)

        if not _wait_for_localstack(endpoint, STARTUP_TIMEOUT):
            raise RuntimeError(
                f"LocalStack failed to start within {STARTUP_TIMEOUT} seconds"
            )

        resources = _create_test_resources(endpoint, TEST_REGION)

        # Create a temporary directory for the web identity token file.
        # LocalStack accepts any token for AssumeRoleWithWebIdentity, so we
        # use a simple test token that looks like a JWT but isn't validated.
        import tempfile
        import os

        temp_dir = tempfile.mkdtemp(prefix="tenzir-localstack-")
        token_file = os.path.join(temp_dir, "web-identity-token")
        # This is a dummy JWT token. LocalStack doesn't validate it.
        test_token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJ0ZXN0LXVzZXIiLCJpc3MiOiJ0ZXN0LmV4YW1wbGUuY29tIiwiYXVkIjoic3RzLmFtYXpvbmF3cy5jb20iLCJleHAiOjk5OTk5OTk5OTl9.test-signature"  # noqa: S105
        with open(token_file, "w") as f:
            f.write(test_token)
        logger.info("Created web identity token file: %s", token_file)
        (
            token_endpoint_server,
            token_endpoint_thread,
            token_endpoint_url,
            token_endpoint_header,
        ) = _start_token_endpoint_server(test_token)
        logger.info("Started web identity token endpoint: %s", token_endpoint_url)

        yield {
            # Standard AWS environment variables
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_ENDPOINT_URL_S3": endpoint,
            "AWS_ENDPOINT_URL_SQS": endpoint,
            "AWS_ENDPOINT_URL_KINESIS": endpoint,
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
            "LOCALSTACK_SQS_QUEUE_NO_DELETE": resources["queues"]["no_delete"]["name"],
            "LOCALSTACK_SQS_QUEUE_NO_DELETE_URL": resources["queues"]["no_delete"][
                "url"
            ],
            "LOCALSTACK_SQS_QUEUE_WEB_IDENTITY_FILE": resources["queues"]["wi_file"][
                "name"
            ],
            "LOCALSTACK_SQS_QUEUE_WEB_IDENTITY_FILE_URL": resources["queues"][
                "wi_file"
            ]["url"],
            "LOCALSTACK_SQS_QUEUE_WEB_IDENTITY_TOKEN": resources["queues"]["wi_token"][
                "name"
            ],
            "LOCALSTACK_SQS_QUEUE_WEB_IDENTITY_TOKEN_URL": resources["queues"][
                "wi_token"
            ]["url"],
            "LOCALSTACK_KINESIS_STREAM_INPUT": resources["kinesis_input_stream"],
            "LOCALSTACK_KINESIS_STREAM_OUTPUT": resources["kinesis_output_stream"],
            # IAM role for assume role tests
            "LOCALSTACK_ROLE_ARN": resources["role_arn"],
            "LOCALSTACK_EXTERNAL_ID": TEST_EXTERNAL_ID,
            # Web identity (AssumeRoleWithWebIdentity) test resources
            "LOCALSTACK_WEB_IDENTITY_ROLE_ARN": resources["web_identity_role_arn"],
            "LOCALSTACK_WEB_IDENTITY_TOKEN_FILE": token_file,
            "LOCALSTACK_WEB_IDENTITY_TOKEN": test_token,
            "LOCALSTACK_WEB_IDENTITY_TOKEN_ENDPOINT": token_endpoint_url,
            "LOCALSTACK_WEB_IDENTITY_TOKEN_ENDPOINT_HEADER": token_endpoint_header,
        }
    finally:
        _stop_token_endpoint_server(token_endpoint_server, token_endpoint_thread)
        if container_id:
            _stop_localstack(runtime, container_id)
        if temp_dir:
            shutil.rmtree(temp_dir, ignore_errors=True)
