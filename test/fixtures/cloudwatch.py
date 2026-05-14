"""CloudWatch Logs LocalStack fixture."""

# /// script
# dependencies = ["boto3"]
# ///

from __future__ import annotations

import json
import logging
import os
import shutil
import socket
import subprocess
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable

logger = logging.getLogger(__name__)

LOCALSTACK_IMAGE = "localstack/localstack:4.4"
SERVICES = "logs,iam,sts"
TEST_REGION = "us-east-1"
TEST_ACCESS_KEY = "test"
TEST_SECRET_KEY = "test"
TEST_EXTERNAL_ID = "tenzir-test-external-id"


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _find_container_runtime() -> str | None:
    for runtime in ("podman", "docker"):
        if shutil.which(runtime) is not None:
            return runtime
    return None


def _start_localstack(runtime: str, port: int) -> str:
    container_name = f"tenzir-test-cloudwatch-{uuid.uuid4().hex[:8]}"
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
    return subprocess.run(
        cmd, capture_output=True, text=True, check=True
    ).stdout.strip()


def _stop_localstack(runtime: str, container_id: str) -> None:
    subprocess.run([runtime, "stop", container_id], capture_output=True, check=False)


def _wait_for_localstack(endpoint: str, timeout: float = 60.0) -> None:
    health_url = f"{endpoint}/_localstack/health"
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(health_url, timeout=5) as response:
                services = json.loads(response.read().decode()).get("services", {})
                if all(
                    services.get(s) in ("available", "running")
                    for s in ("logs", "iam", "sts")
                ):
                    return
        except (urllib.error.URLError, OSError, json.JSONDecodeError):
            pass
        time.sleep(1)
    raise RuntimeError("LocalStack CloudWatch services did not become ready")


def _aws_client(endpoint: str, service: str) -> Any:
    try:
        import boto3
    except ModuleNotFoundError as e:
        raise FixtureUnavailable("The cloudwatch fixture requires boto3.") from e
    return boto3.client(
        service,
        endpoint_url=endpoint,
        region_name=TEST_REGION,
        aws_access_key_id=TEST_ACCESS_KEY,
        aws_secret_access_key=TEST_SECRET_KEY,
    )


def _put_events(
    logs: Any, group: str, stream: str, events: list[dict[str, Any]]
) -> None:
    logs.put_log_events(
        logGroupName=group,
        logStreamName=stream,
        logEvents=events,
    )


def _create_role(iam: Any, suffix: str) -> str:
    role_name = f"tenzir-cloudwatch-role-{suffix}"
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
    response = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=assume_role_policy,
        Description="Test role for CloudWatch Logs integration tests",
    )
    iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn="arn:aws:iam::aws:policy/CloudWatchLogsFullAccess",
    )
    return response["Role"]["Arn"]


def _start_hlc_server(
    path: Path, token: str
) -> tuple[ThreadingHTTPServer, threading.Thread, str]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length).decode()
            status = 500 if self.path == "/error" else 200
            with path.open("a") as f:
                f.write(
                    json.dumps(
                        {
                            "path": self.path,
                            "authorization": self.headers.get("authorization"),
                            "body": json.loads(body),
                            "status": status,
                        }
                    )
                    + "\n"
                )
            self.send_response(status)
            self.end_headers()

        def log_message(self, fmt: str, *args: object) -> None:
            logger.debug("cloudwatch hlc: %s", fmt % args if args else fmt)

    port = _find_free_port()
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{port}/logs"


@fixture(name="cloudwatch", log_teardown=True)
def run() -> Iterator[dict[str, str]]:
    runtime = _find_container_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "A container runtime is required for CloudWatch tests."
        )
    port = _find_free_port()
    endpoint = f"http://127.0.0.1:{port}"
    container_id = None
    temp_dir = tempfile.mkdtemp(prefix="tenzir-cloudwatch-")
    hlc_server = None
    hlc_thread = None
    try:
        container_id = _start_localstack(runtime, port)
        _wait_for_localstack(endpoint)
        logs = _aws_client(endpoint, "logs")
        iam = _aws_client(endpoint, "iam")
        suffix = uuid.uuid4().hex[:8]
        group = f"/tenzir/test/{suffix}"
        filter_stream = "filter-stream"
        get_stream = "get-stream"
        write_stream = "write-stream"
        logs.create_log_group(logGroupName=group)
        for stream in (filter_stream, get_stream, write_stream):
            logs.create_log_stream(logGroupName=group, logStreamName=stream)
        base = 1_777_680_000_000
        _put_events(
            logs,
            group,
            filter_stream,
            [
                {
                    "timestamp": base + 1_000,
                    "message": '{"id":1,"level":"INFO","msg":"ready"}',
                },
                {
                    "timestamp": base + 2_000,
                    "message": '{"id":2,"level":"ERROR","msg":"boom"}',
                },
                {
                    "timestamp": base + 3_000,
                    "message": '{"id":3,"level":"INFO","msg":"done"}',
                },
            ],
        )
        _put_events(
            logs,
            group,
            get_stream,
            [
                {"timestamp": base + 4_000, "message": '{"id":10,"msg":"first"}'},
                {"timestamp": base + 5_000, "message": '{"id":11,"msg":"second"}'},
                {"timestamp": base + 6_000, "message": '{"id":12,"msg":"third"}'},
            ],
        )
        role_arn = _create_role(iam, suffix)
        hlc_path = Path(temp_dir) / "hlc.jsonl"
        hlc_token = "tenzir-test-token"
        hlc_server, hlc_thread, hlc_endpoint = _start_hlc_server(hlc_path, hlc_token)
        yield {
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_ENDPOINT_URL_LOGS": endpoint,
            "AWS_ENDPOINT_URL_STS": endpoint,
            "AWS_ACCESS_KEY_ID": TEST_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": TEST_SECRET_KEY,
            "AWS_REGION": TEST_REGION,
            "AWS_DEFAULT_REGION": TEST_REGION,
            "AWS_EC2_METADATA_DISABLED": "true",
            "LOCALSTACK_CLOUDWATCH_LOG_GROUP": group,
            "LOCALSTACK_CLOUDWATCH_LOG_STREAM_FILTER": filter_stream,
            "LOCALSTACK_CLOUDWATCH_LOG_STREAM_GET": get_stream,
            "LOCALSTACK_CLOUDWATCH_LOG_STREAM_WRITE": write_stream,
            "LOCALSTACK_CLOUDWATCH_ROLE_ARN": role_arn,
            "LOCALSTACK_CLOUDWATCH_EXTERNAL_ID": TEST_EXTERNAL_ID,
            "CLOUDWATCH_HLC_ENDPOINT": hlc_endpoint,
            "CLOUDWATCH_HLC_ERROR_ENDPOINT": hlc_endpoint.replace("/logs", "/error"),
            "CLOUDWATCH_HLC_TOKEN": hlc_token,
            "CLOUDWATCH_HLC_RECORDS": str(hlc_path),
        }
    finally:
        if hlc_server is not None:
            hlc_server.shutdown()
            hlc_server.server_close()
        if hlc_thread is not None:
            hlc_thread.join(timeout=5)
        if container_id:
            _stop_localstack(runtime, container_id)
        shutil.rmtree(temp_dir, ignore_errors=True)
