"""CloudWatch Logs LocalStack fixture."""

# /// script
# dependencies = ["boto3"]
# ///

from __future__ import annotations

import json
import logging
import shutil
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
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

from ._utils import find_free_port

logger = logging.getLogger(__name__)

LOCALSTACK_IMAGE = "localstack/localstack:4.4"
SERVICES = "logs,iam,sts"
TEST_REGION = "us-east-1"
TEST_ACCESS_KEY = "test"
TEST_SECRET_KEY = "test"
TEST_EXTERNAL_ID = "tenzir-test-external-id"


@dataclass(frozen=True)
class CloudWatchHlcAssertions:
    request_count: int | None = None
    ok_request_count: int | None = None
    authorization: str | None = None
    log_group: str | None = None
    log_stream: str | None = None
    path: str | None = None
    messages: list[str] | None = None
    times: list[float] | None = None
    events_have_time: bool | None = None
    max_message_length_less_than: int | None = None


@dataclass(frozen=True)
class CloudWatchAssertions:
    hlc: CloudWatchHlcAssertions | None = None


def _extract_assertions(
    raw: CloudWatchAssertions | dict[str, Any],
) -> CloudWatchAssertions:
    if isinstance(raw, CloudWatchAssertions):
        if raw.hlc is None or isinstance(raw.hlc, CloudWatchHlcAssertions):
            return raw
        return CloudWatchAssertions(hlc=CloudWatchHlcAssertions(**raw.hlc))
    if isinstance(raw, dict):
        hlc = raw.get("hlc")
        if isinstance(hlc, dict):
            hlc = CloudWatchHlcAssertions(**hlc)
        return CloudWatchAssertions(hlc=hlc)
    raise TypeError("cloudwatch fixture assertions must be a mapping")


def _start_localstack(runtime: RuntimeSpec, port: int) -> ManagedContainer:
    container_name = f"tenzir-test-cloudwatch-{uuid.uuid4().hex[:8]}"
    run_args = [
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
    return start_detached(runtime, run_args)


def _stop_localstack(container: ManagedContainer) -> None:
    result = container.stop()
    if result.returncode != 0:
        logger.warning(
            "failed to stop CloudWatch LocalStack container %s: %s",
            container.container_id[:12],
            (result.stderr or result.stdout or "").strip() or "no output",
        )


def _wait_for_localstack(port: int, timeout: float = 60.0) -> None:
    health_url = f"http://127.0.0.1:{port}/_localstack/health"

    def _probe() -> tuple[bool, dict[str, str]]:
        try:
            with urllib.request.urlopen(health_url, timeout=5) as response:
                services = json.loads(response.read().decode()).get("services", {})
                statuses = {s: services.get(s, "") for s in ("logs", "iam", "sts")}
                ready = all(
                    value in ("available", "running") for value in statuses.values()
                )
                return ready, statuses
        except (urllib.error.URLError, OSError, json.JSONDecodeError) as e:
            return False, {"error": str(e)}

    try:
        wait_until_ready(
            _probe,
            timeout_seconds=timeout,
            poll_interval_seconds=1.0,
            timeout_context="CloudWatch LocalStack startup",
        )
    except ContainerReadinessTimeout as e:
        raise RuntimeError(str(e)) from e


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
            status = 500 if self.path.startswith("/error") else 200
            with path.open("a") as f:
                f.write(
                    json.dumps(
                        {
                            "path": self.path,
                            "authorization": self.headers.get("authorization"),
                            "log_group": self.headers.get("x-aws-log-group"),
                            "log_stream": self.headers.get("x-aws-log-stream"),
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

    port = find_free_port()
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, f"http://127.0.0.1:{port}/logs"


def _read_hlc_records(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    return [json.loads(line) for line in path.read_text().splitlines()]


def _hlc_events(body: Any) -> list[dict[str, Any]]:
    if isinstance(body, list):
        return body
    if isinstance(body, dict) and "events" in body:
        return body["events"]
    if isinstance(body, dict):
        return [body]
    raise AssertionError(f"unexpected CloudWatch HLC body: {body!r}")


def _assert_hlc_records(path: Path, assertions: CloudWatchHlcAssertions) -> None:
    requests = _read_hlc_records(path)
    ok_requests = [request for request in requests if request["status"] == 200]
    events = [event for request in requests for event in _hlc_events(request["body"])]
    ok_events = [
        event for request in ok_requests for event in _hlc_events(request["body"])
    ]
    messages = [event["event"] for event in ok_events]
    times = [event["time"] for event in ok_events if "time" in event]
    if (
        assertions.request_count is not None
        and len(requests) != assertions.request_count
    ):
        raise AssertionError(
            f"expected {assertions.request_count} CloudWatch HLC request(s), "
            f"got {len(requests)}"
        )
    if (
        assertions.ok_request_count is not None
        and len(ok_requests) != assertions.ok_request_count
    ):
        raise AssertionError(
            "expected "
            f"{assertions.ok_request_count} successful CloudWatch HLC request(s), "
            f"got {len(ok_requests)}"
        )
    if assertions.authorization is not None:
        observed = requests[0]["authorization"] if requests else None
        if observed != assertions.authorization:
            raise AssertionError(
                f"expected CloudWatch HLC authorization "
                f"{assertions.authorization!r}, got {observed!r}"
            )
    if assertions.path is not None:
        observed = requests[0]["path"] if requests else None
        if observed != assertions.path:
            raise AssertionError(
                f"expected CloudWatch HLC path {assertions.path!r}, got {observed!r}"
            )
    if assertions.log_group is not None:
        observed = requests[0]["log_group"] if requests else None
        if observed != assertions.log_group:
            raise AssertionError(
                f"expected CloudWatch HLC log_group {assertions.log_group!r}, "
                f"got {observed!r}"
            )
    if assertions.log_stream is not None:
        observed = requests[0]["log_stream"] if requests else None
        if observed != assertions.log_stream:
            raise AssertionError(
                f"expected CloudWatch HLC log_stream {assertions.log_stream!r}, "
                f"got {observed!r}"
            )
    if assertions.messages is not None and messages != assertions.messages:
        raise AssertionError(
            f"expected CloudWatch HLC messages {assertions.messages!r}, "
            f"got {messages!r}"
        )
    if assertions.times is not None and times != assertions.times:
        raise AssertionError(
            f"expected CloudWatch HLC times {assertions.times!r}, got {times!r}"
        )
    if assertions.events_have_time is not None:
        observed = all("time" in event for event in events)
        if observed != assertions.events_have_time:
            raise AssertionError(
                f"expected CloudWatch HLC events_have_time="
                f"{assertions.events_have_time}, got {observed}"
            )
    if assertions.max_message_length_less_than is not None and not all(
        len(message) < assertions.max_message_length_less_than for message in messages
    ):
        raise AssertionError(
            "expected all CloudWatch HLC messages to be shorter than "
            f"{assertions.max_message_length_less_than} bytes"
        )


@fixture(name="cloudwatch", log_teardown=True, assertions=CloudWatchAssertions)
def run() -> FixtureHandle:
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "A container runtime is required for CloudWatch tests."
        )
    port = find_free_port()
    endpoint = f"http://127.0.0.1:{port}"
    container = None
    temp_dir = tempfile.mkdtemp(prefix="tenzir-cloudwatch-")
    hlc_server = None
    hlc_thread = None

    def teardown() -> None:
        if hlc_server is not None:
            hlc_server.shutdown()
            hlc_server.server_close()
        if hlc_thread is not None:
            hlc_thread.join(timeout=5)
        if container is not None:
            _stop_localstack(container)
        shutil.rmtree(temp_dir, ignore_errors=True)

    try:
        container = _start_localstack(runtime, port)
        _wait_for_localstack(port)
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
        base = int(time.time() * 1000) - 60_000
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
        group_arn = f"arn:aws:logs:{TEST_REGION}:000000000000:log-group:{group}"
        hlc_path = Path(temp_dir) / "hlc.jsonl"
        hlc_token = "tenzir-test-token"
        hlc_server, hlc_thread, hlc_endpoint = _start_hlc_server(hlc_path, hlc_token)
        env = {
            "AWS_ENDPOINT_URL": endpoint,
            "AWS_ENDPOINT_URL_LOGS": endpoint,
            "AWS_ENDPOINT_URL_STS": endpoint,
            "AWS_ACCESS_KEY_ID": TEST_ACCESS_KEY,
            "AWS_SECRET_ACCESS_KEY": TEST_SECRET_KEY,
            "AWS_REGION": TEST_REGION,
            "AWS_DEFAULT_REGION": TEST_REGION,
            "AWS_EC2_METADATA_DISABLED": "true",
            "LOCALSTACK_CLOUDWATCH_LOG_GROUP": group,
            "LOCALSTACK_CLOUDWATCH_LOG_GROUP_ARN": group_arn,
            "LOCALSTACK_CLOUDWATCH_LOG_STREAM_FILTER": filter_stream,
            "LOCALSTACK_CLOUDWATCH_LOG_STREAM_GET": get_stream,
            "LOCALSTACK_CLOUDWATCH_LOG_STREAM_WRITE": write_stream,
            "LOCALSTACK_CLOUDWATCH_FILTER_START_MS": str(base + 1_500),
            "LOCALSTACK_CLOUDWATCH_FILTER_END_MS": str(base + 2_500),
            "LOCALSTACK_CLOUDWATCH_ROLE_ARN": role_arn,
            "LOCALSTACK_CLOUDWATCH_EXTERNAL_ID": TEST_EXTERNAL_ID,
            "CLOUDWATCH_HLC_ENDPOINT": hlc_endpoint,
            "CLOUDWATCH_HLC_ERROR_ENDPOINT": hlc_endpoint.replace("/logs", "/error"),
            "CLOUDWATCH_HLC_TOKEN": hlc_token,
            "CLOUDWATCH_HLC_RECORDS": str(hlc_path),
        }
    except BaseException:
        teardown()
        raise

    def assert_test(
        *, test: Path, assertions: CloudWatchAssertions | dict[str, Any], **_: Any
    ) -> None:
        del test
        assertion_config = _extract_assertions(assertions)
        if assertion_config.hlc is not None:
            _assert_hlc_records(hlc_path, assertion_config.hlc)

    return FixtureHandle(
        env=env,
        teardown=teardown,
        hooks={"assert_test": assert_test},
    )
