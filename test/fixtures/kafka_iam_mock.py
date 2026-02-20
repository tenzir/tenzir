"""Kafka IAM mock fixture for integration testing.

Provides a Kafka endpoint backed by franz-go `kfake` with custom server-side
SASL/OAUTHBEARER validation for AWS IAM token flows.

Environment variables yielded:
- KAFKA_BOOTSTRAP_SERVERS: Host:port for Kafka bootstrap.
- KAFKA_TOPIC: Default test topic.
- KAFKA_AWS_REGION: Region expected by the mock token validator.
- KAFKA_AWS_ACCESS_KEY_ID: Access key to use in aws_iam options.
- KAFKA_AWS_SECRET_ACCESS_KEY: Secret key to use in aws_iam options.
- KAFKA_AWS_SESSION_TOKEN: Session token to use in aws_iam options.
- KAFKA_IAM_HELPER_HTTP: Host:port for the helper control-plane API.
"""

from __future__ import annotations

import json
import logging
import os
import shutil
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options

from ._utils import find_free_port

logger = logging.getLogger(__name__)

STARTUP_TIMEOUT = 30  # seconds
HEALTH_CHECK_INTERVAL = 0.2  # seconds
HELPER_BUILD_TIMEOUT = 120  # seconds
HELPER_SHUTDOWN_TIMEOUT = 5  # seconds
BROKER_SETTLE_SECONDS = 1.0
LOG_TAIL_LINES = 80

HELPER_DIR = Path(__file__).resolve().parent / "tools" / "kafka_iam_mock"


@dataclass(frozen=True)
class KafkaIamMockOptions:
    topic: str = "tenzir_iam_test"
    partitions: int = 1
    messages: int = 1
    aws_region: str = "us-east-1"
    access_key_id: str = "AKIA_TEST_ACCESS_KEY"
    secret_access_key: str = "test-secret-key"
    session_token: str = ""


def _read_log_tail(path: Path, lines: int = LOG_TAIL_LINES) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return "<log file unavailable>"
    chunks = text.splitlines()
    if not chunks:
        return "<no log output>"
    return "\n".join(chunks[-lines:])


def _build_helper(binary_path: Path) -> None:
    go = shutil.which("go")
    if go is None:
        raise FixtureUnavailable("go toolchain is required but was not found")
    if not HELPER_DIR.is_dir():
        raise FixtureUnavailable(f"helper source directory not found: {HELPER_DIR}")
    env = os.environ.copy()
    env["GOWORK"] = "off"
    cmd = [go, "build", "-o", str(binary_path), "."]
    logger.info("Building kafka_iam_mock helper: %s", " ".join(cmd))
    result = subprocess.run(
        cmd,
        cwd=HELPER_DIR,
        env=env,
        text=True,
        capture_output=True,
        timeout=HELPER_BUILD_TIMEOUT,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or "<no stderr>"
        stdout = result.stdout.strip() or "<no stdout>"
        raise RuntimeError(
            "failed to build kafka_iam_mock helper\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )


def _start_helper(
    binary_path: Path,
    opts: KafkaIamMockOptions,
    broker_port: int,
    http_port: int,
    log_path: Path,
) -> subprocess.Popen[str]:
    cmd = [
        str(binary_path),
        "--broker-port",
        str(broker_port),
        "--http-port",
        str(http_port),
        "--topic",
        opts.topic,
        "--partitions",
        str(opts.partitions),
        "--aws-region",
        opts.aws_region,
        "--access-key-id",
        opts.access_key_id,
        "--secret-access-key",
        opts.secret_access_key,
        "--session-token",
        opts.session_token,
    ]
    logger.info("Starting kafka_iam_mock helper")
    with log_path.open("w", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            cmd,
            stdout=log_file,
            stderr=subprocess.STDOUT,
            text=True,
        )
    return process


def _wait_for_helper(process: subprocess.Popen[str], http_url: str, log_path: Path) -> None:
    deadline = time.monotonic() + STARTUP_TIMEOUT
    health_url = f"{http_url}/health"
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise RuntimeError(
                "kafka_iam_mock helper exited before becoming ready\n"
                f"log tail:\n{_read_log_tail(log_path)}"
            )
        try:
            with urllib.request.urlopen(health_url, timeout=1) as response:
                if response.status == 200:
                    logger.info("kafka_iam_mock helper is ready")
                    return
        except (urllib.error.URLError, TimeoutError, OSError):
            pass
        time.sleep(HEALTH_CHECK_INTERVAL)
    raise RuntimeError(
        f"timed out waiting for kafka_iam_mock helper readiness at {health_url}\n"
        f"log tail:\n{_read_log_tail(log_path)}"
    )


def _post_json(url: str, payload: dict[str, object]) -> None:
    data = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url=url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        if response.status not in (200, 201, 202, 204):
            raise RuntimeError(f"unexpected HTTP status: {response.status}")


def _seed_topic(http_url: str, topic: str, messages: int) -> None:
    payload = {
        "topic": topic,
        "messages": [f"message-{index:04d}" for index in range(1, messages + 1)],
    }
    _post_json(f"{http_url}/seed", payload)


def _get_auth_events(http_url: str) -> dict[str, object]:
    with urllib.request.urlopen(f"{http_url}/auth_events", timeout=10) as response:
        if response.status != 200:
            raise RuntimeError(
                f"unexpected status from auth_events endpoint: {response.status}"
            )
        body = response.read()
    parsed = json.loads(body)
    if not isinstance(parsed, dict):
        raise RuntimeError("auth_events endpoint returned invalid JSON payload")
    return parsed


def _verify_auth_events(http_url: str) -> None:
    payload = _get_auth_events(http_url)
    had_success = bool(payload.get("had_success"))
    if had_success:
        return
    failure_count = int(payload.get("failure_count") or 0)
    events = payload.get("events")
    reason = ""
    if isinstance(events, list) and events:
        last = events[-1]
        if isinstance(last, dict):
            reason = str(last.get("reason") or "")
    detail = f"last_reason={reason}" if reason else "no auth events captured"
    raise RuntimeError(
        "kafka_iam_mock did not observe a successful OAUTHBEARER authentication "
        f"(failures={failure_count}, {detail})"
    )


def _stop_helper(process: subprocess.Popen[str], log_path: Path) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=HELPER_SHUTDOWN_TIMEOUT)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=HELPER_SHUTDOWN_TIMEOUT)
    if process.returncode not in (0, -15):
        logger.warning(
            "kafka_iam_mock helper exited with code %s\nlog tail:\n%s",
            process.returncode,
            _read_log_tail(log_path),
        )


@fixture(options=KafkaIamMockOptions)
def kafka_iam_mock() -> Iterator[dict[str, str]]:
    """Start a kfake-based IAM mock broker and yield broker connection vars."""
    opts = current_options("kafka_iam_mock")
    if opts.partitions < 1:
        raise RuntimeError("kafka_iam_mock option `partitions` must be >= 1")
    if opts.messages < 0:
        raise RuntimeError("kafka_iam_mock option `messages` must be >= 0")
    if not opts.topic:
        raise RuntimeError("kafka_iam_mock option `topic` must not be empty")
    if not opts.aws_region:
        raise RuntimeError("kafka_iam_mock option `aws_region` must not be empty")
    if not opts.access_key_id:
        raise RuntimeError("kafka_iam_mock option `access_key_id` must not be empty")

    broker_port = find_free_port()
    http_port = find_free_port()
    http_url = f"http://127.0.0.1:{http_port}"
    work_dir = Path(tempfile.mkdtemp(prefix="tenzir-kafka-iam-helper-"))
    binary_path = work_dir / "kafka-iam-mock"
    log_path = work_dir / "helper.log"
    process: subprocess.Popen[str] | None = None
    keep_tmp_dirs = bool(os.environ.get("TENZIR_KEEP_TMP_DIRS"))
    try:
        _build_helper(binary_path)
        process = _start_helper(binary_path, opts, broker_port, http_port, log_path)
        _wait_for_helper(process, http_url, log_path)
        if opts.messages > 0:
            _seed_topic(http_url, opts.topic, opts.messages)
        # Give librdkafka a short settle window after seed traffic before tests
        # begin opening SASL/OAUTH connections.
        time.sleep(BROKER_SETTLE_SECONDS)
        env = {
            "KAFKA_BOOTSTRAP_SERVERS": f"127.0.0.1:{broker_port}",
            "KAFKA_TOPIC": opts.topic,
            "KAFKA_AWS_REGION": opts.aws_region,
            "KAFKA_AWS_ACCESS_KEY_ID": opts.access_key_id,
            "KAFKA_AWS_SECRET_ACCESS_KEY": opts.secret_access_key,
            "KAFKA_AWS_SESSION_TOKEN": opts.session_token,
            "KAFKA_IAM_HELPER_HTTP": f"127.0.0.1:{http_port}",
        }
        yield env
        _verify_auth_events(http_url)
    finally:
        if process is not None:
            _stop_helper(process, log_path)
        if keep_tmp_dirs:
            logger.info("keeping kafka_iam_mock temp directory: %s", work_dir)
        else:
            shutil.rmtree(work_dir, ignore_errors=True)
