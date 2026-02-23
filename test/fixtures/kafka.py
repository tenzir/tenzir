"""Kafka fixture for integration testing.

Provides either:
- a containerized Kafka broker (`mode=plain`), or
- a franz-go IAM mock helper (`mode=aws_iam`).

Environment variables yielded:
- KAFKA_BOOTSTRAP_SERVERS: Host:port bootstrap endpoint
- KAFKA_TOPIC: Default topic created for tests

Additional variables in `mode=plain`:
- KAFKA_HOST: Broker hostname (127.0.0.1)
- KAFKA_PORT: Broker port exposed on host (dynamically allocated)
- KAFKA_CONTAINER_ID: Container ID for in-fixture helpers/scripts
- KAFKA_CONTAINER_RUNTIME: Container runtime used (docker/podman)

Additional variables in `mode=aws_iam`:
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
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable, current_context, current_options
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

KAFKA_IMAGE = "apache/kafka:latest"
KAFKA_STARTUP_TIMEOUT = 120  # seconds
KAFKA_HEALTH_CHECK_INTERVAL = 2  # seconds
ALLOWED_COMPRESSION_TYPES = {"none", "gzip", "snappy", "lz4", "zstd"}
ALLOWED_MODES = {"plain", "aws_iam"}
IAM_STARTUP_TIMEOUT = 30  # seconds
IAM_HEALTH_CHECK_INTERVAL = 0.2  # seconds
HELPER_BUILD_TIMEOUT = 120  # seconds
HELPER_SHUTDOWN_TIMEOUT = 5  # seconds
BROKER_SETTLE_SECONDS = 1.0
LOG_TAIL_LINES = 80
HELPER_DIR = Path(__file__).resolve().parent / "tools" / "kafka_iam_mock"
GO_BUILD_PREREQUISITE_MARKERS = (
    "no required module provides package",
    "missing go.sum entry",
    "toolchain not available",
    "requires go >=",
    "requires go1.",
    "unknown revision",
    "forbidden",
    "status code 403",
    "dial tcp",
    "proxyconnect tcp",
    "temporary failure in name resolution",
    "i/o timeout",
    "connection refused",
    "tls: ",
    "x509:",
)


@dataclass(frozen=True)
class KafkaOptions:
    mode: str = "plain"
    topic: str = "tenzir_test"
    partitions: int = 1
    messages: int = 0
    compression: str = "none"
    payload_file: str = ""
    image: str = KAFKA_IMAGE
    aws_region: str = "us-east-1"
    access_key_id: str = "AKIA_TEST_ACCESS_KEY"
    secret_access_key: str = "test-secret-key"
    session_token: str = ""


def _start_kafka(
    runtime: RuntimeSpec,
    port: int,
    image: str,
    payload_file: str,
) -> ManagedContainer:
    """Start Kafka container and return a managed container."""
    container_name = f"tenzir-test-kafka-{uuid.uuid4().hex[:8]}"
    run_args = [
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{port}:9094",
        "-e",
        "KAFKA_NODE_ID=1",
        "-e",
        "KAFKA_PROCESS_ROLES=broker,controller",
        "-e",
        "KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093",
        "-e",
        "KAFKA_LISTENERS=PLAINTEXT://:9092,EXTERNAL://:9094,CONTROLLER://:9093",
        "-e",
        f"KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092,EXTERNAL://127.0.0.1:{port}",
        "-e",
        "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT",
        "-e",
        "KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER",
        "-e",
        "KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT",
        "-e",
        "KAFKA_AUTO_CREATE_TOPICS_ENABLE=true",
        "-e",
        "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1",
    ]
    if payload_file:
        run_args.extend(
            [
                "-v",
                f"{payload_file}:/tmp/tenzir-payload.log:ro",
            ]
        )
    run_args.append(image)
    logger.info("Starting Kafka container with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info("Kafka container started: %s", container.container_id[:12])
    return container


def _stop_kafka(container: ManagedContainer) -> None:
    """Stop and remove Kafka container."""
    logger.info("Stopping Kafka container: %s", container.container_id[:12])
    result = container.stop()
    if result.returncode != 0:
        logger.warning(
            "Failed to stop Kafka container %s: %s",
            container.container_id[:12],
            (result.stderr or result.stdout or "").strip() or "no output",
        )


def _wait_for_kafka(container: ManagedContainer, timeout: float) -> None:
    """Wait for Kafka to become ready."""
    def _probe() -> tuple[bool, dict[str, str]]:
        if not container.is_running():
            logger.debug("Kafka container not running yet")
            return False, {"running": "false"}
        try:
            result = container.exec(
                [
                    "/opt/kafka/bin/kafka-topics.sh",
                    "--bootstrap-server",
                    "localhost:9092",
                    "--list",
                ]
            )
        except ContainerCommandError as exc:
            logger.debug("Kafka readiness probe command failed: %s", exc)
            return False, {"running": "true", "probe_error": str(exc)}
        if result.returncode == 0:
            return True, {"running": "true", "probe_returncode": "0"}
        stderr = result.stderr.strip()
        logger.debug("Kafka not ready yet: %s", stderr)
        return False, {
            "running": "true",
            "probe_returncode": str(result.returncode),
            "probe_stderr": stderr,
        }

    try:
        wait_until_ready(
            _probe,
            timeout_seconds=timeout,
            poll_interval_seconds=KAFKA_HEALTH_CHECK_INTERVAL,
            timeout_context="Kafka startup",
        )
    except ContainerReadinessTimeout as exc:
        raise RuntimeError(str(exc)) from exc
    logger.info("Kafka is ready")


def _create_topic(container: ManagedContainer, topic: str, partitions: int) -> None:
    """Create topic for tests."""
    result = container.exec(
        [
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            str(partitions),
            "--replication-factor",
            "1",
        ],
    )
    if result.returncode != 0:
        raise RuntimeError(f"Failed to create topic: {result.stderr}")


def _seed_topic_with_console_producer(
    container: ManagedContainer,
    topic: str,
    count: int,
    compression: str,
) -> None:
    """Seed a topic with deterministic messages via kafka-console-producer."""
    cmd = [
        "/opt/kafka/bin/kafka-console-producer.sh",
        "--bootstrap-server",
        "localhost:9092",
        "--topic",
        topic,
    ]
    if compression != "none":
        cmd.extend(["--producer-property", f"compression.type={compression}"])
    payload = "".join(f"message-{index:04d}\n" for index in range(1, count + 1))
    result = container.exec(cmd, input=payload)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to seed topic: {result.stderr}")


def _seed_topic_with_perf_producer(
    container: ManagedContainer,
    topic: str,
    count: int,
    compression: str,
) -> None:
    """Seed a topic quickly from a payload sample via producer-perf-test."""
    cmd = [
        "/opt/kafka/bin/kafka-producer-perf-test.sh",
        "--topic",
        topic,
        "--num-records",
        str(count),
        "--payload-file",
        "/tmp/tenzir-payload.log",
    ]
    cmd.extend(
        [
            "--throughput",
            "-1",
            "--producer-props",
            "bootstrap.servers=localhost:9092",
            "acks=1",
            "linger.ms=20",
            "batch.size=262144",
            f"compression.type={compression}",
        ]
    )
    result = container.exec(cmd)
    if result.returncode != 0:
        raise RuntimeError(f"Failed to seed topic: {result.stderr}")


def _seed_topic(
    container: ManagedContainer,
    topic: str,
    messages: int,
    compression: str,
    payload_file: str,
) -> None:
    """Seed a topic from a sample file or deterministic fallback messages."""
    if messages <= 0:
        return
    if payload_file:
        _seed_topic_with_perf_producer(
            container,
            topic,
            messages,
            compression,
        )
    else:
        _seed_topic_with_console_producer(container, topic, messages, compression)


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
    try:
        result = subprocess.run(
            cmd,
            cwd=HELPER_DIR,
            env=env,
            text=True,
            capture_output=True,
            timeout=HELPER_BUILD_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        summary = _summarize_build_output(exc.stdout, exc.stderr)
        raise FixtureUnavailable(
            "kafka_iam_mock helper build timed out "
            f"after {HELPER_BUILD_TIMEOUT}s ({summary})"
        ) from exc
    if result.returncode != 0:
        stderr = result.stderr.strip() or "<no stderr>"
        stdout = result.stdout.strip() or "<no stdout>"
        if _is_prerequisite_build_failure(result.stdout, result.stderr):
            summary = _summarize_prerequisite_build_failure(result.stdout, result.stderr)
            raise FixtureUnavailable(
                "kafka_iam_mock helper prerequisites are unavailable"
                f" ({summary})"
            )
        raise RuntimeError(
            "failed to build kafka_iam_mock helper\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )


def _is_prerequisite_build_failure(stdout: str, stderr: str) -> bool:
    combined = f"{stdout}\n{stderr}"
    lowered = combined.lower()
    return any(marker in lowered for marker in GO_BUILD_PREREQUISITE_MARKERS)


def _summarize_prerequisite_build_failure(stdout: str, stderr: str) -> str:
    candidates = _collect_non_empty_output_lines(stdout, stderr)
    for line in candidates:
        lowered = line.lower()
        if any(marker in lowered for marker in GO_BUILD_PREREQUISITE_MARKERS):
            return line
    return candidates[-1] if candidates else "unknown go build prerequisite failure"


def _summarize_build_output(stdout: object, stderr: object) -> str:
    candidates = _collect_non_empty_output_lines(stdout, stderr)
    return candidates[-1] if candidates else "no build output"


def _collect_non_empty_output_lines(stdout: object, stderr: object) -> list[str]:
    chunks: list[str] = []
    for stream in (stderr, stdout):
        if stream is None:
            continue
        if isinstance(stream, bytes):
            chunks.append(stream.decode("utf-8", errors="replace"))
            continue
        chunks.append(str(stream))
    return [line.strip() for line in "\n".join(chunks).splitlines() if line.strip()]


def _start_helper(
    binary_path: Path,
    opts: KafkaOptions,
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
    deadline = time.monotonic() + IAM_STARTUP_TIMEOUT
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
        time.sleep(IAM_HEALTH_CHECK_INTERVAL)
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


def _seed_iam_topic(http_url: str, topic: str, messages: int) -> None:
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
    events = payload.get("events")
    if not isinstance(events, list):
        raise RuntimeError("auth_events endpoint returned invalid events payload")
    if not events:
        if _is_explicit_error_only_run():
            logger.info(
                "kafka_iam_mock observed no auth events in explicit error-only run; "
                "skipping success assertion"
            )
            return
        raise RuntimeError(
            "kafka_iam_mock did not observe any OAUTHBEARER authentication events"
        )
    had_success = bool(payload.get("had_success"))
    if had_success:
        return
    failure_count = int(payload.get("failure_count") or 0)
    reason = ""
    last = events[-1]
    if isinstance(last, dict):
        reason = str(last.get("reason") or "")
    detail = f"last_reason={reason}" if reason else "no reason on last auth event"
    raise RuntimeError(
        "kafka_iam_mock did not observe a successful OAUTHBEARER authentication "
        f"(failures={failure_count}, {detail})"
    )


def _is_explicit_error_only_run() -> bool:
    ctx = current_context()
    if ctx is None:
        return False
    if not bool(ctx.config.get("error")):
        return False
    return ctx.test.stem.startswith("error_")


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


@fixture(options=KafkaOptions)
def kafka() -> Iterator[dict[str, str]]:
    """Start Kafka and yield environment variables for broker access."""
    opts = current_options("kafka")
    mode = opts.mode.lower()
    if mode not in ALLOWED_MODES:
        allowed = ", ".join(sorted(ALLOWED_MODES))
        raise RuntimeError(f"kafka fixture option `mode` must be one of: {allowed}")
    if opts.partitions < 1:
        raise RuntimeError("kafka fixture option `partitions` must be >= 1")
    if opts.messages < 0:
        raise RuntimeError("kafka fixture option `messages` must be >= 0")
    if not opts.topic:
        raise RuntimeError("kafka fixture option `topic` must not be empty")

    compression = opts.compression.lower()

    if mode == "plain":
        runtime = detect_runtime()
        if runtime is None:
            raise FixtureUnavailable(
                "container runtime (docker/podman) required but not found"
            )
        if compression not in ALLOWED_COMPRESSION_TYPES:
            allowed = ", ".join(sorted(ALLOWED_COMPRESSION_TYPES))
            raise RuntimeError(
                f"kafka fixture option `compression` must be one of: {allowed}"
            )
        if opts.payload_file:
            payload_path = Path(opts.payload_file)
            if not payload_path.is_file():
                raise RuntimeError(
                    "kafka fixture option `payload_file` must reference a file"
                )
            payload_file = str(payload_path.resolve())
        else:
            payload_file = ""

        port = find_free_port()
        container: ManagedContainer | None = None
        try:
            container = _start_kafka(runtime, port, opts.image, payload_file)
            _wait_for_kafka(container, KAFKA_STARTUP_TIMEOUT)
            _create_topic(container, opts.topic, opts.partitions)
            _seed_topic(
                container,
                opts.topic,
                opts.messages,
                compression,
                payload_file,
            )
            bootstrap = f"127.0.0.1:{port}"
            env: dict[str, str] = {
                "KAFKA_HOST": "127.0.0.1",
                "KAFKA_PORT": str(port),
                "KAFKA_BOOTSTRAP_SERVERS": bootstrap,
                "KAFKA_TOPIC": opts.topic,
                "KAFKA_CONTAINER_ID": container.container_id,
                "KAFKA_CONTAINER_RUNTIME": runtime.binary,
            }
            yield env
        finally:
            if container is not None:
                _stop_kafka(container)
        return

    if compression != "none":
        raise RuntimeError(
            "kafka fixture option `compression` must be `none` in `aws_iam` mode"
        )
    if opts.payload_file:
        raise RuntimeError(
            "kafka fixture option `payload_file` is not supported in `aws_iam` mode"
        )
    if not opts.aws_region:
        raise RuntimeError("kafka fixture option `aws_region` must not be empty")
    if not opts.access_key_id:
        raise RuntimeError("kafka fixture option `access_key_id` must not be empty")

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
            _seed_iam_topic(http_url, opts.topic, opts.messages)
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
