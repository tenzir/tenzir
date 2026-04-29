"""NATS fixture for integration testing.

Starts a containerized NATS server with JetStream enabled and creates a stream
through a separate NATS CLI container.

Environment variables yielded:
- NATS_URL: Host URL for the NATS server.
- NATS_SUBJECT: Default subject used by tests.
- NATS_STREAM: JetStream stream name.
- NATS_DURABLE: Durable consumer name for tests.
- NATS_CONTAINER_ID: Server container ID for in-fixture helpers/scripts.
- NATS_CONTAINER_RUNTIME: Container runtime used (docker/podman).
"""

from __future__ import annotations

import logging
import socket
import subprocess
import time
import uuid
from dataclasses import dataclass
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options
from tenzir_test.fixtures.container_runtime import (
    ContainerCommandError,
    ManagedContainer,
    RuntimeSpec,
    detect_runtime,
    start_detached,
)

from ._utils import find_free_port

logger = logging.getLogger(__name__)

NATS_IMAGE = "nats:2.12-alpine"
NATS_BOX_IMAGE = "natsio/nats-box:0.18.0"
NATS_STARTUP_TIMEOUT = 30
NATS_HEALTH_CHECK_INTERVAL = 0.2


@dataclass(frozen=True)
class NatsOptions:
    subject: str = "tenzir.test"
    stream: str = "TENZIR_TEST"
    durable: str = "tenzir_test"
    messages: int = 0
    image: str = NATS_IMAGE
    cli_image: str = NATS_BOX_IMAGE


def _start_nats(runtime: RuntimeSpec, port: int, image: str) -> ManagedContainer:
    container_name = f"tenzir-test-nats-{uuid.uuid4().hex[:8]}"
    run_args = [
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{port}:4222",
        image,
        "-js",
        "-sd",
        "/tmp/nats/jetstream",
        "-p",
        "4222",
    ]
    logger.info("Starting NATS container with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info("NATS container started: %s", container.container_id[:12])
    return container


def _stop_nats(container: ManagedContainer) -> None:
    logger.info("Stopping NATS container: %s", container.container_id[:12])
    result = container.stop()
    if result.returncode != 0:
        logger.warning(
            "Failed to stop NATS container %s: %s",
            container.container_id[:12],
            (result.stderr or result.stdout or "").strip() or "no output",
        )


def _wait_for_nats(port: int, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=1) as sock:
                sock.recv(16)
                return
        except OSError as exc:
            last_error = exc
        time.sleep(NATS_HEALTH_CHECK_INTERVAL)
    detail = f": {last_error}" if last_error else ""
    raise RuntimeError(f"timed out waiting for NATS startup{detail}")


def _run_nats_cli(
    runtime: RuntimeSpec,
    container: ManagedContainer,
    cli_image: str,
    args: list[str],
) -> None:
    cmd = [
        runtime.binary,
        "run",
        "--rm",
        "--network",
        f"container:{container.container_id}",
        cli_image,
        "nats",
        "--server",
        "nats://127.0.0.1:4222",
        *args,
    ]
    result = subprocess.run(cmd, text=True, capture_output=True)
    if result.returncode != 0:
        stderr = result.stderr.strip() or "<no stderr>"
        stdout = result.stdout.strip() or "<no stdout>"
        raise RuntimeError(
            "failed to run NATS CLI command\n"
            f"command: {' '.join(cmd)}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )


def _create_stream(
    runtime: RuntimeSpec,
    container: ManagedContainer,
    opts: NatsOptions,
) -> None:
    _run_nats_cli(
        runtime,
        container,
        opts.cli_image,
        [
            "stream",
            "add",
            opts.stream,
            "--subjects",
            opts.subject,
            "--storage",
            "file",
            "--retention",
            "limits",
            "--discard",
            "old",
            "--defaults",
        ],
    )


def _seed_stream(
    runtime: RuntimeSpec,
    container: ManagedContainer,
    opts: NatsOptions,
) -> None:
    for index in range(1, opts.messages + 1):
        _run_nats_cli(
            runtime,
            container,
            opts.cli_image,
            ["pub", opts.subject, f"message-{index:04d}"],
        )


@fixture(options=NatsOptions)
def nats() -> Iterator[dict[str, str]]:
    """Start NATS with JetStream and yield environment variables."""
    opts = current_options("nats")
    if not opts.subject:
        raise RuntimeError("nats fixture option `subject` must not be empty")
    if not opts.stream:
        raise RuntimeError("nats fixture option `stream` must not be empty")
    if not opts.durable:
        raise RuntimeError("nats fixture option `durable` must not be empty")
    if opts.messages < 0:
        raise RuntimeError("nats fixture option `messages` must be >= 0")

    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )

    port = find_free_port()
    container: ManagedContainer | None = None
    try:
        try:
            container = _start_nats(runtime, port, opts.image)
        except ContainerCommandError as exc:
            raise FixtureUnavailable(f"failed to start NATS container: {exc}") from exc
        _wait_for_nats(port, NATS_STARTUP_TIMEOUT)
        _create_stream(runtime, container, opts)
        _seed_stream(runtime, container, opts)
        env: dict[str, str] = {
            "NATS_URL": f"nats://127.0.0.1:{port}",
            "NATS_SUBJECT": opts.subject,
            "NATS_STREAM": opts.stream,
            "NATS_DURABLE": opts.durable,
            "NATS_CONTAINER_ID": container.container_id,
            "NATS_CONTAINER_RUNTIME": runtime.binary,
        }
        yield env
    finally:
        if container is not None:
            _stop_nats(container)
