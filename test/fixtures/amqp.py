"""RabbitMQ fixture for AMQP integration testing.

Starts a containerized RabbitMQ broker with the management plugin enabled.

Options accepted under ``fixtures: [{amqp: {...}}]``:
- queue: Name of the queue that the test works with (exported as AMQP_QUEUE).
- declare: Declare `queue` as durable queue and bind it to `amq.direct` with
  the queue name as routing key. Without it, the pipeline under test is
  expected to declare the queue.
- messages: Number of messages `message-0001`, `message-0002`, ... to publish
  into `queue`. The fixture publishes them as soon as the queue exists, which
  is either at startup with `declare` or once the pipeline declared it.

Assertions payload accepted under ``assertions.fixtures.amqp``:
- queue.type: Expected queue type, e.g., `classic` or `quorum`.
- queue.durable: Expected durability of the queue.
- queue.auto_delete: Expected auto-delete flag of the queue.
- queue.arguments: Arguments that the queue must have been declared with.

Environment variables yielded:
- AMQP_URL: AMQP URL for the test user and vhost.
- AMQP_HOST: Broker hostname (127.0.0.1).
- AMQP_PORT: AMQP port exposed on the host (dynamically allocated).
- AMQP_MANAGEMENT_URL: Management API base URL.
- AMQP_USER: Test user.
- AMQP_PASSWORD: Test password.
- AMQP_VHOST: Test virtual host.
- AMQP_QUEUE: The `queue` option.
- AMQP_CONTAINER_ID: Container ID for in-fixture helpers/scripts.
- AMQP_CONTAINER_RUNTIME: Container runtime used (docker/podman).
"""

from __future__ import annotations

import json
import logging
import threading
import urllib.error
import urllib.parse
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options
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

RABBITMQ_IMAGE = "rabbitmq:4-management"
RABBITMQ_USER = "tenzir"
RABBITMQ_PASSWORD = "tenzir"
RABBITMQ_VHOST = "/"
STARTUP_TIMEOUT = 90
HEALTH_CHECK_INTERVAL = 1
QUEUE_POLL_INTERVAL = 0.1


@dataclass(frozen=True)
class AmqpOptions:
    image: str = RABBITMQ_IMAGE
    queue: str = ""
    declare: bool = False
    messages: int = 0


@dataclass(frozen=True)
class AmqpQueueAssertions:
    type: str | None = None
    durable: bool | None = None
    auto_delete: bool | None = None
    arguments: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AmqpAssertions:
    queue: AmqpQueueAssertions | None = None


def _start_rabbitmq(
    runtime: RuntimeSpec,
    amqp_port: int,
    management_port: int,
    image: str,
) -> ManagedContainer:
    container_name = f"tenzir-test-amqp-{uuid.uuid4().hex[:8]}"
    run_args = [
        "--rm",
        "--name",
        container_name,
        "-p",
        f"{amqp_port}:5672",
        "-p",
        f"{management_port}:15672",
        "-e",
        f"RABBITMQ_DEFAULT_USER={RABBITMQ_USER}",
        "-e",
        f"RABBITMQ_DEFAULT_PASS={RABBITMQ_PASSWORD}",
        "-e",
        f"RABBITMQ_DEFAULT_VHOST={RABBITMQ_VHOST}",
        image,
    ]
    logger.info("Starting RabbitMQ container with %s", runtime.binary)
    container = start_detached(runtime, run_args)
    logger.info("RabbitMQ container started: %s", container.container_id[:12])
    return container


def _stop_rabbitmq(container: ManagedContainer) -> None:
    logger.info("Stopping RabbitMQ container: %s", container.container_id[:12])
    result = container.stop()
    if result.returncode != 0:
        logger.warning(
            "Failed to stop RabbitMQ container %s: %s",
            container.container_id[:12],
            (result.stderr or result.stdout or "").strip() or "no output",
        )


def _management_request(
    url: str,
    *,
    method: str = "GET",
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    password_manager = urllib.request.HTTPPasswordMgrWithDefaultRealm()
    password_manager.add_password(None, url, RABBITMQ_USER, RABBITMQ_PASSWORD)
    opener = urllib.request.build_opener(
        urllib.request.HTTPBasicAuthHandler(password_manager)
    )
    payload = None
    headers = {}
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=payload, headers=headers, method=method)
    with opener.open(request, timeout=5) as response:
        text = response.read().decode("utf-8")
        return json.loads(text) if text else {}


def _quote(name: str) -> str:
    return urllib.parse.quote(name, safe="")


def _queue_url(management_url: str, queue: str) -> str:
    return f"{management_url}/api/queues/{_quote(RABBITMQ_VHOST)}/{_quote(queue)}"


def _wait_for_rabbitmq(management_url: str, timeout: float) -> None:
    def _probe() -> tuple[bool, dict[str, str]]:
        try:
            overview = _management_request(f"{management_url}/api/overview")
        except (OSError, urllib.error.URLError, json.JSONDecodeError) as exc:
            return False, {"error": str(exc)}
        listeners = overview.get("listeners", [])
        return bool(listeners), {"listeners": str(len(listeners))}

    try:
        wait_until_ready(
            _probe,
            timeout_seconds=timeout,
            poll_interval_seconds=HEALTH_CHECK_INTERVAL,
            timeout_context="RabbitMQ startup",
        )
    except ContainerReadinessTimeout as exc:
        raise RuntimeError(str(exc)) from exc
    logger.info("RabbitMQ is ready")


def _declare_queue(management_url: str, queue: str) -> None:
    _management_request(
        _queue_url(management_url, queue),
        method="PUT",
        body={"durable": True, "auto_delete": False, "arguments": {}},
    )
    vhost = _quote(RABBITMQ_VHOST)
    _management_request(
        f"{management_url}/api/bindings/{vhost}/e/amq.direct/q/{_quote(queue)}",
        method="POST",
        body={"routing_key": queue, "arguments": {}},
    )


def _publish_messages(management_url: str, queue: str, messages: int) -> None:
    # The default exchange routes by queue name, so the messages reach the
    # queue regardless of the bindings that the pipeline under test creates.
    vhost = _quote(RABBITMQ_VHOST)
    url = f"{management_url}/api/exchanges/{vhost}/amq.default/publish"
    for index in range(1, messages + 1):
        result = _management_request(
            url,
            method="POST",
            body={
                "properties": {},
                "routing_key": queue,
                "payload": f"message-{index:04d}",
                "payload_encoding": "string",
            },
        )
        if not result.get("routed"):
            raise RuntimeError(f"message {index} was not routed to queue {queue}")


def _publish_once_declared(
    management_url: str, queue: str, messages: int, stop: threading.Event
) -> None:
    while not stop.wait(QUEUE_POLL_INTERVAL):
        try:
            _management_request(_queue_url(management_url, queue))
        except urllib.error.HTTPError as exc:
            if exc.code != 404:
                logger.warning("failed to look up queue %s: %s", queue, exc)
                return
            continue
        except (OSError, urllib.error.URLError):
            continue
        try:
            _publish_messages(management_url, queue, messages)
        except (OSError, urllib.error.URLError, RuntimeError) as exc:
            logger.warning("failed to publish into queue %s: %s", queue, exc)
        return


def _verify_queue(
    management_url: str, queue: str, expected: AmqpQueueAssertions
) -> list[str]:
    try:
        info = _management_request(_queue_url(management_url, queue))
    except urllib.error.HTTPError as exc:
        return [f"failed to look up queue {queue}: {exc}"]
    problems = []
    for key in ("type", "durable", "auto_delete"):
        want = getattr(expected, key)
        if want is not None and info.get(key) != want:
            problems.append(f"expected {key}={want!r}, got {info.get(key)!r}")
    arguments = info.get("arguments", {})
    for key, want in expected.arguments.items():
        if arguments.get(key) != want:
            problems.append(
                f"expected argument {key}={want!r}, got {arguments.get(key)!r}"
            )
    return problems


def _as_assertions(assertions: AmqpAssertions | dict[str, Any]) -> AmqpAssertions:
    if isinstance(assertions, AmqpAssertions):
        return assertions
    queue = assertions.get("queue")
    if isinstance(queue, dict):
        queue = AmqpQueueAssertions(**queue)
    return AmqpAssertions(queue=queue)


@fixture(options=AmqpOptions, assertions=AmqpAssertions, tags=("container",))
def amqp() -> FixtureHandle:
    """Start RabbitMQ and return environment variables for broker access."""
    opts = current_options("amqp")
    if (opts.declare or opts.messages) and not opts.queue:
        raise RuntimeError("amqp fixture options `declare` and `messages` need `queue`")
    if opts.messages < 0:
        raise RuntimeError("amqp fixture option `messages` must be >= 0")
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )
    amqp_port = find_free_port()
    management_port = find_free_port()
    management_url = f"http://127.0.0.1:{management_port}"
    try:
        container = _start_rabbitmq(runtime, amqp_port, management_port, opts.image)
    except ContainerCommandError as exc:
        raise FixtureUnavailable(f"failed to start RabbitMQ container: {exc}") from exc
    stop = threading.Event()
    publisher: threading.Thread | None = None
    try:
        _wait_for_rabbitmq(management_url, STARTUP_TIMEOUT)
        if opts.declare:
            _declare_queue(management_url, opts.queue)
            _publish_messages(management_url, opts.queue, opts.messages)
        elif opts.messages:
            publisher = threading.Thread(
                target=_publish_once_declared,
                args=(management_url, opts.queue, opts.messages, stop),
                daemon=True,
            )
            publisher.start()
    except Exception:
        _stop_rabbitmq(container)
        raise

    def _assert_test(
        *,
        test: Path,
        assertions: AmqpAssertions | dict[str, Any],
        **_: Any,
    ) -> None:
        expected = _as_assertions(assertions).queue
        if expected is None:
            return
        if not opts.queue:
            raise AssertionError(
                f"{test.name}: queue assertions need the `queue` option"
            )
        problems = _verify_queue(management_url, opts.queue, expected)
        if problems:
            raise AssertionError(
                f"{test.name}: queue {opts.queue} does not match:\n"
                + "\n".join(f"  - {problem}" for problem in problems)
            )

    def _teardown() -> None:
        stop.set()
        if publisher is not None:
            publisher.join(timeout=5)
        _stop_rabbitmq(container)

    quoted_vhost = urllib.parse.quote(RABBITMQ_VHOST, safe="")
    return FixtureHandle(
        env={
            "AMQP_URL": (
                f"amqp://{RABBITMQ_USER}:{RABBITMQ_PASSWORD}"
                f"@127.0.0.1:{amqp_port}/{quoted_vhost}"
            ),
            "AMQP_HOST": "127.0.0.1",
            "AMQP_PORT": str(amqp_port),
            "AMQP_MANAGEMENT_URL": management_url,
            "AMQP_USER": RABBITMQ_USER,
            "AMQP_PASSWORD": RABBITMQ_PASSWORD,
            "AMQP_VHOST": RABBITMQ_VHOST,
            "AMQP_QUEUE": opts.queue,
            "AMQP_CONTAINER_ID": container.container_id,
            "AMQP_CONTAINER_RUNTIME": runtime.binary,
        },
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
