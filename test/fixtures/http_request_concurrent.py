from __future__ import annotations

import http.client
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import current_options

from ._utils import find_free_port

_HOST = "127.0.0.1"


@dataclass(frozen=True)
class HttpRequestConcurrentOptions:
    method: str = "POST"
    path: str = "/"
    body: str = '{"value":1}\n'
    content_type: str = "application/json"

    concurrent: int = 2
    expected_statuses: list[int] | None = None

    initial_delay: float = 0.5
    retry_delay: float = 0.1
    request_timeout: float = 5.0
    max_attempts: int = 300


@dataclass(frozen=True)
class _ConcurrentDispatchResult:
    statuses: list[int]
    retryable_connection_refused: str | None = None
    fatal_error: str | None = None


def _is_connection_refused(exc: BaseException) -> bool:
    return "connection refused" in str(exc).lower()


def _normalize_request_path(path: str) -> str:
    if not path:
        return "/"
    if path.startswith("/"):
        return path
    return "/" + path.lstrip("/")


def _dispatch_concurrent_requests(
    opts: HttpRequestConcurrentOptions,
    *,
    host: str,
    port: int,
) -> _ConcurrentDispatchResult:
    payload = opts.body.encode("utf-8")
    path = _normalize_request_path(opts.path)
    headers = {
        "Content-Type": opts.content_type,
        "Content-Length": str(len(payload)),
    }
    connections: list[http.client.HTTPConnection] = []
    try:
        for _ in range(opts.concurrent):
            conn = http.client.HTTPConnection(
                host,
                port,
                timeout=opts.request_timeout,
            )
            conn.connect()
            conn.putrequest(opts.method, path)
            for key, value in headers.items():
                conn.putheader(key, value)
            conn.endheaders()
            connections.append(conn)
    except (OSError, http.client.HTTPException) as exc:
        for conn in connections:
            conn.close()
        if _is_connection_refused(exc):
            return _ConcurrentDispatchResult(
                statuses=[],
                retryable_connection_refused=str(exc),
            )
        return _ConcurrentDispatchResult(
            statuses=[],
            fatal_error=f"failed to prepare concurrent requests: {exc}",
        )

    results: list[int | None] = [None] * len(connections)
    thread_errors: list[str] = []
    errors_lock = threading.Lock()
    start_bodies = threading.Event()

    def _send_body_and_recv(idx: int, conn: http.client.HTTPConnection) -> None:
        send_error: OSError | None = None
        try:
            start_bodies.wait()
            if payload:
                try:
                    conn.send(payload)
                except OSError as exc:
                    send_error = exc
            response = conn.getresponse()
            response.read()
            with errors_lock:
                results[idx] = response.status
        except (OSError, http.client.HTTPException) as exc:
            message = f"failed while reading concurrent HTTP response: {exc}"
            if send_error is not None:
                message = f"{message}; body send failed with: {send_error}"
            with errors_lock:
                thread_errors.append(message)
        finally:
            conn.close()

    threads = [
        threading.Thread(target=_send_body_and_recv, args=(idx, conn), daemon=True)
        for idx, conn in enumerate(connections)
    ]
    for thread in threads:
        thread.start()
    start_bodies.set()
    for thread in threads:
        thread.join()

    if thread_errors:
        return _ConcurrentDispatchResult(
            statuses=[],
            fatal_error="; ".join(thread_errors),
        )

    statuses: list[int] = []
    for result in results:
        if result is None:
            return _ConcurrentDispatchResult(
                statuses=[],
                fatal_error="internal error: missing concurrent HTTP result",
            )
        statuses.append(result)
    return _ConcurrentDispatchResult(statuses=statuses)


@fixture(name="http_request_concurrent", options=HttpRequestConcurrentOptions)
def http_request_concurrent() -> FixtureHandle:
    opts = current_options("http_request_concurrent")
    if not isinstance(opts, HttpRequestConcurrentOptions):
        raise RuntimeError("http_request_concurrent fixture options failed to parse")
    if opts.concurrent < 1:
        raise RuntimeError(
            "`http_request_concurrent.concurrent` must be greater than 0"
        )
    if opts.expected_statuses is None:
        raise RuntimeError("`http_request_concurrent.expected_statuses` is required")
    if len(opts.expected_statuses) != opts.concurrent:
        raise RuntimeError(
            "`http_request_concurrent.expected_statuses` length must equal "
            f"`concurrent` (got {len(opts.expected_statuses)} for {opts.concurrent})"
        )

    port = find_free_port()
    endpoint = f"{_HOST}:{port}"
    errors: list[str] = []
    sent_count = [0]
    stopped_early = [False]
    stop_event = threading.Event()

    def _worker() -> None:
        if opts.initial_delay > 0:
            time.sleep(opts.initial_delay)
        connection_refused_error: str | None = None
        sent = False
        for _attempt in range(max(opts.max_attempts, 1)):
            if stop_event.is_set():
                stopped_early[0] = True
                return
            result = _dispatch_concurrent_requests(
                opts,
                host=_HOST,
                port=port,
            )
            if result.retryable_connection_refused is not None:
                connection_refused_error = result.retryable_connection_refused
                time.sleep(opts.retry_delay)
                continue
            if result.fatal_error is not None:
                errors.append(result.fatal_error)
                return
            got = sorted(result.statuses)
            want = sorted(opts.expected_statuses)
            if got != want:
                errors.append(f"expected concurrent HTTP statuses {want}, got {got}")
            sent_count[0] = len(result.statuses)
            sent = True
            break
        if not sent:
            attempts = max(opts.max_attempts, 1)
            if connection_refused_error is not None:
                errors.append(
                    "connection refused while delivering concurrent HTTP requests to "
                    f"http://{endpoint}{_normalize_request_path(opts.path)} "
                    f"after {attempts} attempts: {connection_refused_error}"
                )

    worker = threading.Thread(target=_worker, daemon=True)
    worker.start()

    def _assert_test(
        *, test: Path, assertions: dict[str, Any] | None = None, **_: Any
    ) -> None:
        _ = (test, assertions)

    def _teardown() -> None:
        stop_event.set()
        worker.join(timeout=2)
        if worker.is_alive():
            raise RuntimeError(
                "http_request_concurrent fixture worker did not stop within 2 seconds"
            )
        if errors:
            raise AssertionError("; ".join(errors))
        if not stopped_early[0] and sent_count[0] < opts.concurrent:
            raise AssertionError(
                f"expected to send {opts.concurrent} requests, sent {sent_count[0]}"
            )

    return FixtureHandle(
        env={"HTTP_REQUEST_ENDPOINT": endpoint},
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
