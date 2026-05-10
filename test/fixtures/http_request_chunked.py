from __future__ import annotations

import base64
import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import current_options

from ._utils import find_free_port

_HOST = "127.0.0.1"


@dataclass(frozen=True)
class ChunkedHttpRequestOptions:
    initial_delay: float = 0.5
    retry_delay: float = 0.1
    request_timeout: float = 0.2
    max_attempts_per_request: int = 15
    inter_request_delay: float = 0.0
    requests: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class _RequestSpec:
    method: str
    path: str
    headers: dict[str, str]
    body_chunks: list[bytes]
    body_chunk_delay: float
    expected_status: int
    stop_on_connection_drop: bool


def _make_request_spec(entry: dict[str, Any]) -> _RequestSpec:
    method = str(entry.get("method", "POST"))
    path = str(entry.get("path", "/"))
    headers = dict(entry.get("headers", {}))
    expected_status = int(entry.get("expected_status", 200))
    body_chunk_delay = float(entry.get("body_chunk_delay", 0.0))
    stop_on_connection_drop = bool(entry.get("stop_on_connection_drop", False))
    body_chunks_base64 = entry.get("body_chunks_base64")
    if body_chunks_base64 is not None:
        body_chunks = [base64.b64decode(chunk) for chunk in body_chunks_base64]
    elif (body_base64 := entry.get("body_base64")) is not None:
        body_chunks = [base64.b64decode(body_base64)]
    else:
        body = str(entry.get("body", ""))
        body_chunks = [body.encode("utf-8")]
    return _RequestSpec(
        method=method,
        path=path,
        headers=headers,
        body_chunks=body_chunks,
        body_chunk_delay=body_chunk_delay,
        expected_status=expected_status,
        stop_on_connection_drop=stop_on_connection_drop,
    )


def _parse_status(response: bytes) -> int | None:
    status_line, _, _ = response.partition(b"\r\n")
    parts = status_line.decode("ascii", errors="replace").split()
    if len(parts) < 2:
        return None
    try:
        return int(parts[1])
    except ValueError:
        return None


@fixture(name="http_request_chunked", options=ChunkedHttpRequestOptions)
def http_request_chunked() -> FixtureHandle:
    opts = current_options("http_request_chunked")
    if not isinstance(opts, ChunkedHttpRequestOptions):
        raise RuntimeError("http_request_chunked fixture options failed to parse")
    if not opts.requests:
        raise RuntimeError("http_request_chunked requires at least one request")
    port = find_free_port()
    endpoint = f"{_HOST}:{port}"
    request_specs = [_make_request_spec(entry) for entry in opts.requests]
    errors: list[str] = []
    sent_count = [0]
    stopped_early = [False]
    stop_event = threading.Event()
    first_request_at = time.monotonic() + opts.initial_delay

    def _send_request(spec: _RequestSpec) -> int | None:
        with socket.create_connection(
            (_HOST, port), timeout=opts.request_timeout
        ) as sock:
            sock.settimeout(opts.request_timeout)
            headers = {
                "Host": endpoint,
                "Connection": "close",
                "Transfer-Encoding": "chunked",
                **spec.headers,
            }
            request_head = (
                f"{spec.method} {spec.path} HTTP/1.1\r\n"
                + "".join(f"{name}: {value}\r\n" for name, value in headers.items())
                + "\r\n"
            ).encode("ascii")
            sock.sendall(request_head)
            send_failed = False
            for i, chunk in enumerate(spec.body_chunks):
                if i > 0 and spec.body_chunk_delay > 0:
                    time.sleep(spec.body_chunk_delay)
                try:
                    sock.sendall(f"{len(chunk):X}\r\n".encode("ascii"))
                    if chunk:
                        sock.sendall(chunk)
                    sock.sendall(b"\r\n")
                except OSError:
                    send_failed = True
                    break
            if not send_failed:
                try:
                    sock.sendall(b"0\r\n\r\n")
                except OSError:
                    pass
            try:
                sock.shutdown(socket.SHUT_WR)
            except OSError:
                pass
            response = bytearray()
            while True:
                try:
                    chunk = sock.recv(4096)
                except socket.timeout:
                    break
                except OSError:
                    break
                if not chunk:
                    break
                response.extend(chunk)
            return _parse_status(bytes(response))

    def _wait_until_ready() -> bool:
        while not stop_event.is_set():
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(opts.request_timeout)
                    if sock.connect_ex((_HOST, port)) == 0:
                        return True
            except OSError:
                pass
            stop_event.wait(opts.retry_delay)
        return False

    def _worker() -> None:
        if not _wait_until_ready():
            return
        remaining_initial_delay = first_request_at - time.monotonic()
        if remaining_initial_delay > 0:
            stop_event.wait(remaining_initial_delay)
        for spec in request_specs:
            if stop_event.is_set():
                return
            sent = False
            last_error: str | None = None
            for _attempt in range(max(opts.max_attempts_per_request, 1)):
                if stop_event.is_set():
                    return
                try:
                    status = _send_request(spec)
                    if status is None and spec.stop_on_connection_drop:
                        stopped_early[0] = True
                        return
                    if status != spec.expected_status:
                        errors.append(
                            f"expected HTTP status {spec.expected_status}, got {status}"
                        )
                    sent_count[0] += 1
                    sent = True
                    break
                except OSError as exc:
                    last_error = str(exc)
                    if (
                        spec.stop_on_connection_drop
                        and "connection refused" not in last_error.lower()
                    ):
                        stopped_early[0] = True
                        return
                    stop_event.wait(opts.retry_delay)
            if not sent:
                errors.append(
                    f"failed to deliver chunked HTTP request to {endpoint}: {last_error}"
                )
                return
            if opts.inter_request_delay > 0:
                stop_event.wait(opts.inter_request_delay)

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
                "http_request_chunked fixture worker did not stop within 2 seconds"
            )
        if errors:
            raise AssertionError("; ".join(errors))
        if sent_count[0] < len(request_specs) and not stopped_early[0]:
            raise AssertionError(
                f"expected to send {len(request_specs)} requests, sent {sent_count[0]}"
            )

    return FixtureHandle(
        env={"HTTP_REQUEST_ENDPOINT": endpoint},
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
