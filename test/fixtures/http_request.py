from __future__ import annotations

import shutil
import socket
import ssl
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin
from urllib.request import Request, urlopen

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import current_options

from ._utils import find_free_port, generate_self_signed_cert

_HOST = "127.0.0.1"


@dataclass(frozen=True)
class HttpRequestOptions:
    generate_tls_cert: bool = False

    # Single-request mode (backwards-compatible with the former http_client shape).
    method: str = "POST"
    path: str = "/"
    body: str = '{"value":1}\n'
    headers: dict[str, str] = field(
        default_factory=lambda: {"Content-Type": "application/json"}
    )
    tls: bool = False
    repeat: int = 1
    expected_status: int | None = None
    expected_body: str | None = None
    stop_on_connection_drop: bool = False

    # Retry/dispatch behavior.
    initial_delay: float = 0.5
    retry_delay: float = 0.1
    request_timeout: float = 0.2
    max_attempts_per_request: int = 15
    inter_request_delay: float = 0.0

    # Multi-request mode.
    # Each entry can override request-level fields.
    requests: list[dict] | None = None


@dataclass(frozen=True)
class _RequestSpec:
    method: str
    path: str
    body: str
    headers: dict[str, str]
    tls: bool
    expected_status: int | None
    expected_body: str | None
    stop_on_connection_drop: bool


def _to_request_specs(opts: HttpRequestOptions) -> list[_RequestSpec]:
    if opts.requests is not None:
        specs: list[_RequestSpec] = []
        for entry in opts.requests:
            specs = specs + _to_request_specs(HttpRequestOptions(**entry))
        return specs
    return [
        _RequestSpec(
            method=opts.method,
            path=opts.path,
            body=opts.body,
            headers=dict(opts.headers),
            tls=opts.tls,
            expected_status=opts.expected_status,
            expected_body=opts.expected_body,
            stop_on_connection_drop=opts.stop_on_connection_drop,
        )
        for _ in range(opts.repeat)
    ]


@fixture(name="http_request", options=HttpRequestOptions)
def http_request() -> FixtureHandle:
    opts = current_options("http_request")
    if not isinstance(opts, HttpRequestOptions):
        raise RuntimeError("http_request fixture options failed to parse")
    port = find_free_port()
    endpoint = f"{_HOST}:{port}"
    errors: list[str] = []
    sent_count = [0]
    stopped_early = [False]
    stop_event = threading.Event()
    tls_dir: Path | None = None
    tls_cert_and_key: Path | None = None
    tls_ca: Path | None = None
    if opts.generate_tls_cert:
        tls_dir = Path(tempfile.mkdtemp(prefix="http-request-tls-"))
        _cert, _key, tls_ca, tls_cert_and_key = generate_self_signed_cert(
            tls_dir, common_name=endpoint, san_entries=[f"IP:{_HOST}"]
        )
    request_specs = _to_request_specs(opts)
    first_request_at = time.monotonic() + opts.initial_delay

    def _wait_until_ready(*, tls: bool) -> bool:
        while not stop_event.is_set():
            try:
                with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                    sock.settimeout(opts.request_timeout)
                    if not tls:
                        if sock.connect_ex((_HOST, port)) == 0:
                            return True
                        stop_event.wait(opts.retry_delay)
                        continue
                    if tls_ca is not None:
                        context = ssl.create_default_context(cafile=str(tls_ca))
                    else:
                        context = ssl.create_default_context()
                        context.check_hostname = False
                        context.verify_mode = ssl.CERT_NONE
                    with context.wrap_socket(sock, server_hostname=_HOST) as tls_sock:
                        tls_sock.connect((_HOST, port))
                        return True
            except (OSError, ssl.SSLError):
                pass
            stop_event.wait(opts.retry_delay)
        return False

    def _worker() -> None:
        if not _wait_until_ready(tls=bool(request_specs and request_specs[0].tls)):
            return
        remaining_initial_delay = first_request_at - time.monotonic()
        if remaining_initial_delay > 0:
            stop_event.wait(remaining_initial_delay)
        for req_idx, spec in enumerate(request_specs):
            if stop_event.is_set():
                stopped_early[0] = True
                return
            proto = "https" if spec.tls else "http"
            target_url = urljoin(f"{proto}://{endpoint}/", spec.path.lstrip("/"))
            payload = spec.body.encode("utf-8")
            headers = dict(spec.headers)
            sent = False
            ssl_context: ssl.SSLContext | None = None
            if spec.tls and tls_dir and tls_ca is not None:
                ssl_context = ssl.create_default_context(cafile=str(tls_ca))
            connection_refused_error: str | None = None
            for _ in range(max(opts.max_attempts_per_request, 1)):
                if stop_event.is_set():
                    stopped_early[0] = True
                    return
                req = Request(
                    target_url, data=payload, method=spec.method, headers=headers
                )
                try:
                    with urlopen(
                        req, timeout=opts.request_timeout, context=ssl_context
                    ) as response:
                        body = response.read().decode("utf-8", errors="replace")
                        if (
                            spec.expected_status != None
                            and response.status != spec.expected_status
                        ):
                            errors.append(
                                f"request {req_idx}: expected HTTP status "
                                f"{spec.expected_status}, got {response.status}"
                            )
                        if (
                            spec.expected_body is not None
                            and body != spec.expected_body
                        ):
                            errors.append(
                                f"request {req_idx}: expected HTTP body "
                                f"{spec.expected_body!r}, got {body!r}"
                            )
                        sent_count[0] += 1
                        sent = True
                        break
                except HTTPError as exc:
                    body = exc.read().decode("utf-8", errors="replace")
                    if (
                        spec.expected_status != None
                        and exc.code != spec.expected_status
                    ):
                        errors.append(
                            f"request {req_idx}: expected HTTP status "
                            f"{spec.expected_status}, got {exc.code}"
                        )
                    if spec.expected_body is not None and body != spec.expected_body:
                        errors.append(
                            f"request {req_idx}: expected HTTP body "
                            f"{spec.expected_body!r}, got {body!r}"
                        )
                    sent_count[0] += 1
                    sent = True
                    break
                except (URLError, OSError) as exc:
                    exc_str = str(exc)
                    if "connection refused" in exc_str.lower():
                        connection_refused_error = exc_str
                    elif spec.stop_on_connection_drop:
                        stopped_early[0] = True
                        return
                    stop_event.wait(opts.retry_delay)
            if not sent:
                attempts = max(opts.max_attempts_per_request, 1)
                if connection_refused_error is not None:
                    errors.append(
                        "connection refused while delivering HTTP request to "
                        f"{target_url} after {attempts} attempts: "
                        f"{connection_refused_error}"
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
        try:
            if worker.is_alive():
                raise RuntimeError(
                    "http_request fixture worker did not stop within 2 seconds"
                )
            if errors:
                raise AssertionError("; ".join(errors))
            expected_count = len(request_specs)
            if not stopped_early[0] and sent_count[0] < expected_count:
                raise AssertionError(
                    f"expected to send {expected_count} requests, sent {sent_count[0]}"
                )
        finally:
            if tls_dir is not None:
                shutil.rmtree(tls_dir, ignore_errors=True)

    env = {
        "HTTP_REQUEST_ENDPOINT": endpoint,
    }
    if tls_cert_and_key is not None:
        env["HTTP_REQUEST_TLS_CERTFILE"] = str(tls_cert_and_key)
    if tls_ca is not None:
        env["HTTP_REQUEST_TLS_CAFILE"] = str(tls_ca)

    return FixtureHandle(
        env=env,
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
