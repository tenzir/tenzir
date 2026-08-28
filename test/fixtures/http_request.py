from __future__ import annotations

import base64
import json
import shutil
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
    body_base64: str | None = None
    json_body: Any | None = None
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
    delay_before: float = 0.0

    # Capture values from a JSON response for `from_capture` references in
    # later structured JSON request bodies.
    capture_json: dict[str, str] = field(default_factory=dict)

    # Multi-request mode.
    # Each entry can override request-level fields.
    requests: list[dict] | None = None


@dataclass(frozen=True)
class HttpRequestAssertions:
    responses: list[dict[str, Any]] = field(default_factory=list)


@dataclass(frozen=True)
class _RequestSpec:
    method: str
    path: str
    body: bytes
    json_body: Any | None
    headers: dict[str, str]
    tls: bool
    expected_status: int | None
    expected_body: str | None
    stop_on_connection_drop: bool
    delay_before: float
    capture_json: dict[str, str]


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
            body=(
                base64.b64decode(opts.body_base64)
                if opts.body_base64 is not None
                else opts.body.encode("utf-8")
            ),
            json_body=opts.json_body,
            headers=dict(opts.headers),
            tls=opts.tls,
            expected_status=opts.expected_status,
            expected_body=opts.expected_body,
            stop_on_connection_drop=opts.stop_on_connection_drop,
            delay_before=opts.delay_before,
            capture_json=dict(opts.capture_json),
        )
        for _ in range(opts.repeat)
    ]


def _resolve_captures(value: Any, captures: dict[str, Any]) -> Any:
    if isinstance(value, dict):
        if set(value) == {"from_capture"}:
            name = value["from_capture"]
            if not isinstance(name, str):
                raise TypeError("`from_capture` must name a captured value")
            if name not in captures:
                raise ValueError(f"unknown captured value: {name!r}")
            return captures[name]
        return {key: _resolve_captures(item, captures) for key, item in value.items()}
    if isinstance(value, list):
        return [_resolve_captures(item, captures) for item in value]
    return value


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON object key: {key!r}")
        result[key] = value
    return result


def _json_path(value: Any, path: str) -> Any:
    current = value
    for segment in path.split("."):
        if not isinstance(current, dict) or segment not in current:
            raise ValueError(f"JSON response has no value at {path!r}")
        current = current[segment]
    return current


@fixture(
    name="http_request", options=HttpRequestOptions, assertions=HttpRequestAssertions
)
def http_request() -> FixtureHandle:
    opts = current_options("http_request")
    if not isinstance(opts, HttpRequestOptions):
        raise TypeError("http_request fixture options failed to parse")
    port = find_free_port()
    endpoint = f"{_HOST}:{port}"
    errors: list[str] = []
    responses: dict[int, tuple[int, str]] = {}
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
    variables: dict[str, Any] = {}

    def _capture_response(spec: _RequestSpec, body: str) -> None:
        if not spec.capture_json:
            return
        parsed = json.loads(body, object_pairs_hook=_unique_json_object)
        for name, path in spec.capture_json.items():
            variables[name] = _json_path(parsed, path)

    def _worker() -> None:
        remaining_initial_delay = first_request_at - time.monotonic()
        if remaining_initial_delay > 0:
            stop_event.wait(remaining_initial_delay)
        for req_idx, spec in enumerate(request_specs):
            if spec.delay_before > 0:
                stop_event.wait(spec.delay_before)
            if stop_event.is_set():
                stopped_early[0] = True
                return
            proto = "https" if spec.tls else "http"
            target_url = urljoin(f"{proto}://{endpoint}/", spec.path.lstrip("/"))
            payload = spec.body
            headers = dict(spec.headers)
            if spec.json_body is not None:
                try:
                    resolved = _resolve_captures(spec.json_body, variables)
                except (TypeError, ValueError) as exc:
                    errors.append(f"request {req_idx}: {exc}")
                    return
                payload = json.dumps(resolved, separators=(",", ":")).encode()
                headers.setdefault("Content-Type", "application/json")
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
                        responses[req_idx] = (response.status, body)
                        try:
                            _capture_response(spec, body)
                        except (json.JSONDecodeError, ValueError) as exc:
                            errors.append(f"request {req_idx}: {exc}")
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
                    responses[req_idx] = (exc.code, body)
                    try:
                        _capture_response(spec, body)
                    except (json.JSONDecodeError, ValueError) as capture_error:
                        errors.append(f"request {req_idx}: {capture_error}")
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
        *,
        test: Path,
        assertions: HttpRequestAssertions | dict[str, Any],
        **_: Any,
    ) -> None:
        config = (
            assertions
            if isinstance(assertions, HttpRequestAssertions)
            else HttpRequestAssertions(**assertions)
        )
        worker.join(timeout=2)
        if worker.is_alive():
            raise AssertionError(f"{test.name}: HTTP requests did not finish")
        for response_assertion in config.responses:
            request_index = response_assertion.get("request")
            if not isinstance(request_index, int):
                raise TypeError("response assertion requires an integer `request`")
            if request_index not in responses:
                raise AssertionError(
                    f"{test.name}: request {request_index} has no response"
                )
            status, body = responses[request_index]
            expected_status = response_assertion.get("status")
            if expected_status is not None and status != expected_status:
                raise AssertionError(
                    f"{test.name}: request {request_index} expected HTTP status "
                    f"{expected_status}, got {status}"
                )
            expected_body = response_assertion.get("body")
            if expected_body is not None and body != expected_body:
                raise AssertionError(
                    f"{test.name}: request {request_index} expected HTTP body "
                    f"{expected_body!r}, got {body!r}"
                )
            json_assertions = response_assertion.get("json", [])
            if not json_assertions:
                continue
            parsed = json.loads(body, object_pairs_hook=_unique_json_object)
            for json_assertion in json_assertions:
                path = json_assertion.get("path", "")
                actual = _json_path(parsed, path) if path else parsed
                capture_name = json_assertion.get("key_from_capture")
                if capture_name is not None:
                    if capture_name not in variables:
                        raise ValueError(f"unknown captured value: {capture_name!r}")
                    key = str(variables[capture_name])
                    if not isinstance(actual, dict) or key not in actual:
                        raise AssertionError(
                            f"{test.name}: request {request_index} JSON value "
                            f"at {path!r} has no key {key!r}"
                        )
                    actual = actual[key]
                if "equals" in json_assertion and actual != json_assertion["equals"]:
                    raise AssertionError(
                        f"{test.name}: request {request_index} JSON value at "
                        f"{path!r} expected {json_assertion['equals']!r}, "
                        f"got {actual!r}"
                    )
                expected_type = json_assertion.get("type")
                if expected_type == "integer" and (
                    not isinstance(actual, int) or isinstance(actual, bool)
                ):
                    raise AssertionError(
                        f"{test.name}: request {request_index} JSON value at "
                        f"{path!r} expected an integer, got {actual!r}"
                    )
                minimum = json_assertion.get("min")
                if minimum is not None and actual < minimum:
                    raise AssertionError(
                        f"{test.name}: request {request_index} JSON value at "
                        f"{path!r} expected at least {minimum!r}, got {actual!r}"
                    )
                maximum = json_assertion.get("max")
                if maximum is not None and actual > maximum:
                    raise AssertionError(
                        f"{test.name}: request {request_index} JSON value at "
                        f"{path!r} expected at most {maximum!r}, got {actual!r}"
                    )

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
