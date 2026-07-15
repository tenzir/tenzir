"""HTTP echo server fixture for testing client-side HTTP operators.

Usage overview:

- Tests declare ``fixtures: [http_server]`` in their frontmatter to opt in.
- Importing this module registers the ``@fixture`` definition below.
- The fixture starts a temporary HTTP echo server and exports:
  - **HTTP_FIXTURE_URL** - Base URL of the server (``http[s]://127.0.0.1:<port>/``).
  - **HTTP_FIXTURE_METHOD_URL** - Echoes the request method as ``{"method":"…"}``.
  - **HTTP_FIXTURE_METHOD_URL_NO_SCHEME** - Same as above, but without URL scheme.
  - **HTTP_FIXTURE_HEADER_URL** - Echoes the ``X-Test`` header as ``{"x_test":"…"}``.
  - **HTTP_FIXTURE_HOST_URL** - Echoes the ``Host`` header as ``{"host":"…"}``.
  - **HTTP_FIXTURE_BODY_URL** - Echoes the request body as ``{"body":"…"}``.
  - **HTTP_FIXTURE_STATUS_404_URL** - Always replies 404 with ``{"error":"not-found"}``.
  - **HTTP_FIXTURE_STATUS_400_URL** - Always replies 400 with ``{"error":"bad-request"}``.
  - **HTTP_FIXTURE_STATUS_401_URL** - Always replies 401 with ``{"error":"unauthorized"}``.
  - **HTTP_FIXTURE_STATUS_403_URL** - Always replies 403 with ``{"error":"forbidden"}``.
  - **HTTP_FIXTURE_RETRY_429_AFTER_URL** - Replies 429 with ``Retry-After: 1``, then 200 after waiting long enough.
  - **HTTP_FIXTURE_RETRY_503_URL** - Replies 503 twice, then 200.
  - **HTTP_FIXTURE_EARLY_200_URL** - Replies 200 immediately without reading the whole request body.
  - **HTTP_FIXTURE_EARLY_503_URL** - Replies 503 immediately without reading the whole request body.
  - **HTTP_FIXTURE_RETRY_503_BACKOFF_URL** - Replies 503 twice, then 200 only if retries follow the configured backoff.
  - **HTTP_FIXTURE_RETRY_EXHAUSTED_503_URL** - Always replies 503 with ``{"error":"service-unavailable"}``.
  - **HTTP_FIXTURE_GZIP_EMPTY_URL** - Replies with an empty gzip-encoded body.
  - **HTTP_FIXTURE_GZIP_JSON_URL** - Replies with gzip-encoded JSON body.
  - **HTTP_FIXTURE_TLS_CERTFILE** - Path to the server certificate (TLS only).
  - **HTTP_FIXTURE_TLS_KEYFILE** - Path to the server private key (TLS only).
  - **HTTP_FIXTURE_TLS_CAFILE** - Path to the CA certificate (TLS only).
  - **HTTP_FIXTURE_TLS_CERTKEYFILE** - Combined cert+key file (TLS only).

The fixture uses the generator-style ``@fixture`` decorator: it starts the
server, yields the environment mapping, and tears everything down automatically.
"""

from __future__ import annotations

import gzip
import json
import os
import shutil
import ssl
import tempfile
import threading
import time
import dataclasses
from email.message import Message
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator
from urllib.parse import urlsplit

from tenzir_test import fixture
from tenzir_test.fixtures import current_options

from ._utils import generate_self_signed_cert

_GZIP_EMPTY = "/content-encoding/gzip-empty"
_GZIP_JSON = "/content-encoding/gzip-json"
_RETRY_429_AFTER = "/status/retry-429-after"
_RETRY_503 = "/status/retry-503"
_EARLY_200 = "/status/early-200"
_EARLY_503 = "/status/early-503"
_RETRY_503_BACKOFF = "/status/retry-503-backoff"
_RETRY_EXHAUSTED_503 = "/status/retry-exhausted-503"
_STATUS_400 = "/status/bad-request"
_STATUS_401 = "/status/unauthorized"
_STATUS_403 = "/status/forbidden"
_SPLUNK_EXPORT = "/services/search/v2/jobs/export"
_SPLUNK_EMPTY = "/splunk/empty"
_SPLUNK_ERROR_MESSAGE = "/splunk/error-message"
_SPLUNK_FATAL_MESSAGE = "/splunk/fatal-message"
_SPLUNK_MALFORMED = "/splunk/malformed"
_SPLUNK_MISSING_RESULT = "/splunk/missing-result"
_SPLUNK_RETRY_429 = "/splunk/retry-429"
_SPLUNK_RETRY_503 = "/splunk/retry-503"
_SPLUNK_SPLIT = "/splunk/split"
_SPLUNK_STREAM = "/splunk/stream"
_SPLUNK_TIMEOUT = "/splunk/timeout"
_SPLUNK_UNAUTHORIZED = "/splunk/unauthorized"
_SPLUNK_WARNING_MESSAGE = "/splunk/warning-message"

_SPLUNK_RESULTS = (
    b'{"preview":false,"offset":0,"result":{"_time":"2026-07-14 '
    b'14:54:45.000 GMT","_raw":"raw-one","host":"host-a","source":'
    b'"source-a","sourcetype":"type-a","custom":"one","number":42}}\n'
    b'{"preview":false,"offset":1,"lastrow":true,"result":{"_time":'
    b'"2026-07-14 14:54:46.000 GMT","_raw":"raw-two","host":"host-b",'
    b'"source":"source-b","sourcetype":"type-b","custom":"two",'
    b'"number":43}}\n'
)


@dataclasses.dataclass(frozen=True)
class HttpServerOptions:
    @dataclasses.dataclass(frozen=True)
    class ExpectedRequest:
        count: int = 1
        method: str = ""
        path: str = ""
        body: str = ""
        body_alt: str = ""
        header_name: str = ""
        header_value: str = ""

    tls: bool = False
    proxy: bool = False
    expected: list[ExpectedRequest | dict[str, object]] = dataclasses.field(
        default_factory=list
    )


def _normalize_expected_request(
    value: HttpServerOptions.ExpectedRequest | dict[str, object],
) -> HttpServerOptions.ExpectedRequest:
    if isinstance(value, HttpServerOptions.ExpectedRequest):
        return value
    if not isinstance(value, dict):
        return HttpServerOptions.ExpectedRequest()
    count = value.get("count", 1)
    if not isinstance(count, int):
        try:
            count = int(count)
        except (TypeError, ValueError):
            count = 1
    return HttpServerOptions.ExpectedRequest(
        count=count,
        method=str(value.get("method", "")),
        path=str(value.get("path", "")),
        body=str(value.get("body", "")),
        body_alt=str(value.get("body_alt", "")),
        header_name=str(value.get("header_name", "")),
        header_value=str(value.get("header_value", "")),
    )


def _normalize_expected(
    values: list[HttpServerOptions.ExpectedRequest | dict[str, object]],
) -> list[HttpServerOptions.ExpectedRequest]:
    result: list[HttpServerOptions.ExpectedRequest] = []
    for value in values:
        expected = _normalize_expected_request(value)
        count = max(0, expected.count)
        result.extend(dataclasses.replace(expected, count=1) for _ in range(count))
    return result


def _make_handler(
    expected_requests: list[HttpServerOptions.ExpectedRequest],
    errors: list[str],
    request_count: list[int],
):
    retry_429_after_attempts = [0]
    retry_429_after_first_at = [0.0]
    retry_503_attempts = [0]
    retry_503_backoff_attempts = [0]
    retry_503_backoff_first_gap_at = [0.0]
    retry_503_backoff_second_gap_at = [0.0]

    class RecordingEchoHandler(BaseHTTPRequestHandler):
        def _validate_request(self, path: str, body: bytes) -> None:
            request_count[0] += 1
            request_index = request_count[0] - 1
            if not expected_requests:
                return
            if request_index >= len(expected_requests):
                errors.append(f"received unexpected request #{request_index + 1}")
                return
            expected_request = expected_requests[request_index]
            if expected_request.method:
                expected_method = expected_request.method.upper()
                if self.command != expected_method:
                    errors.append(
                        f"expected request method {expected_method}, got {self.command}"
                    )
            if expected_request.path and path != expected_request.path:
                errors.append(
                    f"expected request path {expected_request.path}, got {path}"
                )
            body_text = body.decode("utf-8", errors="replace")
            if expected_request.body:
                body_matches = body_text == expected_request.body
                if expected_request.body_alt:
                    body_matches = body_matches or (
                        body_text == expected_request.body_alt
                    )
                if not body_matches:
                    errors.append(
                        "expected request body "
                        f"{expected_request.body!r} or "
                        f"{expected_request.body_alt!r}, "
                        f"got {body_text!r}"
                    )
            if expected_request.header_name:
                header = self.headers.get(expected_request.header_name, "")
                if header != expected_request.header_value:
                    errors.append(
                        "expected request header "
                        f"{expected_request.header_name}: "
                        f"{expected_request.header_value!r}, "
                        f"got {header!r}"
                    )

        def _read_body(self) -> bytes:
            transfer_encoding = self.headers.get("Transfer-Encoding", "")
            if transfer_encoding.lower() == "chunked":
                chunks: list[bytes] = []
                while True:
                    size_line = self.rfile.readline()
                    if not size_line:
                        return b"".join(chunks)
                    size_str = size_line.strip().split(b";", 1)[0]
                    try:
                        chunk_size = int(size_str, 16)
                    except ValueError:
                        return b"".join(chunks)
                    if chunk_size == 0:
                        # Consume trailer headers.
                        while True:
                            trailer_line = self.rfile.readline()
                            if trailer_line in (b"", b"\n", b"\r\n"):
                                break
                        break
                    chunk = self.rfile.read(chunk_size)
                    chunks.append(chunk)
                    # Consume trailing CRLF after each chunk.
                    self.rfile.read(2)
                return b"".join(chunks)
            length_header = self.headers.get("Content-Length")
            try:
                content_length = int(length_header) if length_header else 0
            except ValueError:
                content_length = 0
            if content_length <= 0:
                return b""
            return self.rfile.read(content_length) or b""

        def _normalize_headers(self) -> Message:
            headers = Message()
            for key, value in self.headers.items():
                if key == "User-Agent" and value.startswith("Tenzir/"):
                    value = "Tenzir/*.*.*"
                elif key == "Accept-Encoding":
                    value = "*"
                elif key == "Host" and ":" in value:
                    host, _, _port = value.partition(":")
                    value = f"{host}:*"
                headers[key] = value
            return headers

        def _reply(
            self,
            payload: bytes,
            extra_headers: list[tuple[str, str]] | None = None,
            status: HTTPStatus = HTTPStatus.OK,
        ) -> None:
            try:
                content_type = self.headers.get("Content-Type", "application/json")
                if not content_type:
                    content_type = "application/json"
                self.send_response(status)
                self.send_header("Content-Type", content_type)
                if extra_headers:
                    for key, value in extra_headers:
                        self.send_header(key, value)
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                if payload:
                    self.wfile.write(payload)
            except (BrokenPipeError, ConnectionResetError):
                return

        def _reply_stream(self, chunks: list[bytes], delay: float = 0) -> None:
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("Connection", "close")
            self.end_headers()
            self.close_connection = True
            try:
                for chunk in chunks:
                    self.wfile.write(chunk)
                    self.wfile.flush()
                    if delay:
                        time.sleep(delay)
            except (BrokenPipeError, ConnectionResetError):
                return

        def _handle_request(self, body: bytes) -> None:
            path = urlsplit(self.path).path
            self._validate_request(path, body)
            if path == "/options/method":
                self._reply(f'{{"method":"{self.command}"}}\n'.encode())
                return
            if path == "/options/header":
                header = self.headers.get("X-Test", "")
                self._reply(f'{{"x_test":"{header}"}}\n'.encode())
                return
            if path == "/options/host":
                host = self.headers.get("Host", "")
                self._reply(f'{{"host":"{host}"}}\n'.encode())
                return
            if path == "/options/body":
                self._reply(
                    json.dumps(
                        {"body": body.decode("utf-8", errors="replace")}
                    ).encode()
                    + b"\n"
                )
                return
            if path == "/status/not-found":
                self._reply(
                    b'{"error":"not-found"}\n',
                    status=HTTPStatus.NOT_FOUND,
                )
                return
            if path == _STATUS_400:
                self._reply(
                    b'{"error":"bad-request"}\n',
                    status=HTTPStatus.BAD_REQUEST,
                )
                return
            if path == _STATUS_401:
                self._reply(
                    b'{"error":"unauthorized"}\n',
                    status=HTTPStatus.UNAUTHORIZED,
                )
                return
            if path == _STATUS_403:
                self._reply(
                    b'{"error":"forbidden"}\n',
                    status=HTTPStatus.FORBIDDEN,
                )
                return
            if path == _SPLUNK_UNAUTHORIZED + _SPLUNK_EXPORT:
                self._reply(
                    b'{"messages":[{"type":"ERROR","text":"Unauthorized"}]}\n',
                    status=HTTPStatus.UNAUTHORIZED,
                )
                return
            if path == _SPLUNK_EMPTY + _SPLUNK_EXPORT:
                self._reply(b"")
                return
            if path == _SPLUNK_ERROR_MESSAGE + _SPLUNK_EXPORT:
                self._reply(
                    b'{"preview":false,"messages":[{"type":"ERROR",'
                    b'"text":"The lookup table does not exist."}],'
                    b'"lastrow":true}\n'
                )
                return
            if path == _SPLUNK_FATAL_MESSAGE + _SPLUNK_EXPORT:
                self._reply(
                    b'{"messages":[{"type":"FATAL",'
                    b'"text":"The search could not be parsed."}]}\n'
                )
                return
            if path == _SPLUNK_MALFORMED + _SPLUNK_EXPORT:
                self._reply(b'{"result":{"broken":true}\n')
                return
            if path == _SPLUNK_MISSING_RESULT + _SPLUNK_EXPORT:
                self._reply(b'{"preview":false,"lastrow":true}\n')
                return
            if path == _SPLUNK_TIMEOUT + _SPLUNK_EXPORT:
                time.sleep(0.25)
                self._reply(_SPLUNK_RESULTS)
                return
            if path == _SPLUNK_WARNING_MESSAGE + _SPLUNK_EXPORT:
                self._reply(
                    b'{"preview":false,"messages":[{"type":"WARN",'
                    b'"text":"Some search peers did not return results."},'
                    b'{"type":"INFO","text":"Search mode is normal."},'
                    b'{"type":"DEBUG","text":"Search finalized."}]}\n' + _SPLUNK_RESULTS
                )
                return
            if path == _SPLUNK_RETRY_429 + _SPLUNK_EXPORT:
                retry_429_after_attempts[0] += 1
                if retry_429_after_attempts[0] == 1:
                    self._reply(
                        b'{"messages":[{"type":"WARN","text":"Busy"}]}\n',
                        [("Retry-After", "1")],
                        status=HTTPStatus.TOO_MANY_REQUESTS,
                    )
                else:
                    self._reply(_SPLUNK_RESULTS)
                return
            if path == _SPLUNK_RETRY_503 + _SPLUNK_EXPORT:
                retry_503_attempts[0] += 1
                if retry_503_attempts[0] <= 2:
                    self._reply(
                        b'{"messages":[{"type":"WARN","text":"Busy"}]}\n',
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                    )
                else:
                    self._reply(_SPLUNK_RESULTS)
                return
            if path == _SPLUNK_SPLIT + _SPLUNK_EXPORT:
                self._reply_stream(
                    [
                        _SPLUNK_RESULTS[:17],
                        _SPLUNK_RESULTS[17:113],
                        _SPLUNK_RESULTS[113:227],
                        _SPLUNK_RESULTS[227:],
                    ]
                )
                return
            if path == _SPLUNK_STREAM + _SPLUNK_EXPORT:
                chunks = [
                    (
                        '{"preview":false,"offset":%d,"result":{"number":%d}}\n'
                        % (number, number)
                    ).encode()
                    for number in range(100)
                ]
                self._reply_stream(chunks, delay=0.01)
                return
            if path == _SPLUNK_EXPORT:
                self._reply(_SPLUNK_RESULTS)
                return
            if path == _RETRY_429_AFTER:
                retry_429_after_attempts[0] += 1
                now = time.monotonic()
                if retry_429_after_attempts[0] == 1:
                    retry_429_after_first_at[0] = now
                    self._reply(
                        b'{"error":"too-many-requests"}\n',
                        [("Retry-After", "1")],
                        status=HTTPStatus.TOO_MANY_REQUESTS,
                    )
                elif now - retry_429_after_first_at[0] >= 0.9:
                    self._reply(b'{"ok":true}\n')
                else:
                    self._reply(
                        b'{"error":"retried-too-early"}\n',
                        status=HTTPStatus.TOO_MANY_REQUESTS,
                    )
                return
            if path == _RETRY_503:
                retry_503_attempts[0] += 1
                if retry_503_attempts[0] <= 2:
                    self._reply(
                        b'{"error":"service-unavailable"}\n',
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                    )
                else:
                    self._reply(b'{"ok":true}\n')
                return
            if path == _RETRY_503_BACKOFF:
                retry_503_backoff_attempts[0] += 1
                now = time.monotonic()
                if retry_503_backoff_attempts[0] == 1:
                    retry_503_backoff_first_gap_at[0] = now
                    self._reply(
                        b'{"error":"service-unavailable"}\n',
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                    )
                elif retry_503_backoff_attempts[0] == 2:
                    retry_503_backoff_second_gap_at[0] = now
                    self._reply(
                        b'{"error":"service-unavailable"}\n',
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                    )
                elif (
                    retry_503_backoff_second_gap_at[0]
                    - retry_503_backoff_first_gap_at[0]
                    >= 0.40
                    and now - retry_503_backoff_second_gap_at[0] >= 0.90
                ):
                    self._reply(b'{"ok":true}\n')
                else:
                    self._reply(
                        b'{"error":"retried-too-early"}\n',
                        status=HTTPStatus.SERVICE_UNAVAILABLE,
                    )
                return
            if path == _RETRY_EXHAUSTED_503:
                self._reply(
                    b'{"error":"service-unavailable"}\n',
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                )
                return
            if path == _GZIP_EMPTY:
                self._reply(
                    gzip.compress(b""),
                    [("Content-Encoding", "gzip")],
                )
                return
            if path == _GZIP_JSON:
                self._reply(
                    gzip.compress(b'{"compressed":true}\n'),
                    [("Content-Encoding", "gzip")],
                )
                return
            self._reply(body)

        def log_message(self, *_: object) -> None:  # noqa: D401
            return

        def do_GET(self) -> None:  # noqa: N802
            self._handle_request(b"")

        def do_POST(self) -> None:  # noqa: N802
            path = urlsplit(self.path).path
            if path == _EARLY_200:
                self._validate_request(path, b"")
                self._reply(
                    b'{"status":"accepted"}\n',
                    status=HTTPStatus.OK,
                )
                time.sleep(2)
                return
            if path == _EARLY_503:
                self._validate_request(path, b"")
                self._reply(
                    b'{"error":"service-unavailable"}\n',
                    status=HTTPStatus.SERVICE_UNAVAILABLE,
                )
                time.sleep(2)
                return
            body = self._read_body() or b"{}"
            self._handle_request(body)

        def do_PUT(self) -> None:  # noqa: N802
            body = self._read_body() or b"{}"
            self._handle_request(body)

    return RecordingEchoHandler


@fixture(name="http_server", options=HttpServerOptions)
def run() -> Iterator[dict[str, str]]:
    opts = current_options("http_server")
    opts = dataclasses.replace(
        opts,
        expected=_normalize_expected(opts.expected),
    )
    temp_dir: Path | None = None
    tls_env: dict[str, str] = {}
    if opts.tls:
        temp_dir = Path(tempfile.mkdtemp(prefix="http-server-tls-"))
        cert_path, key_path, ca_path, cert_and_key_path = generate_self_signed_cert(
            temp_dir,
            common_name="127.0.0.1",
        )
        tls_env = {
            "HTTP_FIXTURE_TLS_CERTFILE": str(cert_path),
            "HTTP_FIXTURE_TLS_KEYFILE": str(key_path),
            "HTTP_FIXTURE_TLS_CAFILE": str(ca_path),
            "HTTP_FIXTURE_TLS_CERTKEYFILE": str(cert_and_key_path),
        }
    errors: list[str] = []
    request_count = [0]
    handler = _make_handler(opts.expected, errors, request_count)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    if opts.tls:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(
            certfile=tls_env["HTTP_FIXTURE_TLS_CERTFILE"],
            keyfile=tls_env["HTTP_FIXTURE_TLS_KEYFILE"],
        )
        server.socket = context.wrap_socket(server.socket, server_side=True)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        port = server.server_address[1]
        scheme = "https" if opts.tls else "http"
        base_url = f"{scheme}://127.0.0.1:{port}"
        env = {
            "HTTP_FIXTURE_URL": f"{base_url}/",
            "HTTP_FIXTURE_METHOD_URL": f"{base_url}/options/method",
            "HTTP_FIXTURE_METHOD_URL_NO_SCHEME": f"127.0.0.1:{port}/options/method",
            "HTTP_FIXTURE_HEADER_URL": f"{base_url}/options/header",
            "HTTP_FIXTURE_HOST_URL": f"{base_url}/options/host",
            "HTTP_FIXTURE_BODY_URL": f"{base_url}/options/body",
            "HTTP_FIXTURE_STATUS_404_URL": f"{base_url}/status/not-found",
            "HTTP_FIXTURE_STATUS_400_URL": f"{base_url}{_STATUS_400}",
            "HTTP_FIXTURE_STATUS_401_URL": f"{base_url}{_STATUS_401}",
            "HTTP_FIXTURE_STATUS_403_URL": f"{base_url}{_STATUS_403}",
            "HTTP_FIXTURE_RETRY_429_AFTER_URL": f"{base_url}{_RETRY_429_AFTER}",
            "HTTP_FIXTURE_RETRY_503_URL": f"{base_url}{_RETRY_503}",
            "HTTP_FIXTURE_EARLY_200_URL": f"{base_url}{_EARLY_200}",
            "HTTP_FIXTURE_EARLY_503_URL": f"{base_url}{_EARLY_503}",
            "HTTP_FIXTURE_RETRY_503_BACKOFF_URL": f"{base_url}{_RETRY_503_BACKOFF}",
            "HTTP_FIXTURE_RETRY_EXHAUSTED_503_URL": f"{base_url}{_RETRY_EXHAUSTED_503}",
            "HTTP_FIXTURE_GZIP_EMPTY_URL": f"{base_url}{_GZIP_EMPTY}",
            "HTTP_FIXTURE_GZIP_JSON_URL": f"{base_url}{_GZIP_JSON}",
            "HTTP_FIXTURE_SPLUNK_EMPTY_URL": f"{base_url}{_SPLUNK_EMPTY}",
            "HTTP_FIXTURE_SPLUNK_ERROR_MESSAGE_URL": (
                f"{base_url}{_SPLUNK_ERROR_MESSAGE}"
            ),
            "HTTP_FIXTURE_SPLUNK_FATAL_MESSAGE_URL": (
                f"{base_url}{_SPLUNK_FATAL_MESSAGE}"
            ),
            "HTTP_FIXTURE_SPLUNK_MALFORMED_URL": f"{base_url}{_SPLUNK_MALFORMED}",
            "HTTP_FIXTURE_SPLUNK_MISSING_RESULT_URL": f"{base_url}{_SPLUNK_MISSING_RESULT}",
            "HTTP_FIXTURE_SPLUNK_RETRY_429_URL": f"{base_url}{_SPLUNK_RETRY_429}",
            "HTTP_FIXTURE_SPLUNK_RETRY_503_URL": f"{base_url}{_SPLUNK_RETRY_503}",
            "HTTP_FIXTURE_SPLUNK_SPLIT_URL": f"{base_url}{_SPLUNK_SPLIT}",
            "HTTP_FIXTURE_SPLUNK_STREAM_URL": f"{base_url}{_SPLUNK_STREAM}",
            "HTTP_FIXTURE_SPLUNK_TIMEOUT_URL": f"{base_url}{_SPLUNK_TIMEOUT}",
            "HTTP_FIXTURE_SPLUNK_UNAUTHORIZED_URL": f"{base_url}{_SPLUNK_UNAUTHORIZED}",
            "HTTP_FIXTURE_SPLUNK_WARNING_MESSAGE_URL": (
                f"{base_url}{_SPLUNK_WARNING_MESSAGE}"
            ),
        }
        if opts.proxy:
            env.update(
                {
                    "HTTP_FIXTURE_SPLUNK_TIMEOUT_URL": (
                        f"http://splunk-timeout.invalid{_SPLUNK_TIMEOUT}"
                    ),
                    "HTTP_PROXY": base_url,
                    "http_proxy": base_url,
                    "NO_PROXY": "127.0.0.1,localhost",
                    "no_proxy": "127.0.0.1,localhost",
                }
            )
        env.update(tls_env)
        yield env
    finally:
        server.shutdown()
        worker.join()
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)
        if opts.expected and request_count[0] != len(opts.expected):
            errors.append(
                f"expected request count {len(opts.expected)}, got {request_count[0]}"
            )
        if errors:
            raise RuntimeError(errors[0])
