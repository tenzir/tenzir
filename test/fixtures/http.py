"""HTTP echo fixture that records every request.

Usage overview:

- Tests declare ``fixtures: [http]`` in their frontmatter to opt in.
- Importing this module registers the ``@fixture`` definition below.
- ``mode=server`` (default) starts an HTTP server for client-style operators.
  It exports:
  - **HTTP_FIXTURE_URL** – Fully qualified URL pointing at the temporary HTTP
    server.
  - **HTTP_FIXTURE_ENDPOINT** – Fully qualified endpoint URL, identical to
    ``HTTP_FIXTURE_URL``.
  - **HTTP_CAPTURE_FILE** – Path to a transcript of the most recent request.
- ``mode=client`` starts a retrying HTTP client for server-style operators
  like ``accept_http``. It exports:
  - **HTTP_FIXTURE_ACCEPT_ENDPOINT** – ``host:port`` endpoint for
    ``accept_http``.
  - **HTTP_FIXTURE_ACCEPT_URL** – Fully qualified target URL.
  - **HTTP_CAPTURE_FILE** – Path to a transcript of the request/response.

The fixture uses the generator-style ``@fixture`` decorator: it starts the
service, yields the environment mapping, and tears everything down
automatically.
"""

from __future__ import annotations

import http.client
import json
import os
import shutil
import ssl
import tempfile
import threading
import time
from dataclasses import dataclass, field, replace
from email.message import Message
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator
from urllib.parse import urlsplit

from tenzir_test import fixture
from tenzir_test.fixtures import current_options

from ._utils import find_free_port, generate_self_signed_cert

_LINK_BASIC_PAGE_1 = "/link-pagination/basic/1"
_LINK_BASIC_PAGE_2 = "/link-pagination/basic/2"
_LINK_CHAIN_PREFIX = "/link-pagination/chain/"
_LINK_CHAIN_LAST = 5
_LINK_EDGE_PAGE_1 = "/link-pagination/edge/1"
_LINK_EDGE_PAGE_2 = "/link-pagination/edge/2"
_LINK_UNREACHABLE_PAGE_1 = "/link-pagination/unreachable/1"
_LINK_MULTI_SINGLE_PAGE_1 = "/link-pagination/multi-single/1"
_LINK_MULTI_SINGLE_PAGE_2 = "/link-pagination/multi-single/2"
_LINK_MULTI_MULTI_PAGE_1 = "/link-pagination/multi-multi/1"
_LINK_MULTI_MULTI_PAGE_2 = "/link-pagination/multi-multi/2"
_LINK_MULTI_MULTI_PAGE_3 = "/link-pagination/multi-multi/3"
_LINK_NONE_PAGE_1 = "/link-pagination/no-link/1"


@dataclass(frozen=True)
class HttpOptions:
    @dataclass(frozen=True)
    class ExpectedRequest:
        count: int = 0
        method: str = ""
        path: str = ""
        body: str = ""
        body_alt: str = ""
        header_name: str = ""
        header_value: str = ""

    # "server" exposes HTTP_FIXTURE_URL, "client" drives accept_http.
    mode: str = "server"
    tls: bool = False
    expected_request: ExpectedRequest | dict[str, object] = field(
        default_factory=ExpectedRequest
    )
    method: str = "POST"
    path: str = "/"
    body: str = "{}"
    content_type: str = "application/json"
    expected_status: int = 200
    expected_response_body: str = ""
    connect_delay: float = 0.0


def _normalize_expected_request(
    value: HttpOptions.ExpectedRequest | dict[str, object],
) -> HttpOptions.ExpectedRequest:
    if isinstance(value, HttpOptions.ExpectedRequest):
        return value
    if not isinstance(value, dict):
        return HttpOptions.ExpectedRequest()
    count = value.get("count", 0)
    if not isinstance(count, int):
        try:
            count = int(count)
        except (TypeError, ValueError):
            count = 0
    return HttpOptions.ExpectedRequest(
        count=count,
        method=str(value.get("method", "")),
        path=str(value.get("path", "")),
        body=str(value.get("body", "")),
        body_alt=str(value.get("body_alt", "")),
        header_name=str(value.get("header_name", "")),
        header_value=str(value.get("header_value", "")),
    )


def _make_handler(
    capture_path: Path,
    opts: HttpOptions,
    errors: list[str],
    request_count: list[int],
):
    class RecordingEchoHandler(BaseHTTPRequestHandler):
        def _validate_request(self, path: str, body: bytes) -> None:
            request_count[0] += 1
            expected_request = opts.expected_request
            if expected_request.method:
                expected_method = expected_request.method.upper()
                if self.command != expected_method:
                    errors.append(
                        "expected request method "
                        f"{expected_method}, got {self.command}"
                    )
            if (
                expected_request.path
                and path != expected_request.path
            ):
                errors.append(
                    f"expected request path {expected_request.path}, got {path}"
                )
            if expected_request.body:
                body_text = body.decode("utf-8", errors="replace")
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

        def _record_request(self, body: bytes) -> None:
            headers = self._normalize_headers()
            parts = [f'"{self.requestline}" 200 -', ""]
            for key, value in headers.items():
                parts.append(f"{key}: {value}")
            parts.append("")
            parts.append(body.decode("utf-8"))
            capture_path.write_text("\n".join(parts), encoding="utf-8")

        def _reply(
            self,
            payload: bytes,
            extra_headers: list[tuple[str, str]] | None = None,
            status: HTTPStatus = HTTPStatus.OK,
        ) -> None:
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

        def _page_payload(self, page: int) -> bytes:
            return f'{{"page":{page}}}\n'.encode()

        def _handle_request(self, body: bytes) -> None:
            self._record_request(body)
            path = urlsplit(self.path).path
            self._validate_request(path, body)
            if path == "/options/method":
                self._reply(f'{{"method":"{self.command}"}}\n'.encode())
                return
            if path == "/options/header":
                header = self.headers.get("X-Test", "")
                self._reply(f'{{"x_test":"{header}"}}\n'.encode())
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
            if path == _LINK_BASIC_PAGE_1:
                self._reply(
                    self._page_payload(1),
                    [("Link", f'<{_LINK_BASIC_PAGE_2}>; rel="next"')],
                )
                return
            if path == _LINK_BASIC_PAGE_2:
                self._reply(self._page_payload(2))
                return
            if path.startswith(_LINK_CHAIN_PREFIX):
                page_str = path.removeprefix(_LINK_CHAIN_PREFIX)
                if page_str.isdigit():
                    page = int(page_str)
                    if 1 <= page <= _LINK_CHAIN_LAST:
                        headers: list[tuple[str, str]] | None = None
                        if page < _LINK_CHAIN_LAST:
                            headers = [
                                (
                                    "Link",
                                    f'<{_LINK_CHAIN_PREFIX}{page + 1}>; rel="next"',
                                )
                            ]
                        self._reply(self._page_payload(page), headers)
                        return
            if path == _LINK_EDGE_PAGE_1:
                self._reply(
                    self._page_payload(1),
                    [
                        (
                            "Link",
                            f'<{_LINK_EDGE_PAGE_2}>; rel="prev next"; '
                            'title="a\\"b,c;d", '
                            '</link-pagination/edge/ignored>; rel="last"',
                        ),
                    ],
                )
                return
            if path == _LINK_EDGE_PAGE_2:
                self._reply(self._page_payload(2))
                return
            if path == _LINK_UNREACHABLE_PAGE_1:
                # Return an empty body so that only the Link header matters.
                # This avoids a race between the page-1 parse subpipeline and
                # the immediate connection-refused from the unreachable next
                # URL, which produces non-deterministic output across platforms.
                self._reply(
                    b"",
                    [
                        (
                            "Link",
                            "<http://127.0.0.1:9/link-pagination/unreachable/next>;"
                            ' rel="next"',
                        ),
                    ],
                    status=HTTPStatus.NO_CONTENT,
                )
                return
            if path == _LINK_MULTI_SINGLE_PAGE_1:
                self._reply(
                    self._page_payload(1),
                    [
                        ("Link", '</link-pagination/multi-single/ignored>; rel="prev"'),
                        ("Link", f'<{_LINK_MULTI_SINGLE_PAGE_2}>; rel="next"'),
                        ("Link", '</link-pagination/multi-single/ignored>; rel="last"'),
                    ],
                )
                return
            if path == _LINK_MULTI_SINGLE_PAGE_2:
                self._reply(self._page_payload(2))
                return
            if path == _LINK_MULTI_MULTI_PAGE_1:
                self._reply(
                    self._page_payload(1),
                    [
                        ("Link", f'<{_LINK_MULTI_MULTI_PAGE_2}>; rel="next"'),
                        ("Link", f'<{_LINK_MULTI_MULTI_PAGE_3}>; rel="next"'),
                    ],
                )
                return
            if path == _LINK_MULTI_MULTI_PAGE_2:
                self._reply(self._page_payload(2))
                return
            if path == _LINK_MULTI_MULTI_PAGE_3:
                self._reply(self._page_payload(3))
                return
            if path == _LINK_NONE_PAGE_1:
                self._reply(self._page_payload(1))
                return
            self._reply(body)

        def log_message(self, *_: object) -> None:  # noqa: D401
            return

        def do_GET(self) -> None:  # noqa: N802
            self._handle_request(b"{}")

        def do_POST(self) -> None:  # noqa: N802
            body = self._read_body() or b"{}"
            self._handle_request(body)

        def do_PUT(self) -> None:  # noqa: N802
            body = self._read_body() or b"{}"
            self._handle_request(body)

    return RecordingEchoHandler


def _run_server(
    capture_path: Path,
    opts: HttpOptions,
    tls_env: dict[str, str],
) -> Iterator[dict[str, str]]:
    errors: list[str] = []
    request_count = [0]
    handler = _make_handler(capture_path, opts, errors, request_count)
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
        url = f"{base_url}/"
        env = {
            "HTTP_FIXTURE_URL": url,
            "HTTP_FIXTURE_ENDPOINT": url,
            "HTTP_FIXTURE_LINK_URL": f"{base_url}{_LINK_BASIC_PAGE_1}",
            "HTTP_FIXTURE_LINK_CHAIN_URL": f"{base_url}{_LINK_CHAIN_PREFIX}1",
            "HTTP_FIXTURE_LINK_EDGE_URL": f"{base_url}{_LINK_EDGE_PAGE_1}",
            "HTTP_FIXTURE_LINK_UNREACHABLE_URL": (
                f"{base_url}{_LINK_UNREACHABLE_PAGE_1}"
            ),
            "HTTP_FIXTURE_LINK_MULTI_SINGLE_URL": (
                f"{base_url}{_LINK_MULTI_SINGLE_PAGE_1}"
            ),
            "HTTP_FIXTURE_LINK_MULTI_MULTI_URL": f"{base_url}{_LINK_MULTI_MULTI_PAGE_1}",
            "HTTP_FIXTURE_LINK_NONE_URL": f"{base_url}{_LINK_NONE_PAGE_1}",
            "HTTP_FIXTURE_METHOD_URL": f"{base_url}/options/method",
            "HTTP_FIXTURE_HEADER_URL": f"{base_url}/options/header",
            "HTTP_FIXTURE_BODY_URL": f"{base_url}/options/body",
            "HTTP_FIXTURE_STATUS_404_URL": f"{base_url}/status/not-found",
            "HTTP_CAPTURE_FILE": str(capture_path),
        }
        env.update(tls_env)
        yield env
    finally:
        server.shutdown()
        worker.join()
        expected_request = opts.expected_request
        if (
            expected_request.count
            and request_count[0] != expected_request.count
        ):
            errors.append(
                "expected request count "
                f"{expected_request.count}, got {request_count[0]}"
            )
        if errors:
            raise RuntimeError(errors[0])


def _run_client(
    opts: HttpOptions,
    capture_path: Path,
    tls_env: dict[str, str],
) -> Iterator[dict[str, str]]:
    host = "127.0.0.1"
    port = find_free_port()
    endpoint = f"{host}:{port}"
    scheme = "https" if opts.tls else "http"
    url = f"{scheme}://{endpoint}{opts.path}"
    stop_event = threading.Event()
    done_event = threading.Event()
    errors: list[str] = []
    method = opts.method.upper()
    request_body = opts.body.encode("utf-8")
    headers = {}
    if opts.content_type:
        headers["Content-Type"] = opts.content_type

    def _send_request() -> None:
        ssl_context = None
        if opts.tls:
            ssl_context = ssl._create_unverified_context()
        if opts.connect_delay > 0:
            time.sleep(opts.connect_delay)
        while not stop_event.is_set():
            conn: http.client.HTTPConnection | http.client.HTTPSConnection | None = None
            try:
                if opts.tls:
                    conn = http.client.HTTPSConnection(
                        host,
                        port,
                        timeout=1.0,
                        context=ssl_context,
                    )
                else:
                    conn = http.client.HTTPConnection(host, port, timeout=1.0)
                conn.request(method, opts.path, body=request_body, headers=headers)
                response = conn.getresponse()
                response_body = response.read()
                response_text = response_body.decode("utf-8", errors="replace")
                if response.status != opts.expected_status:
                    errors.append(
                        f"expected HTTP status {opts.expected_status}, got {response.status}"
                    )
                    done_event.set()
                    return
                if opts.expected_response_body and response_text != opts.expected_response_body:
                    errors.append(
                        "expected HTTP response body "
                        f"{opts.expected_response_body!r}, got {response_text!r}"
                    )
                    done_event.set()
                    return
                lines = [f'"{method} {opts.path} HTTP/1.1" {response.status} -', ""]
                for key, value in headers.items():
                    lines.append(f"{key}: {value}")
                lines.append("")
                lines.append(request_body.decode("utf-8", errors="replace"))
                lines.append("")
                lines.append(response_text)
                capture_path.write_text("\n".join(lines), encoding="utf-8")
                done_event.set()
                return
            except (
                ConnectionRefusedError,
                TimeoutError,
                OSError,
                http.client.HTTPException,
            ):
                time.sleep(0.1)
            finally:
                if conn is not None:
                    conn.close()

    worker = threading.Thread(target=_send_request, daemon=True)
    worker.start()

    try:
        env = {
            "HTTP_FIXTURE_ACCEPT_ENDPOINT": endpoint,
            "HTTP_FIXTURE_ACCEPT_URL": url,
            "HTTP_FIXTURE_URL": url,
            "HTTP_FIXTURE_ENDPOINT": url,
            "HTTP_CAPTURE_FILE": str(capture_path),
        }
        env.update(tls_env)
        yield env
    finally:
        stop_event.set()
        worker.join(timeout=1)
        if not done_event.is_set() and not errors:
            raise RuntimeError("HTTP fixture client did not complete request")
        if errors:
            raise RuntimeError(errors[0])


@fixture(name="http", options=HttpOptions)
def run() -> Iterator[dict[str, str]]:
    capture_fd, capture_path_str = tempfile.mkstemp(
        prefix="http-fixture-", suffix=".log"
    )
    os.close(capture_fd)
    capture_path = Path(capture_path_str)
    opts = current_options("http")
    opts = replace(
        opts,
        expected_request=_normalize_expected_request(opts.expected_request),
    )
    temp_dir: Path | None = None
    tls_env: dict[str, str] = {}
    if opts.tls:
        temp_dir = Path(tempfile.mkdtemp(prefix="http-tls-"))
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
    try:
        if opts.mode == "server":
            yield from _run_server(capture_path, opts, tls_env)
            return
        if opts.mode == "client":
            yield from _run_client(opts, capture_path, tls_env)
            return
        raise RuntimeError(
            "http fixture option `mode` must be `server` or `client`"
        )
    finally:
        if capture_path.exists():
            capture_path.unlink()
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)
