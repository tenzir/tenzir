"""HTTP echo server fixture for testing client-side HTTP operators.

Usage overview:

- Tests declare ``fixtures: [http_server]`` in their frontmatter to opt in.
- Importing this module registers the ``@fixture`` definition below.
- The fixture starts a temporary HTTP echo server and exports:
  - **HTTP_FIXTURE_URL** - Base URL of the server (``http[s]://127.0.0.1:<port>/``).
  - **HTTP_FIXTURE_METHOD_URL** - Echoes the request method as ``{"method":"…"}``.
  - **HTTP_FIXTURE_METHOD_URL_NO_SCHEME** - Same as above, but without URL scheme.
  - **HTTP_FIXTURE_HEADER_URL** - Echoes the ``X-Test`` header as ``{"x_test":"…"}``.
  - **HTTP_FIXTURE_BODY_URL** - Echoes the request body as ``{"body":"…"}``.
  - **HTTP_FIXTURE_STATUS_404_URL** - Always replies 404 with ``{"error":"not-found"}``.
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
from dataclasses import dataclass, field, replace
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


@dataclass(frozen=True)
class HttpServerOptions:
    @dataclass(frozen=True)
    class ExpectedRequest:
        count: int = 0
        method: str = ""
        path: str = ""
        body: str = ""
        body_alt: str = ""
        header_name: str = ""
        header_value: str = ""

    tls: bool = False
    expected_request: ExpectedRequest | dict[str, object] = field(
        default_factory=ExpectedRequest
    )


def _normalize_expected_request(
    value: HttpServerOptions.ExpectedRequest | dict[str, object],
) -> HttpServerOptions.ExpectedRequest:
    if isinstance(value, HttpServerOptions.ExpectedRequest):
        return value
    if not isinstance(value, dict):
        return HttpServerOptions.ExpectedRequest()
    count = value.get("count", 0)
    if not isinstance(count, int):
        try:
            count = int(count)
        except (TypeError, ValueError):
            count = 0
    return HttpServerOptions.ExpectedRequest(
        count=count,
        method=str(value.get("method", "")),
        path=str(value.get("path", "")),
        body=str(value.get("body", "")),
        body_alt=str(value.get("body_alt", "")),
        header_name=str(value.get("header_name", "")),
        header_value=str(value.get("header_value", "")),
    )


def _make_handler(
    opts: HttpServerOptions,
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
                        f"expected request method {expected_method}, got {self.command}"
                    )
            if expected_request.path and path != expected_request.path:
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
            body = self._read_body() or b"{}"
            self._handle_request(body)

        def do_PUT(self) -> None:  # noqa: N802
            body = self._read_body() or b"{}"
            self._handle_request(body)

    return RecordingEchoHandler


@fixture(name="http_server", options=HttpServerOptions)
def run() -> Iterator[dict[str, str]]:
    opts = current_options("http_server")
    opts = replace(
        opts,
        expected_request=_normalize_expected_request(opts.expected_request),
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
    handler = _make_handler(opts, errors, request_count)
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
            "HTTP_FIXTURE_BODY_URL": f"{base_url}/options/body",
            "HTTP_FIXTURE_STATUS_404_URL": f"{base_url}/status/not-found",
            "HTTP_FIXTURE_GZIP_EMPTY_URL": f"{base_url}{_GZIP_EMPTY}",
            "HTTP_FIXTURE_GZIP_JSON_URL": f"{base_url}{_GZIP_JSON}",
        }
        env.update(tls_env)
        yield env
    finally:
        server.shutdown()
        worker.join()
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)
        expected_request = opts.expected_request
        if expected_request.count and request_count[0] != expected_request.count:
            errors.append(
                "expected request count "
                f"{expected_request.count}, got {request_count[0]}"
            )
        if errors:
            raise RuntimeError(errors[0])
