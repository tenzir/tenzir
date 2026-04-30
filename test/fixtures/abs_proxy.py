"""Azure Blob Storage proxy fixtures for sink fault injection tests."""

from __future__ import annotations

import http.client
import socket
import threading
import time
import urllib.parse
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable
from tenzir_test.fixtures.container_runtime import ManagedContainer, detect_runtime

from ._cloud_storage import BUCKET, PUBLIC_BUCKET
from ._utils import find_free_port
from .abs import (
    ACCOUNT_KEY,
    ACCOUNT_NAME,
    STARTUP_TIMEOUT,
    _setup_azurite_data,
    _start_azurite,
    _stop_container,
    _wait_for_azurite,
)

logger = logging.getLogger(__name__)

_HOP_BY_HOP_HEADERS = {
    "connection",
    "content-length",
    "expect",
    "keep-alive",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}

_RESPONSE_HOP_BY_HOP_HEADERS = _HOP_BY_HOP_HEADERS - {"transfer-encoding"}


def _response_content_length(
    command: str,
    response_headers: list[tuple[str, str]],
    payload: bytes,
) -> str:
    if command == "HEAD":
        for key, value in response_headers:
            if key.lower() == "content-length":
                return value
    return str(len(payload))


@dataclass(frozen=True)
class _ProxyConfig:
    upstream_port: int
    delay_seconds: float = 0.0


class _ProxyServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], config: _ProxyConfig) -> None:
        super().__init__(server_address, _AbsProxyHandler)
        self.config = config


class _AbsProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: _ProxyServer

    def handle_expect_100(self) -> bool:
        return True

    def do_DELETE(self) -> None:
        self._proxy()

    def do_GET(self) -> None:
        self._proxy()

    def do_HEAD(self) -> None:
        self._proxy()

    def do_POST(self) -> None:
        self._proxy()

    def do_PATCH(self) -> None:
        self._proxy()

    def do_PUT(self) -> None:
        self._proxy()

    def log_message(self, format: str, *args: object) -> None:
        logger.debug("abs proxy: " + format, *args)

    def _send_empty_response(self, status: int, reason: str) -> None:
        self.close_connection = True
        self.send_response(status, reason)
        self.send_header("Connection", "close")
        self.send_header("Content-Length", "0")
        self.end_headers()
        self.wfile.flush()

    def _is_create_container_request(self) -> bool:
        if self.command != "PUT":
            return False
        query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(self.path).query,
            keep_blank_values=True,
        )
        return query.get("restype") == ["container"]

    def _read_body(self) -> bytes:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return b""
        expect = self.headers.get("Expect", "")
        if expect.lower() == "100-continue":
            self.send_response_only(100)
            self.end_headers()
            self.wfile.flush()
        return self.rfile.read(length)

    def _proxy_chunked(self) -> None:
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in _HOP_BY_HOP_HEADERS and key.lower() != "host"
        }
        headers["Transfer-Encoding"] = "chunked"
        conn = http.client.HTTPConnection(
            "127.0.0.1",
            self.server.config.upstream_port,
            timeout=120,
        )
        try:
            conn.putrequest(self.command, self.path)
            for key, value in headers.items():
                conn.putheader(key, value)
            conn.endheaders()
            while True:
                size_line = self.rfile.readline()
                if not size_line:
                    break
                conn.send(size_line)
                size_str = size_line.strip().split(b";", 1)[0]
                try:
                    chunk_size = int(size_str, 16)
                except ValueError:
                    break
                if chunk_size == 0:
                    while True:
                        trailer_line = self.rfile.readline()
                        conn.send(trailer_line)
                        if trailer_line in (b"", b"\n", b"\r\n"):
                            break
                    break
                chunk = self.rfile.read(chunk_size)
                conn.send(chunk)
                crlf = self.rfile.read(2)
                conn.send(crlf)
            response = conn.getresponse()
            payload = response.read()
            response_headers = response.getheaders()
            status = response.status
            reason = response.reason
            self.close_connection = True
            self.send_response(status, reason)
            for key, value in response_headers:
                if key.lower() in _RESPONSE_HOP_BY_HOP_HEADERS:
                    continue
                if key.lower() == "connection":
                    continue
                self.send_header(key, value)
            self.send_header("Connection", "close")
            self.send_header(
                "Content-Length",
                _response_content_length(self.command, response_headers, payload),
            )
            self.end_headers()
            if self.command != "HEAD" and payload:
                try:
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError, socket.timeout):
                    return
            self.wfile.flush()
        finally:
            conn.close()

    def _proxy(self) -> None:
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            self._proxy_chunked()
            return
        body = self._read_body()
        if self._is_create_container_request():
            self._send_empty_response(201, "Created")
            return
        if self.server.config.delay_seconds > 0:
            time.sleep(self.server.config.delay_seconds)
        headers = {
            key: value
            for key, value in self.headers.items()
            if key.lower() not in _HOP_BY_HOP_HEADERS
        }
        conn = http.client.HTTPConnection(
            "127.0.0.1",
            self.server.config.upstream_port,
            timeout=120,
        )
        try:
            conn.request(self.command, self.path, body=body, headers=headers)
            response = conn.getresponse()
            payload = response.read()
            response_headers = response.getheaders()
            status = response.status
            reason = response.reason
            self.close_connection = True
            self.send_response(status, reason)
            for key, value in response_headers:
                if key.lower() in _HOP_BY_HOP_HEADERS:
                    continue
                if key.lower() == "connection":
                    continue
                self.send_header(key, value)
            self.send_header("Connection", "close")
            self.send_header(
                "Content-Length",
                _response_content_length(self.command, response_headers, payload),
            )
            self.end_headers()
            if self.command != "HEAD" and payload:
                try:
                    self.wfile.write(payload)
                except (BrokenPipeError, ConnectionResetError, socket.timeout):
                    return
            self.wfile.flush()
        finally:
            conn.close()


def _start_proxy(config: _ProxyConfig) -> tuple[_ProxyServer, threading.Thread, int]:
    port = find_free_port()
    server = _ProxyServer(("127.0.0.1", port), config)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, port


def _make_handle(
    container: ManagedContainer, proxy: _ProxyServer, proxy_port: int
) -> FixtureHandle:
    def _teardown() -> None:
        try:
            proxy.shutdown()
            proxy.server_close()
        finally:
            _stop_container(container, "Azurite")

    return FixtureHandle(
        env={
            "ABS_ENDPOINT": f"127.0.0.1:{proxy_port}",
            "ABS_ACCOUNT_NAME": ACCOUNT_NAME,
            "ABS_ACCOUNT_KEY": ACCOUNT_KEY,
            "ABS_CONTAINER": BUCKET,
            "ABS_PUBLIC_CONTAINER": PUBLIC_BUCKET,
        },
        teardown=_teardown,
    )


@fixture(name="abs_slow_proxy")
def abs_slow_proxy() -> FixtureHandle:
    """Azurite behind a slow forwarding proxy to widen close-race windows."""
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )
    upstream_port = find_free_port()
    container = _start_azurite(runtime, upstream_port)
    try:
        _wait_for_azurite(upstream_port, STARTUP_TIMEOUT)
        _setup_azurite_data(upstream_port)
        proxy, _, proxy_port = _start_proxy(
            _ProxyConfig(
                upstream_port=upstream_port,
                delay_seconds=0.01,
            )
        )
    except Exception:
        _stop_container(container, "Azurite")
        raise
    return _make_handle(container, proxy, proxy_port)
