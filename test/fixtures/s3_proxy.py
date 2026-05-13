"""S3 proxy fixtures for sink fault injection tests."""

from __future__ import annotations

import http.client
import logging
import threading
import urllib.parse
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable
from tenzir_test.fixtures.container_runtime import detect_runtime

from ._cloud_storage import BUCKET, PUBLIC_BUCKET
from ._utils import find_free_port
from .s3 import (
    ACCESS_KEY,
    SECRET_KEY,
    STARTUP_TIMEOUT,
    _setup_localstack_data,
    _start_localstack,
    _stop_container,
    _wait_for_localstack,
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


@dataclass(frozen=True)
class _ProxyConfig:
    upstream_port: int
    fail_complete_multipart: bool = False


class _ProxyServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, server_address: tuple[str, int], config: _ProxyConfig) -> None:
        super().__init__(server_address, _S3ProxyHandler)
        self.config = config


class _S3ProxyHandler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server: _ProxyServer

    def do_DELETE(self) -> None:
        self._proxy()

    def do_GET(self) -> None:
        self._proxy()

    def do_HEAD(self) -> None:
        self._proxy()

    def do_POST(self) -> None:
        self._proxy()

    def do_PUT(self) -> None:
        self._proxy()

    def log_message(self, format: str, *args: object) -> None:
        logger.debug("s3 proxy: " + format, *args)

    def _read_body(self) -> bytes:
        expect = self.headers.get("Expect", "")
        if expect.lower() == "100-continue":
            self.send_response_only(100)
            self.end_headers()
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return b""
        return self.rfile.read(length)

    def _should_fail(self) -> bool:
        if not self.server.config.fail_complete_multipart:
            return False
        if self.command != "POST":
            return False
        # S3 signals multipart phases via blank-value flags (`?uploads` for
        # initiate, `?uploadId=X` for complete); keep_blank_values ensures
        # the initiate case is actually distinguishable.
        query = urllib.parse.parse_qs(
            urllib.parse.urlsplit(self.path).query, keep_blank_values=True
        )
        return "uploadId" in query and "uploads" not in query

    def _proxy(self) -> None:
        body = self._read_body()
        if self._should_fail():
            self.send_response(500, "Injected CompleteMultipartUpload failure")
            self.send_header("Content-Length", "0")
            self.end_headers()
            return
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
            self.send_response(response.status, response.reason)
            for key, value in response.getheaders():
                if key.lower() in _HOP_BY_HOP_HEADERS:
                    continue
                self.send_header(key, value)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if self.command != "HEAD" and payload:
                self.wfile.write(payload)
        finally:
            conn.close()


def _start_proxy(config: _ProxyConfig) -> tuple[_ProxyServer, threading.Thread, int]:
    port = find_free_port()
    server = _ProxyServer(("127.0.0.1", port), config)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread, port


@fixture(name="s3_fail_complete")
def s3_fail_complete() -> FixtureHandle:
    """LocalStack behind a proxy that fails CompleteMultipartUpload."""
    runtime = detect_runtime()
    if runtime is None:
        raise FixtureUnavailable(
            "container runtime (docker/podman) required but not found"
        )
    upstream_port = find_free_port()
    container = _start_localstack(runtime, upstream_port)
    try:
        _wait_for_localstack(upstream_port, STARTUP_TIMEOUT)
        _setup_localstack_data(container)
        proxy, _, proxy_port = _start_proxy(
            _ProxyConfig(
                upstream_port=upstream_port,
                fail_complete_multipart=True,
            )
        )
    except Exception:
        _stop_container(container, "LocalStack")
        raise

    def _teardown() -> None:
        try:
            proxy.shutdown()
            proxy.server_close()
        finally:
            _stop_container(container, "LocalStack")

    return FixtureHandle(
        env={
            "S3_ENDPOINT": f"127.0.0.1:{proxy_port}",
            "S3_ACCESS_KEY": ACCESS_KEY,
            "S3_SECRET_KEY": SECRET_KEY,
            "S3_BUCKET": BUCKET,
            "S3_PUBLIC_BUCKET": PUBLIC_BUCKET,
            "S3_CONTAINER_RUNTIME": runtime.binary,
            "S3_CONTAINER_ID": container.container_id,
        },
        teardown=_teardown,
    )
