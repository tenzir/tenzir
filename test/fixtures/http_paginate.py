"""HTTP server fixture providing link-pagination endpoints.

Exports:
  HTTP_FIXTURE_LINK_CHAIN_URL        — chain of 5 pages, each linking to the next
  HTTP_FIXTURE_LINK_NONE_URL         — single page with no Link header
  HTTP_FIXTURE_LINK_EDGE_URL         — page with a complex Link header (multi-token
                                       rel, embedded commas/quotes in title)
  HTTP_FIXTURE_LINK_MULTI_SINGLE_URL — multiple Link headers, exactly one rel=next
  HTTP_FIXTURE_LINK_MULTI_MULTI_URL  — multiple Link headers, two rel=next targets
                                       (only the first is followed)
  HTTP_FIXTURE_LINK_UNREACHABLE_URL  — 204 with Link pointing to port 9 (always refused)
  HTTP_FIXTURE_RETRY_FLAKY_URL       — drops first 2 connections, then returns JSON
  HTTP_FIXTURE_LAMBDA_URL            — page chain where body carries `next` URL
  HTTP_FIXTURE_LARGE_ERROR_URL       — HTTP 500 with large streaming response body
  HTTP_FIXTURE_META_LAMBDA_URL        — next URL carried in X-Next-Page header (tests
                                        that pagination lambdas can read metadata_field)
"""

from __future__ import annotations

import socket
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator

from tenzir_test import fixture

_CHAIN_PREFIX = "/paginate/chain/"
_CHAIN_LAST = 5
_NONE_PAGE = "/paginate/none/1"
_EDGE_PAGE_1 = "/paginate/edge/1"
_EDGE_PAGE_2 = "/paginate/edge/2"
_MULTI_SINGLE_PAGE_1 = "/paginate/multi-single/1"
_MULTI_SINGLE_PAGE_2 = "/paginate/multi-single/2"
_MULTI_MULTI_PAGE_1 = "/paginate/multi-multi/1"
_MULTI_MULTI_PAGE_2 = "/paginate/multi-multi/2"
_MULTI_MULTI_PAGE_3 = "/paginate/multi-multi/3"
_UNREACHABLE_PAGE = "/paginate/unreachable/1"
_RETRY_FLAKY_PAGE = "/paginate/retry/flaky"
_LAMBDA_PAGE_1 = "/paginate/lambda/1"
_LAMBDA_PAGE_2 = "/paginate/lambda/2"
_LARGE_ERROR_PAGE = "/paginate/error/large"
_META_LAMBDA_PAGE_1 = "/paginate/meta-lambda/1"
_META_LAMBDA_PAGE_2 = "/paginate/meta-lambda/2"
# Port 9 is IANA Discard Protocol — always refuses connections on most systems.
_UNREACHABLE_NEXT = "http://127.0.0.1:9/paginate/unreachable/next"


def _page(n: int) -> bytes:
    return f'{{"page":{n}}}\n'.encode()


class _Handler(BaseHTTPRequestHandler):
    retry_flaky_attempts = 0
    retry_flaky_lock = threading.Lock()

    def _reply(
        self,
        body: bytes,
        link_headers: list[str] | None = None,
        status: HTTPStatus = HTTPStatus.OK,
    ) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        if link_headers:
            for lh in link_headers:
                self.send_header("Link", lh)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def do_GET(self) -> None:  # noqa: N802
        from urllib.parse import urlsplit

        path = urlsplit(self.path).path
        # Chain pagination: /paginate/chain/<n>
        if path.startswith(_CHAIN_PREFIX):
            page_str = path.removeprefix(_CHAIN_PREFIX)
            if page_str.isdigit():
                page = int(page_str)
                if 1 <= page <= _CHAIN_LAST:
                    link = None
                    if page < _CHAIN_LAST:
                        link = [f'<{_CHAIN_PREFIX}{page + 1}>; rel="next"']
                    self._reply(_page(page), link)
                    return
        # No Link header — pagination stops immediately.
        if path == _NONE_PAGE:
            self._reply(_page(1))
            return
        # Edge: complex Link header with multi-token rel and embedded
        # commas/quotes in a title parameter.
        if path == _EDGE_PAGE_1:
            self._reply(
                _page(1),
                [
                    f'<{_EDGE_PAGE_2}>; rel="prev next"; title="a\\"b,c;d",'
                    ' </paginate/edge/ignored>; rel="last"',
                ],
            )
            return
        if path == _EDGE_PAGE_2:
            self._reply(_page(2))
            return
        # Multiple separate Link headers, one of which has rel=next.
        if path == _MULTI_SINGLE_PAGE_1:
            self._reply(
                _page(1),
                [
                    '</paginate/multi-single/ignored>; rel="prev"',
                    f'<{_MULTI_SINGLE_PAGE_2}>; rel="next"',
                    '</paginate/multi-single/ignored>; rel="last"',
                ],
            )
            return
        if path == _MULTI_SINGLE_PAGE_2:
            self._reply(_page(2))
            return
        # Multiple Link headers each containing a distinct rel=next target.
        # Only the first encountered rel=next is followed.
        if path == _MULTI_MULTI_PAGE_1:
            self._reply(
                _page(1),
                [
                    f'<{_MULTI_MULTI_PAGE_2}>; rel="next"',
                    f'<{_MULTI_MULTI_PAGE_3}>; rel="next"',
                ],
            )
            return
        if path == _MULTI_MULTI_PAGE_2:
            self._reply(_page(2))
            return
        if path == _MULTI_MULTI_PAGE_3:
            self._reply(_page(3))
            return
        # Unreachable: empty 204 body with a Link pointing to a refused port.
        if path == _UNREACHABLE_PAGE:
            self._reply(
                b"",
                [f'<{_UNREACHABLE_NEXT}>; rel="next"'],
                status=HTTPStatus.NO_CONTENT,
            )
            return
        # Flaky endpoint: drops first two TCP connections, then succeeds.
        if path == _RETRY_FLAKY_PAGE:
            with type(self).retry_flaky_lock:
                type(self).retry_flaky_attempts += 1
                attempt = type(self).retry_flaky_attempts
            if attempt <= 2:
                try:
                    self.connection.shutdown(socket.SHUT_RDWR)
                except OSError:
                    pass
                self.connection.close()
                return
            self._reply(f'{{"attempt":{attempt}}}\n'.encode())
            return
        # Lambda pagination: next URL comes from the response body.
        if path == _LAMBDA_PAGE_1:
            self._reply(f'{{"page":1,"next":"{_LAMBDA_PAGE_2}"}}\n'.encode())
            return
        if path == _LAMBDA_PAGE_2:
            self._reply(b'{"page":2,"next":null}\n')
            return
        # Meta-lambda pagination: next URL carried in X-Next-Page response header.
        # Used to verify that pagination lambdas can read metadata_field fields.
        if path == _META_LAMBDA_PAGE_1:
            body = b'{"page":1}\n'
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "application/json")
            self.send_header("X-Next-Page", _META_LAMBDA_PAGE_2)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return
        if path == _META_LAMBDA_PAGE_2:
            self._reply(b'{"page":2}\n')
            return
        # Large error body for queue-draining regression tests.
        if path == _LARGE_ERROR_PAGE:
            chunk = b"x" * 1024
            self.send_response(HTTPStatus.INTERNAL_SERVER_ERROR)
            self.send_header("Content-Type", "application/octet-stream")
            self.send_header("Content-Length", str(len(chunk) * 1024))
            self.end_headers()
            for _ in range(1024):
                try:
                    self.wfile.write(chunk)
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    break
            return
        self.send_error(404)

    def log_message(self, *_: object) -> None:  # noqa: D401
        return


@fixture(name="http_paginate")
def run() -> Iterator[dict[str, str]]:
    _Handler.retry_flaky_attempts = 0
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    try:
        port = server.server_address[1]
        base = f"http://127.0.0.1:{port}"
        yield {
            "HTTP_FIXTURE_LINK_CHAIN_URL": f"{base}{_CHAIN_PREFIX}1",
            "HTTP_FIXTURE_LINK_NONE_URL": f"{base}{_NONE_PAGE}",
            "HTTP_FIXTURE_LINK_EDGE_URL": f"{base}{_EDGE_PAGE_1}",
            "HTTP_FIXTURE_LINK_MULTI_SINGLE_URL": f"{base}{_MULTI_SINGLE_PAGE_1}",
            "HTTP_FIXTURE_LINK_MULTI_MULTI_URL": f"{base}{_MULTI_MULTI_PAGE_1}",
            "HTTP_FIXTURE_LINK_UNREACHABLE_URL": f"{base}{_UNREACHABLE_PAGE}",
            "HTTP_FIXTURE_RETRY_FLAKY_URL": f"{base}{_RETRY_FLAKY_PAGE}",
            "HTTP_FIXTURE_LAMBDA_URL": f"{base}{_LAMBDA_PAGE_1}",
            "HTTP_FIXTURE_LARGE_ERROR_URL": f"{base}{_LARGE_ERROR_PAGE}",
            "HTTP_FIXTURE_META_LAMBDA_URL": f"{base}{_META_LAMBDA_PAGE_1}",
        }
    finally:
        server.shutdown()
        worker.join()
