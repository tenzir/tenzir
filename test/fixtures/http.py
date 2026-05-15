"""HTTP echo fixture that records every request.

Usage overview:

- Tests declare ``fixtures: [http]`` in their frontmatter to opt in.
- Importing this module registers the ``@fixture`` definition below.
- **HTTP_FIXTURE_URL** – Fully qualified URL pointing at the temporary HTTP
  server. Pipelines that perform client-style requests (e.g., ``load_http``)
  can use it directly.
- **HTTP_FIXTURE_ENDPOINT** – Fully qualified endpoint URL, identical to
  ``HTTP_FIXTURE_URL``; provided for compatibility with operators that expect an
  ``endpoint`` parameter.
- **HTTP_CAPTURE_FILE** – Path to a plain-text transcript of the most recent
  request (request line, headers, body), suitable for response assertions.

The fixture uses the generator-style ``@fixture`` decorator: it starts the
server, yields the environment mapping, and tears everything down
automatically.
"""

from __future__ import annotations

import os
import tempfile
import threading
from email.message import Message
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Iterator
from urllib.parse import urlsplit

from tenzir_test import fixture

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
_INFER_JSON_HEADER = "/infer/header-only"
_INFER_CSV_EXTENSION = "/infer/extension.csv"
_INFER_HEADER_OVERRIDES_EXTENSION = "/infer/header-overrides.csv"
_INFER_EXPLICIT_OVERRIDE = "/infer/explicit.txt"
_INFER_UNKNOWN_EXTENSION = "/infer/unknown.bin"
_INFER_UNSUPPORTED_HEADER = "/infer/unsupported.json"
_INFER_HTTP_ERROR = "/infer/http-error.bin"


def _make_handler(capture_path: Path):
    class RecordingEchoHandler(BaseHTTPRequestHandler):
        def _read_body(self) -> bytes:
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
            content_type: str | None = None,
        ) -> None:
            if content_type is None:
                content_type = self.headers.get("Content-Type", "application/json")
                if not content_type:
                    content_type = "application/json"
            self.send_response(status)
            if content_type:
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
            if path == _INFER_JSON_HEADER:
                self._reply(
                    b'{"answer":42}\n',
                    content_type="Application/JSON; charset=utf-8",
                )
                return
            if path == _INFER_CSV_EXTENSION:
                self._reply(b"answer\n42\n", content_type="")
                return
            if path == _INFER_HEADER_OVERRIDES_EXTENSION:
                self._reply(b'{"answer":42}\n', content_type="application/json")
                return
            if path == _INFER_EXPLICIT_OVERRIDE:
                self._reply(b"answer\n42\n", content_type="text/plain")
                return
            if path == _INFER_UNKNOWN_EXTENSION:
                self._reply(b"answer\n42\n", content_type="")
                return
            if path == _INFER_UNSUPPORTED_HEADER:
                self._reply(
                    b'{"answer":42}\n',
                    content_type="application/octet-stream",
                )
                return
            if path == _INFER_HTTP_ERROR:
                self._reply(
                    b'{"error":"not-found"}\n',
                    status=HTTPStatus.NOT_FOUND,
                    content_type="application/octet-stream",
                )
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


@fixture(name="http")
def run() -> Iterator[dict[str, str]]:
    capture_fd, capture_path_str = tempfile.mkstemp(
        prefix="http-fixture-", suffix=".log"
    )
    os.close(capture_fd)
    capture_path = Path(capture_path_str)
    handler = _make_handler(capture_path)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()

    try:
        port = server.server_address[1]
        base_url = f"http://127.0.0.1:{port}"
        url = f"{base_url}/"
        yield {
            "HTTP_FIXTURE_URL": url,
            "HTTP_FIXTURE_ENDPOINT": url,
            "HTTP_FIXTURE_LINK_URL": f"{base_url}{_LINK_BASIC_PAGE_1}",
            "HTTP_FIXTURE_LINK_CHAIN_URL": f"{base_url}{_LINK_CHAIN_PREFIX}1",
            "HTTP_FIXTURE_LINK_EDGE_URL": f"{base_url}{_LINK_EDGE_PAGE_1}",
            "HTTP_FIXTURE_LINK_UNREACHABLE_URL": f"{base_url}{_LINK_UNREACHABLE_PAGE_1}",
            "HTTP_FIXTURE_LINK_MULTI_SINGLE_URL": f"{base_url}{_LINK_MULTI_SINGLE_PAGE_1}",
            "HTTP_FIXTURE_LINK_MULTI_MULTI_URL": f"{base_url}{_LINK_MULTI_MULTI_PAGE_1}",
            "HTTP_FIXTURE_LINK_NONE_URL": f"{base_url}{_LINK_NONE_PAGE_1}",
            "HTTP_FIXTURE_INFER_JSON_HEADER_URL": f"{base_url}{_INFER_JSON_HEADER}",
            "HTTP_FIXTURE_INFER_CSV_EXTENSION_URL": f"{base_url}{_INFER_CSV_EXTENSION}",
            "HTTP_FIXTURE_INFER_HEADER_OVERRIDES_EXTENSION_URL": f"{base_url}{_INFER_HEADER_OVERRIDES_EXTENSION}",
            "HTTP_FIXTURE_INFER_EXPLICIT_OVERRIDE_URL": f"{base_url}{_INFER_EXPLICIT_OVERRIDE}",
            "HTTP_FIXTURE_INFER_UNKNOWN_EXTENSION_URL": f"{base_url}{_INFER_UNKNOWN_EXTENSION}",
            "HTTP_FIXTURE_INFER_UNSUPPORTED_HEADER_URL": f"{base_url}{_INFER_UNSUPPORTED_HEADER}",
            "HTTP_FIXTURE_INFER_HTTP_ERROR_URL": f"{base_url}{_INFER_HTTP_ERROR}",
            "HTTP_CAPTURE_FILE": str(capture_path),
        }
    finally:
        server.shutdown()
        worker.join()
        if capture_path.exists():
            capture_path.unlink()
