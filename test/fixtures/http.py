"""HTTP echo fixture that records every request.

Usage overview:

- Tests declare ``fixtures: [http]`` in their frontmatter to opt in.
- Importing this module registers the ``@fixture`` definition below.
- Consumers receive two environment variables:
  - ``HTTP_FIXTURE_URL`` points at a temporary HTTP server that echoes JSON
    payloads back to the caller (so ``load_http`` can read the response).
  - ``HTTP_CAPTURE_FILE`` contains the last request as a plain-text transcript
    (request line, headers, and body), enabling assertions about `save_http`
    behaviour.

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

from tenzir_test import fixture


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

        def _reply(self, payload: bytes) -> None:
            content_type = self.headers.get("Content-Type", "application/json")
            if not content_type:
                content_type = "application/json"
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if payload:
                self.wfile.write(payload)

        def log_message(self, *_: object) -> None:  # noqa: D401
            return

        def do_POST(self) -> None:  # noqa: N802
            body = self._read_body() or b"{}"
            self._record_request(body)
            self._reply(body)

        def do_PUT(self) -> None:  # noqa: N802
            body = self._read_body() or b"{}"
            self._record_request(body)
            self._reply(body)

    return RecordingEchoHandler


@fixture(name="http")
def run() -> Iterator[dict[str, str]]:
    capture_fd, capture_path_str = tempfile.mkstemp(prefix="http-fixture-", suffix=".log")
    os.close(capture_fd)
    capture_path = Path(capture_path_str)
    handler = _make_handler(capture_path)
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()

    try:
        port = server.server_address[1]
        url = f"http://127.0.0.1:{port}/"
        yield {
            "HTTP_FIXTURE_URL": url,
            "HTTP_CAPTURE_FILE": str(capture_path),
        }
    finally:
        server.shutdown()
        worker.join()
        if capture_path.exists():
            capture_path.unlink()
