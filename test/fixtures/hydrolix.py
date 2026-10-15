"""Mock Hydrolix HTTP streaming ingest endpoint for `to_hydrolix` tests.

The fixture serves `POST /ingest/event` and exports **HYDROLIX_URL**, the base
URL of the server. It records the events of every accepted request, keyed by
destination: the value of `X-Hdx-Table`, followed by `/` and the value of
`X-Hdx-Transform` if present.

The destination table selects the response, so that one test can exercise
several behaviors:

- `test.partial`: HTTP 207 that rejects the second event of each request.
- `test.busy`: HTTP 429 with `Retry-After: 1` once, then HTTP 200.
- `test.unavailable`: always HTTP 503.
- `test.missing`: HTTP 400, as for a table that does not exist.
- `test.unauthorized`: HTTP 401.
- `test.echo`: HTTP 400 whose message repeats the credentials of the request:
  the `Authorization`, `X-Hdx-Token`, and `X-Secret` headers, and the request
  target with its query.
- `test.echo_partial`: HTTP 207 whose rejection reason repeats the same
  credentials.
- `test.patient`: HTTP 429 with a `Retry-After` beyond any duration once, then
  HTTP 200.
- any other table: HTTP 200.

Requests without the expected bearer token receive HTTP 401, and requests with
a wrong table access token receive HTTP 403.
"""

from __future__ import annotations

import gzip
import json
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlsplit

from tenzir_test import FixtureHandle, current_options, fixture

_PATH = "/ingest/event"


@dataclass(frozen=True)
class Options:
    token: str = "test-token"
    table_token: str = ""


@dataclass(frozen=True)
class Assertions:
    requests: int | None = None
    destinations: int | None = None
    event_count: int | None = None
    events: dict[str, list[dict[str, Any]]] | None = None
    headers: dict[str, str] = field(default_factory=dict)
    compressed: bool | None = None


class Server(ThreadingHTTPServer):
    def __init__(self, options: Options) -> None:
        super().__init__(("127.0.0.1", 0), Handler)
        self.options = options
        self.lock = threading.Lock()
        self.requests: list[dict[str, Any]] = []
        self.events: dict[str, list[dict[str, Any]]] = {}
        self.attempts: dict[str, int] = {}
        self.failures: list[str] = []


class Handler(BaseHTTPRequestHandler):
    def reply(self, status: int, body: dict[str, Any], **headers: str) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        for name, value in headers.items():
            self.send_header(name, value)
        self.end_headers()
        try:
            self.wfile.write(payload)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self) -> None:  # noqa: N802
        server = self.server
        assert isinstance(server, Server)
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        try:
            self.handle_ingest(server, body)
        except (AssertionError, ValueError) as error:
            with server.lock:
                server.failures.append(str(error))
            self.reply(400, {"code": 400, "message": "fixture assertion failed"})

    def handle_ingest(self, server: Server, body: bytes) -> None:
        assert urlsplit(self.path).path == _PATH, f"unexpected path {self.path}"
        options = server.options
        if self.headers.get("Authorization") != f"Bearer {options.token}":
            self.reply(401, {"code": 401, "message": "invalid token"})
            return
        if options.table_token and (
            self.headers.get("X-Hdx-Token") != options.table_token
        ):
            self.reply(403, {"code": 403, "message": "invalid table token"})
            return
        assert self.headers.get("Content-Type") == "application/json", self.headers.get(
            "Content-Type"
        )
        table = self.headers.get("X-Hdx-Table", "")
        transform = self.headers.get("X-Hdx-Transform")
        encoding = self.headers.get("Content-Encoding")
        if encoding == "gzip":
            body = gzip.decompress(body)
        else:
            assert encoding is None, f"unexpected encoding {encoding}"
        text = body.decode()
        assert text.endswith("\n"), "body must end with a newline"
        events = [json.loads(line) for line in text.splitlines()]
        assert all(isinstance(event, dict) for event in events), events
        destination = table if transform is None else f"{table}/{transform}"
        with server.lock:
            attempt = server.attempts.get(destination, 0) + 1
            server.attempts[destination] = attempt
            server.requests.append(
                {
                    "destination": destination,
                    "compressed": encoding == "gzip",
                    "headers": dict(self.headers.items()),
                }
            )
        if table == "test.busy" and attempt == 1:
            self.reply(
                429, {"code": 429, "message": "rate exceeded"}, **{"Retry-After": "1"}
            )
            return
        if table == "test.patient" and attempt == 1:
            self.reply(
                429,
                {"code": 429, "message": "rate exceeded"},
                **{"Retry-After": "9223372036854775807"},
            )
            return
        if table == "test.unavailable":
            self.reply(503, {"code": 503, "message": "not ready"})
            return
        if table == "test.missing":
            self.reply(400, {"code": 400, "message": "no table test.missing"})
            return
        if table == "test.unauthorized":
            self.reply(401, {"code": 401, "message": "token expired"})
            return
        if table in ("test.echo", "test.echo_partial"):
            echoed = ", ".join(
                f"{name}={self.headers.get(name)}"
                for name in ("Authorization", "X-Hdx-Token", "X-Secret")
            )
            echoed += f", target={self.path}"
            if table == "test.echo":
                self.reply(400, {"code": 400, "message": f"rejected {echoed}"})
            else:
                self.reply(
                    207,
                    {
                        "code": 207,
                        "message": {
                            "success_count": 0,
                            "errors": [f"rejected {echoed}"] * len(events),
                        },
                    },
                )
            return
        if table == "test.partial":
            accepted = [event for i, event in enumerate(events) if i != 1]
            rejected = len(events) - len(accepted)
            with server.lock:
                server.events.setdefault(destination, []).extend(accepted)
            self.reply(
                207,
                {
                    "code": 207,
                    "message": {
                        "success_count": len(accepted),
                        "errors": ["value of unsigned column is negative -1"]
                        * rejected,
                    },
                },
            )
            return
        with server.lock:
            server.events.setdefault(destination, []).extend(events)
        self.reply(200, {"code": 200, "message": "success"})

    def log_message(self, *_: object) -> None:
        pass


@fixture(name="hydrolix", options=Options, assertions=Assertions)
def run() -> FixtureHandle:
    options = current_options("hydrolix")
    assert isinstance(options, Options)
    server = Server(options)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    env = {"HYDROLIX_URL": f"http://127.0.0.1:{server.server_port}"}

    def assert_test(*, assertions: Assertions | dict[str, Any], **_: Any) -> None:
        assert not server.failures, server.failures
        if isinstance(assertions, dict):
            assertions = Assertions(**assertions)
        if assertions.requests is not None:
            assert len(server.requests) == assertions.requests, server.requests
        if assertions.destinations is not None:
            assert len(server.events) == assertions.destinations, server.events
        if assertions.event_count is not None:
            count = sum(len(events) for events in server.events.values())
            assert count == assertions.event_count, server.events
        if assertions.events is not None:

            def canonical(events: list[dict[str, Any]]) -> list[str]:
                return sorted(json.dumps(event, sort_keys=True) for event in events)

            actual = {key: canonical(value) for key, value in server.events.items()}
            expected = {
                key: canonical(value) for key, value in assertions.events.items()
            }
            assert actual == expected, server.events
        for name, value in assertions.headers.items():
            for request in server.requests:
                headers = {k.lower(): v for k, v in request["headers"].items()}
                assert headers.get(name.lower()) == value, (name, request["headers"])
        if assertions.compressed is not None:
            for request in server.requests:
                assert request["compressed"] == assertions.compressed, request

    def teardown() -> None:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
        assert not server.failures, server.failures

    return FixtureHandle(
        env=env,
        teardown=teardown,
        hooks={"assert_test": assert_test},
    )
