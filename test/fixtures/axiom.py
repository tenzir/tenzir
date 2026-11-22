"""Deterministic Axiom edge query and ingest fixture."""

from __future__ import annotations

import gzip
import json
import os
import shlex
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, current_options, fixture

_TOKEN = "xaat-fixture"
_TRACE_ID = "trace-axiom-123"


@dataclass(frozen=True)
class Options:
    case: str = "query"


@dataclass(frozen=True)
class Assertions:
    queries: int = 0
    ingestions: int = 0
    apl: str | None = None
    start_time: str | None = None
    end_time: str | None = None
    events: list[dict[str, Any]] | None = None


class Server(ThreadingHTTPServer):
    def __init__(self, options: Options) -> None:
        super().__init__(("127.0.0.1", 0), Handler)
        self.options = options
        self.queries = 0
        self.ingestions = 0
        self.request_body: dict[str, Any] | None = None
        self.events: list[dict[str, Any]] = []
        self.failures: list[str] = []


class Handler(BaseHTTPRequestHandler):
    def log_message(self, format: str, *args: Any) -> None:
        return

    def send_json(self, status: int, body: object) -> None:
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.send_header("X-Axiom-Trace-Id", _TRACE_ID)
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:
        assert isinstance(self.server, Server)
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length)
        if self.server.options.case == "auth-echo":
            auth = self.headers.get("Authorization", "")
            if self.path == "/v1/query/_apl?format=tabular":
                self.server.queries += 1
            elif self.path == "/v1/ingest/test":
                self.server.ingestions += 1
            self.send_json(
                401,
                {"code": 401, "message": f"unauthorized: echoed {auth}"},
            )
            return
        if self.headers.get("Authorization") != f"Bearer {_TOKEN}":
            self.server.failures.append(
                f"unexpected Authorization: {self.headers.get('Authorization')!r}"
            )
            self.send_json(401, {"code": 401, "message": "unauthorized"})
            return
        if self.path == "/v1/query/_apl?format=tabular":
            self.handle_query(raw)
        elif self.path == "/v1/ingest/test":
            self.handle_ingest(raw)
        else:
            self.server.failures.append(f"unexpected path: {self.path}")
            self.send_json(404, {"code": 404, "message": "not found"})

    def handle_query(self, raw: bytes) -> None:
        assert isinstance(self.server, Server)
        self.server.queries += 1
        body = json.loads(raw.decode())
        self.server.request_body = body
        if body.get("defaultLimit") != 50000:
            self.server.failures.append(
                f"expected defaultLimit=50000, got {body.get('defaultLimit')!r}"
            )
        if self.server.options.case == "retry-timeout":
            payload = json.dumps(
                {"code": 429, "message": "rate limit exceeded"}
            ).encode()
            self.send_response(429)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Retry-After", "5")
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.server.options.case == "warnings":
            self.send_json(
                200,
                {
                    "format": "tabular",
                    "status": {
                        "isPartial": True,
                        "isEstimate": True,
                        "messages": [
                            {
                                "priority": "warn",
                                "code": "default_limit_warning",
                                "msg": "A default limit of 1000 was applied",
                            },
                            {
                                "priority": "warn",
                                "code": "license_limit_for_query_warning",
                                "msg": "Query time range is restricted due to license limit",
                            },
                            {
                                "priority": "warn",
                                "code": "max_limit_warning",
                                "msg": "A maximum limit was applied",
                            },
                        ],
                    },
                    "tables": [
                        {
                            "name": "0",
                            "fields": [
                                {"name": "_time", "type": "datetime"},
                                {"name": "geo.country", "type": "string"},
                                {"name": "elapsed", "type": "timespan"},
                                {"name": "ratio", "type": "float"},
                            ],
                            "columns": [
                                ["2026-11-22T01:02:03.123456789Z"],
                                ["DE"],
                                ["15ms"],
                                [2],
                            ],
                        }
                    ],
                },
            )
            return
        # Default "query" case returns the pushed-down summary layout columns.
        self.send_json(
            200,
            {
                "format": "tabular",
                "status": {"isPartial": False},
                "tables": [
                    {
                        "name": "0",
                        "fields": [
                            {"name": "__tenzir_column_0", "type": "datetime"},
                            {"name": "method", "type": "string"},
                            {"name": "__tenzir_column_1", "type": "integer"},
                            {"name": "__tenzir_column_2", "type": "integer"},
                        ],
                        "columns": [
                            ["2026-11-22T00:00:00Z"],
                            ["GET"],
                            [42],
                            [7],
                        ],
                    }
                ],
            },
        )

    def handle_ingest(self, raw: bytes) -> None:
        assert isinstance(self.server, Server)
        self.server.ingestions += 1
        if self.headers.get("Content-Type") != "application/x-ndjson":
            self.server.failures.append(
                f"unexpected Content-Type: {self.headers.get('Content-Type')!r}"
            )
        if self.headers.get("Content-Encoding") != "gzip":
            self.server.failures.append(
                f"unexpected Content-Encoding: {self.headers.get('Content-Encoding')!r}"
            )
        payload = gzip.decompress(raw).decode()
        lines = [line for line in payload.splitlines() if line]
        for line in lines:
            self.server.events.append(json.loads(line))
        if self.server.options.case == "ingest-malformed-ack":
            payload = b"<html><body>ok</body></html>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("X-Axiom-Trace-Id", _TRACE_ID)
            self.end_headers()
            self.wfile.write(payload)
            return
        if self.server.options.case == "ingest-failures":
            self.send_json(
                200,
                {
                    "ingested": max(0, len(lines) - 1),
                    "failed": 1,
                    "failures": [
                        {
                            "timestamp": "1969-01-01T00:00:00Z",
                            "error": "event timestamp too old",
                        }
                    ],
                    "messages": [
                        {
                            "priority": "warn",
                            "code": "column_limit_drop",
                            "msg": "field limit reached",
                        }
                    ],
                },
            )
            return
        self.send_json(200, {"ingested": len(lines), "failed": 0, "failures": []})


@fixture(name="axiom", options=Options, assertions=Assertions)
def run() -> FixtureHandle:
    options = current_options("axiom")
    assert isinstance(options, Options)
    server = Server(options)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    env = {
        "AXIOM_URL": f"http://127.0.0.1:{server.server_port}",
        "AXIOM_TOKEN": _TOKEN,
    }
    if binary := os.environ.get("TENZIR_BINARY"):
        build = Path(shlex.split(binary)[0]).resolve().parent.parent
        if (build / "lib/tenzir/plugins").is_dir():
            env["TENZIR_PLUGIN_DIRS"] = str(build / "lib/tenzir/plugins")
            env["TENZIR_PLUGINS"] = "axiom"
        if (build / "lib").is_dir():
            env["DYLD_LIBRARY_PATH"] = str(build / "lib")

    def assert_test(*, assertions: Assertions, **_: Any) -> None:
        assert not server.failures, server.failures
        assert server.queries == assertions.queries, (
            server.queries,
            assertions.queries,
        )
        assert server.ingestions == assertions.ingestions, (
            server.ingestions,
            assertions.ingestions,
        )
        if assertions.apl is not None:
            assert server.request_body is not None
            assert server.request_body.get("apl") == assertions.apl, (
                server.request_body.get("apl"),
                assertions.apl,
            )
        if assertions.start_time is not None:
            assert server.request_body is not None
            assert server.request_body.get("startTime") == assertions.start_time, (
                server.request_body.get("startTime"),
                assertions.start_time,
            )
            assert server.request_body.get("endTime") == assertions.end_time, (
                server.request_body.get("endTime"),
                assertions.end_time,
            )
        if assertions.events is not None:
            assert server.events == assertions.events, (
                server.events,
                assertions.events,
            )

    def teardown() -> None:
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
        assert not server.failures, server.failures

    return FixtureHandle(env=env, teardown=teardown, hooks={"assert_test": assert_test})
