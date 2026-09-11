"""Deterministic Log Analytics query and Entra token endpoints."""

from __future__ import annotations

import copy
import json
import os
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs

from tenzir_test import FixtureHandle, current_options, fixture

_QUERY = '  SecurityEvent\n| project Message = "azüre", TimeGenerated  '
_WORKSPACE = "01234567-89ab-cdef-0123-456789abcdef"
_COLUMNS = [
    {"name": "TimeGenerated", "type": "datetime"},
    {"name": "Count", "type": "long"},
    {"name": "Small", "type": "int"},
    {"name": "Ratio", "type": "real"},
    {"name": "Enabled", "type": "bool"},
    {"name": "Message", "type": "string"},
    {"name": "Id", "type": "guid"},
    {"name": "Elapsed", "type": "timespan"},
    {"name": "Amount", "type": "decimal"},
    {"name": "NullTime", "type": "datetime"},
    {"name": "Details", "type": "dynamic"},
]
_ROW = [
    "2026-01-02T03:04:05.1234567Z",
    9223372036854775807,
    -2147483648,
    1.5,
    True,
    "192.0.2.1",
    _WORKSPACE,
    "-1.02:03:04.1234567",
    "12345678901234567890.123456789012345678",
    None,
    {"text": "2026-01-01T00:00:00Z", "a.b": [1, 2, None]},
]


@dataclass(frozen=True)
class Options:
    case: str = "typed"
    bounded: bool = False
    wait: int = 180


@dataclass(frozen=True)
class Assertions:
    queries: int = 1
    tokens: int = 1


class Server(ThreadingHTTPServer):
    def __init__(self, options: Options) -> None:
        super().__init__(("127.0.0.1", 0), Handler)
        self.options = options
        self.queries = 0
        self.tokens = 0
        self.body: bytes | None = None
        self.failures: list[str] = []
        self.request_received = threading.Event()
        self.release_response = threading.Event()


class Handler(BaseHTTPRequestHandler):
    def send_json(self, status: int, body: object, **headers: str) -> None:
        self.send_body(status, json.dumps(body).encode(), **headers)

    def send_body(self, status: int, body: bytes, **headers: str) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        assert isinstance(self.server, Server)
        self.send_header(
            "x-ms-request-id",
            "DO NOT LOG RESULT DATA"
            if self.server.options.case == "unsafe-request-id"
            else "azure-request-123",
        )
        for name, value in headers.items():
            self.send_header(name, value)
        try:
            self.end_headers()
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self) -> None:  # noqa: N802
        assert isinstance(self.server, Server)
        assert self.path == "/ready"
        self.send_json(200, {"ready": self.server.request_received.wait(10)})

    def do_POST(self) -> None:  # noqa: N802
        assert isinstance(self.server, Server)
        try:
            self.handle_request(self.server)
        except (AssertionError, KeyError, ValueError) as error:
            self.server.failures.append(str(error))
            self.send_json(400, {"error": "fixture assertion failed"})

    def handle_request(self, server: Server) -> None:
        body = self.rfile.read(int(self.headers["Content-Length"]))
        case = server.options.case
        if self.path == "/fixture-tenant/oauth2/v2.0/token":
            server.tokens += 1
            assert parse_qs(body.decode()) == {
                "client_id": ["fixture-client"],
                "client_secret": ["fixture-secret"],
                "grant_type": ["client_credentials"],
                "scope": ["https://api.loganalytics.io/.default"],
            }, "unexpected token request"
            if case == "bad-auth" or (case == "refresh-failed" and server.tokens > 1):
                self.send_json(
                    200, {"access_token": {"secret": "DO-NOT-LOG-CREDENTIAL"}}
                )
                return
            self.send_json(
                200,
                {
                    "access_token": f"fixture-token-{server.tokens}",
                    # The refresh scenario's Retry-After exceeds this lifetime.
                    "expires_in": 1 if case.startswith("refresh") else 3600,
                },
            )
            return
        server.queries += 1
        assert self.path == f"/v1/workspaces/{_WORKSPACE}/query", self.path
        assert self.headers["Authorization"] == f"Bearer fixture-token-{server.tokens}"
        assert self.headers["Content-Type"] == "application/json; charset=utf-8"
        assert self.headers["Prefer"] == f"wait={server.options.wait}"
        if server.body is not None:
            assert body == server.body, "query changed during retry"
        server.body = body
        request = json.loads(body)
        expected = {"query": _QUERY}
        if server.options.bounded:
            expected["timespan"] = (
                "2026-01-01T00:00:00.000000000Z/2026-01-02T00:00:00.000000000Z"
            )
        assert request == expected, request
        if case == "cancelled":
            server.request_received.set()
            assert server.release_response.wait(20), "cancellation test did not finish"
            self.send_json(200, {"tables": []})
            return
        status = {
            "retry": 429,
            "retry-503": 503,
            "retry-504": 504,
            "refresh": 429,
            "refresh-failed": 429,
            "exhausted": 429,
            "unauthorized": 401,
            "forbidden": 403,
            "bad-query": 400,
        }.get(case)
        if status and (
            server.queries == 1
            or case in {"exhausted", "unauthorized", "forbidden", "bad-query"}
        ):
            self.send_json(
                status,
                {"error": {"code": "Error", "message": "DO-NOT-LOG-DATA"}},
                **{"Retry-After": "2" if case.startswith("refresh") else "0"},
            )
            return
        if case == "retry-disconnect" and server.queries == 1:
            self.send_response(200)
            self.send_header("Content-Length", "100")
            self.end_headers()
            self.wfile.write(b'{"tables": [')
            self.close_connection = True
            return
        if case == "no-content":
            self.send_body(204, b"")
            return
        if case == "empty-body":
            self.send_body(200, b"")
            return
        if case == "malformed-json":
            self.send_body(200, b"{")
            return
        if case == "oversized":
            self.send_response(200)
            self.send_header("Content-Length", str(129 * 1024 * 1024))
            self.end_headers()
            try:
                for _ in range(129):
                    self.wfile.write(b" " * (1024 * 1024))
            except (BrokenPipeError, ConnectionResetError):
                pass
            return
        columns = copy.deepcopy(_COLUMNS)
        rows = [copy.deepcopy(_ROW)]
        table = {"name": "PrimaryResult", "columns": columns, "rows": rows}
        envelope = {"tables": [table]}
        if case == "empty":
            rows.clear()
        elif case == "empty-tables":
            envelope["tables"].clear()
        elif case == "multiple":
            envelope["tables"].append(
                {
                    "name": "Other",
                    "columns": [{"name": "Value", "type": "string"}],
                    "rows": [["second"]],
                }
            )
        elif case == "metadata":
            envelope.update(statistics={"query": {}}, render={"visualization": "table"})
        elif case == "reordered":
            table = {"rows": rows, "name": "PrimaryResult", "columns": columns}
            envelope = {"statistics": {}, "tables": [table]}
        elif case == "encoded-scalars":
            rows[0][1:5] = ["-9223372036854775808", "2147483647", "1.25e2", "false"]
        elif case in {"decimal-number", "decimal-integer", "decimal-exponent"}:
            rows[0][8] = "EXACT-DECIMAL"
        elif case == "dynamic-overflow":
            rows[0][-1] = 100000000000000000000000000000
        elif case == "encoded-dynamic-overflow":
            rows[0][-1] = "100000000000000000000000000000"
        elif case == "nulls":
            rows[:] = [[None] * len(columns)]
        elif case in {"dynamic", "dynamic-scalars", "mixed-list"}:
            columns[:] = [{"name": "Value", "type": "dynamic"}]
            values = {
                "dynamic": [
                    {"a.b": "192.0.2.1"},
                    [1, 2],
                    '{"text":"1h"}',
                    "plain",
                    None,
                ],
                "dynamic-scalars": ["42", "true", "null", '"42"'],
                "mixed-list": [[1, "x", True, {"a": 2}]],
            }[case]
            rows[:] = [[value] for value in values]
        elif case == "timespan-limits":
            columns[:] = [{"name": "Value", "type": "timespan"}]
            rows[:] = [
                [value]
                for value in [
                    "00:00:00",
                    "00:00:00.0000001",
                    "106751.23:47:16.854775807",
                    "-106751.23:47:16.854775808",
                ]
            ]
        elif case == "many-rows":
            columns[:] = [{"name": "Value", "type": "long"}]
            rows[:] = [[i] for i in range(9000)]
        elif case == "late-invalid":
            columns[:] = [{"name": "Value", "type": "long"}]
            rows[:] = [[i] for i in range(9000)] + [["DO-NOT-LOG-DATA"]]
        elif case == "late-table-invalid":
            envelope["tables"].append(
                {
                    "name": "Other",
                    "columns": [{"name": "Value", "type": "long"}],
                    "rows": [["DO-NOT-LOG-DATA"]],
                }
            )
        elif case == "overflow":
            rows[0][1] = 9223372036854775808
        elif case == "underflow":
            rows[0][1] = -9223372036854775809
        elif case == "int-overflow":
            rows[0][2] = 2147483648
        elif case == "bad-datetime":
            rows[0][0] = "DO-NOT-LOG-DATA"
        elif case == "bad-bool":
            rows[0][4] = "yes"
        elif case == "bad-real":
            rows[0][3] = "NaN"
        elif case == "bad-string":
            rows[0][5] = 42
        elif case == "bad-decimal":
            rows[0][8] = "NaN"
        elif case == "bad-timespan":
            rows[0][7] = "00:60:00"
        elif case == "timespan-overflow":
            rows[0][7] = "106751.23:47:16.854775808"
        elif case == "timespan-days-overflow":
            rows[0][7] = "18446744073709551615.00:00:00"
        elif case == "duplicate-column":
            columns[1]["name"] = columns[0]["name"]
        elif case == "unknown-type":
            columns[0]["type"] = "DO-NOT-LOG-DATA"
        elif case == "missing-column":
            rows[0].pop()
        elif case == "extra-column":
            rows[0].append(42)
        elif case == "bad-row":
            rows[:] = [{"value": 42}]
        elif case in {"partial", "unsafe-request-id"}:
            envelope["error"] = {"code": "PartialError", "message": "DO-NOT-LOG-DATA"}
        elif case == "truncated":
            envelope["isTruncated"] = True
        elif case == "next-link":
            envelope["nextLink"] = "https://example.invalid/DO-NOT-LOG-DATA"
        elif case == "duplicate-table":
            envelope["tables"].append(table)
        elif case == "no-columns":
            columns.clear()
        elif case == "deep-dynamic":
            nested: object = 42
            for _ in range(102):
                nested = [nested]
            rows[0][-1] = nested
        elif case == "value-budget":
            columns[:] = [{"name": "Value", "type": "dynamic"}]
            rows[:] = [[[None] * (2 * 1024 * 1024)]]
        elif case == "too-many-rows":
            columns[:] = [{"name": "Value", "type": "long"}]
            rows[:] = [[0]] * 500001
        elif case == "too-many-columns":
            columns[:] = [{"name": f"c{i}", "type": "long"} for i in range(1025)]
            rows.clear()
        raw = json.dumps(envelope).encode()
        if case == "decimal-number":
            raw = raw.replace(
                b'"EXACT-DECIMAL"', b"12345678901234567890.123456789012345678"
            )
        elif case == "decimal-integer":
            raw = raw.replace(b'"EXACT-DECIMAL"', b"12345678901234567890123456789")
        elif case == "decimal-exponent":
            raw = raw.replace(
                b'"EXACT-DECIMAL"', b"-1.2345678901234567890123456789e+20"
            )
        elif case == "duplicate-envelope":
            raw = raw[:-1] + b', "tables": []}'
        elif case == "duplicate-dynamic":
            raw = raw.replace(
                b'{"text": "2026-01-01T00:00:00Z", "a.b": [1, 2, null]}',
                b'{"x": 1, "x": 2}',
            )
        self.send_body(200, raw)

    def log_message(self, *_: object) -> None:
        pass


@fixture(name="azure_log_analytics", options=Options, assertions=Assertions)
def run() -> FixtureHandle:
    options = current_options("azure_log_analytics")
    assert isinstance(options, Options)
    server = Server(options)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    url = f"http://127.0.0.1:{server.server_port}"
    env = {
        "AZLOG_URL": url,
        "AZLOG_AUTHORITY": url,
        "AZLOG_WORKSPACE": _WORKSPACE,
        "AZLOG_QUERY": _QUERY,
    }
    if binary := os.environ.get("TENZIR_BINARY"):
        build = Path(binary).resolve().parent.parent
        if (build / "lib/tenzir/plugins").is_dir():
            env["TENZIR_PLUGIN_DIRS"] = str(build / "lib/tenzir/plugins")
            env["TENZIR_PLUGINS"] = "to_azure_log_analytics"
        if (build / "lib").is_dir():
            env["DYLD_LIBRARY_PATH"] = str(build / "lib")

    def assert_test(*, assertions: Assertions, **_: Any) -> None:
        assert not server.failures, server.failures
        assert server.queries == assertions.queries, (
            server.queries,
            assertions.queries,
        )
        assert server.tokens == assertions.tokens, (server.tokens, assertions.tokens)

    def teardown() -> None:
        server.release_response.set()
        server.shutdown()
        thread.join(timeout=2)
        server.server_close()
        assert not server.failures, server.failures

    return FixtureHandle(
        env=env,
        teardown=teardown,
        hooks={"assert_test": assert_test},
    )
