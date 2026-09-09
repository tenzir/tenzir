"""Deterministic Graph hunting and Entra endpoints; never contacts Microsoft."""

from __future__ import annotations

import json
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator

from tenzir_test import fixture

from .microsoft_graph import (
    _ACCESS_TOKEN,
    _CLIENT_ID,
    _CLIENT_SECRET,
    _HOST,
    _TENANT_ID,
    _TokenHandler,
    _TokenServer,
    _json_response,
    _json_response_with_headers,
    _plugin_env,
)

# Includes significant whitespace, Unicode, quotes, and a newline.
_QUERY = '  print Message = "défender"\n| project Message  '
_SCHEMA = [
    {"name": "Timestamp", "type": "DateTime"},
    {"name": "Count", "type": "Int64"},
    {"name": "Ratio", "type": "Double"},
    {"name": "Enabled", "type": "Boolean"},
    {"name": "Message", "type": "String"},
    {"name": "NullTime", "type": "DateTime"},
    {"name": "Details", "type": "Dynamic"},
]
_ROW = {
    "Timestamp": "2026-01-02T03:04:05.1234567Z",
    "Count": 9223372036854775807,
    "Ratio": 1.5,
    "Enabled": True,
    "Message": "192.0.2.1",
    "NullTime": None,
    "Details": {"text": "2026-01-01T00:00:00Z", "values": [1, 2, None]},
}


class _Server(ThreadingHTTPServer):
    calls = 0
    query = ""
    request_body: bytes | None = None

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.failures: list[str] = []


class _Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        try:
            self.handle_query()
        except (AssertionError, KeyError, ValueError) as error:
            assert isinstance(self.server, _Server)
            self.server.failures.append(str(error))
            _json_response(self, 400, {"error": "fixture assertion failed"})

    def handle_query(self) -> None:
        server = self.server
        assert isinstance(server, _Server)
        server.calls += 1
        assert self.path == "/v1.0/security/runHuntingQuery"
        assert self.headers["Content-Type"] == "application/json; charset=utf-8"
        body = self.rfile.read(int(self.headers["Content-Length"]))
        if server.request_body is not None:
            assert body == server.request_body, "request changed during retry"
        server.request_body = body
        request = json.loads(body)
        query = request["Query"]
        server.query = query
        assert set(request) <= {"Query", "Timespan"}
        refreshing = query in {"refresh", "refresh-failed"}
        token = f"{_ACCESS_TOKEN}-{server.calls}" if refreshing else _ACCESS_TOKEN
        assert self.headers["Authorization"] == f"Bearer {token}"
        if query == "bounded" or refreshing:
            assert request["Timespan"] == (
                "2026-01-01T00:00:00.000000000Z/2026-01-02T00:00:00.000000000Z"
            )
        else:
            assert "Timespan" not in request
        if refreshing and server.calls == 1:
            _json_response_with_headers(
                self,
                429,
                {"error": {"code": "TooManyRequests"}},
                [("Retry-After", "2")],
            )
            return
        if query in {
            "retry",
            "retry-503",
            "retry-504",
            "exhausted",
            "unauthorized",
            "forbidden",
            "bad-query",
        }:
            if not query.startswith("retry") or server.calls == 1:
                status = {
                    "unauthorized": 401,
                    "forbidden": 403,
                    "bad-query": 400,
                    "retry-503": 503,
                    "retry-504": 504,
                }.get(query, 429)
                _json_response_with_headers(
                    self,
                    status,
                    {
                        "error": {
                            "code": {
                                401: "InvalidAuthenticationToken",
                                403: "Forbidden",
                                429: "TooManyRequests",
                                503: "ServiceUnavailable",
                                504: "GatewayTimeout",
                            }.get(status, "BadRequest"),
                            "message": "DO-NOT-LOG-DATA",
                        }
                    },
                    [("Retry-After", "0"), ("request-id", "abc-123")],
                )
                return
        schema = [dict(column) for column in _SCHEMA]
        rows = [dict(_ROW)]
        envelope: dict[str, object] = {"schema": schema, "results": rows}
        if query == _QUERY:
            pass
        elif query in {"bounded", "retry", "retry-503", "retry-504", "refresh"}:
            pass
        elif query == "empty":
            rows.clear()
        elif query == "empty-schema":
            schema.clear()
            rows.clear()
        elif query == "dynamic-only":
            envelope = {
                "schema": [{"name": "Value", "type": "Dynamic"}],
                "results": [{"Value": {"a.b": "192.0.2.1"}}, {"Value": [1, 2]}],
            }
        elif query == "dynamic":
            rows = [
                dict(_ROW, Details=value)
                for value in [
                    '{"nested":{"IP":"192.0.2.1"}}',
                    [1, 2],
                    42,
                    True,
                    "plain",
                    None,
                ]
            ]
            envelope["results"] = rows
        elif query == "dynamic-scalars":
            envelope = {
                "schema": [{"name": "Value", "type": "Dynamic"}],
                "results": [{"Value": value} for value in ["42", "true", "null"]],
            }
        elif query == "mixed-list":
            envelope = {
                "schema": [{"name": "Value", "type": "Dynamic"}],
                "results": [{"Value": [1, "x", True, {"a": 2}]}],
            }
        elif query == "strings":
            rows = [
                dict(_ROW, Message=value)
                for value in [
                    "2026-01-01T00:00:00Z",
                    "1h",
                    "192.0.2.0/24",
                    "true",
                    "42",
                ]
            ]
            envelope["results"] = rows
        elif query == "late-invalid":
            rows.append(dict(_ROW, Count="DO-NOT-LOG-DATA"))
        elif query == "overflow":
            rows[0]["Count"] = 9223372036854775808
        elif query == "negative-limit":
            rows[0]["Count"] = -9223372036854775808
        elif query == "underflow":
            rows[0]["Count"] = -9223372036854775809
        elif query == "malformed-json":
            self.send_response(200)
            self.send_header("Content-Length", "1")
            self.end_headers()
            self.wfile.write(b"{")
            return
        elif query == "bad-datetime":
            rows[0]["Timestamp"] = "DO-NOT-LOG-DATA"
        elif query == "bad-bool":
            rows[0]["Enabled"] = "true"
        elif query == "bad-string":
            rows[0]["Message"] = 42
        elif query == "unknown-type":
            schema[0]["type"] = "Unsupported"
        elif query == "duplicate-column":
            schema.append(dict(schema[0]))
        elif query == "missing-column":
            del rows[0]["Count"]
        elif query == "extra-column":
            rows[0]["Unexpected"] = "DO-NOT-LOG-DATA"
        elif query == "unsafe-request-id":
            envelope["error"] = {"code": "PartialError"}
            _json_response_with_headers(
                self, 200, envelope, [("request-id", "DO NOT LOG RESULT DATA")]
            )
            return
        elif query == "partial":
            envelope["error"] = {"code": "PartialError", "message": "DO-NOT-LOG-DATA"}
        elif query == "truncated":
            envelope["isTruncated"] = True
        elif query == "next-link":
            envelope["@odata.nextLink"] = "https://example.invalid"
        elif query == "bad-schema":
            envelope["schema"] = None
        elif query == "bad-results":
            envelope["results"] = None
        else:
            raise AssertionError(f"unexpected query: {query!r}")
        _json_response_with_headers(
            self, HTTPStatus.OK, envelope, [("request-id", "defender-request-123")]
        )

    def log_message(self, *_: object) -> None:
        pass


class _BadTokenHandler(_TokenHandler):
    def do_POST(self) -> None:  # noqa: N802
        _json_response(self, 200, {"access_token": {"secret": "DO-NOT-LOG-CREDENTIAL"}})


class _RefreshingTokenHandler(_TokenHandler):
    fail_refresh = False

    def do_POST(self) -> None:  # noqa: N802
        assert isinstance(self.server, _TokenServer)
        self.rfile.read(int(self.headers["Content-Length"]))
        self.server.token_requests += 1
        attempt = self.server.token_requests
        if self.fail_refresh and attempt > 1:
            _json_response(self, 200, {"access_token": {"secret": "DO-NOT-LOG"}})
            return
        _json_response(
            self,
            200,
            {"access_token": f"{_ACCESS_TOKEN}-{attempt}", "expires_in": 1},
        )


class _FailedRefreshTokenHandler(_RefreshingTokenHandler):
    fail_refresh = True


def _run(
    bad_auth: bool = False,
    token_handler: type[_TokenHandler] = _TokenHandler,
) -> Iterator[dict[str, str]]:
    token = _TokenServer((_HOST, 0), _BadTokenHandler if bad_auth else token_handler)
    graph = _Server((_HOST, 0), _Handler)
    threads = [
        threading.Thread(target=s.serve_forever, daemon=True) for s in (token, graph)
    ]
    for thread in threads:
        thread.start()
    try:
        yield {
            **_plugin_env(),
            "DEFENDER_URL": f"http://{_HOST}:{graph.server_port}",
            "DEFENDER_AUTHORITY": f"http://{_HOST}:{token.server_port}",
            "DEFENDER_TENANT": _TENANT_ID,
            "DEFENDER_CLIENT": _CLIENT_ID,
            "DEFENDER_SECRET": _CLIENT_SECRET,
            "DEFENDER_QUERY": _QUERY,
        }
        assert not graph.failures, graph.failures
        expected_calls = {
            "retry": 2,
            "retry-503": 2,
            "retry-504": 2,
            "exhausted": 6,
            "refresh": 2,
        }.get(graph.query, 1)
        if graph.query in {"refresh", "refresh-failed"}:
            assert token.token_requests == 2, token.token_requests
        assert graph.calls == (0 if bad_auth else expected_calls), (
            graph.query,
            graph.calls,
        )
    finally:
        token.shutdown()
        graph.shutdown()
        for thread in threads:
            thread.join(timeout=2)
        token.server_close()
        graph.server_close()


@fixture(name="microsoft_defender")
def run() -> Iterator[dict[str, str]]:
    yield from _run()


@fixture(name="microsoft_defender_refresh")
def run_refresh() -> Iterator[dict[str, str]]:
    yield from _run(token_handler=_RefreshingTokenHandler)


@fixture(name="microsoft_defender_refresh_failed")
def run_refresh_failed() -> Iterator[dict[str, str]]:
    yield from _run(token_handler=_FailedRefreshTokenHandler)


@fixture(name="microsoft_defender_bad_auth")
def run_bad_auth() -> Iterator[dict[str, str]]:
    yield from _run(bad_auth=True)
