"""Microsoft Graph fixture for integration testing.

The fixture emulates the Microsoft identity platform client-credentials token
endpoint and a small subset of Microsoft Graph resources used by the operator
integration tests.

Environment variables yielded:
- MS_GRAPH_FIXTURE_BASE_URL_V1
- MS_GRAPH_FIXTURE_BASE_URL_BETA
- MS_GRAPH_FIXTURE_AUTHORITY
- MS_GRAPH_FIXTURE_TENANT_ID
- MS_GRAPH_FIXTURE_CLIENT_ID
- MS_GRAPH_FIXTURE_CLIENT_SECRET
"""

from __future__ import annotations

import json
import logging
import threading
import time
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator, cast
from urllib.parse import parse_qs, urlsplit

from tenzir_test import fixture

from ._utils import find_free_port

logger = logging.getLogger(__name__)

_HOST = "127.0.0.1"
_TENANT_ID = "test-tenant"
_CLIENT_ID = "test-client"
_CLIENT_SECRET = "test-secret"
_ACCESS_TOKEN = "test-ms-graph-token"
_SKIPTOKEN = "page-2"
_DELTA_SKIPTOKEN = "delta-page-2"
_DELTA_TOKEN_INITIAL = "initial-token"
_DELTA_TOKEN_SECOND = "second-token"
_FIRST_PAGE_DELAY_SECONDS = 2.1


def _json_response(handler: BaseHTTPRequestHandler, code: int, obj: object) -> None:
    body = json.dumps(obj).encode()
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


def _json_response_with_headers(
    handler: BaseHTTPRequestHandler,
    code: int,
    obj: object,
    headers: list[tuple[str, str]],
) -> None:
    body = json.dumps(obj).encode()
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    for name, value in headers:
        handler.send_header(name, value)
    handler.end_headers()
    handler.wfile.write(body)


class _TokenServer(ThreadingHTTPServer):
    token_requests: int = 0


class _GraphServer(ThreadingHTTPServer):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.lock = threading.Lock()
        self.requests: dict[str, int] = {}

    def record_request(self, path: str) -> int:
        with self.lock:
            count = self.requests.get(path, 0) + 1
            self.requests[path] = count
            return count


class _TokenHandler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802
        parts = urlsplit(self.path)
        expected_path = f"/{_TENANT_ID}/oauth2/v2.0/token"
        if parts.path != expected_path:
            _json_response(self, HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        length = int(self.headers.get("Content-Length", "0"))
        body = self.rfile.read(length).decode()
        form = parse_qs(body, keep_blank_values=True)
        expected = {
            "client_id": [_CLIENT_ID],
            "client_secret": [_CLIENT_SECRET],
            "grant_type": ["client_credentials"],
            "scope": ["https://graph.microsoft.com/.default"],
        }
        for key, value in expected.items():
            if form.get(key) != value:
                _json_response(
                    self,
                    HTTPStatus.BAD_REQUEST,
                    {"error": f"unexpected {key}: {form.get(key)!r}"},
                )
                return
        server = cast(_TokenServer, self.server)
        server.token_requests += 1
        if server.token_requests > 1:
            _json_response(
                self,
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "temporary auth outage"},
            )
            return
        _json_response(
            self,
            HTTPStatus.OK,
            {
                "token_type": "Bearer",
                "expires_in": 4,
                "access_token": _ACCESS_TOKEN,
            },
        )

    def log_message(self, *_: object) -> None:
        return


class _GraphHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        if self.headers.get("Authorization") != f"Bearer {_ACCESS_TOKEN}":
            _json_response(self, HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
            return
        parts = urlsplit(self.path)
        query = parse_qs(parts.query, keep_blank_values=True)
        base_url = f"http://{_HOST}:{self.server.server_port}"
        if parts.path == "/v1.0/users/untrusted-next-link":
            _json_response(
                self,
                HTTPStatus.OK,
                {
                    "@odata.context": f"{base_url}/v1.0/$metadata#users",
                    "@odata.nextLink": "https://example.invalid/v1.0/users",
                    "value": [],
                },
            )
            return
        if parts.path == "/v1.0/users/retry-after":
            server = cast(_GraphServer, self.server)
            if server.record_request(parts.path) == 1:
                _json_response_with_headers(
                    self,
                    HTTPStatus.TOO_MANY_REQUESTS,
                    {"error": "too many requests"},
                    [("Retry-After", "1")],
                )
                return
            _json_response(
                self,
                HTTPStatus.OK,
                {
                    "@odata.context": f"{base_url}/v1.0/$metadata#users",
                    "value": [{"id": "retry-user-1", "displayName": "Retry User"}],
                },
            )
            return
        if parts.path == "/v1.0/users/retry-exhausted":
            _json_response_with_headers(
                self,
                HTTPStatus.SERVICE_UNAVAILABLE,
                {"error": "service unavailable", "request": "final"},
                [("Retry-After", "0")],
            )
            return
        if parts.path == "/v1.0/users/permanent-error":
            _json_response(
                self,
                HTTPStatus.BAD_REQUEST,
                {"error": "bad request"},
            )
            return
        if parts.path == "/v1.0/users/delta":
            if query.get("$filter") == ["malformed-delta-link"]:
                _json_response(
                    self,
                    HTTPStatus.OK,
                    {
                        "@odata.context": f"{base_url}/v1.0/$metadata#users",
                        "@odata.deltaLink": "http://[::1",
                        "value": [],
                    },
                )
                return
            if query.get("$filter") == ["missing-delta-link"]:
                _json_response(
                    self,
                    HTTPStatus.OK,
                    {
                        "@odata.context": f"{base_url}/v1.0/$metadata#users",
                        "value": [],
                    },
                )
                return
            if query.get("$filter") == ["untrusted-delta-link"]:
                _json_response(
                    self,
                    HTTPStatus.OK,
                    {
                        "@odata.context": f"{base_url}/v1.0/$metadata#users",
                        "@odata.deltaLink": "https://example.invalid/v1.0/users/delta",
                        "value": [],
                    },
                )
                return
            if query.get("$deltatoken") == [_DELTA_TOKEN_INITIAL]:
                _json_response(
                    self,
                    HTTPStatus.OK,
                    {
                        "@odata.context": f"{base_url}/v1.0/$metadata#users",
                        "@odata.deltaLink": (
                            f"{base_url}/v1.0/users/delta?"
                            f"$deltatoken={_DELTA_TOKEN_SECOND}"
                        ),
                        "value": [
                            {
                                "id": "user-4",
                                "displayName": "Dorothy Vaughan",
                                "userPrincipalName": "dorothy@example.com",
                            }
                        ],
                    },
                )
                return
            if query.get("$deltatoken") == [_DELTA_TOKEN_SECOND]:
                _json_response(
                    self,
                    HTTPStatus.OK,
                    {
                        "@odata.context": f"{base_url}/v1.0/$metadata#users",
                        "@odata.deltaLink": (
                            f"{base_url}/v1.0/users/delta?"
                            f"$deltatoken={_DELTA_TOKEN_SECOND}"
                        ),
                        "value": [],
                    },
                )
                return
            if query.get("$skiptoken") == [_DELTA_SKIPTOKEN]:
                _json_response(
                    self,
                    HTTPStatus.OK,
                    {
                        "@odata.context": f"{base_url}/v1.0/$metadata#users",
                        "@odata.deltaLink": (
                            f"{base_url}/v1.0/users/delta?"
                            f"$deltatoken={_DELTA_TOKEN_INITIAL}"
                        ),
                        "value": [
                            {
                                "@odata.etag": 'W/"user-3"',
                                "id": "user-3",
                                "displayName": "Katherine Johnson",
                                "userPrincipalName": "katherine@example.com",
                            }
                        ],
                    },
                )
                return
            _json_response(
                self,
                HTTPStatus.OK,
                {
                    "@odata.context": f"{base_url}/v1.0/$metadata#users",
                    "@odata.nextLink": (
                        f"{base_url}/v1.0/users/delta?$skiptoken={_DELTA_SKIPTOKEN}"
                    ),
                    "value": [
                        {
                            "@odata.etag": 'W/"user-1"',
                            "id": "user-1",
                            "displayName": "Ada Lovelace",
                            "userPrincipalName": "ada@example.com",
                        },
                        {
                            "@odata.type": "#microsoft.graph.user",
                            "id": "user-2",
                            "displayName": "Grace Hopper",
                            "userPrincipalName": "grace@example.com",
                        },
                    ],
                },
            )
            return
        if parts.path == "/v1.0/users":
            if query.get("$skiptoken") == [_SKIPTOKEN]:
                _json_response(
                    self,
                    HTTPStatus.OK,
                    {
                        "@odata.context": f"{base_url}/v1.0/$metadata#users",
                        "value": [
                            {
                                "@odata.etag": 'W/"user-3"',
                                "id": "user-3",
                                "displayName": "Katherine Johnson",
                                "userPrincipalName": "katherine@example.com",
                            }
                        ],
                    },
                )
                return
            time.sleep(_FIRST_PAGE_DELAY_SECONDS)
            _json_response(
                self,
                HTTPStatus.OK,
                {
                    "@odata.context": f"{base_url}/v1.0/$metadata#users",
                    "@odata.nextLink": f"{base_url}/v1.0/users?$skiptoken={_SKIPTOKEN}",
                    "value": [
                        {
                            "@odata.etag": 'W/"user-1"',
                            "id": "user-1",
                            "displayName": "Ada Lovelace",
                            "userPrincipalName": "ada@example.com",
                            "manager": {
                                "@odata.type": "#microsoft.graph.user",
                                "id": "manager-1",
                            },
                        },
                        {
                            "@odata.type": "#microsoft.graph.user",
                            "id": "user-2",
                            "displayName": "Grace Hopper",
                            "userPrincipalName": "grace@example.com",
                        },
                    ],
                },
            )
            return
        if parts.path == "/v1.0/groups":
            if parts.query:
                body = {
                    "@odata.context": f"{base_url}/v1.0/$metadata#groups",
                    "value": [
                        {
                            "id": "group-1",
                            "displayName": "Security",
                            "securityEnabled": True,
                        }
                    ],
                }
            else:
                body = {
                    "@odata.context": f"{base_url}/v1.0/$metadata#groups",
                    "value": [{"id": "group-1", "displayName": "Security"}],
                }
            _json_response(self, HTTPStatus.OK, body)
            return
        if parts.path == "/v1.0/devices/delta":
            _json_response(
                self,
                HTTPStatus.OK,
                {
                    "@odata.context": f"{base_url}/v1.0/$metadata#devices",
                    "@odata.deltaLink": (
                        f"{base_url}/v1.0/devices/delta?"
                        f"$deltatoken={_DELTA_TOKEN_SECOND}"
                    ),
                    "value": [{"id": "device-1", "displayName": "Laptop"}],
                },
            )
            return
        if parts.path == "/beta/users":
            _json_response(
                self,
                HTTPStatus.OK,
                {
                    "@odata.context": f"{base_url}/beta/$metadata#users",
                    "value": [
                        {
                            "id": "beta-user-1",
                            "displayName": "Beta User",
                            "signInActivity": {
                                "lastSignInDateTime": "2026-05-13T12:00:00Z"
                            },
                        }
                    ],
                },
            )
            return
        _json_response(self, HTTPStatus.NOT_FOUND, {"error": "not found"})

    def log_message(self, *_: object) -> None:
        return


def _plugin_env() -> dict[str, str]:
    import os
    from pathlib import Path

    env: dict[str, str] = {}
    if binary := os.environ.get("TENZIR_BINARY"):
        build_dir = Path(binary).resolve().parent.parent
        plugin_dir = build_dir / "lib" / "tenzir" / "plugins"
        if plugin_dir.is_dir():
            env["TENZIR_PLUGIN_DIRS"] = str(plugin_dir)
            env["TENZIR_PLUGINS"] = "microsoft_graph"
        lib_dir = build_dir / "lib"
        if lib_dir.is_dir():
            env["DYLD_LIBRARY_PATH"] = str(lib_dir)
    return env


@fixture(name="microsoft_graph")
def run() -> Iterator[dict[str, str]]:
    token_server = _TokenServer((_HOST, 0), _TokenHandler)
    graph_server = _GraphServer((_HOST, find_free_port()), _GraphHandler)
    token_thread = threading.Thread(target=token_server.serve_forever, daemon=True)
    graph_thread = threading.Thread(target=graph_server.serve_forever, daemon=True)
    token_thread.start()
    graph_thread.start()
    try:
        base_url = f"http://{_HOST}:{graph_server.server_port}"
        env = {
            "MS_GRAPH_FIXTURE_BASE_URL_V1": f"{base_url}/v1.0/",
            "MS_GRAPH_FIXTURE_BASE_URL_BETA": f"{base_url}/beta/",
            "MS_GRAPH_FIXTURE_AUTHORITY": f"http://{_HOST}:{token_server.server_port}",
            "MS_GRAPH_FIXTURE_TENANT_ID": _TENANT_ID,
            "MS_GRAPH_FIXTURE_CLIENT_ID": _CLIENT_ID,
            "MS_GRAPH_FIXTURE_CLIENT_SECRET": _CLIENT_SECRET,
        }
        env.update(_plugin_env())
        yield env
    finally:
        token_server.shutdown()
        graph_server.shutdown()
        token_thread.join(timeout=2)
        graph_thread.join(timeout=2)
