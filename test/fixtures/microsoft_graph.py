"""Microsoft Graph fixture for integration testing.

The Graph side is intentionally shaped like Microsoft Graph OData collection
responses. The auth side emulates the Microsoft identity platform client
credentials token endpoint used by app-only Graph access.

Environment variables yielded:
- MS_GRAPH_FIXTURE_AUTHORITY: authority URL for auth.authority
- MS_GRAPH_FIXTURE_BASE_URL: base URL for from_microsoft_graph _base_url
- MS_GRAPH_FIXTURE_TENANT_ID
- MS_GRAPH_FIXTURE_CLIENT_ID
- MS_GRAPH_FIXTURE_CLIENT_SECRET
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator
from urllib.parse import parse_qs, urlsplit

from tenzir_test import fixture

_HOST = "127.0.0.1"
_TENANT_ID = "test-tenant"
_CLIENT_ID = "test-client"
_CLIENT_SECRET = "test-secret"
_ACCESS_TOKEN = "test-ms-graph-token"
_SKIPTOKEN = "page-2"


def _json_response(handler: BaseHTTPRequestHandler, code: int, obj: object) -> None:
    body = json.dumps(obj).encode()
    handler.send_response(code)
    handler.send_header("Content-Type", "application/json")
    handler.send_header("Content-Length", str(len(body)))
    handler.end_headers()
    handler.wfile.write(body)


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
        _json_response(
            self,
            HTTPStatus.OK,
            {
                "token_type": "Bearer",
                "expires_in": 3600,
                "access_token": _ACCESS_TOKEN,
            },
        )

    def log_message(self, *_: object) -> None:
        return


class _GraphHandler(BaseHTTPRequestHandler):
    def _check_auth(self) -> bool:
        if self.headers.get("Authorization") != f"Bearer {_ACCESS_TOKEN}":
            _json_response(self, HTTPStatus.UNAUTHORIZED, {"error": "unauthorized"})
            return False
        return True

    def do_GET(self) -> None:  # noqa: N802
        if not self._check_auth():
            return
        parts = urlsplit(self.path)
        query = parse_qs(parts.query, keep_blank_values=True)
        if parts.path == "/v1.0/users" and query == {"$skiptoken": [_SKIPTOKEN]}:
            self._second_page()
            return
        if parts.path == "/v1.0/users":
            if query.get("$select") != ["id,displayName,userPrincipalName"]:
                _json_response(
                    self,
                    HTTPStatus.BAD_REQUEST,
                    {"error": f"unexpected $select: {query.get('$select')!r}"},
                )
                return
            if query.get("$top") != ["2"]:
                _json_response(
                    self,
                    HTTPStatus.BAD_REQUEST,
                    {"error": f"unexpected $top: {query.get('$top')!r}"},
                )
                return
            base = f"http://{self.headers['Host']}/v1.0"
            _json_response(
                self,
                HTTPStatus.OK,
                {
                    "@odata.context": f"{base}/$metadata#users",
                    "@odata.nextLink": f"{base}/users?$skiptoken={_SKIPTOKEN}",
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
            _json_response(
                self,
                HTTPStatus.OK,
                {
                    "@odata.context": f"http://{self.headers['Host']}/v1.0/$metadata#groups",
                    "value": [
                        {"id": "group-1", "displayName": "Security"},
                    ],
                },
            )
            return
        _json_response(self, HTTPStatus.NOT_FOUND, {"error": "not found"})

    def _second_page(self) -> None:
        _json_response(
            self,
            HTTPStatus.OK,
            {
                "@odata.context": f"http://{self.headers['Host']}/v1.0/$metadata#users",
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

    def log_message(self, *_: object) -> None:
        return


@fixture(name="microsoft_graph")
def run() -> Iterator[dict[str, str]]:
    token_server = ThreadingHTTPServer((_HOST, 0), _TokenHandler)
    graph_server = ThreadingHTTPServer((_HOST, 0), _GraphHandler)
    token_thread = threading.Thread(target=token_server.serve_forever, daemon=True)
    graph_thread = threading.Thread(target=graph_server.serve_forever, daemon=True)
    token_thread.start()
    graph_thread.start()
    try:
        env = {
            "MS_GRAPH_FIXTURE_AUTHORITY": f"http://{_HOST}:{token_server.server_port}",
            "MS_GRAPH_FIXTURE_BASE_URL": f"http://{_HOST}:{graph_server.server_port}/v1.0/",
            "MS_GRAPH_FIXTURE_TENANT_ID": _TENANT_ID,
            "MS_GRAPH_FIXTURE_CLIENT_ID": _CLIENT_ID,
            "MS_GRAPH_FIXTURE_CLIENT_SECRET": _CLIENT_SECRET,
        }
        if binary := os.environ.get("TENZIR_BINARY"):
            build_dir = Path(binary).resolve().parent.parent
            plugin_dir = build_dir / "lib" / "tenzir" / "plugins"
            if plugin_dir.is_dir():
                env["TENZIR_PLUGIN_DIRS"] = str(plugin_dir)
                env["TENZIR_PLUGINS"] = "all"
            lib_dir = build_dir / "lib"
            if lib_dir.is_dir():
                env["DYLD_LIBRARY_PATH"] = str(lib_dir)
        yield env
    finally:
        token_server.shutdown()
        graph_server.shutdown()
        token_thread.join(timeout=2)
        graph_thread.join(timeout=2)
