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
import shlex
import threading
import time
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, cast
from urllib.parse import parse_qs, quote, urlsplit

from tenzir_test import FixtureHandle, current_options, fixture

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
    expires_in: int = 4


@dataclass(frozen=True)
class GraphOptions:
    # The number of users in the paged `/v1.0/users` dataset. Zero keeps the
    # legacy two-page dataset.
    users: int = 0
    # Optional display names for the first users of the paged dataset.
    display_names: list[str | None] | None = None
    # The page size of the paged dataset without `$top`.
    page_size: int = 3
    # The lifetime of access tokens in seconds. The token provider refreshes
    # after half of a short lifetime.
    token_lifetime: int = 4


# The properties that `List users` returns without `$select`, per the
# Microsoft Graph v1.0 reference: "By default, only a limited set of
# properties are returned (businessPhones, displayName, givenName, id,
# jobTitle, mail, mobilePhone, officeLocation, preferredLanguage, surname, and
# userPrincipalName)."
_USERS_DEFAULT_PROPERTIES = (
    "businessPhones",
    "displayName",
    "givenName",
    "id",
    "jobTitle",
    "mail",
    "mobilePhone",
    "officeLocation",
    "preferredLanguage",
    "surname",
    "userPrincipalName",
)
# "The default and maximum page sizes are 100 and 999 user objects".
_USERS_MAX_PAGE_SIZE = 999


def _paged_user(index: int) -> dict[str, object]:
    """Returns user `index`, with properties beyond the default set."""
    return {
        "businessPhones": [f"+1 555 01{index:02}"] if index % 2 else [],
        "displayName": f"User {index}",
        "givenName": "User",
        "id": f"user-{index}",
        "jobTitle": "Engineer" if index % 3 == 0 else "Analyst",
        "mail": f"user{index}@example.com",
        "mobilePhone": None,
        "officeLocation": None if index % 4 == 0 else f"Room {index}",
        "preferredLanguage": "en-US",
        "surname": f"{index}",
        "userPrincipalName": f"user{index}@example.com",
        # Returned only with `$select`.
        "accountEnabled": index % 2 == 1,
        "department": "Security" if index % 3 == 0 else "Operations",
    }


class _GraphServer(ThreadingHTTPServer):
    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self.lock = threading.Lock()
        self.requests: dict[str, int] = {}
        self.request_log: list[str] = []
        # The property values that the paged `List users` dataset served.
        self.served_values = 0
        self.options = GraphOptions()

    def log_request(self, target: str) -> None:
        with self.lock:
            self.request_log.append(target)

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
                "expires_in": server.expires_in,
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
        server = cast(_GraphServer, self.server)
        server.log_request(self.path)
        parts = urlsplit(self.path)
        query = parse_qs(parts.query, keep_blank_values=True)
        base_url = f"http://{_HOST}:{self.server.server_port}"
        invalid_pages: dict[str, object] = {
            "invalid-envelope": [],
            "missing-value": {},
            "invalid-value": {"value": {}},
            "invalid-item": {"value": [{"id": "not-emitted"}, None]},
            "invalid-next-link": {"value": [], "@odata.nextLink": 42},
            "invalid-delta-link": {"value": [], "@odata.deltaLink": False},
        }
        if parts.path.startswith("/v1.0/test/"):
            scenario = parts.path.removeprefix("/v1.0/test/")
            if scenario in invalid_pages:
                _json_response(self, HTTPStatus.OK, invalid_pages[scenario])
                return
            raw_pages = {
                "invalid-json": b'{"value": [',
                "duplicate-keys": (
                    b'{"value": [{"id": "first", "id": "ignored", '
                    b'"nested": {"x": 1, "x": 2}}]}'
                ),
                "oversized-integer": b'{"value": [{"x": 18446744073709551616}]}',
            }
            if scenario in raw_pages:
                body = raw_pages[scenario]
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)
                return
        if parts.path == "/v1.0/users/mixed":
            if query.get("cursor") == ["page-2"]:
                _json_response(
                    self,
                    HTTPStatus.OK,
                    {
                        "value": [
                            {
                                "@odata.etag": "omit-me",
                                "id": "mixed-1",
                                "value": 42,
                                "literal.key": "literal",
                                "boolean_text": "true",
                                "nested": {
                                    "@odata.etag": "keep-me",
                                    "@odata.type": "#microsoft.graph.user",
                                    "empty": {},
                                    "list": [],
                                    "boolean_text": "false",
                                },
                                "items": [
                                    None,
                                    False,
                                    -1,
                                    18446744073709551615,
                                    1.25,
                                    "10.0.0.1",
                                    "10.0.0.0/24",
                                    "2026-05-13T12:00:00Z",
                                    "1s",
                                    "true",
                                    "false",
                                    "null",
                                    "42",
                                    "1.25",
                                    True,
                                    {"value": 42},
                                    ["nested"],
                                ],
                            },
                            {"id": "mixed-2", "value": "string"},
                            {"id": "mixed-3", "value": [1, "two"]},
                            {
                                "id": "mixed-4",
                                "value": None,
                                "@removed": {"reason": "deleted"},
                            },
                            {},
                        ],
                    },
                )
            else:
                _json_response(
                    self,
                    HTTPStatus.OK,
                    {"value": [], "@odata.nextLink": "?cursor=page-2"},
                )
            return
        if parts.path == "/v1.0/users/empty-delta/delta":
            step = query.get("step", ["initial"])[0]
            next_step = "changed" if step == "empty" else "empty"
            _json_response(
                self,
                HTTPStatus.OK,
                {
                    "@odata.deltaLink": f"?step={next_step}",
                    "value": (
                        [{"id": "removed-user", "@removed": {"reason": "deleted"}}]
                        if step == "changed"
                        else []
                    ),
                },
            )
            return
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
        if parts.path == "/v1.0/users" and server.options.users > 0:
            self._paged_users(server.options, query, base_url)
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

    def _paged_users(
        self,
        options: GraphOptions,
        query: dict[str, list[str]],
        base_url: str,
    ) -> None:
        """Serves `List users` over a deterministic dataset.

        This models the request contract, not proof of backend equivalence:
        `$top` sets the page size up to 999, `$select` returns the requested
        properties, unknown properties fail the request, and `@odata.nextLink`
        carries the query options with an opaque `$skiptoken`. The only
        supported explicit `$filter` is `accountEnabled eq true|false`.
        """
        for key in query:
            if key not in ("$select", "$top", "$filter", "$skiptoken"):
                _json_response(
                    self,
                    HTTPStatus.BAD_REQUEST,
                    {"error": f"unsupported query option {key}"},
                )
                return
        users = [_paged_user(i) for i in range(1, options.users + 1)]
        for user, name in zip(users, options.display_names or []):
            user["displayName"] = name
        if filters := query.get("$filter"):
            values = {
                "accountEnabled eq true": True,
                "accountEnabled eq false": False,
            }
            if len(filters) != 1 or filters[0] not in values:
                _json_response(
                    self, HTTPStatus.BAD_REQUEST, {"error": "unsupported filter"}
                )
                return
            users = [u for u in users if u["accountEnabled"] == values[filters[0]]]
        page_size = options.page_size
        if top := query.get("$top"):
            page_size = min(int(top[0]), _USERS_MAX_PAGE_SIZE)
        properties: list[str] = list(_USERS_DEFAULT_PROPERTIES)
        if select := query.get("$select"):
            properties = select[0].split(",")
            unknown = [p for p in properties if p not in _paged_user(1)]
            if unknown:
                _json_response(
                    self,
                    HTTPStatus.BAD_REQUEST,
                    {"error": f"could not find a property named {unknown[0]}"},
                )
                return
        offset = 0
        if skiptoken := query.get("$skiptoken"):
            offset = int(skiptoken[0].removeprefix("offset-"))
        page = users[offset : offset + page_size]
        server = cast(_GraphServer, self.server)
        with server.lock:
            server.served_values += len(page) * len(properties)
        body: dict[str, object] = {
            "@odata.context": f"{base_url}/v1.0/$metadata#users",
            "value": [{p: user[p] for p in properties} for user in page],
        }
        if offset + page_size < len(users):
            params = [
                f"{key}={quote(values[0], safe=',')}"
                for key, values in query.items()
                if key != "$skiptoken"
            ]
            params.append(f"$skiptoken=offset-{offset + page_size}")
            body["@odata.nextLink"] = f"{base_url}/v1.0/users?{'&'.join(params)}"
        _json_response(self, HTTPStatus.OK, body)

    def log_message(self, *_: object) -> None:
        return


def _plugin_env() -> dict[str, str]:
    import os
    from pathlib import Path

    env: dict[str, str] = {}
    if binary := os.environ.get("TENZIR_BINARY"):
        build_dir = Path(shlex.split(binary)[0]).resolve().parent.parent
        plugin_dir = build_dir / "lib" / "tenzir" / "plugins"
        if plugin_dir.is_dir():
            env["TENZIR_PLUGIN_DIRS"] = str(plugin_dir)
            env["TENZIR_PLUGINS"] = "microsoft_graph"
        lib_dir = build_dir / "lib"
        if lib_dir.is_dir():
            env["DYLD_LIBRARY_PATH"] = str(lib_dir)
    return env


@dataclass(frozen=True)
class GraphAssertions:
    # The exact request targets, in order.
    requests: list[str] | None = None
    # The first request targets, in order.
    request_prefix: list[str] | None = None
    # An upper bound on the number of requests, for racy downstream stops.
    max_requests: int | None = None
    # The exact number of token requests.
    token_requests: int | None = None
    # The exact number of top-level property values sent by the paged
    # `List users` dataset, independent of the operator's metrics counters.
    served_values: int | None = None
    # A lower bound on the served property values, for racy downstream stops.
    min_served_values: int | None = None


@fixture(name="microsoft_graph", options=GraphOptions, assertions=GraphAssertions)
def run() -> FixtureHandle:
    options = current_options("microsoft_graph")
    if not isinstance(options, GraphOptions):
        options = GraphOptions()
    token_server = _TokenServer((_HOST, 0), _TokenHandler)
    token_server.expires_in = options.token_lifetime
    try:
        graph_server = _GraphServer((_HOST, 0), _GraphHandler)
    except BaseException:
        token_server.server_close()
        raise
    graph_server.options = options
    token_thread = threading.Thread(target=token_server.serve_forever, daemon=True)
    graph_thread = threading.Thread(target=graph_server.serve_forever, daemon=True)
    token_thread.start()
    graph_thread.start()
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

    def assert_test(*, assertions: GraphAssertions | dict[str, Any], **_: Any) -> None:
        if not isinstance(assertions, GraphAssertions):
            assert not assertions, assertions
            return
        with graph_server.lock:
            requests = list(graph_server.request_log)
            served_values = graph_server.served_values
        if assertions.requests is not None:
            assert requests == assertions.requests, requests
        if assertions.request_prefix is not None:
            assert requests[: len(assertions.request_prefix)] == (
                assertions.request_prefix
            ), requests
        if assertions.max_requests is not None:
            assert len(requests) <= assertions.max_requests, requests
        if assertions.token_requests is not None:
            assert token_server.token_requests == assertions.token_requests, (
                token_server.token_requests
            )
        if assertions.served_values is not None:
            assert served_values == assertions.served_values, served_values
        if assertions.min_served_values is not None:
            assert served_values >= assertions.min_served_values, served_values

    def teardown() -> None:
        token_server.shutdown()
        graph_server.shutdown()
        token_thread.join(timeout=2)
        graph_thread.join(timeout=2)
        token_server.server_close()
        graph_server.server_close()

    return FixtureHandle(env=env, teardown=teardown, hooks={"assert_test": assert_test})
