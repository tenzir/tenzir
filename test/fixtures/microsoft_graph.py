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
import re
import shlex
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
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
    # The number of groups in a paged `/v1.0/groups` dataset. Zero keeps the
    # legacy single group.
    groups: int = 0
    # The number of sign-ins in a paged `/v1.0/auditLogs/signIns` dataset.
    sign_ins: int = 0
    # The number of entries in a paged `/v1.0/auditLogs/directoryAudits`
    # dataset.
    directory_audits: int = 0


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
        "createdDateTime": f"2024-01-{index:02}T00:00:00Z",
    }


def _paged_group(index: int) -> dict[str, object]:
    """Returns group `index` of the paged `/v1.0/groups` dataset."""
    return {
        "id": f"group-{index}",
        "displayName": f"Group {index}",
        "mail": f"group{index}@example.com",
        "securityEnabled": index % 2 == 1,
        "renewedDateTime": f"2024-02-{index:02}T00:00:00Z",
    }


def _sign_in(index: int) -> dict[str, object]:
    """Returns sign-in `index` of the paged `/v1.0/auditLogs/signIns`
    dataset."""
    return {
        "id": f"sign-in-{index}",
        "createdDateTime": f"2024-07-{index:02}T12:00:00Z",
        "userPrincipalName": f"user{index}@example.com",
        "appDisplayName": "Graph Explorer" if index % 2 else "Office",
        "location": {"city": "Berlin" if index % 3 else "Paris"},
        "status": {"errorCode": 0 if index % 4 else 50126},
    }


def _directory_audit(index: int) -> dict[str, object]:
    """Returns entry `index` of the paged `/v1.0/auditLogs/directoryAudits`
    dataset."""
    return {
        "id": f"audit-{index}",
        "activityDateTime": f"2024-08-{index:02}T08:00:00Z",
        "activityDisplayName": "Add user" if index % 2 else "Update user",
        "loggedByService": "Core Directory",
        "initiatedBy": {"user": {"userPrincipalName": f"admin{index}@example.com"}},
    }


class _FilterError(ValueError):
    pass


_FILTER_TOKEN = re.compile(
    r"\s*(?:(?P<string>'(?:[^']|'')*')"
    r"|(?P<datetime>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z)"
    r"|(?P<number>-?\d+)"
    r"|(?P<punct>[(),])"
    r"|(?P<word>[A-Za-z_][A-Za-z0-9_/]*))"
)


def _parse_filter(
    text: str, schema: dict[str, object]
) -> Callable[[dict[str, object]], bool]:
    """Parses the subset of OData `$filter` that the operator spells.

    Strings compare without regard to case, as Microsoft Graph compares the
    string properties of directory objects. Properties that `schema`, a
    representative item, lacks, unknown operators, and invalid syntax raise
    `_FilterError`, which the fixture answers with HTTP 400.
    """
    tokens: list[tuple[str, object]] = []
    position = 0
    text = text.strip()
    while position < len(text):
        match = _FILTER_TOKEN.match(text, position)
        if not match or match.end() == position:
            raise _FilterError(f"invalid filter at {position}")
        position = match.end()
        kind = match.lastgroup
        value = match.group(kind)
        if kind == "string":
            tokens.append(("literal", value[1:-1].replace("''", "'")))
        elif kind == "datetime":
            tokens.append(("literal", _parse_datetime(value)))
        elif kind == "number":
            tokens.append(("literal", int(value)))
        elif kind == "word" and value in ("true", "false"):
            tokens.append(("literal", value == "true"))
        else:
            tokens.append((kind, value))
    index = 0

    def peek() -> tuple[str, object] | None:
        return tokens[index] if index < len(tokens) else None

    def take(kind: str, value: object = None) -> object:
        nonlocal index
        token = peek()
        if (
            token is None
            or token[0] != kind
            or (value is not None and token[1] != value)
        ):
            raise _FilterError(f"expected {value or kind}")
        index += 1
        return token[1]

    def literal() -> object:
        return take("literal")

    def disjunction() -> Callable[[dict[str, object]], bool]:
        operands = [conjunction()]
        while peek() == ("word", "or"):
            take("word", "or")
            operands.append(conjunction())
        return lambda item: any(f(item) for f in operands)

    def conjunction() -> Callable[[dict[str, object]], bool]:
        operands = [atom()]
        while peek() == ("word", "and"):
            take("word", "and")
            operands.append(atom())
        return lambda item: all(f(item) for f in operands)

    def atom() -> Callable[[dict[str, object]], bool]:
        if peek() == ("punct", "("):
            take("punct", "(")
            result = disjunction()
            take("punct", ")")
            return result
        path = cast(str, take("word"))
        if path.lower() == "startswith" and peek() == ("punct", "("):
            take("punct", "(")
            field = cast(str, take("word"))
            _lookup(schema, field)
            take("punct", ",")
            prefix = literal()
            take("punct", ")")
            if not isinstance(prefix, str):
                raise _FilterError("startswith takes a string")
            return lambda item: _starts_with(_lookup(item, field), prefix)
        _lookup(schema, path)
        operator = take("word")
        if operator == "in":
            take("punct", "(")
            values = [literal()]
            while peek() == ("punct", ","):
                take("punct", ",")
                values.append(literal())
            take("punct", ")")
            return lambda item: any(
                _compare(_lookup(item, path), "eq", v) for v in values
            )
        if operator not in ("eq", "ge", "le"):
            raise _FilterError(f"unsupported operator {operator}")
        value = literal()
        return lambda item: _compare(_lookup(item, path), cast(str, operator), value)

    result = disjunction()
    if index != len(tokens):
        raise _FilterError("trailing tokens")
    return result


def _parse_datetime(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def _lookup(item: dict[str, object], path: str) -> object:
    value: object = item
    for segment in path.split("/"):
        if not isinstance(value, dict) or segment not in value:
            raise _FilterError(
                "Unsupported or invalid query filter clause specified for "
                f"property '{path}'"
            )
        value = value[segment]
    return value


def _starts_with(value: object, prefix: str) -> bool:
    return isinstance(value, str) and value.lower().startswith(prefix.lower())


def _compare(value: object, operator: str, literal: object) -> bool:
    if isinstance(literal, datetime):
        if not isinstance(value, str):
            return False
        lhs: object = _parse_datetime(value)
        rhs: object = literal
    elif isinstance(literal, str):
        if not isinstance(value, str):
            return False
        lhs, rhs = value.lower(), literal.lower()
    else:
        lhs, rhs = value, literal
    if operator == "eq":
        return lhs == rhs
    if operator == "ge":
        return lhs >= rhs  # type: ignore[operator]
    return lhs <= rhs  # type: ignore[operator]


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
        if parts.path == "/v1.0/groups" and server.options.groups > 0:
            groups = [_paged_group(i) for i in range(1, server.options.groups + 1)]
            self._paged_collection(
                "groups", groups, _paged_group(1), None, 999, 3, query, base_url
            )
            return
        if parts.path == "/v1.0/auditLogs/signIns":
            sign_ins = [_sign_in(i) for i in range(1, server.options.sign_ins + 1)]
            self._paged_collection(
                "auditLogs/signIns",
                sign_ins,
                _sign_in(1),
                None,
                1000,
                3,
                query,
                base_url,
            )
            return
        if parts.path == "/v1.0/auditLogs/directoryAudits":
            audits = [
                _directory_audit(i)
                for i in range(1, server.options.directory_audits + 1)
            ]
            self._paged_collection(
                "auditLogs/directoryAudits",
                audits,
                _directory_audit(1),
                None,
                1000,
                3,
                query,
                base_url,
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
        users = [_paged_user(i) for i in range(1, options.users + 1)]
        for user, name in zip(users, options.display_names or []):
            user["displayName"] = name
        self._paged_collection(
            "users",
            users,
            _paged_user(1),
            _USERS_DEFAULT_PROPERTIES,
            _USERS_MAX_PAGE_SIZE,
            options.page_size,
            query,
            base_url,
            count_values=True,
        )

    def _paged_collection(
        self,
        path: str,
        items: list[dict[str, object]],
        schema: dict[str, object],
        default_properties: tuple[str, ...] | None,
        max_page_size: int,
        page_size: int,
        query: dict[str, list[str]],
        base_url: str,
        count_values: bool = False,
    ) -> None:
        """Serves a collection over a deterministic dataset.

        This models the request contract, not proof of backend equivalence:
        `$top` sets the page size up to `max_page_size`, `$select` returns the
        requested properties, properties that `schema`, a representative
        item, lacks fail the request whether or not any item matches,
        `$filter` evaluates the subset of OData that `from_microsoft_graph`
        spells, comparing strings without regard to case, and
        `@odata.nextLink` carries the query options with an opaque
        `$skiptoken`.
        """
        for key in query:
            if key not in ("$select", "$top", "$filter", "$skiptoken"):
                _json_response(
                    self,
                    HTTPStatus.BAD_REQUEST,
                    {"error": f"unsupported query option {key}"},
                )
                return
        if filters := query.get("$filter"):
            try:
                if len(filters) != 1:
                    raise _FilterError("repeated $filter")
                predicate = _parse_filter(filters[0], schema)
                items = [item for item in items if predicate(item)]
            except _FilterError as error:
                _json_response(
                    self,
                    HTTPStatus.BAD_REQUEST,
                    {
                        "error": {
                            "code": "Request_UnsupportedQuery",
                            "message": str(error),
                        }
                    },
                )
                return
        if top := query.get("$top"):
            page_size = min(int(top[0]), max_page_size)
        if default_properties is not None:
            properties: list[str] = list(default_properties)
        else:
            properties = list(schema)
        if select := query.get("$select"):
            properties = select[0].split(",")
            unknown = [p for p in properties if p not in schema]
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
        page = items[offset : offset + page_size]
        server = cast(_GraphServer, self.server)
        if count_values:
            with server.lock:
                server.served_values += len(page) * len(properties)
        body: dict[str, object] = {
            "@odata.context": f"{base_url}/v1.0/$metadata#{path}",
            "value": [{p: item[p] for p in properties} for item in page],
        }
        if offset + page_size < len(items):
            params = [
                f"{key}={quote(values[0], safe=',')}"
                for key, values in query.items()
                if key != "$skiptoken"
            ]
            params.append(f"$skiptoken=offset-{offset + page_size}")
            body["@odata.nextLink"] = f"{base_url}/v1.0/{path}?{'&'.join(params)}"
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
