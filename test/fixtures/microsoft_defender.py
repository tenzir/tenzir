"""Deterministic Graph hunting and Entra endpoints; never contacts Microsoft."""

from __future__ import annotations

import copy
import json
import threading
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterator

from tenzir_test import current_assertions, current_options, fixture

from . import kql_observables

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
    {"name": "Enabled", "type": "SByte"},
    {"name": "Message", "type": "String"},
    {"name": "NullTime", "type": "DateTime"},
    {"name": "Details", "type": "Object"},
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
# Deterministic data for pushdown: case-variant and IP-shaped strings, nulls,
# a Guid that decodes to a TQL string, and a dynamic column whose values have
# different types, including dynamic strings that look like numbers or JSON.
_PUSHDOWN_SCHEMA = [
    {"name": "Timestamp", "type": "DateTime"},
    {"name": "Id", "type": "Int64"},
    {"name": "DeviceName", "type": "String"},
    {"name": "RemoteIP", "type": "String"},
    {"name": "RemotePort", "type": "Int32"},
    {"name": "Allowed", "type": "SByte"},
    {"name": "ReportId", "type": "Guid"},
    {"name": "Extra", "type": "Object"},
]
_PUSHDOWN_ROWS = [
    ["2026-01-01T00:00:00Z", 0, "host-a", "10.0.0.1", 443, True, None, 443],
    ["2026-01-01T12:00:00Z", 1, "HOST-A", "::ffff:10.0.0.1", 80, False, None, '"443"'],
    ["2026-01-02T00:00:00Z", 2, "host-b", "10.0.0.01", None, None, None, "443"],
    ["2026-01-02T12:00:00Z", 3, "", "fe80::1", 8443, True, None, None],
    [
        "2026-01-03T00:00:00Z",
        4,
        "host-ä",
        "",
        443,
        None,
        "00000000-0000-0000-0000-000000000000",
        {"port": 443},
    ],
]


# The wire format of runHuntingQuery, as observed against a real tenant: Graph
# spells `bool` as `SByte` with the values 0 and 1, `dynamic` as `Object`, and
# `decimal` as `SqlDecimal`; it annotates 64-bit integers and booleans with
# `<column>@odata.type`, and the objects of dynamic values with `@odata.type`
# and `<key>@odata.type`; it sends `null` as `""` for strings, as `null` for
# decimals and dynamic values, and as `{}` otherwise, after all other columns.
# The fixture always annotates, as if Graph ignored the connector's request for
# `odata.metadata=none`, except for the `metadata-none` case.
_ANNOTATIONS = {"Int64": "#Int64", "SByte": "#SByte"}
_NULLS = {"String": "", "SqlDecimal": None, "Object": None}


def _wire_dynamic(value: object, annotate: bool) -> object:
    if isinstance(value, dict):
        result: dict[str, object] = {}
        if annotate:
            result["@odata.type"] = "#microsoft.graph.security.dynamicColumnValue"
        for key, item in value.items():
            if annotate and isinstance(item, int) and not isinstance(item, bool):
                result[f"{key}@odata.type"] = "#Int64"
            result[key] = _wire_dynamic(item, annotate)
        return result
    if isinstance(value, list):
        return [_wire_dynamic(item, annotate) for item in value]
    return value


def _wire(schema: object, rows: object, annotate: bool) -> object:
    """Turns logical rows, with `None` for `null`, into the wire format."""
    if not isinstance(schema, list) or not isinstance(rows, list):
        return rows
    types = {
        column.get("name"): column.get("type")
        for column in schema
        if isinstance(column, dict)
    }
    result = []
    for row in rows:
        if not isinstance(row, dict):
            result.append(row)
            continue
        values: dict[str, object] = {}
        nulls: dict[str, object] = {}
        for name, value in row.items():
            kind = types.get(name)
            if value is None:
                nulls[name] = copy.deepcopy(_NULLS.get(kind, {}))
                continue
            if kind == "SByte" and isinstance(value, bool):
                value = int(value)
            integer = isinstance(value, int) and not isinstance(value, bool)
            if kind == "Object":
                if annotate and integer:
                    values[f"{name}@odata.type"] = "#Int64"
                elif annotate and isinstance(value, list) and value:
                    if all(
                        isinstance(item, int) and not isinstance(item, bool)
                        for item in value
                    ):
                        values[f"{name}@odata.type"] = "#Collection(Int64)"
                    elif all(isinstance(item, str) for item in value):
                        values[f"{name}@odata.type"] = "#Collection(String)"
                value = _wire_dynamic(value, annotate)
            elif annotate and integer and kind in _ANNOTATIONS:
                values[f"{name}@odata.type"] = _ANNOTATIONS[kind]
            values[name] = value
        values.update(nulls)
        result.append(values)
    return result


def _wire_envelope(envelope: dict[str, object], annotate: bool) -> dict[str, object]:
    result = dict(envelope)
    if "results" in result:
        result["results"] = _wire(result.get("schema"), result["results"], annotate)
    if annotate:
        result = {
            "@odata.context": "https://graph.microsoft.com/v1.0/$metadata"
            "#microsoft.graph.security.huntingQueryResults",
            **result,
        }
    return result


# Every column type and null spelling, mirroring a `datatable` query against a
# real tenant.
_ALL_TYPES_SCHEMA = [
    {"name": "s", "type": "String"},
    {"name": "g", "type": "Guid"},
    {"name": "d", "type": "SqlDecimal"},
    {"name": "i", "type": "Int32"},
    {"name": "l", "type": "Int64"},
    {"name": "r", "type": "Double"},
    {"name": "b", "type": "SByte"},
    {"name": "t", "type": "DateTime"},
    {"name": "ts", "type": "TimeSpan"},
    {"name": "dy", "type": "Object"},
    {"name": "dys", "type": "Object"},
    {"name": "dyq", "type": "Object"},
    {"name": "dyn", "type": "Object"},
    {"name": "dya", "type": "Object"},
]


def _all_types_rows() -> list[dict[str, object]]:
    names = [column["name"] for column in _ALL_TYPES_SCHEMA]
    return [
        {
            "s": "abc",
            "g": "00000000-0000-0000-0000-000000000001",
            "d": "1.5",
            "i": 1,
            "l": 2,
            "r": 1.5,
            "b": True,
            "t": "2026-01-01T00:00:00.1234567Z",
            "ts": "PT1H",
            # Graph fails with HTTP 500 for arrays that mix types.
            "dy": {"a": 1, "b": {"c": [1, 2]}},
            "dys": "443",
            "dyq": '"443"',
            "dyn": 443,
            "dya": [443],
        },
        {
            "s": "",
            "g": "00000000-0000-0000-0000-000000000002",
            "d": "-12345678901234567890.123456789",
            "i": -2147483648,
            "l": -9223372036854775808,
            "r": 0,
            "b": False,
            "t": "2026-01-01T00:00:00Z",
            "ts": "-P1DT2H3M4.5S",
            "dy": [],
            "dys": "",
            "dyq": "{}",
            "dyn": 1.5,
            "dya": {},
        },
        dict.fromkeys(names),
    ]


_TIMESPANS = [
    "PT1H",
    "P1DT2H3M4.5S",
    "-PT0.0000001S",
    "PT0S",
    "P2D",
    "PT1M",
    "P106751DT23H47M16.854775807S",
    "-P106751DT23H47M16.854775807S",
    # The smallest duration, one tick beyond the negated largest one.
    "-P106751DT23H47M16.854775808S",
    # The extremes that Graph spells with its 100 ns resolution.
    "P106751DT23H47M16.8547758S",
    "-P106751DT23H47M16.8547758S",
]

# A synthetic response shaped like `DeviceNetworkEvents | take 3`, with the
# column types that a real tenant reports.
_DEVICE_NETWORK_EVENTS_SCHEMA = [
    {"name": "Timestamp", "type": "DateTime"},
    {"name": "DeviceId", "type": "String"},
    {"name": "DeviceName", "type": "String"},
    {"name": "ActionType", "type": "String"},
    {"name": "RemoteIP", "type": "String"},
    {"name": "RemotePort", "type": "Int32"},
    {"name": "RemoteUrl", "type": "String"},
    {"name": "LocalIP", "type": "String"},
    {"name": "LocalPort", "type": "Int32"},
    {"name": "Protocol", "type": "String"},
    {"name": "InitiatingProcessFileName", "type": "String"},
    {"name": "InitiatingProcessFileSize", "type": "Int64"},
    {"name": "InitiatingProcessId", "type": "Int64"},
    {"name": "InitiatingProcessCreationTime", "type": "DateTime"},
    {"name": "ReportId", "type": "Int64"},
    {"name": "AdditionalFields", "type": "String"},
    {"name": "IsInitiatingProcessRemoteSession", "type": "SByte"},
    {"name": "InitiatingProcessRemoteSessionIP", "type": "String"},
]
_DEVICE_NETWORK_EVENTS_ROWS = [
    {
        "Timestamp": "2026-01-01T00:00:01.1234567Z",
        "DeviceId": "device-a",
        "DeviceName": "host-a.example.com",
        "ActionType": "ConnectionSuccess",
        "RemoteIP": "192.0.2.10",
        "RemotePort": 443,
        "RemoteUrl": "www.example.com",
        "LocalIP": "198.51.100.5",
        "LocalPort": 50123,
        "Protocol": "Tcp",
        "InitiatingProcessFileName": "browser.exe",
        "InitiatingProcessFileSize": 4294967296,
        "InitiatingProcessId": 4242,
        "InitiatingProcessCreationTime": "2026-01-01T00:00:00Z",
        "ReportId": 9007199254740993,
        "AdditionalFields": '{"direction":"Outbound"}',
        "IsInitiatingProcessRemoteSession": False,
        "InitiatingProcessRemoteSessionIP": None,
    },
    {
        "Timestamp": "2026-01-01T00:00:02Z",
        "DeviceId": "device-b",
        "DeviceName": "host-b.example.com",
        "ActionType": "ListeningConnectionCreated",
        "RemoteIP": None,
        "RemotePort": None,
        "RemoteUrl": None,
        "LocalIP": "::ffff:198.51.100.6",
        "LocalPort": 3389,
        "Protocol": "Tcp",
        "InitiatingProcessFileName": "svchost.exe",
        "InitiatingProcessFileSize": 57344,
        "InitiatingProcessId": 1000,
        "InitiatingProcessCreationTime": None,
        "ReportId": 2,
        "AdditionalFields": None,
        "IsInitiatingProcessRemoteSession": True,
        "InitiatingProcessRemoteSessionIP": "203.0.113.7",
    },
    {
        "Timestamp": "2026-01-01T00:00:03.5Z",
        "DeviceId": "device-a",
        "DeviceName": "host-a.example.com",
        "ActionType": "ConnectionFailed",
        "RemoteIP": "10.0.0.01",
        "RemotePort": 443,
        "RemoteUrl": "",
        "LocalIP": "198.51.100.5",
        "LocalPort": 50124,
        "Protocol": "Tcp",
        "InitiatingProcessFileName": "BROWSER.EXE",
        "InitiatingProcessFileSize": 4294967296,
        "InitiatingProcessId": 4242,
        "InitiatingProcessCreationTime": "2026-01-01T00:00:00Z",
        "ReportId": 3,
        "AdditionalFields": "",
        "IsInitiatingProcessRemoteSession": None,
        "InitiatingProcessRemoteSessionIP": "",
    },
]


@dataclass(frozen=True)
class Options:
    # The indices of the pushdown rows that the service returns, emulating the
    # KQL that the request appends. All rows if unset.
    #
    # Tests derive `rows` and `project` by hand from documented KQL semantics.
    # They encode assumptions about the service's behavior for the asserted
    # request, pending validation against real tenants, and do not prove what
    # the service returns.
    rows: list[int] | None = None
    # The columns that the service returns, in order, emulating an appended
    # `project`. A name without a column yields a dynamic `null`.
    project: list[str] | None = None


@dataclass(frozen=True)
class Assertions:
    # The text that follows the first line of a query that names a case: the
    # stages that the source appends.
    appended: str = ""


class _Server(ThreadingHTTPServer):
    calls = 0
    query = ""
    appended = ""
    request_body: bytes | None = None
    rows: list[int] | None = None
    project: list[str] | None = None

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
        assert self.headers["Accept"] == "application/json;odata.metadata=none"
        body = self.rfile.read(int(self.headers["Content-Length"]))
        if server.request_body is not None:
            assert body == server.request_body, "request changed during retry"
        server.request_body = body
        request = json.loads(body)
        query = request["Query"]
        # The source preserves the user's query, here the name of a case on
        # the first line, and may append stages, which teardown compares.
        if not query.startswith(_QUERY):
            query, newline, appended = query.partition("\n")
            server.appended = newline + appended
        server.query = query
        assert set(request) <= {"Query", "Timespan"}
        refreshing = query in {"refresh", "refresh-failed"}
        token = f"{_ACCESS_TOKEN}-{server.calls}" if refreshing else _ACCESS_TOKEN
        assert self.headers["Authorization"] == f"Bearer {token}"
        if query in {"bounded", "pushdown-bounded"} or refreshing:
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
        elif query in {
            "pushdown",
            "pushdown-bounded",
            "observables",
            "observable-adversarial",
        }:
            source_schema = _PUSHDOWN_SCHEMA
            source_rows = _PUSHDOWN_ROWS
            if query in {"observables", "observable-adversarial"}:
                adversarial = query == "observable-adversarial"
                definitions = (
                    kql_observables.ADVERSARIAL_COLUMNS
                    if adversarial
                    else kql_observables.COLUMNS
                )
                types = {
                    "long": "Int64",
                    "string": "String",
                    "datetime": "DateTime",
                    "dynamic": "Object",
                }
                source_schema = [
                    {"name": name, "type": types[kind]} for name, kind in definitions
                ]
                source_rows = (
                    kql_observables.ADVERSARIAL_ROWS
                    if adversarial
                    else kql_observables.ROWS
                )
            selected = server.rows
            if selected is None:
                selected = list(range(len(source_rows)))
            schema = copy.deepcopy(source_schema)
            names = [column["name"] for column in schema]
            rows = [dict(zip(names, copy.deepcopy(source_rows[i]))) for i in selected]
            if server.project is not None:
                types = {column["name"]: column["type"] for column in schema}
                schema = [
                    {"name": name, "type": types.get(name, "Dynamic")}
                    for name in server.project
                ]
                rows = [
                    {name: row.get(name) for name in server.project} for row in rows
                ]
            envelope = {"schema": schema, "results": rows}
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
        elif query in {"late-invalid", "late-batch-invalid"}:
            if query == "late-batch-invalid":
                rows[:] = [dict(_ROW)] * 9000
            rows.append(dict(_ROW, Count="DO-NOT-LOG-DATA"))
        elif query == "many-rows":
            schema[:] = [{"name": "Count", "type": "Int64"}]
            rows[:] = [{"Count": i} for i in range(9000)]
        elif query == "real-uint":
            rows[0]["Ratio"] = 18446744073709551615
        elif query == "dynamic-overflow":
            rows[0]["Details"] = {"nested": [9223372036854775808]}
        elif query == "field-order":
            rows[0] = dict(reversed(list(rows[0].items())))
        elif query == "dynamic-first":
            schema[:] = [schema[-1], *schema[:-1]]
        elif query in {
            "duplicate-envelope",
            "duplicate-row",
            "duplicate-dynamic",
            "trailing",
        }:
            raw = json.dumps(_wire_envelope(envelope, True)).encode()
            if query == "duplicate-envelope":
                raw = raw[:-1] + b', "results": []}'
            elif query == "duplicate-row":
                raw = raw.replace(
                    b'"Count": 9223372036854775807', b'"Count": 1, "Count": 2'
                )
            elif query == "duplicate-dynamic":
                old = b'"text": "2026-01-01T00:00:00Z", "values": [1, 2, null]'
                assert old in raw
                raw = raw.replace(
                    old, b'"sensitive": "DO-NOT-LOG-DATA", "sensitive": 2'
                )
            else:
                raw += b' {"DO-NOT-LOG-DATA":true}'
            self.send_response(200)
            self.send_header("Content-Length", str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)
            return
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
        elif query in {"all-types", "metadata-none"}:
            envelope = {"schema": _ALL_TYPES_SCHEMA, "results": _all_types_rows()}
        elif query == "timespans":
            envelope = {
                "schema": [{"name": "Value", "type": "TimeSpan"}],
                "results": [{"Value": value} for value in _TIMESPANS],
            }
        elif query in {
            "bad-timespan",
            "timespan-overflow",
            "timespan-max-overflow",
            "timespan-min-overflow",
            "timespan-tick-overflow",
            "bad-sbyte",
            "bad-decimal",
        }:
            kind, value = {
                "bad-timespan": ("TimeSpan", "01:00:00"),
                # KQL's largest timespan exceeds Tenzir's nanosecond range.
                "timespan-overflow": ("TimeSpan", "P10675199DT2H48M5.4775807S"),
                # One tick beyond either end of Tenzir's range.
                "timespan-max-overflow": ("TimeSpan", "P106751DT23H47M16.854775808S"),
                "timespan-min-overflow": ("TimeSpan", "-P106751DT23H47M16.854775809S"),
                # The next 100 ns tick that Graph sends beyond the range.
                "timespan-tick-overflow": ("TimeSpan", "-P106751DT23H47M16.8547759S"),
                "bad-sbyte": ("SByte", 2),
                "bad-decimal": ("SqlDecimal", "NaN"),
            }[query]
            envelope = {
                "schema": [{"name": "Value", "type": kind}],
                "results": [{"Value": value}],
            }
        elif query == "undeclared-annotation":
            rows[0]["Other@odata.type"] = "#Int64"
        elif query == "device-network-events":
            envelope = {
                "schema": copy.deepcopy(_DEVICE_NETWORK_EVENTS_SCHEMA),
                "results": copy.deepcopy(_DEVICE_NETWORK_EVENTS_ROWS),
            }
        elif query in {"annotation-shaped", "annotation-shaped-none"}:
            # Keys that only look like annotations are payload. Graph itself
            # drops or rejects such keys, but the connector keeps them.
            envelope = {
                "schema": [{"name": "d", "type": "Object"}],
                "results": [
                    {
                        "d": {
                            "x": 1,
                            "x@odata.custom": "p",
                            "@odata.context": "c",
                            "z@odata.type": "#Int64",
                            "n": {"y": 2, "y@odata.custom": "q", "@odata.id": "r"},
                        }
                    },
                    {"d": [{"x": 1}, {"x@odata.type": "user"}]},
                ],
            }
            if query == "annotation-shaped-none":
                # Without metadata, nothing is an annotation.
                envelope["results"].append(
                    {"d": {"x": 1, "x@odata.type": "user", "@odata.type": "u"}}
                )
        elif query == "array-of-objects":
            # Graph sends objects inside dynamic arrays as JSON text, exactly
            # as it sends an array of JSON strings.
            envelope = {
                "schema": [{"name": "d", "type": "Object"}],
                "results": [{"d": ['{"x":1}']}, {"d": {"o": ['{"x":1}']}}],
            }
        elif query == "multi-statement;":
            # Graph returns only the table of the first statement.
            envelope = {
                "schema": [{"name": "a", "type": "Int64"}],
                "results": [{"a": 1}, {"a": 2}],
            }
        else:
            raise AssertionError(f"unexpected query: {query!r}")
        envelope = _wire_envelope(
            envelope, query not in {"metadata-none", "annotation-shaped-none"}
        )
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
    options: Options | None = None,
    assertions: Assertions | None = None,
) -> Iterator[dict[str, str]]:
    token = _TokenServer((_HOST, 0), _BadTokenHandler if bad_auth else token_handler)
    graph = _Server((_HOST, 0), _Handler)
    graph.rows = options.rows if options else None
    graph.project = options.project if options else None
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
        expected_appended = assertions.appended if assertions else ""
        assert graph.appended == expected_appended, (
            graph.appended,
            expected_appended,
        )
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


@fixture(name="microsoft_defender", options=Options, assertions=Assertions)
def run() -> Iterator[dict[str, str]]:
    options = current_options("microsoft_defender")
    assertions = current_assertions("microsoft_defender")
    yield from _run(
        options=options if isinstance(options, Options) else None,
        assertions=assertions if isinstance(assertions, Assertions) else None,
    )


@fixture(name="microsoft_defender_refresh")
def run_refresh() -> Iterator[dict[str, str]]:
    yield from _run(token_handler=_RefreshingTokenHandler)


@fixture(name="microsoft_defender_refresh_failed")
def run_refresh_failed() -> Iterator[dict[str, str]]:
    yield from _run(token_handler=_FailedRefreshTokenHandler)


@fixture(name="microsoft_defender_bad_auth")
def run_bad_auth() -> Iterator[dict[str, str]]:
    yield from _run(bad_auth=True)
