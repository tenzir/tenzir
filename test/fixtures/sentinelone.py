"""Mock SentinelOne Data Lake API server for integration tests."""

from __future__ import annotations

import gzip
import json
import os
import re
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlsplit

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options

from ._utils import generate_self_signed_cert

_HOST = "127.0.0.1"
_EXPECTED_TOKEN = "test-token-s1-12345"


@dataclass(frozen=True)
class SentinelOneOptions:
    tls: bool = False


@dataclass(frozen=True)
class SentinelOneAssertions:
    expected_add_events: list[dict[str, object]] | None = None
    queries_cancelled: bool = False
    # The launches of the test, in order. Each entry compares only the keys
    # that it has: `type`, `query` (the PowerQuery or the LOG filter),
    # `limit` and `cursor` of a LOG page, `start`, `end`, and `status`.
    expected_requests: list[dict[str, object]] | None = None


# Predefined result data blocks keyed by the explicit `pq.query` that names
# them, possibly followed by stages.
#
# The final LRQ poll nests this tabular payload in `data`:
#   { "columns": [{"name": "col"},...], "values": [[val,...],...]  }
#
# Special floating-point values that cannot be expressed in JSON are encoded
# as single-key objects: {"special": "NaN"}, {"special": "+infinity"}, etc.
_STATIC_RESPONSES: dict[str, object] = {
    "pushdown_booleans": {
        "columns": [{"name": "id"}, {"name": "flag"}],
        "values": [[1, True], [2, "true"], [3, False], [4, "false"], [5, None]],
    },
    "pushdown_semantics": {
        "columns": [{"name": name} for name in ("id", "n", "addr", "text")],
        "values": [
            [1, 42, "192.0.2.1", "prefix-MATCH-suffix"],
            [2, "42", "192.0.2.2", "prefix-MATCH-suffix"],
            [3, 42, "not-an-ip", "prefix-MATCH-suffix"],
            [4, 42, "192.0.2.4", "prefix-MATCH-suffix"],
            [5, 43, "192.0.2.5", "prefix-MATCH-suffix"],
            [6, None, None, None],
        ],
    },
    # Columns beyond the default `timestamp` and `message` are returned only
    # when a generated query requests them.
    "pushdown_projection": {
        "columns": [
            {"name": "timestamp"},
            {"name": "message"},
            {"name": "event.type"},
            {"name": "account.id"},
            {"name": "dataSource.name"},
        ],
        "values": [
            [1704067200000000000, "resolved", "DNS Resolved", "a1", "SentinelOne"],
            [1704067201000000000, "opened", "IP Connect", "a1", "Other"],
            [1704067202000000000, "resolved", "DNS Resolved", "a2", "SentinelOne"],
        ],
    },
    "select_batch_boundary": {
        "columns": [{"name": "n"}],
        "values": [[i] for i in range(8193)],
    },
    "select_invalid_json": '{"columns":[{"name":"x"}],"values":[[1],[true,]]}',
    "select_invalid_trailing": '{"columns":[],"values":[]} trailing',
    "select_invalid_column": {
        "columns": [{"name": None}],
        "values": [[1]],
    },
    "select_invalid_row": {
        "columns": [{"name": "n"}],
        "values": [[i] for i in range(8193)] + [[]],
    },
    "select_invalid_special": {
        "columns": [{"name": "x"}],
        "values": [[1], [{"special": "other"}]],
    },
    "select_invalid_timestamp": {
        "columns": [{"name": "timestamp"}],
        "values": [["2024-01-01T00:00:00Z"]],
    },
    "select_inference": {
        "columns": [
            {"name": "flag"},
            {"name": "number"},
            {"name": "time"},
            {"name": "duration"},
            {"name": "true"},
        ],
        "values": [["true", "42", "2024-01-01T00:00:00Z", "1h", "false"]],
    },
    "select_field_collisions": {
        "columns": [{"name": "x"}, {"name": "x"}, {"name": "a.b"}, {"name": "a.c"}],
        "values": [[1, 2, "true", "192.0.2.1"], [3, 4, "false", None]],
    },
    "select_empty_records": {
        "columns": [],
        "values": [[], []],
    },
    "select_mixed_column": {
        "columns": [{"name": "value"}],
        "values": [[1], ["two"], [None]],
    },
    # Basic two-row response for smoke-test purposes.
    "select_basic": {
        "columns": [
            {"name": "timestamp"},
            {"name": "event_id"},
            {"name": "message"},
        ],
        # timestamps are nanoseconds since the Unix epoch
        "values": [
            [1704067200000000000, 42, "login attempt"],
            [1704067201000000000, 43, "logout"],
        ],
    },
    # Empty result set — no values rows, schema still present.
    "select_empty": {
        "columns": [{"name": "timestamp"}, {"name": "event_id"}],
        "values": [],
    },
    # Special floating-point sentinel objects: NaN, +inf, -inf.
    "select_floats": {
        "columns": [{"name": "a"}, {"name": "b"}, {"name": "c"}],
        "values": [
            [
                {"special": "NaN"},
                {"special": "+infinity"},
                {"special": "-infinity"},
            ],
        ],
    },
    # One row that exercises every branch of parse(): null, bool, int64,
    # uint64 (> INT64_MAX so simdjson returns uint64_t), double, and string.
    # The "timestamp" column gets the dedicated nanosecond→time conversion;
    # the rest go through parse().
    "select_all_types": {
        "columns": [
            {"name": "timestamp"},
            {"name": "active"},
            {"name": "signed_delta"},
            {"name": "large_count"},
            {"name": "ratio"},
            {"name": "label"},
            {"name": "nothing"},
        ],
        "values": [
            [
                1704067200000000000,  # timestamp → tenzir time
                True,  # bool
                -99,  # int64 (negative)
                10000000000000000000,  # uint64 (> INT64_MAX = 9.22e18)
                2.718281828,  # double
                "alpha",  # string
                None,  # null
            ],
        ],
    },
    # Dotted column names are split by unflattened_field() into nested
    # records, so "src.ip" becomes {src: {ip: ...}}.
    "select_nested_fields": {
        "columns": [
            {"name": "timestamp"},
            {"name": "src.ip"},
            {"name": "src.port"},
            {"name": "dst.ip"},
            {"name": "dst.port"},
            {"name": "bytes"},
        ],
        "values": [
            [1704067200000000000, "10.0.0.1", 54321, "192.168.1.1", 443, 1024],
            [1704067201000000000, "10.0.0.2", 54322, "192.168.1.1", 443, 2048],
            [1704067202000000000, "10.0.0.3", 54323, "192.168.1.2", 8080, 512],
        ],
    },
    # Eight rows of user-activity data to verify multi-row output throughput.
    "select_many_rows": {
        "columns": [
            {"name": "timestamp"},
            {"name": "id"},
            {"name": "user"},
            {"name": "action"},
            {"name": "success"},
        ],
        "values": [
            [1704067200000000000, 1, "alice", "login", True],
            [1704067201000000000, 2, "alice", "read", True],
            [1704067202000000000, 3, "bob", "login", True],
            [1704067203000000000, 4, "bob", "write", True],
            [1704067204000000000, 5, "charlie", "login", False],
            [1704067205000000000, 6, "alice", "delete", False],
            [1704067206000000000, 7, "bob", "read", True],
            [1704067207000000000, 8, "alice", "logout", True],
        ],
    },
    # Strings that non_number_parser auto-infers as IP addresses and subnets.
    # A parallel test uses raw=true to keep them as plain strings.
    "select_typed_strings": {
        "columns": [
            {"name": "timestamp"},
            {"name": "src_addr"},
            {"name": "dst_addr"},
            {"name": "network"},
            {"name": "hostname"},
        ],
        "values": [
            [1704067200000000000, "10.0.0.1", "203.0.113.42", "10.0.0.0/8", "db-01"],
            [1704067201000000000, "10.0.0.2", "203.0.113.99", "10.0.0.0/8", "web-01"],
        ],
    },
    # The server returns HTTP 500 for this sentinel query so that the operator
    # emits an error diagnostic.
    "select_http_error": None,
}


# The events of the `aggregate*` account scopes, which generated reads and
# aggregate queries share. A missing field is `None`. The `true` keys collide
# with the text "true" after type inference. There is no flat `endpoint`
# field, but `endpoint.name` makes `endpoint` a record in TQL.
_AGGREGATE_COLUMNS = [
    "timestamp",
    "event.type",
    "dataSource.name",
    "flag",
    "endpoint.name",
]
_AGGREGATE_EVENTS = [
    ("DNS Resolved", "SentinelOne", "true", "host-a"),
    ("IP Connect", "SentinelOne", True, "host-b"),
    ("DNS Resolved", "Other", "true", None),
    ("DNS Resolved", "SentinelOne", True, "host-a"),
    ("Process Creation", "SentinelOne", "true", "host-b"),
    (None, "Other", True, None),
    ("IP Connect", "SentinelOne", "true", "host-a"),
]


def _aggregate_population(scope: str, payload: dict) -> dict:
    """The events of an `aggregate*` scope within the request window."""
    start = datetime.fromisoformat(payload["startTime"]).timestamp()
    end = datetime.fromisoformat(payload["endTime"]).timestamp()
    values = []
    for i, event in enumerate(_AGGREGATE_EVENTS):
        seconds = 1704067200 + i
        if not start <= seconds < end:
            continue
        # Only the sparse scopes have an event without `event.type`.
        if event[0] is None and not scope.startswith("aggregate_sparse"):
            continue
        values.append([seconds * 1_000_000_000, *event])
    return {
        "columns": [{"name": name} for name in _AGGREGATE_COLUMNS],
        "values": values,
    }


# -- PowerQuery evaluation ---------------------------------------------------
#
# The mock evaluates the subset of PowerQuery that the operator generates, and
# that tests send as explicit queries, over the events of a dataset. Like
# SentinelOne, it reads a missing field as `null`, matches `!=` on events
# without the field, sorts `null` first in ascending order, and caps a
# PowerQuery without `limit` at 1,000 rows.

_MAX_LIMIT = 1_600_000
_DEFAULT_CAP = 1_000


class QueryError(Exception):
    """A query that SentinelOne rejects."""


_TOKEN = re.compile(
    r"""\s*(?:
      (?P<string>"(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')
    | (?P<number>\d+(?:\.\d+)?)
    | (?P<matcher>(?:starts_with|ends_with|contains):matchcase)
    | (?P<name>[A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)*)
    | (?P<op>==|!=|<=|>=|<|>|=|\*|!|\(|\)|,|-)
    )""",
    re.VERBOSE,
)


def _tokenize(text: str) -> list[tuple[str, str]]:
    tokens = []
    i = 0
    while i < len(text):
        if text[i:].strip() == "":
            break
        match = _TOKEN.match(text, i)
        if match is None or match.end() == i:
            raise QueryError(f"Don't understand [{text[i:]}]")
        kind = match.lastgroup
        assert kind is not None
        tokens.append((kind, match[kind]))
        i = match.end()
    return tokens


def _unquote(text: str) -> str:
    body = text[1:-1]
    return re.sub(r"\\(.)", r"\1", body)


class _Parser:
    def __init__(self, text: str) -> None:
        self.tokens = _tokenize(text)
        self.i = 0

    def peek(self) -> tuple[str, str] | None:
        return self.tokens[self.i] if self.i < len(self.tokens) else None

    def take(self, value: str | None = None) -> tuple[str, str]:
        token = self.peek()
        if token is None or (value is not None and token[1] != value):
            raise QueryError(f"Don't understand [expected {value}]")
        self.i += 1
        return token

    def accept(self, value: str) -> bool:
        token = self.peek()
        if token is not None and token[1] == value:
            self.i += 1
            return True
        return False

    def done(self) -> bool:
        return self.i == len(self.tokens)

    # Expressions are tuples: ("field", name), ("literal", value),
    # ("cmp", op, lhs, rhs), ("in", lhs, values), ("present", field),
    # ("match", kind, lhs, needle), ("and"|"or", lhs, rhs), ("not", operand).
    def expression(self) -> tuple:
        lhs = self.conjunction()
        while self.accept("or"):
            lhs = ("or", lhs, self.conjunction())
        return lhs

    def conjunction(self) -> tuple:
        lhs = self.unary()
        while self.accept("and"):
            lhs = ("and", lhs, self.unary())
        return lhs

    def unary(self) -> tuple:
        if self.accept("!"):
            return ("not", self.unary())
        return self.comparison()

    def comparison(self) -> tuple:
        lhs = self.primary()
        token = self.peek()
        if token is None:
            return lhs
        if token[1] in {"==", "!=", "<", "<=", ">", ">="}:
            self.take()
            return ("cmp", token[1], lhs, self.primary())
        if token[1] == "=":
            self.take()
            self.take("*")
            if lhs[0] != "field":
                raise QueryError("Don't understand [= *]")
            return ("present", lhs[1])
        if token[1] == "in":
            self.take()
            self.take("(")
            values = [self.value()]
            while self.accept(","):
                values.append(self.value())
            self.take(")")
            return ("in", lhs, values)
        if token[0] == "matcher":
            self.take()
            return ("match", token[1].split(":")[0], lhs, self.primary())
        return lhs

    def value(self) -> object:
        node = self.primary()
        if node[0] != "literal":
            raise QueryError("Don't understand [list element]")
        return node[1]

    def primary(self) -> tuple:
        token = self.take()
        kind, text = token
        if text == "(":
            node = self.expression()
            self.take(")")
            return node
        if text == "-":
            number = self.take()
            if number[0] != "number":
                raise QueryError("Don't understand [-]")
            return ("literal", -_number(number[1]))
        if kind == "string":
            return ("literal", _unquote(text))
        if kind == "number":
            return ("literal", _number(text))
        if kind == "name":
            if text == "true":
                return ("literal", True)
            if text == "false":
                return ("literal", False)
            if text == "null":
                raise QueryError("Illegal value [null]")
            return ("field", text)
        raise QueryError(f"Don't understand [{text}]")


def _number(text: str) -> int | float:
    return float(text) if "." in text else int(text)


def _is_number(value: object) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _value(node: tuple, row: dict) -> object:
    if node[0] == "field":
        return row.get(node[1])
    if node[0] == "literal":
        return node[1]
    raise QueryError("Don't understand [predicate as value]")


def _numeric(value: object) -> object:
    """Read a numeric string as a number, as PowerQuery's scan phase does."""
    if isinstance(value, str) and re.fullmatch(r"-?\d+(?:\.\d+)?", value):
        return _number(value.removeprefix("-")) * (-1 if value[0] == "-" else 1)
    return value


def _equal(lhs: object, rhs: object, coerce: bool) -> bool:
    if lhs is None or rhs is None:
        return False
    if coerce and (_is_number(lhs) or _is_number(rhs)):
        lhs, rhs = _numeric(lhs), _numeric(rhs)
    if _is_number(lhs) and _is_number(rhs):
        return lhs == rhs
    return type(lhs) is type(rhs) and lhs == rhs


def _holds(node: tuple, row: dict, coerce: bool = False) -> bool:
    """Whether `node` holds for `row`. Before `columns` or `group`, PowerQuery
    compares numeric strings with numbers as numbers (`coerce`)."""
    kind = node[0]
    if kind == "and":
        return _holds(node[1], row, coerce) and _holds(node[2], row, coerce)
    if kind == "or":
        return _holds(node[1], row, coerce) or _holds(node[2], row, coerce)
    if kind == "not":
        return not _holds(node[1], row, coerce)
    if kind == "present":
        return row.get(node[1]) is not None
    if kind == "in":
        value = _value(node[1], row)
        return any(_equal(value, x, coerce) for x in node[2])
    if kind == "match":
        value = _value(node[2], row)
        needle = _value(node[3], row)
        if not isinstance(value, str) or not isinstance(needle, str):
            return False
        if node[1] == "starts_with":
            return value.startswith(needle)
        if node[1] == "ends_with":
            return value.endswith(needle)
        return needle in value
    if kind == "cmp":
        op = node[1]
        lhs = _value(node[2], row)
        rhs = _value(node[3], row)
        if op == "==":
            return _equal(lhs, rhs, coerce)
        if op == "!=":
            return not _equal(lhs, rhs, coerce)
        comparable = (_is_number(lhs) and _is_number(rhs)) or (
            isinstance(lhs, str) and isinstance(rhs, str)
        )
        if not comparable:
            return False
        return {
            "<": lhs < rhs,
            "<=": lhs <= rhs,
            ">": lhs > rhs,
            ">=": lhs >= rhs,
        }[op]
    # A bare field searches the full text of events, which the operator never
    # spells.
    raise QueryError("Don't understand [bare field]")


def _parse_predicate(text: str) -> tuple:
    parser = _Parser(text)
    node = parser.expression()
    if not parser.done():
        raise QueryError(f"Don't understand [{text}]")
    return node


def _split_pipes(query: str) -> list[str]:
    """Split a query at `|` outside of strings, and drop its comments."""
    parts = []
    current = ""
    quote = None
    i = 0
    while i < len(query):
        c = query[i]
        if not quote and query.startswith("//", i):
            end = query.find("\n", i)
            i = len(query) if end == -1 else end
            continue
        if not quote and query.startswith("/*", i):
            end = query.find("*/", i)
            if end == -1:
                raise QueryError("Unable to parse the entire query: comment")
            i = end + 2
            continue
        if quote:
            current += c
            if c == "\\" and i + 1 < len(query):
                current += query[i + 1]
                i += 2
                continue
            if c == quote:
                quote = None
        elif c in "\"'":
            quote = c
            current += c
        elif c == "|":
            parts.append(current)
            current = ""
        else:
            current += c
        i += 1
    if quote:
        raise QueryError("Unable to parse the entire query: unterminated string")
    parts.append(current)
    return [part.strip() for part in parts]


def _assignments(text: str) -> list[tuple[str, tuple | None]]:
    """Parse `name = expr, name, ...`, where the expression may be a call."""
    parser = _Parser(text)
    result = []
    while True:
        name = parser.take()
        if name[0] != "name":
            raise QueryError(f"Don't understand [{text}]")
        expr = None
        if parser.accept("="):
            token = parser.peek()
            if token is not None and token[1] == "count":
                parser.take()
                parser.take("(")
                arg = None
                if not parser.accept(")"):
                    arg = parser.take()
                    if arg[0] != "name":
                        raise QueryError(f"Don't understand [{text}]")
                    parser.take(")")
                expr = ("count", arg[1] if arg else None)
            else:
                expr = parser.primary()
        result.append((name[1], expr))
        if parser.done():
            return result
        parser.take(",")


def _stage(rows: list[dict], columns: list[str], stage: str, coerce: bool) -> tuple:
    """Apply one PowerQuery stage to `rows`, whose fields are `columns`."""
    command, _, rest = stage.partition(" ")
    rest = rest.strip()
    if command == "filter":
        node = _parse_predicate(rest)
        return [row for row in rows if _holds(node, row, coerce)], columns
    if command == "columns":
        assignments = _assignments(rest)
        result = []
        for row in rows:
            out = {}
            for name, expr in assignments:
                if expr is not None and expr[0] == "count":
                    raise QueryError("Don't understand [count in columns]")
                out[name] = row.get(name) if expr is None else _value(expr, row)
            result.append(out)
        return result, [name for name, _ in assignments]
    if command == "group":
        aggregates_text, _, keys_text = rest.partition(" by ")
        aggregates = _assignments(aggregates_text)
        keys = _assignments(keys_text) if keys_text else []
        if any(expr is None or expr[0] != "count" for _, expr in aggregates):
            raise QueryError(f"Don't understand [{aggregates_text}]")
        groups: dict[tuple, dict] = {}
        for row in rows:
            key = [row.get(name) if e is None else _value(e, row) for name, e in keys]
            # Group by type and value, as PowerQuery does.
            group = tuple((type(v).__name__, v) for v in key)
            state = groups.setdefault(
                group,
                {
                    **{name: value for (name, _), value in zip(keys, key)},
                    **{name: 0 for name, _ in aggregates},
                },
            )
            for name, (_, arg) in aggregates:
                if arg is None or row.get(arg) is not None:
                    state[name] += 1
        result = list(groups.values())
        if not keys and not result:
            result = [{name: 0 for name, _ in aggregates}]
        return result, [name for name, _ in keys] + [name for name, _ in aggregates]
    if command == "sort":
        result = list(rows)
        for key in reversed([part.strip() for part in rest.split(",")]):
            descending = key.startswith("-")
            name = key.removeprefix("-")

            def order(row: dict, name: str = name) -> tuple:
                value = row.get(name)
                if value is None:
                    return (0, 0, "")
                if _is_number(value):
                    return (1, value, "")
                return (2, 0, str(value))

            # Nulls sort first ascending and last descending.
            result.sort(key=order, reverse=descending)
        return result, columns
    if command == "limit":
        if not re.fullmatch(r"\d+", rest) or not 1 <= int(rest) <= _MAX_LIMIT:
            raise QueryError(f"query syntax error: invalid limit [{rest}]")
        return rows[: int(rest)], columns
    raise QueryError(f"Don't understand [{command}]")


def _rows(table: dict) -> tuple[list[dict], list[str]]:
    names = [column["name"] for column in table["columns"]]
    return [dict(zip(names, row)) for row in table["values"]], names


def _table(rows: list[dict], columns: list[str]) -> dict:
    return {
        "columns": [{"name": name} for name in columns],
        "values": [[row.get(name) for name in columns] for row in rows],
    }


def _hybrid_rows(scope: str) -> list[dict]:
    count = 3 if scope in {"hybrid_discovery", "hybrid_partial"} else 1005
    return [
        {
            "timestamp": 1704067200000000000,
            "id": i,
            "event.type": "DNS Resolved",
            "event.dns.request": f"host-{i}",
            "key": "id",
            "flag": "true",
            "addr": "192.0.2.1",
            "items": [1, {"nested": "false"}],
            "event.extra": "kept",
        }
        for i in range(count)
    ]


def _population(scope: str, payload: dict) -> tuple[list[dict], list[str]]:
    """The events of the account scope `scope` within the request window."""
    if scope.startswith("aggregate"):
        table = _aggregate_population(scope, payload)
        rows, columns = _rows(table)
        # A missing field is absent from the event.
        rows = [{k: v for k, v in row.items() if v is not None} for row in rows]
        return rows, columns
    if scope == "pq_maximum":
        return [{"x": 0}] * _MAX_LIMIT, ["x"]
    if scope == "hybrid_numeric_floats":
        rows, columns = _rows(_STATIC_RESPONSES["select_floats"])
    elif scope.startswith("hybrid_numeric_") or scope == "hybrid_inline":
        rows, columns = _rows(_STATIC_RESPONSES["select_basic"])
    elif scope.startswith("hybrid_"):
        rows = _hybrid_rows(scope)
        columns = list(rows[0])
    elif scope in {"pushdown_projection", "pushdown_semantics", "pushdown_booleans"}:
        rows, columns = _rows(_STATIC_RESPONSES[scope])
    elif scope == "pushdown_empty":
        return [], []
    else:
        rows, columns = _rows(_STATIC_RESPONSES["select_basic"])
    start = datetime.fromisoformat(payload["startTime"]).timestamp()
    end = datetime.fromisoformat(payload["endTime"]).timestamp()
    rows = [
        row
        for row in rows
        if "timestamp" not in row or start <= row["timestamp"] / 1e9 < end
    ]
    return rows, columns


def _capped_rows(base: str, payload: dict) -> list[dict]:
    """1,005 events, five more than the default cap returns."""
    start = datetime.fromisoformat(payload["startTime"]).timestamp()
    end = datetime.fromisoformat(payload["endTime"]).timestamp()
    reverse = base == "pushdown_capped_reverse"
    rows = []
    for i in range(1005):
        seconds = 1704067200 + (1004 - i if reverse else i)
        if start <= seconds < end:
            rows.append({"timestamp": seconds * 1_000_000_000, "id": i})
    return rows


def _named_base(base: str, payload: dict) -> tuple[list[dict], list[str]] | None:
    """The result of an explicit query that names a test dataset."""
    if base == "select_scope":
        rows = [
            {
                "tenant": payload.get("tenant"),
                "accounts": ",".join(payload.get("accountIds", [])),
            }
        ]
        return rows, ["tenant", "accounts"]
    if base == "select_echo_times":
        return [{"start": payload["startTime"], "end": payload["endTime"]}], [
            "start",
            "end",
        ]
    if base in {"pushdown_capped", "pushdown_capped_reverse"}:
        return _capped_rows(base, payload), ["timestamp", "id"]
    if base.startswith("pushdown_empty"):
        return [], []
    if base in _RETRY_QUERIES:
        return _rows(_STATIC_RESPONSES["select_basic"])
    data = _STATIC_RESPONSES.get(base)
    if isinstance(data, dict):
        return _rows(data)
    return None


# Explicit queries whose transport misbehaves before it returns the basic
# result.
_RETRY_QUERIES = {
    "select_retry",
    "select_launch_retry",
    "select_launch_long_retry",
    "select_launch_retry_signed_overflow",
    "select_launch_retry_unsigned_max",
    "select_launch_retry_beyond_unsigned",
    "select_launch_retry_invalid",
    "select_poll_retry_overflow",
    "select_transport_retry",
    "select_delete_retry",
}


def _check_query(payload: dict) -> None:
    """Reject queries that SentinelOne would not parse, as at launch."""
    if payload["queryType"] == "LOG":
        if payload["log"]["filter"].strip():
            _parse_predicate(payload["log"]["filter"])
        return
    parts = _split_pipes(payload["pq"]["query"])
    for stage in parts[1:]:
        command, _, rest = stage.partition(" ")
        if command == "filter":
            _parse_predicate(rest)
        elif command in {"columns", "group"}:
            for text in rest.split(" by "):
                _assignments(text)
        elif command == "limit":
            if not re.fullmatch(r"\d+", rest.strip()) or not (
                1 <= int(rest) <= _MAX_LIMIT
            ):
                raise QueryError(f"query syntax error: invalid limit [{rest}]")
        elif command != "sort":
            raise QueryError(f"Don't understand [{command}]")


def _evaluate_pq(payload: dict, scope: str) -> dict | str:
    """Evaluate a PowerQuery as SentinelOne would, or return a static body."""
    query = payload["pq"]["query"]
    parts = _split_pipes(query)
    base, stages = parts[0], parts[1:]
    explicit = bool(base)
    if explicit and not stages and base in _STATIC_RESPONSES:
        # Keep static fixtures byte-for-byte, also malformed ones.
        data = _STATIC_RESPONSES[base]
        if data is None:
            raise QueryError(f"unknown query: {query!r}")
        return data
    named = _named_base(base, payload) if explicit else None
    if named is not None:
        rows, columns = named
    else:
        rows, columns = _population(scope, payload)
        if explicit:
            # A plain query filters the events of the scope.
            node = _parse_predicate(base)
            rows = [row for row in rows if _holds(node, row, True)]
    shaped = False
    limited = False
    for stage in stages:
        rows, columns = _stage(rows, columns, stage, not shaped)
        command = stage.partition(" ")[0]
        shaped = shaped or command in {"columns", "group"}
        limited = limited or command == "limit"
    if named is None and not shaped:
        # Whole events show only `timestamp` and `message`.
        columns = [
            name
            for name in ("timestamp", "message")
            if name != "timestamp" or name in columns
        ]
    data = _table(rows, columns)
    if not limited and len(data["values"]) > _DEFAULT_CAP:
        data["values"] = data["values"][:_DEFAULT_CAP]
        data["warnings"] = ["Result set limited to 1000 rows by default"]
    return data


def _evaluate_log(payload: dict, scope: str) -> list[dict]:
    rows, _ = _population(scope, payload)
    text = payload["log"]["filter"]
    if text.strip():
        node = _parse_predicate(text)
        # LOG queries also see the metadata that `_log_page` adds.
        rows = [
            row
            for row in rows
            if _holds(node, {"severity": 3, "threadId": "worker", **row}, True)
        ]
    return rows


def _log_page(rows: list[dict], payload: dict) -> dict:
    matches = []
    for i, row in enumerate(rows):
        values = dict(row)
        matches.append(
            {
                "cursor": f"match-{i}",
                "timestamp": values.pop("timestamp", 1704067200000000000),
                "severity": 3,
                "threadId": "worker",
                "values": values,
            }
        )
    log = payload["log"]
    offset = int(log["cursor"].removeprefix("match-")) if "cursor" in log else 0
    # Inclusive opaque cursors, not timestamp arithmetic; equal timestamps
    # deliberately span page boundaries in the dense fixture.
    return {
        "matches": matches[offset : offset + log["limit"]],
        # This estimate is deliberately wrong. It is not an exhaustion signal.
        "estimatedMatchCount": 1,
    }


def _request_text(payload: dict) -> str:
    if payload["queryType"] == "PQ":
        return payload["pq"]["query"]
    return payload["log"]["filter"]


def _base(payload: dict) -> str:
    """The first segment of a PowerQuery, which names explicit test queries."""
    if payload["queryType"] != "PQ":
        return ""
    try:
        return _split_pipes(payload["pq"]["query"])[0]
    except QueryError:
        return payload["pq"]["query"]


def _validate_add_events_payload(payload: object) -> str | None:
    if not isinstance(payload, dict):
        return "payload must be a JSON object"
    session = payload.get("session")
    if not isinstance(session, str) or not session:
        return "session must be a non-empty string"
    events = payload.get("events")
    if not isinstance(events, list) or not events:
        return "events must be a non-empty array"
    session_info = payload.get("sessionInfo")
    if session_info is not None and not isinstance(session_info, dict):
        return "sessionInfo must be an object"
    for i, event in enumerate(events):
        if not isinstance(event, dict):
            return f"events[{i}] must be an object"
        attrs = event.get("attrs")
        if not isinstance(attrs, dict):
            return f"events[{i}].attrs must be an object"
        ts = event.get("ts")
        if ts is not None and not isinstance(ts, str):
            return f"events[{i}].ts must be a string"
        sev = event.get("sev")
        if sev is not None and not isinstance(sev, int):
            return f"events[{i}].sev must be an integer"
    return None


def _make_handler(
    capture_path: str, lrq_capture_path: str
) -> type[BaseHTTPRequestHandler]:
    lock = threading.Lock()
    queries: dict[str, dict[str, Any]] = {}
    launches: dict[str, int] = {}

    def capture(**entry: Any) -> None:
        with lock:
            with open(lrq_capture_path, "a") as file:
                file.write(json.dumps(entry | {"time": time.monotonic()}) + "\n")

    class SentinelOneHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: object) -> None:
            # Silence the default per-request log lines.
            pass

        def do_POST(self) -> None:
            if self.path not in {"/sdl/v2/api/queries", "/api/addEvents"}:
                self._respond(404, {"error": f"unknown path: {self.path}"})
                return

            # Validate the bearer token exactly as the real API would.
            auth = self.headers.get("Authorization", "")
            if auth != f"Bearer {_EXPECTED_TOKEN}":
                self._respond(401, {"error": "unauthorized"})
                return

            length = int(self.headers.get("Content-Length", 0))
            raw_body = self.rfile.read(length)
            encoding = self.headers.get("Content-Encoding", "")
            normalized_encoding = encoding.split(",", 1)[0].strip().lower()
            if normalized_encoding == "gzip":
                try:
                    raw_body = gzip.decompress(raw_body)
                except Exception as exc:
                    self._respond(400, {"error": f"bad gzip: {exc}"})
                    return
            elif normalized_encoding:
                self._respond(415, {"error": f"unsupported encoding: {encoding}"})
                return
            try:
                payload = json.loads(raw_body.decode())
            except json.JSONDecodeError as exc:
                self._respond(400, {"error": f"bad JSON: {exc}"})
                return

            if self.path == "/api/addEvents":
                if err := _validate_add_events_payload(payload):
                    self._respond(400, {"error": err})
                    return
                with lock:
                    with open(capture_path, "a") as file:
                        file.write(json.dumps(payload, sort_keys=True) + "\n")
                self._respond(200, {})
                return

            error = self._validate_launch(payload)
            if error:
                capture(method="POST", payload=payload, status=400, error=error)
                self._respond(400, {"error": error})
                return
            text = _request_text(payload)
            query = _base(payload)
            grouped = "| group" in text
            launches[text] = launches.get(text, 0) + 1
            if payload.get("accountIds") == ["aggregate_bad_launch_retry"] and grouped:
                capture(method="POST", payload=payload, status=429)
                self._respond(
                    429, {"error": "rate limited"}, {"Retry-After": "invalid"}
                )
                return
            if payload.get("accountIds") == ["aggregate_rejected"] and grouped:
                capture(method="POST", payload=payload, status=500)
                self._respond(500, {"error": "Don't understand [group]"})
                return
            if "launch_error" in text:
                capture(method="POST", payload=payload, status=500)
                self._respond(500, {"error": "Unable to parse the entire query"})
                return
            if query == "select_http_error":
                capture(method="POST", payload=payload, status=500)
                self._respond(500, {"error": "internal server error"})
                return
            retry_after = {
                "select_launch_retry": "1",
                "select_launch_long_retry": "20",
                "select_launch_retry_timeout": "60",
                "select_launch_retry_overflow": str(2**63 - 1),
                "select_launch_retry_signed_overflow": str(2**63),
                "select_launch_retry_unsigned_max": str(2**64 - 1),
                "select_launch_retry_beyond_unsigned": str(2**64),
                "select_launch_retry_invalid": "invalid",
            }.get(query)
            if retry_after is not None and launches[text] == 1:
                capture(method="POST", payload=payload, status=429)
                self._respond(
                    429, {"error": "rate limited"}, {"Retry-After": retry_after}
                )
                return
            try:
                _check_query(payload)
            except QueryError as exc:
                capture(method="POST", payload=payload, status=400, rejected=str(exc))
                self._respond(400, {"message": str(exc)})
                return
            qid = str(len(queries) + 1)
            tag = f"route-{qid}"
            queries[qid] = {
                "payload": payload,
                "tag": tag,
                "polls": 0,
                "progress": 0,
                "last_step": 0,
                "deleted": False,
                "deletes": 0,
            }
            # The query exists, but the client never receives its ID. Replaying
            # either launch creates another query that the client cannot cancel.
            if query.startswith("select_launch_transport_error"):
                capture(method="POST", payload=payload, status=0, id=qid)
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            if query.startswith("select_launch_server_error") or (
                payload.get("accountIds") == ["aggregate_launch_error"] and grouped
            ):
                capture(method="POST", payload=payload, status=503, id=qid)
                self._respond(503, {"error": "response failed after query creation"})
                return
            capture(method="POST", payload=payload, status=200, id=qid)
            if payload.get("accountIds") == ["hybrid_inline"]:
                self._respond(
                    200,
                    {
                        "id": qid,
                        "stepsCompleted": 2,
                        "stepsTotal": 2,
                        "data": _log_page(
                            _evaluate_log(payload, "hybrid_inline"), payload
                        ),
                    },
                    {"X-Dataset-Query-Forward-Tag": tag},
                )
                return
            self._respond(
                200,
                {"id": qid, "stepsCompleted": 0, "stepsTotal": 2},
                {"X-Dataset-Query-Forward-Tag": tag},
            )

        def _validate_launch(self, payload: dict[str, Any]) -> str | None:
            if payload.get("queryType") not in {"PQ", "LOG"}:
                return "expected queryType PQ or LOG"
            if payload.get("queryPriority") != "LOW":
                return "expected LOW priority"
            if payload["queryType"] == "PQ":
                pq = payload.get("pq", {})
                if pq.get("resultType") != "TABLE" or not isinstance(
                    pq.get("query"), str
                ):
                    return "expected a TABLE PowerQuery"
                if "log" in payload:
                    return "PQ must not include LOG parameters"
            else:
                log = payload.get("log", {})
                if (
                    not isinstance(log.get("filter"), str)
                    or not isinstance(log.get("limit"), int)
                    or not 2 <= log["limit"] <= 5000
                    or set(log) - {"filter", "limit", "cursor"}
                    or "pq" in payload
                ):
                    return "invalid LOG request"
                if "X-Dataset-Query-Forward-Tag" in self.headers:
                    return "a new LOG job must not reuse another job's routing tag"
            if "accountIds" in payload:
                ids = payload["accountIds"]
                if (
                    payload.get("tenant") is not False
                    or not ids
                    or not all(isinstance(x, str) for x in ids)
                ):
                    return "expected non-empty accountIds with tenant false"
            elif not isinstance(payload.get("tenant"), bool):
                return "expected a boolean tenant"
            bounds = []
            for key in ("startTime", "endTime"):
                value = payload.get(key)
                if not isinstance(value, str) or not re.fullmatch(
                    r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z", value
                ):
                    return f"expected an ISO-8601 {key}"
                bounds.append(datetime.fromisoformat(value))
            if bounds[0] > bounds[1]:
                return "invalid query window"
            return None

        def _active_query(self) -> tuple[str, dict[str, Any]] | None:
            qid = urlsplit(self.path).path.removeprefix("/sdl/v2/api/queries/")
            state = queries.get(qid)
            if (
                not state
                or self.headers.get("Authorization") != f"Bearer {_EXPECTED_TOKEN}"
                or self.headers.get("X-Dataset-Query-Forward-Tag") != state["tag"]
            ):
                capture(method=self.command, id=qid, status=400, error="bad routing")
                self._respond(400, {"error": "missing query, token, or forward tag"})
                return None
            return qid, state

        def do_GET(self) -> None:
            active = self._active_query()
            if active is None:
                return
            qid, state = active
            seen = parse_qs(urlsplit(self.path).query).get("lastStepSeen")
            if seen != [str(state["last_step"])] or state["deleted"]:
                capture(method="GET", id=qid, status=400, error="bad poll")
                self._respond(400, {"error": "invalid lastStepSeen or deleted query"})
                return
            state["polls"] += 1
            text = _request_text(state["payload"])
            query = _base(state["payload"])
            grouped = "| group" in text
            scope = state["payload"].get("accountIds", [""])[0]
            continuation = "cursor" in state["payload"].get("log", {})
            if continuation and scope in {"hybrid_retry", "hybrid_timeout"}:
                if state["polls"] == 1:
                    capture(method="GET", id=qid, status=429)
                    self._respond(
                        429,
                        {"error": "rate limited"},
                        {"Retry-After": "0" if scope == "hybrid_retry" else "3"},
                    )
                    return
            if query == "select_retry" and state["polls"] == 1:
                capture(method="GET", id=qid, status=429)
                self._respond(429, {"error": "rate limited"}, {"Retry-After": "1"})
                return
            if query == "select_poll_retry_overflow" and state["polls"] == 1:
                capture(method="GET", id=qid, status=429)
                self._respond(
                    429, {"error": "rate limited"}, {"Retry-After": str(2**64 - 1)}
                )
                return
            # Polls of these aggregate queries ask for unusable retry delays.
            retry_after = {
                "aggregate_long_retry": "20",
                "aggregate_bad_retry": "invalid",
            }.get(scope)
            if retry_after is not None and grouped:
                capture(method="GET", id=qid, status=429)
                self._respond(
                    429, {"error": "rate limited"}, {"Retry-After": retry_after}
                )
                return
            # Every poll of this aggregate query fails, so retries cannot help.
            if scope == "aggregate_poll_error" and grouped:
                capture(method="GET", id=qid, status=0)
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            if query == "select_transport_retry" and state["polls"] == 1:
                capture(method="GET", id=qid, status=0)
                self.close_connection = True
                self.connection.shutdown(socket.SHUT_RDWR)
                self.connection.close()
                return
            if query == "select_deadline_retry":
                capture(method="GET", id=qid, status=429)
                self._respond(429, {"error": "rate limited"}, {"Retry-After": "10"})
                return
            if query == "select_hung_poll":
                capture(method="GET", id=qid, status=0)
                # Send no response. Wait for the client to time out and close
                # this connection before serving its cleanup DELETE.
                self.close_connection = True
                self.connection.settimeout(15)
                try:
                    if self.connection.recv(1):
                        capture(method="GET", id=qid, error="unexpected request data")
                except ConnectionResetError:
                    # Timing out can reset the connection instead of sending EOF.
                    pass
                return
            if query == "select_slow_retry":
                capture(method="GET", id=qid, status=429)
                self._respond(429, {"error": "rate limited"}, {"Retry-After": "60"})
                return
            if query == "select_expired":
                capture(method="GET", id=qid, status=404)
                self._respond(404, {"code": "not_found"})
                return
            if query == "select_poll_error":
                capture(method="GET", id=qid, status=400)
                self._respond(400, {"error": "invalid PowerQuery"})
                return
            if query == "select_retry_exhausted":
                capture(method="GET", id=qid, status=503)
                self._respond(503, {"error": "temporarily unavailable"})
                return
            if "poll_error" in text:
                capture(method="GET", id=qid, status=500)
                self._respond(500, {"error": "undefined field 'poll_error'"})
                return
            if query.startswith("pushdown_auth_error"):
                capture(method="GET", id=qid, status=403)
                self._respond(403, {"error": "access denied"})
                return
            # Two in-progress polls before the final result.
            # Do not expose partial rows: LRQ results belong to the final poll.
            state["progress"] += 1
            step = 0 if query == "select_running" else min(state["progress"] - 1, 2)
            total = 0 if state["progress"] == 1 or query == "select_running" else 2
            if query == "select_stalled":
                step, total = 1, 2
            # Wire-contract cases need no slow-query simulation. Keep lifecycle
            # coverage in the existing select_* scenarios.
            if not query.startswith("select_"):
                step = total = 2
            state["last_step"] = step
            capture(method="GET", id=qid, status=200, step=step)
            # Malformed polls of an aggregate query.
            if grouped and scope in {
                "aggregate_no_data",
                "aggregate_bad_poll",
                "aggregate_no_steps",
            }:
                self._respond(
                    200,
                    {
                        "aggregate_no_data": {"stepsCompleted": 2, "stepsTotal": 2},
                        "aggregate_bad_poll": "not json",
                        "aggregate_no_steps": {"data": {}},
                    }[scope],
                )
                return
            if total == 0 or step < total:
                self._respond(
                    200,
                    {
                        "stepsCompleted": step,
                        "stepsTotal": total,
                        "data": {"columns": [{"name": "partial"}], "values": [[True]]},
                    },
                )
                return
            try:
                if state["payload"]["queryType"] == "LOG":
                    data = {"rows": _evaluate_log(state["payload"], scope)}
                else:
                    data = _evaluate_pq(state["payload"], scope)
            except QueryError as exc:
                capture(method="GET", id=qid, status=400, rejected=str(exc))
                self._respond(400, {"message": str(exc)})
                return
            if isinstance(data, str):
                # Keep malformed JSON fixtures byte-for-byte to retain their
                # parser diagnostics while exercising cleanup after a poll.
                self._respond(200, data)
                return
            if state["payload"]["queryType"] == "LOG":
                data = _log_page(data["rows"], state["payload"])
                if scope == "hybrid_mixed":
                    for match in data["matches"]:
                        values = match["values"]
                        shape = values["id"] % 3
                        if shape == 0:
                            values["dataSource.name"] = "SentinelOne"
                        else:
                            for name in list(values):
                                if name.startswith("event."):
                                    del values[name]
                            if shape == 2:
                                values.update(event=None, dataSource=None)
                if scope == "hybrid_bad_boundary" and continuation:
                    data["matches"][0]["cursor"] = "wrong-boundary"
                if scope in {"hybrid_numeric_positive", "hybrid_numeric_negative"}:
                    number = (
                        2**64 if scope == "hybrid_numeric_positive" else -(2**63) - 1
                    )
                    data["matches"][-1]["values"]["oversized"] = [{"nested": number}]
                if scope == "hybrid_bad_values":
                    data["matches"][-1]["values"] = []
                if scope == "hybrid_partial_log":
                    data["partialResultsDueToTimeLimit"] = True
                if scope.startswith("aggregate"):
                    # LOG events do not carry the fields that they lack.
                    for match in data["matches"]:
                        values = match["values"]
                        for name in [k for k, v in values.items() if v is None]:
                            del values[name]
                self._respond(
                    200, {"stepsCompleted": step, "stepsTotal": total, "data": data}
                )
                return
            data = dict(data)
            data["columns"] = [
                dict(column, cellType="UNKNOWN", decimalPlaces=0)
                for column in data["columns"]
            ]
            data["matchCount"] = len(data["values"])
            if scope in {"hybrid_partial", "aggregate_partial"}:
                data["omittedEvents"] = 1.0
            self._respond(
                200, {"stepsCompleted": step, "stepsTotal": total, "data": data}
            )

        def do_DELETE(self) -> None:
            active = self._active_query()
            if active is None:
                return
            qid, state = active
            state["deletes"] += 1
            if (
                _base(state["payload"]) == "select_delete_retry"
                and state["deletes"] == 1
            ):
                capture(method="DELETE", id=qid, status=503)
                self._respond(503, {"error": "temporarily unavailable"})
                return
            state["deleted"] = True
            code = 404 if _base(state["payload"]) == "select_expired" else 200
            capture(method="DELETE", id=qid, status=code)
            self._respond(code, {})

        def _respond(
            self, code: int, obj: object, headers: dict[str, str] | None = None
        ) -> None:
            body = obj.encode() if isinstance(obj, str) else json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(body)

    return SentinelOneHandler


@fixture(
    name="sentinelone",
    options=SentinelOneOptions,
    assertions=SentinelOneAssertions,
)
def sentinelone() -> FixtureHandle:
    """Start a mock SentinelOne Data Lake API server on a random port.

    Exports:
      S1_FIXTURE_URL   - base URL of the mock server
      S1_FIXTURE_TOKEN - bearer token expected by the server
      S1_FIXTURE_CAPTURE_FILE - JSONL file containing captured addEvents calls
      S1_FIXTURE_LRQ_CAPTURE_FILE - JSONL file containing LRQ requests
      S1_FIXTURE_CAFILE - CA certificate path when tls=true
    """
    opts = current_options("sentinelone")
    fd, capture_path = tempfile.mkstemp(prefix="sentinelone-capture-", suffix=".jsonl")
    os.close(fd)
    lrq_fd, lrq_capture_path = tempfile.mkstemp(
        prefix="sentinelone-lrq-", suffix=".jsonl"
    )
    os.close(lrq_fd)
    temp_dir: Path | None = None
    tls_env: dict[str, str] = {}
    if opts.tls:
        temp_dir = Path(tempfile.mkdtemp(prefix="sentinelone-tls-"))
        try:
            cert_path, key_path, ca_path, _ = generate_self_signed_cert(
                temp_dir,
                common_name=_HOST,
                san_entries=[f"IP:{_HOST}"],
            )
        except (FileNotFoundError, subprocess.CalledProcessError) as exc:
            shutil.rmtree(temp_dir, ignore_errors=True)
            if os.path.exists(capture_path):
                os.remove(capture_path)
            os.remove(lrq_capture_path)
            raise FixtureUnavailable(f"openssl unavailable: {exc}") from exc
        tls_env = {
            "S1_FIXTURE_CAFILE": str(ca_path),
        }
    server = HTTPServer((_HOST, 0), _make_handler(capture_path, lrq_capture_path))
    if opts.tls:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
        server.socket = context.wrap_socket(server.socket, server_side=True)
    port = server.server_address[1]
    scheme = "https" if opts.tls else "http"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    env = {
        "S1_FIXTURE_URL": f"{scheme}://{_HOST}:{port}",
        "S1_FIXTURE_TOKEN": _EXPECTED_TOKEN,
        "S1_FIXTURE_CAPTURE_FILE": capture_path,
        "S1_FIXTURE_LRQ_CAPTURE_FILE": lrq_capture_path,
    }
    env.update(tls_env)
    # The number of captured requests that earlier tests of the suite made.
    checked = [0]

    def _request(call: dict[str, Any]) -> dict[str, object]:
        payload = call["payload"]
        result: dict[str, object] = {
            "type": payload["queryType"],
            "query": _request_text(payload),
            "start": payload["startTime"],
            "end": payload["endTime"],
            "status": call["status"],
        }
        if payload["queryType"] == "LOG":
            result["limit"] = payload["log"]["limit"]
            result["cursor"] = payload["log"].get("cursor")
        return result

    def _assert_test(
        *, assertions: SentinelOneAssertions | dict[str, Any], **_: object
    ) -> None:
        if isinstance(assertions, dict):
            assertions = SentinelOneAssertions(**assertions)
        lines = Path(lrq_capture_path).read_text().splitlines()
        recent = [json.loads(line) for line in lines[checked[0] :] if line]
        checked[0] = len(lines)
        if assertions.expected_requests is not None:
            actual = [_request(call) for call in recent if call["method"] == "POST"]
            projected = [
                {key: request.get(key) for key in expected}
                for request, expected in zip(actual, assertions.expected_requests)
            ]
            if len(actual) != len(assertions.expected_requests) or (
                projected != assertions.expected_requests
            ):
                raise AssertionError(
                    "expected SentinelOne requests "
                    f"{assertions.expected_requests!r}, got {actual!r}"
                )
        if assertions.queries_cancelled:
            calls = [
                json.loads(line)
                for line in Path(lrq_capture_path).read_text().splitlines()
                if line
            ]
            errors = [call for call in calls if "error" in call]
            assert not errors, errors
            # Only acknowledged launches expose an ID that can be cancelled.
            # Ambiguous launches are checked for non-replay by lifecycle.py.
            launched = {
                call["id"]
                for call in calls
                if call["method"] == "POST" and call["status"] == 200 and "id" in call
            }
            cancelled = {
                call["id"]
                for call in calls
                if call["method"] == "DELETE" and call["status"] in {200, 404}
            }
            assert launched == cancelled, (launched, cancelled)
        if assertions.expected_add_events is None:
            return
        captured = [
            json.loads(line)
            for line in Path(capture_path).read_text().splitlines()
            if line
        ]
        normalized = []
        for payload in captured:
            if not isinstance(payload, dict):
                raise AssertionError(
                    f"expected a SentinelOne addEvents object, got {payload!r}"
                )
            session = payload.get("session")
            if not isinstance(session, str) or not session:
                raise AssertionError(
                    f"expected a non-empty SentinelOne session, got {session!r}"
                )
            normalized.append(
                {key: value for key, value in payload.items() if key != "session"}
            )
        if normalized != assertions.expected_add_events:
            raise AssertionError(
                "expected SentinelOne addEvents payloads "
                f"{assertions.expected_add_events!r}, got {normalized!r}"
            )

    def _teardown() -> None:
        server.shutdown()
        thread.join(timeout=2)
        if os.path.exists(capture_path):
            os.remove(capture_path)
        if os.path.exists(lrq_capture_path):
            os.remove(lrq_capture_path)
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)

    return FixtureHandle(
        env=env,
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
