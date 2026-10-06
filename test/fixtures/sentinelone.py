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


# Predefined result data blocks keyed by `pq.query` in the launch body.
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


# A trailing `columns` stage before an optional `limit`, as generated from TQL.
_COLUMNS_STAGE = re.compile(r"(?:^| )\| columns ([^|]+?)(?= \| |$)")


def _split_columns(query: str) -> tuple[str, list[str] | None]:
    """Separate the projection of a generated query from its other stages."""
    match = _COLUMNS_STAGE.search(query)
    if match is None:
        return query, None
    rest = (query[: match.start()] + query[match.end() :]).strip()
    if not rest:
        # A projection alone reads the whole request window.
        rest = "| filter (1 == 1)"
    return rest, [name.strip() for name in match.group(1).split(",")]


def _project(data: dict, columns: list[str] | None) -> dict:
    """Model PowerQuery's table projection of a generated query.

    Without a `columns` stage, SentinelOne returns only `timestamp` and
    `message`. Requested fields that an event lacks are null. Every real event
    has a `timestamp`; fixtures without one model it as absent instead.
    """
    names = [column["name"] for column in data["columns"]]
    if columns is None:
        columns = ["timestamp", "message"]
    columns = [name for name in columns if name != "timestamp" or name in names]
    indexes = [names.index(name) if name in names else None for name in columns]
    return {
        **data,
        "columns": [{"name": name} for name in columns],
        "values": [
            [None if i is None else row[i] for i in indexes] for row in data["values"]
        ],
    }


def _request_query(payload: dict) -> str:
    if payload["queryType"] == "PQ":
        return payload["pq"]["query"]
    return "| filter " + (payload["log"]["filter"] or "(1 == 1)")


def _log_page(data: dict, payload: dict) -> dict:
    names = [column["name"] for column in data["columns"]]
    matches = []
    for i, row in enumerate(data["values"]):
        values = dict(zip(names, row))
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
            query = _request_query(payload)
            launches[query] = launches.get(query, 0) + 1
            if query.startswith("| filter ((launch_error) == (1))"):
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
            if retry_after is not None and launches[query] == 1:
                capture(method="POST", payload=payload, status=429)
                self._respond(
                    429, {"error": "rate limited"}, {"Retry-After": retry_after}
                )
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
            if query.startswith("select_launch_server_error"):
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
                        "data": _log_page(_STATIC_RESPONSES["select_basic"], payload),
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
            elif payload.get("tenant") is not True:
                return "expected tenant true"
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
            query = _request_query(state["payload"])
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
            if query.startswith("| filter ((poll_error) == (1))"):
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
            if query.startswith("pushdown_") or query.startswith("| "):
                step = total = 2
            state["last_step"] = step
            capture(method="GET", id=qid, status=200, step=step)
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
            # Generated queries project their response; explicit ones do not.
            generated = query.startswith("| ")
            query, projection = _split_columns(query) if generated else (query, None)
            if query == "select_echo_times":
                data = {
                    "columns": [{"name": "start"}, {"name": "end"}],
                    "values": [
                        [state["payload"]["startTime"], state["payload"]["endTime"]]
                    ],
                }
            elif query in {
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
            }:
                data = _STATIC_RESPONSES["select_basic"]
            elif query.startswith("pushdown_empty"):
                # The caller asserts exact query text and bounds in the capture.
                # This accepts arbitrary lexical probes, not arbitrary PQ syntax.
                data = {"columns": [], "values": []}
            elif query.startswith("pushdown_capped"):
                # Model the server's order: window and query first, cap last.
                # There are five more rows than the default cap can return.
                match = re.fullmatch(
                    r"pushdown_capped(?:_reverse)?(?: \| filter \(\(id\) >= \((\d+)\)\))?"
                    r"(?: \| limit (\d+))?",
                    query,
                )
                if match is None:
                    self._respond(400, {"error": "unexpected capped query"})
                    return
                start = datetime.fromisoformat(
                    state["payload"]["startTime"]
                ).timestamp()
                end = datetime.fromisoformat(state["payload"]["endTime"]).timestamp()
                minimum = int(match[1]) if match[1] is not None else 0
                limit = int(match[2]) if match[2] is not None else 1000
                reverse = query.startswith("pushdown_capped_reverse")
                values = []
                for i in range(1005):
                    seconds = 1704067200 + (1004 - i if reverse else i)
                    if i >= minimum and start <= seconds < end:
                        values.append([seconds * 1_000_000_000, i])
                data = {
                    "columns": [{"name": "timestamp"}, {"name": "id"}],
                    "values": values[:limit],
                }
            elif query.startswith("| ") and scope.startswith("hybrid_numeric_"):
                data = _STATIC_RESPONSES[
                    "select_floats"
                    if scope == "hybrid_numeric_floats"
                    else "select_basic"
                ]
            elif query.startswith("| ") and scope.startswith("hybrid_"):
                data = {
                    "columns": [
                        {"name": name}
                        for name in (
                            "timestamp",
                            "id",
                            "event.type",
                            "event.dns.request",
                            "key",
                            "flag",
                            "addr",
                            "items",
                            "event.extra",
                        )
                    ],
                    "values": [
                        [
                            1704067200000000000,
                            i,
                            "DNS Resolved",
                            f"host-{i}",
                            "id",
                            "true",
                            "192.0.2.1",
                            [1, {"nested": "false"}],
                            "kept",
                        ]
                        for i in range(
                            3
                            if scope in {"hybrid_discovery", "hybrid_partial"}
                            else 1005
                        )
                    ],
                }
                # Generated PQ inherits the native 1,000-row cap. LOG pages
                # can continue beyond it, even though every timestamp ties.
                if state["payload"]["queryType"] == "PQ":
                    data["values"] = data["values"][:1000]
            elif query.startswith("| ") and state["payload"].get("accountIds") == [
                "pushdown_empty"
            ]:
                # Exact query spelling is asserted by the wire-contract tests.
                data = {"columns": [], "values": []}
            elif query.startswith("| ") and state["payload"].get("accountIds") == [
                "pushdown_projection"
            ]:
                original = _STATIC_RESPONSES["pushdown_projection"]
                values = original["values"]
                if query == '| filter ((event.type) == ("DNS Resolved"))':
                    values = [row for row in values if row[2] == "DNS Resolved"]
                elif query == '| filter ((dataSource.name) == ("SentinelOne"))':
                    values = [row for row in values if row[4] == "SentinelOne"]
                elif query != "| filter (1 == 1)":
                    self._respond(400, {"error": "unexpected projection prefilter"})
                    return
                data = {**original, "values": values}
            elif query.startswith("| ") and state["payload"].get("accountIds") in (
                ["pushdown_semantics"],
                ["pushdown_booleans"],
            ):
                scope = state["payload"]["accountIds"][0]
                original = _STATIC_RESPONSES[scope]
                values = original["values"]
                if scope == "pushdown_semantics":
                    expected = "| filter ((n) == (42))"
                    if query in (expected, expected + " and (missing = *)"):
                        # Emulate loose coercion and a raw field omitted from
                        # the returned columns. Both need local rechecking.
                        values = [row for row in values if row[1] in (42, "42")]
                    elif query not in {"| limit 1000", "| filter (1 == 1)"}:
                        self._respond(400, {"error": "unexpected prefilter"})
                        return
                elif query == '| filter (((flag) == (true)) or ((flag) == ("true")))':
                    values = values[:2]
                elif query not in {"| limit 1000", "| filter (1 == 1)"}:
                    self._respond(400, {"error": "unexpected boolean prefilter"})
                    return
                data = {**original, "values": values}
            elif query in {
                "| filter (1 == 1)",
                "| filter ((event_id) == (42))",
                "| filter ((event_id) > (40))",
                '| filter ((message) starts_with:matchcase("login"))',
                "| limit 1",
            }:
                original = _STATIC_RESPONSES["select_basic"]
                start = datetime.fromisoformat(
                    state["payload"]["startTime"]
                ).timestamp()
                end = datetime.fromisoformat(state["payload"]["endTime"]).timestamp()
                values = [
                    row for row in original["values"] if start <= row[0] / 1e9 < end
                ]
                if query in {
                    "| filter ((event_id) == (42))",
                    '| filter ((message) starts_with:matchcase("login"))',
                }:
                    values = [row for row in values if row[1] == 42]
                if query == "| limit 1":
                    values = values[:1]
                data = {**original, "values": values}
            else:
                data = _STATIC_RESPONSES.get(query)
            if data is None:
                self._respond(400, {"error": f"unknown query: {query!r}"})
                return
            if isinstance(data, str):
                # Keep malformed JSON fixtures byte-for-byte to retain their
                # parser diagnostics while exercising cleanup after a poll.
                self._respond(200, data)
                return
            if state["payload"]["queryType"] == "LOG":
                data = _log_page(data, state["payload"])
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
                self._respond(
                    200, {"stepsCompleted": step, "stepsTotal": total, "data": data}
                )
                return
            if generated:
                data = _project(data, projection)
            data = dict(data)
            data["columns"] = [
                dict(column, cellType="UNKNOWN", decimalPlaces=0)
                for column in data["columns"]
            ]
            data["matchCount"] = len(data["values"])
            if scope == "hybrid_partial":
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
                _request_query(state["payload"]) == "select_delete_retry"
                and state["deletes"] == 1
            ):
                capture(method="DELETE", id=qid, status=503)
                self._respond(503, {"error": "temporarily unavailable"})
                return
            state["deleted"] = True
            code = 404 if _request_query(state["payload"]) == "select_expired" else 200
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

    def _assert_test(
        *, assertions: SentinelOneAssertions | dict[str, Any], **_: object
    ) -> None:
        if isinstance(assertions, dict):
            assertions = SentinelOneAssertions(**assertions)
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
