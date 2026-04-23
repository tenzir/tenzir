"""Mock SentinelOne Data Lake API server for integration tests."""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Iterator

from tenzir_test import fixture

_HOST = "127.0.0.1"
_EXPECTED_TOKEN = "test-token-s1-12345"

# Predefined columnar responses keyed by the `query` field in the POST body.
#
# The SentinelOne response format is:
#   { "columns": [{"name": "col"},...], "values": [[val,...],...]  }
#
# Special floating-point values that cannot be expressed in JSON are encoded
# as single-key objects: {"special": "NaN"}, {"special": "+infinity"}, etc.
_STATIC_RESPONSES: dict[str, object] = {
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


def _make_handler() -> type[BaseHTTPRequestHandler]:
    class SentinelOneHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: object) -> None:
            # Silence the default per-request log lines.
            pass

        def do_POST(self) -> None:
            if self.path != "/api/powerQuery":
                self._respond(404, {"error": f"unknown path: {self.path}"})
                return

            # Validate the bearer token exactly as the real API would.
            auth = self.headers.get("Authorization", "")
            if auth != f"Bearer {_EXPECTED_TOKEN}":
                self._respond(401, {"error": "unauthorized"})
                return

            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode()
            try:
                payload = json.loads(body)
            except json.JSONDecodeError as exc:
                self._respond(400, {"error": f"bad JSON: {exc}"})
                return

            query = payload.get("query", "")

            if query == "select_http_error":
                self._respond(500, {"error": "internal server error"})
                return

            # The echo-times query reads startTime/endTime from the request
            # body and returns them as data columns so the test can verify that
            # the operator correctly encodes the start/end arguments.
            if query == "select_echo_times":
                start_ns = payload.get("startTime")
                end_ns = payload.get("endTime")
                self._respond(
                    200,
                    {
                        "columns": [{"name": "start_ns"}, {"name": "end_ns"}],
                        "values": [[start_ns, end_ns]],
                    },
                )
                return

            if query not in _STATIC_RESPONSES:
                self._respond(400, {"error": f"unknown query: {query!r}"})
                return

            self._respond(200, _STATIC_RESPONSES[query])

        def _respond(self, code: int, obj: object) -> None:
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    return SentinelOneHandler


@fixture()
def sentinelone() -> Iterator[dict[str, str]]:
    """Start a mock SentinelOne Data Lake API server on a random port.

    Exports:
      S1_FIXTURE_URL   - base URL of the mock server (http://127.0.0.1:<port>)
      S1_FIXTURE_TOKEN - bearer token expected by the server
    """
    server = HTTPServer((_HOST, 0), _make_handler())
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield {
            "S1_FIXTURE_URL": f"http://{_HOST}:{port}",
            "S1_FIXTURE_TOKEN": _EXPECTED_TOKEN,
        }
    finally:
        server.shutdown()
        thread.join(timeout=2)
