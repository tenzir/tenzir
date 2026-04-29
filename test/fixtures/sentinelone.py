"""Mock SentinelOne Data Lake API server for integration tests."""

from __future__ import annotations

import gzip
import json
import os
import shutil
import ssl
import subprocess
import tempfile
import threading
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options

from ._utils import generate_self_signed_cert

_HOST = "127.0.0.1"
_EXPECTED_TOKEN = "test-token-s1-12345"


@dataclass(frozen=True)
class SentinelOneOptions:
    tls: bool = False


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


def _make_handler(capture_path: str) -> type[BaseHTTPRequestHandler]:
    lock = threading.Lock()

    class SentinelOneHandler(BaseHTTPRequestHandler):
        def log_message(self, fmt: str, *args: object) -> None:
            # Silence the default per-request log lines.
            pass

        def do_POST(self) -> None:
            if self.path not in {"/api/powerQuery", "/api/addEvents"}:
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


@fixture(name="sentinelone", options=SentinelOneOptions)
def sentinelone() -> Iterator[dict[str, str]]:
    """Start a mock SentinelOne Data Lake API server on a random port.

    Exports:
      S1_FIXTURE_URL   - base URL of the mock server
      S1_FIXTURE_TOKEN - bearer token expected by the server
      S1_FIXTURE_CAPTURE_FILE - JSONL file containing captured addEvents calls
      S1_FIXTURE_CAFILE - CA certificate path when tls=true
    """
    opts = current_options("sentinelone")
    fd, capture_path = tempfile.mkstemp(prefix="sentinelone-capture-", suffix=".jsonl")
    os.close(fd)
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
            raise FixtureUnavailable(f"openssl unavailable: {exc}") from exc
        tls_env = {
            "S1_FIXTURE_CAFILE": str(ca_path),
        }
    server = HTTPServer((_HOST, 0), _make_handler(capture_path))
    if opts.tls:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
        server.socket = context.wrap_socket(server.socket, server_side=True)
    port = server.server_address[1]
    scheme = "https" if opts.tls else "http"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        env = {
            "S1_FIXTURE_URL": f"{scheme}://{_HOST}:{port}",
            "S1_FIXTURE_TOKEN": _EXPECTED_TOKEN,
            "S1_FIXTURE_CAPTURE_FILE": capture_path,
        }
        env.update(tls_env)
        yield env
    finally:
        server.shutdown()
        thread.join(timeout=2)
        if os.path.exists(capture_path):
            os.remove(capture_path)
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)
