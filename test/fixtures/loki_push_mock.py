"""Mock Grafana Loki Push API receiver fixture."""

from __future__ import annotations

import gzip
import json
import re
import shutil
import tempfile
import threading
import time
from dataclasses import dataclass, field as dataclass_field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture

_LABEL_NAME_RE = re.compile(r"^[a-zA-Z_][a-zA-Z0-9_]*$")


def _read_varint(buf: bytes, pos: int) -> tuple[int, int]:
    result = 0
    shift = 0
    while True:
        byte = buf[pos]
        pos += 1
        result |= (byte & 0x7F) << shift
        if byte < 0x80:
            return result, pos
        shift += 7


def _decode_int64(value: int) -> int:
    if value >= 1 << 63:
        return value - (1 << 64)
    return value


def _decode_int32(value: int) -> int:
    value &= 0xFFFFFFFF
    if value >= 1 << 31:
        return value - (1 << 32)
    return value


def _snappy_decompress(buf: bytes) -> bytes:
    expected_size, pos = _read_varint(buf, 0)
    out = bytearray()
    while pos < len(buf):
        tag = buf[pos]
        pos += 1
        tag_type = tag & 0x03
        if tag_type == 0:
            length = tag >> 2
            if length < 60:
                length += 1
            else:
                size_len = length - 59
                length = int.from_bytes(buf[pos : pos + size_len], "little") + 1
                pos += size_len
            out.extend(buf[pos : pos + length])
            pos += length
            continue
        if tag_type == 1:
            length = 4 + ((tag >> 2) & 0x07)
            offset = ((tag & 0xE0) << 3) | buf[pos]
            pos += 1
        elif tag_type == 2:
            length = 1 + (tag >> 2)
            offset = int.from_bytes(buf[pos : pos + 2], "little")
            pos += 2
        else:
            length = 1 + (tag >> 2)
            offset = int.from_bytes(buf[pos : pos + 4], "little")
            pos += 4
        for _ in range(length):
            out.append(out[-offset])
    if len(out) != expected_size:
        raise ValueError(
            f"snappy size mismatch: expected {expected_size}, got {len(out)}"
        )
    return bytes(out)


def _fields(buf: bytes):
    pos = 0
    while pos < len(buf):
        key, pos = _read_varint(buf, pos)
        number = key >> 3
        wire_type = key & 0x07
        if wire_type == 0:
            value, pos = _read_varint(buf, pos)
        elif wire_type == 1:
            value = buf[pos : pos + 8]
            pos += 8
        elif wire_type == 2:
            length, pos = _read_varint(buf, pos)
            value = buf[pos : pos + length]
            pos += length
        elif wire_type == 5:
            value = buf[pos : pos + 4]
            pos += 4
        else:
            raise ValueError(f"unsupported wire type {wire_type}")
        yield number, wire_type, value


def _parse_timestamp(buf: bytes) -> int:
    seconds = 0
    nanos = 0
    for number, _wire_type, field in _fields(buf):
        if number == 1:
            seconds = _decode_int64(field)
        elif number == 2:
            nanos = _decode_int32(field)
    if not (0 <= nanos <= 999_999_999):
        raise ValueError(f"invalid protobuf timestamp nanos: {nanos}")
    return seconds * 1_000_000_000 + nanos


def _parse_label_pair(buf: bytes) -> tuple[str, str]:
    name = ""
    value = ""
    for number, _wire_type, field in _fields(buf):
        if number == 1:
            name = field.decode("utf-8")
        elif number == 2:
            value = field.decode("utf-8")
    return name, value


def _parse_entry(buf: bytes) -> dict[str, object]:
    timestamp_ns = 0
    line = ""
    structured_metadata: dict[str, str] = {}
    for number, _wire_type, field in _fields(buf):
        if number == 1:
            timestamp_ns = _parse_timestamp(field)
        elif number == 2:
            line = field.decode("utf-8")
        elif number == 3:
            key, value = _parse_label_pair(field)
            structured_metadata[key] = value
    result: dict[str, object] = {
        "timestamp_ns": timestamp_ns,
        "line": line,
    }
    if structured_metadata:
        result["structured_metadata"] = structured_metadata
    return result


def _parse_push_request_proto(buf: bytes) -> list[dict[str, object]]:
    streams: list[dict[str, object]] = []
    for number, _wire_type, field in _fields(buf):
        if number == 1:
            labels = ""
            entries: list[dict[str, object]] = []
            for s_num, _s_wire, s_field in _fields(field):
                if s_num == 1:
                    labels = s_field.decode("utf-8")
                elif s_num == 2:
                    entries.append(_parse_entry(s_field))
            streams.append({"labels": labels, "entries": entries})
    return streams


@dataclass
class MockState:
    path: Path
    lock: threading.Lock = dataclass_field(default_factory=threading.Lock)
    retry_counts: dict[str, int] = dataclass_field(default_factory=dict)
    active_by_tenant: dict[str | None, int] = dataclass_field(default_factory=dict)
    barrier: threading.Barrier = dataclass_field(
        default_factory=lambda: threading.Barrier(2)
    )
    window_start_ns: int = dataclass_field(default_factory=time.time_ns)

    def record(self, item: dict[str, object]) -> None:
        with self.lock:
            with self.path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(item, sort_keys=True) + "\n")

    def take_and_reset(self) -> tuple[list[dict[str, object]], int, int]:
        now_ns = time.time_ns()
        with self.lock:
            start_ns = self.window_start_ns
            text = self.path.read_text(encoding="utf-8")
            self.path.write_text("", encoding="utf-8")
            self.retry_counts.clear()
            self.active_by_tenant.clear()
            self.barrier = threading.Barrier(2)
            self.window_start_ns = time.time_ns()
        return (
            [json.loads(line) for line in text.splitlines() if line],
            start_ns,
            now_ns,
        )

    def bump_retry(self, key: str) -> int:
        with self.lock:
            count = self.retry_counts.get(key, 0) + 1
            self.retry_counts[key] = count
            return count

    def enter_tenant(self, tenant: str | None) -> int:
        with self.lock:
            active = self.active_by_tenant.get(tenant, 0) + 1
            self.active_by_tenant[tenant] = active
            return active

    def leave_tenant(self, tenant: str | None) -> None:
        with self.lock:
            active = self.active_by_tenant.get(tenant, 0) - 1
            if active <= 0:
                self.active_by_tenant.pop(tenant, None)
            else:
                self.active_by_tenant[tenant] = active


@dataclass(frozen=True)
class LokiPushMockAssertions:
    requests: dict[str, list[dict[str, object]]] = dataclass_field(default_factory=dict)
    unordered_tests: list[str] = dataclass_field(default_factory=list)


def _make_handler(state: MockState):
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            content_type = self.headers.get("Content-Type")
            content_encoding = self.headers.get("Content-Encoding")
            tenant = self.headers.get("X-Scope-OrgID")
            extra_headers: dict[str, str] = {}
            for name in ("Authorization", "X-Custom"):
                val = self.headers.get(name)
                if val is not None:
                    extra_headers[name] = val

            # Simulate transient 429 rate-limiting on the first two attempts.
            if self.path == "/retry-429/loki/api/v1/push":
                attempt = state.bump_retry("429")
                if attempt <= 2:
                    msg = b"ingestion rate limit exceeded"
                    state.record(
                        {
                            "path": self.path,
                            "status": 429,
                            "attempt": attempt,
                        }
                    )
                    self.send_response(HTTPStatus.TOO_MANY_REQUESTS)
                    self.send_header("Retry-After", "0")
                    self.send_header("Content-Type", "text/plain")
                    self.send_header("Content-Length", str(len(msg)))
                    self.end_headers()
                    self.wfile.write(msg)
                    return

            # Simulate misrouted HTTP 200 OK (must be rejected; Loki returns 204).
            if self.path == "/ok-200/loki/api/v1/push":
                msg = b"<html>login page</html>"
                state.record(
                    {
                        "path": self.path,
                        "status": 200,
                    }
                )
                self.send_response(HTTPStatus.OK)
                self.send_header("Content-Type", "text/html")
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                try:
                    self.wfile.write(msg)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return

            # Simulate HTTP 260 that sends 1 body byte and stalls past timeout.
            if self.path == "/stalled-260/loki/api/v1/push":
                state.record(
                    {
                        "path": self.path,
                        "status": 260,
                        "stalled": "1-byte",
                    }
                )
                self.send_response(260, "Ingestion Blocked")
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", "100")
                self.end_headers()
                try:
                    self.wfile.write(b"x")
                    self.wfile.flush()
                    time.sleep(0.35)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                return

            # Simulate HTTP 400 that sends 0 body bytes and stalls past timeout.
            if self.path == "/stalled-400/loki/api/v1/push":
                state.record(
                    {
                        "path": self.path,
                        "status": 400,
                        "stalled": "0-bytes",
                    }
                )
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", "100")
                self.end_headers()
                try:
                    self.wfile.flush()
                    time.sleep(0.35)
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                return

            # Simulate Loki blocked ingestion status 260 (must fail without retry).
            if self.path == "/blocked-260/loki/api/v1/push":
                msg = b"ingestion blocked for tenant"
                state.record(
                    {
                        "path": self.path,
                        "status": 260,
                    }
                )
                self.send_response(260, "Ingestion Blocked")
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                self.wfile.write(msg)
                return

            # Simulate permanent 400 rejection echoing sensitive headers in the
            # response body (must not retry and must not leak body/secrets).
            if self.path == "/bad-request/loki/api/v1/push":
                auth = self.headers.get("Authorization", "")
                msg = (
                    f"entry too far behind auth={auth} tenant={tenant or ''}"
                ).encode("utf-8")
                state.record(
                    {
                        "path": self.path,
                        "status": 400,
                    }
                )
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                try:
                    self.wfile.write(msg)
                except BrokenPipeError:
                    pass
                return

            # Simulate oversized (>64 KiB) 400 error body containing a canary.
            if self.path == "/huge-error/loki/api/v1/push":
                auth = self.headers.get("Authorization", "")
                msg = (f"canary={auth} " + ("x" * 131072)).encode("utf-8")
                state.record(
                    {
                        "path": self.path,
                        "status": 400,
                        "huge": True,
                    }
                )
                self.send_response(HTTPStatus.BAD_REQUEST)
                self.send_header("Content-Type", "text/plain")
                self.send_header("Content-Length", str(len(msg)))
                self.end_headers()
                try:
                    self.wfile.write(msg)
                except (BrokenPipeError, ConnectionResetError):
                    pass
                return

            # Prove cross-tenant requests genuinely overlap concurrently via a
            # 2-party barrier.
            if self.path == "/barrier-2/loki/api/v1/push":
                try:
                    state.barrier.wait(timeout=5.0)
                except threading.BrokenBarrierError:
                    self.send_error(
                        HTTPStatus.INTERNAL_SERVER_ERROR,
                        "concurrent barrier timed out",
                    )
                    return

            # Prove same-tenant requests are strictly serialized even when the
            # first batch sleeps and retries after a 429.
            if self.path == "/hold-same-tenant/loki/api/v1/push":
                active = state.enter_tenant(tenant)
                try:
                    if active > 1:
                        state.record(
                            {
                                "path": self.path,
                                "status": 409,
                                "concurrent_same_tenant": active,
                            }
                        )
                        self.send_error(
                            HTTPStatus.CONFLICT,
                            "concurrent same-tenant request observed",
                        )
                        return
                    attempt = state.bump_retry("hold-same-tenant")
                    if attempt == 1:
                        time.sleep(0.05)
                        msg = b"slow rate limit"
                        state.record(
                            {
                                "path": self.path,
                                "status": 429,
                                "attempt": 1,
                            }
                        )
                        self.send_response(HTTPStatus.TOO_MANY_REQUESTS)
                        self.send_header("Retry-After", "0")
                        self.send_header("Content-Type", "text/plain")
                        self.send_header("Content-Length", str(len(msg)))
                        self.end_headers()
                        self.wfile.write(msg)
                        return
                    if attempt == 2:
                        time.sleep(0.05)
                    record = self._decode_push(
                        body, content_type, content_encoding, tenant, extra_headers
                    )
                    if record is None:
                        return
                    state.record(record)
                    self.send_response(HTTPStatus.NO_CONTENT)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                finally:
                    state.leave_tenant(tenant)

            record = self._decode_push(
                body, content_type, content_encoding, tenant, extra_headers
            )
            if record is None:
                return
            state.record(record)
            self.send_response(HTTPStatus.NO_CONTENT)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def _decode_push(
            self,
            body: bytes,
            content_type: str | None,
            content_encoding: str | None,
            tenant: str | None,
            extra_headers: dict[str, str],
        ) -> dict[str, object] | None:
            record: dict[str, object] = {
                "path": self.path,
                "content_type": content_type,
                "content_encoding": content_encoding,
                "tenant": tenant,
            }
            if extra_headers:
                record["headers"] = extra_headers

            if content_type == "application/x-protobuf":
                if content_encoding is not None:
                    self.send_error(
                        HTTPStatus.BAD_REQUEST,
                        f"unexpected Content-Encoding for protobuf: {content_encoding}",
                    )
                    return None
                decoded = _snappy_decompress(body)
                record["streams"] = _parse_push_request_proto(decoded)
            elif content_type == "application/json":
                if content_encoding == "gzip":
                    raw_json = gzip.decompress(body)
                elif content_encoding is None:
                    raw_json = body
                else:
                    self.send_error(
                        HTTPStatus.UNSUPPORTED_MEDIA_TYPE,
                        f"unsupported Content-Encoding: {content_encoding}",
                    )
                    return None
                payload = json.loads(raw_json.decode("utf-8"))
                # Enforce Loki JSON push receiver invariants: string
                # nanosecond timestamps and valid Prometheus/Loki label names.
                for stream in payload.get("streams", []):
                    for key in stream.get("stream", {}):
                        if not _LABEL_NAME_RE.match(key):
                            self.send_error(
                                HTTPStatus.BAD_REQUEST,
                                f"invalid label name: {key}",
                            )
                            return None
                    for item in stream.get("values", []):
                        if not isinstance(item[0], str):
                            self.send_error(
                                HTTPStatus.BAD_REQUEST,
                                "JSON timestamp must be a string",
                            )
                            return None
                record["streams"] = payload["streams"]
            else:
                self.send_error(
                    HTTPStatus.BAD_REQUEST,
                    f"unsupported Content-Type: {content_type}",
                )
                return None
            return record

        def log_message(self, *_: object) -> None:
            return

    return Handler


@fixture(
    name="loki_push_mock",
    assertions=LokiPushMockAssertions,
)
def run() -> FixtureHandle:
    temp_dir = Path(tempfile.mkdtemp(prefix="loki-push-mock-"))
    state = MockState(temp_dir / "requests.jsonl")
    state.path.touch()
    server = ThreadingHTTPServer(("127.0.0.1", 0), _make_handler(state))
    worker = threading.Thread(target=server.serve_forever, daemon=True)
    worker.start()
    port = server.server_address[1]

    def teardown() -> None:
        server.shutdown()
        worker.join()
        server.server_close()
        shutil.rmtree(temp_dir, ignore_errors=True)

    def _normalize_recent(
        obs: list[dict[str, object]],
        exp: list[dict[str, object]],
        start_ns: int,
        end_ns: int,
    ) -> None:
        if len(obs) != len(exp):
            return
        for obs_req, exp_req in zip(obs, exp):
            obs_streams = obs_req.get("streams")
            exp_streams = exp_req.get("streams")
            if not (isinstance(obs_streams, list) and isinstance(exp_streams, list)):
                continue
            for obs_s, exp_s in zip(obs_streams, exp_streams):
                obs_entries = obs_s.get("entries")
                exp_entries = exp_s.get("entries")
                if not (
                    isinstance(obs_entries, list) and isinstance(exp_entries, list)
                ):
                    continue
                for obs_e, exp_e in zip(obs_entries, exp_entries):
                    if exp_e.get("timestamp_ns") == "<recent>":
                        ts = obs_e.get("timestamp_ns")
                        if isinstance(ts, int) and start_ns <= ts <= end_ns:
                            obs_e["timestamp_ns"] = "<recent>"

    def assert_test(
        *, test: Path, assertions: LokiPushMockAssertions, **_: Any
    ) -> None:
        observed, start_ns, end_ns = state.take_and_reset()
        expected = assertions.requests.get(test.name)
        if expected is None:
            return
        if test.name in assertions.unordered_tests:
            observed = sorted(observed, key=lambda x: json.dumps(x, sort_keys=True))
            expected = sorted(expected, key=lambda x: json.dumps(x, sort_keys=True))
        _normalize_recent(observed, expected, start_ns, end_ns)
        if observed != expected:
            expected_json = json.dumps(expected, indent=2, sort_keys=True)
            actual = json.dumps(observed, indent=2, sort_keys=True)
            raise AssertionError(
                "Loki push capture mismatch\n"
                f"expected:\n{expected_json}\nactual:\n{actual}"
            )

    return FixtureHandle(
        env={
            "LOKI_BASE_URL": f"http://127.0.0.1:{port}",
            "LOKI_PUSH_URL": f"http://127.0.0.1:{port}/loki/api/v1/push",
            "LOKI_CUSTOM_URL": f"http://127.0.0.1:{port}/custom/push",
            "LOKI_RETRY_429_URL": f"http://127.0.0.1:{port}/retry-429/loki/api/v1/push",
            "LOKI_BAD_REQUEST_URL": f"http://127.0.0.1:{port}/bad-request/loki/api/v1/push",
            "LOKI_BLOCKED_260_URL": f"http://127.0.0.1:{port}/blocked-260/loki/api/v1/push",
            "LOKI_OK_200_URL": f"http://127.0.0.1:{port}/ok-200/loki/api/v1/push",
            "LOKI_STALLED_260_URL": f"http://127.0.0.1:{port}/stalled-260/loki/api/v1/push",
            "LOKI_STALLED_400_URL": f"http://127.0.0.1:{port}/stalled-400/loki/api/v1/push",
            "LOKI_HUGE_ERROR_URL": f"http://127.0.0.1:{port}/huge-error/loki/api/v1/push",
            "LOKI_BARRIER_2_URL": f"http://127.0.0.1:{port}/barrier-2/loki/api/v1/push",
            "LOKI_HOLD_SAME_TENANT_URL": f"http://127.0.0.1:{port}/hold-same-tenant/loki/api/v1/push",
        },
        teardown=teardown,
        hooks={"assert_test": assert_test},
    )
