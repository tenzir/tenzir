"""In-process mock Splunk S2S ("cooked mode") receiver.

Accepts S2S connections from `to_splunk mode="s2s"`, validates the 400-byte
connection signature, replies to the v3 capability frame with a control
message, and decodes every event frame into its key/value map. Runs entirely
in-process; no Splunk container required.

Environment variables yielded:
- SPLUNK_S2S_MOCK_URL: `<host>:<port>` endpoint of the mock receiver.

Assertions payload accepted under ``assertions.fixtures.splunk_s2s_mock``:
- count: optional number of decoded event frames expected.
- contains: substring or list of substrings expected across `_raw` values.
- events: list of key/value maps; the i-th map must be a subset of the i-th
  decoded event frame (wire keys, e.g. `_MetaData:Index`, `MetaData:Host`,
  `_meta`, `_time`, `_raw`).
"""

from __future__ import annotations

import socket
import struct
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture

from ._utils import find_free_port

_HOST = "127.0.0.1"
_SIGNATURE_SIZE = 400
_SIGNATURE_PREFIX = b"--splunk-cooked-mode-v"
_CONTROL_REPLY = "cap_response=success;v4=true;channel_limit=300;pl=7"
_ASSERTION_WAIT_TIMEOUT = 10.0
_ASSERTION_WAIT_INTERVAL = 0.05


@dataclass(frozen=True)
class SplunkS2sMockAssertions:
    count: int | None = None
    contains: list[str] = field(default_factory=list)
    events: list[dict[str, str]] = field(default_factory=list)

    def __post_init__(self) -> None:
        contains = self.contains
        if isinstance(contains, str):
            object.__setattr__(self, "contains", [contains])


@dataclass
class _State:
    lock: threading.Lock = field(default_factory=threading.Lock)
    events: list[dict[str, str]] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


def _encode_string(value: str) -> bytes:
    encoded = value.encode()
    return struct.pack("!I", len(encoded) + 1) + encoded + b"\x00"


def _control_reply_frame() -> bytes:
    payload = (
        struct.pack("!I", 1)
        + _encode_string("__s2s_control_msg")
        + _encode_string(_CONTROL_REPLY)
        + struct.pack("!I", 0)
        + _encode_string("_raw")
    )
    return struct.pack("!I", len(payload)) + payload


def _decode_frame(payload: bytes) -> dict[str, str]:
    (num_maps,) = struct.unpack_from("!I", payload, 0)
    pos = 4
    pairs: dict[str, str] = {}
    for _ in range(num_maps):
        (key_len,) = struct.unpack_from("!I", payload, pos)
        pos += 4
        key = payload[pos : pos + key_len - 1].decode("utf-8", "replace")
        pos += key_len
        (value_len,) = struct.unpack_from("!I", payload, pos)
        pos += 4
        value = payload[pos : pos + value_len - 1].decode("utf-8", "replace")
        pos += value_len
        pairs[key] = value
    (footer_zero,) = struct.unpack_from("!I", payload, pos)
    if footer_zero != 0:
        raise ValueError("missing dummy-zero footer")
    pos += 4
    (footer_len,) = struct.unpack_from("!I", payload, pos)
    footer = payload[pos + 4 : pos + 4 + footer_len - 1]
    if footer != b"_raw":
        raise ValueError(f"unexpected frame footer: {footer!r}")
    return pairs


def _handle_connection(
    conn: socket.socket, state: _State, stop_event: threading.Event
) -> None:
    buffer = bytearray()

    def read_exact(size: int) -> bytes | None:
        while len(buffer) < size:
            if stop_event.is_set():
                return None
            try:
                data = conn.recv(65536)
            except socket.timeout:
                continue
            except OSError:
                return None
            if not data:
                return None
            buffer.extend(data)
        out = bytes(buffer[:size])
        del buffer[:size]
        return out

    with conn:
        conn.settimeout(0.2)
        signature = read_exact(_SIGNATURE_SIZE)
        if signature is None or not signature.startswith(_SIGNATURE_PREFIX):
            with state.lock:
                state.errors.append(f"invalid signature: {signature!r:.64}")
            return
        while not stop_event.is_set():
            header = read_exact(4)
            if header is None:
                return
            (msg_size,) = struct.unpack("!I", header)
            payload = read_exact(msg_size)
            if payload is None:
                with state.lock:
                    state.errors.append("connection closed mid-frame")
                return
            try:
                pairs = _decode_frame(payload)
            except (ValueError, struct.error) as exc:
                with state.lock:
                    state.errors.append(f"malformed frame: {exc}")
                return
            if "__s2s_capabilities" in pairs:
                conn.sendall(_control_reply_frame())
                continue
            with state.lock:
                state.events.append(pairs)


def _run_server(
    server: socket.socket, state: _State, stop_event: threading.Event
) -> None:
    with server:
        server.settimeout(0.1)
        while not stop_event.is_set():
            try:
                conn, _ = server.accept()
            except socket.timeout:
                continue
            except OSError:
                return
            handler = threading.Thread(
                target=_handle_connection,
                args=(conn, state, stop_event),
                daemon=True,
            )
            handler.start()


@fixture(assertions=SplunkS2sMockAssertions)
def splunk_s2s_mock() -> FixtureHandle:
    port = find_free_port()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((_HOST, port))
    server.listen(8)
    state = _State()
    stop_event = threading.Event()
    worker = threading.Thread(
        target=_run_server, args=(server, state, stop_event), daemon=True
    )
    worker.start()

    def _assert_test(
        *,
        test: Path,
        assertions: SplunkS2sMockAssertions | dict[str, Any],
        **_: Any,
    ) -> None:
        if isinstance(assertions, dict):
            assertions = SplunkS2sMockAssertions(**assertions)
        deadline = time.monotonic() + _ASSERTION_WAIT_TIMEOUT
        while True:
            with state.lock:
                events = [dict(event) for event in state.events]
                errors = list(state.errors)
            if errors:
                raise AssertionError(f"{test.name}: mock receiver errors: {errors}")
            raw_values = [event.get("_raw", "") for event in events]
            failure: str | None = None
            if assertions.count is not None and len(events) != assertions.count:
                failure = (
                    f"expected {assertions.count} event frame(s), "
                    f"got {len(events)}: {events}"
                )
            elif not all(
                any(needle in raw for raw in raw_values)
                for needle in assertions.contains
            ):
                failure = (
                    f"expected `_raw` values to contain {assertions.contains}, "
                    f"got {raw_values}"
                )
            elif len(events) < len(assertions.events):
                failure = (
                    f"expected at least {len(assertions.events)} event frame(s), "
                    f"got {len(events)}: {events}"
                )
            else:
                for index, expected in enumerate(assertions.events):
                    actual = events[index]
                    mismatched = {
                        key: actual.get(key)
                        for key, value in expected.items()
                        if actual.get(key) != value
                    }
                    if mismatched:
                        failure = (
                            f"event {index} mismatch: expected {expected}, got {actual}"
                        )
                        break
            if failure is None:
                return
            if time.monotonic() >= deadline:
                raise AssertionError(f"{test.name}: {failure}")
            time.sleep(_ASSERTION_WAIT_INTERVAL)

    def _teardown() -> None:
        stop_event.set()
        worker.join(timeout=2)

    return FixtureHandle(
        env={"SPLUNK_S2S_MOCK_URL": f"{_HOST}:{port}"},
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
