from __future__ import annotations

import os
import shutil
import socket
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import current_options

_CLIENT_RETRY_DELAY = 0.01
_ASSERTION_WAIT_TIMEOUT = 2.0
_ASSERTION_WAIT_INTERVAL = 0.01


@dataclass(frozen=True)
class UdsOptions:
    client_send_delay: float = 0.0
    client_split_payload_at: int | None = None
    client_split_send_delay: float = 0.0
    inter_connection_delay: float = 0.0
    mode: str = "client"  # "client" sends data; "server" receives data
    payload: str = "foo\n"
    payloads: list[str] | None = None


@dataclass(frozen=True)
class UdsAssertions:
    server_received_contains: str | None = None
    server_received_equals_hex: str | None = None
    client_received_contains: str | None = None
    server_sent_contains: str | None = None


@dataclass
class _UdsState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    server_received: bytearray = field(default_factory=bytearray)
    client_received: bytearray = field(default_factory=bytearray)
    server_sent: bytearray = field(default_factory=bytearray)
    server_capture_complete: bool = False


def _bytes_from_hex(value: str, field: str) -> bytes:
    try:
        return bytes.fromhex(value)
    except ValueError as exc:
        raise TypeError(f"uds fixture assertion `{field}` must be hexadecimal") from exc


@fixture(options=UdsOptions, assertions=UdsAssertions)
def uds() -> FixtureHandle:
    opts = current_options("uds")
    if opts.mode not in {"client", "server"}:
        raise RuntimeError("uds fixture option `mode` must be one of: client, server")
    temp_dir = Path(tempfile.mkdtemp(prefix="uds-"))
    socket_path = temp_dir / "test.sock"
    stop_event = threading.Event()
    state = _UdsState()
    payload = opts.payload.encode()
    payloads = [entry.encode() for entry in opts.payloads] if opts.payloads else None
    server_capture_path: str | None = None
    client_capture_path: str | None = None
    if opts.mode == "server":
        fd, server_capture_path = tempfile.mkstemp(prefix="uds-server-", suffix=".log")
        os.close(fd)
    else:
        fd, client_capture_path = tempfile.mkstemp(prefix="uds-client-", suffix=".log")
        os.close(fd)

    if opts.mode == "client":
        worker = threading.Thread(
            target=_run_client_worker,
            kwargs={
                "socket_path": socket_path,
                "stop_event": stop_event,
                "state": state,
                "payload": payload,
                "payloads": payloads,
                "capture_path": client_capture_path,
                "send_delay": opts.client_send_delay,
                "split_payload_at": opts.client_split_payload_at,
                "split_send_delay": opts.client_split_send_delay,
                "inter_connection_delay": opts.inter_connection_delay,
            },
            daemon=True,
        )
    else:
        worker = threading.Thread(
            target=_run_server_worker,
            kwargs={
                "socket_path": socket_path,
                "stop_event": stop_event,
                "state": state,
                "payload": payload,
                "capture_path": server_capture_path,
            },
            daemon=True,
        )
    worker.start()
    if opts.mode == "server":
        deadline = time.monotonic() + 2
        while not socket_path.exists():
            if time.monotonic() >= deadline:
                raise RuntimeError("uds fixture server did not create socket path")
            time.sleep(_CLIENT_RETRY_DELAY)

    env: dict[str, str] = {"UDS_PATH": str(socket_path)}
    if server_capture_path is not None:
        env["UDS_FILE"] = server_capture_path
    if client_capture_path is not None:
        env["UDS_CLIENT_FILE"] = client_capture_path

    def _assert_test(
        *,
        test: Path,
        assertions: UdsAssertions | dict[str, Any],
        **_: Any,
    ) -> None:
        if isinstance(assertions, dict):
            assertions = UdsAssertions(**assertions)
        expected_server_received: bytes | None = None
        if assertions.server_received_equals_hex is not None:
            expected_server_received = _bytes_from_hex(
                assertions.server_received_equals_hex,
                "server_received_equals_hex",
            )
        deadline = time.monotonic() + _ASSERTION_WAIT_TIMEOUT
        while True:
            with state.lock:
                server_received_bytes = bytes(state.server_received)
                server_received = server_received_bytes.decode(
                    "utf-8", errors="replace"
                )
                client_received = state.client_received.decode(
                    "utf-8", errors="replace"
                )
                server_sent = state.server_sent.decode("utf-8", errors="replace")
                server_capture_complete = state.server_capture_complete
            missing_message = None
            if (
                assertions.server_received_contains is not None
                and assertions.server_received_contains not in server_received
            ):
                missing_message = (
                    f"{test.name}: expected fixture server capture to contain "
                    f"{assertions.server_received_contains!r}, got {server_received!r}"
                )
            elif expected_server_received is not None:
                if not server_capture_complete:
                    missing_message = (
                        f"{test.name}: expected fixture server capture bytes "
                        f"{expected_server_received.hex()}, got "
                        f"{server_received_bytes.hex()} so far"
                    )
                elif server_received_bytes != expected_server_received:
                    missing_message = (
                        f"{test.name}: expected fixture server capture bytes "
                        f"{expected_server_received.hex()}, got "
                        f"{server_received_bytes.hex()}"
                    )
            elif (
                assertions.client_received_contains is not None
                and assertions.client_received_contains not in client_received
            ):
                missing_message = (
                    f"{test.name}: expected fixture client capture to contain "
                    f"{assertions.client_received_contains!r}, got {client_received!r}"
                )
            elif (
                assertions.server_sent_contains is not None
                and assertions.server_sent_contains not in server_sent
            ):
                missing_message = (
                    f"{test.name}: expected fixture server send buffer to contain "
                    f"{assertions.server_sent_contains!r}, got {server_sent!r}"
                )
            if missing_message is None:
                return
            if time.monotonic() >= deadline:
                raise AssertionError(missing_message)
            time.sleep(_ASSERTION_WAIT_INTERVAL)

    def _teardown() -> None:
        stop_event.set()
        worker.join(timeout=2)
        if worker.is_alive():
            raise RuntimeError("uds fixture worker did not stop within 2 seconds")
        if server_capture_path is not None and os.path.exists(server_capture_path):
            os.remove(server_capture_path)
        if client_capture_path is not None and os.path.exists(client_capture_path):
            os.remove(client_capture_path)
        shutil.rmtree(temp_dir, ignore_errors=True)

    return FixtureHandle(
        env=env,
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )


def _run_client_worker(
    socket_path: Path,
    stop_event: threading.Event,
    state: _UdsState,
    payload: bytes,
    payloads: list[bytes] | None,
    capture_path: str | None,
    send_delay: float,
    split_payload_at: int | None,
    split_send_delay: float,
    inter_connection_delay: float,
) -> None:
    next_payloads = iter(payloads) if payloads is not None else None
    current_payload: bytes | None = None
    while not stop_event.is_set():
        if next_payloads is None:
            current_payload = payload
        elif current_payload is None:
            try:
                current_payload = next(next_payloads)
            except StopIteration:
                return
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(1)
            sock.connect(str(socket_path))
        except OSError:
            time.sleep(_CLIENT_RETRY_DELAY)
            continue
        try:
            with sock:
                sock.settimeout(0.2)
                if send_delay > 0:
                    stop_event.wait(send_delay)
                if current_payload:
                    if split_payload_at is None:
                        sock.sendall(current_payload)
                    else:
                        sock.sendall(current_payload[:split_payload_at])
                        if split_send_delay > 0:
                            stop_event.wait(split_send_delay)
                        sock.sendall(current_payload[split_payload_at:])
                try:
                    sock.shutdown(socket.SHUT_WR)
                except OSError:
                    pass
                idle_deadline = time.monotonic() + 2
                with (
                    open(capture_path, "ab") if capture_path else open(os.devnull, "wb")
                ) as capture:
                    while not stop_event.is_set():
                        try:
                            chunk = sock.recv(4096)
                        except socket.timeout:
                            if time.monotonic() >= idle_deadline:
                                break
                            continue
                        except OSError:
                            break
                        if not chunk:
                            break
                        capture.write(chunk)
                        capture.flush()
                        with state.lock:
                            state.client_received.extend(chunk)
                        idle_deadline = time.monotonic() + 2
        except OSError:
            pass
        current_payload = None
        if inter_connection_delay > 0:
            stop_event.wait(inter_connection_delay)
        time.sleep(_CLIENT_RETRY_DELAY)


def _run_server_worker(
    socket_path: Path,
    stop_event: threading.Event,
    state: _UdsState,
    payload: bytes,
    capture_path: str | None,
) -> None:
    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as server:
        try:
            server.bind(str(socket_path))
        except OSError:
            return
        server.listen(1)
        server.settimeout(0.1)
        while not stop_event.is_set():
            try:
                conn, _ = server.accept()
            except socket.timeout:
                continue
            with state.lock:
                state.server_capture_complete = False
            try:
                with conn:
                    conn.settimeout(0.2)
                    if payload:
                        conn.sendall(payload)
                        with state.lock:
                            state.server_sent.extend(payload)
                    idle_deadline = time.monotonic() + 2
                    with (
                        open(capture_path, "ab")
                        if capture_path
                        else open(os.devnull, "wb")
                    ) as capture:
                        while not stop_event.is_set():
                            try:
                                data = conn.recv(4096)
                            except socket.timeout:
                                if time.monotonic() >= idle_deadline:
                                    break
                                continue
                            except OSError:
                                break
                            if not data:
                                break
                            capture.write(data)
                            capture.flush()
                            with state.lock:
                                state.server_received.extend(data)
                            idle_deadline = time.monotonic() + 2
            except OSError:
                pass
            finally:
                with state.lock:
                    state.server_capture_complete = True
