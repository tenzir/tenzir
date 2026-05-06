from __future__ import annotations

import os
import shutil
import socket
import ssl
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import current_options

from ._utils import find_free_port, generate_self_signed_cert

_HOST = "127.0.0.1"
_COMMON_NAME = "tenzir-node.example.org"
_CLIENT_RETRY_DELAY = 0.01
_ASSERTION_WAIT_TIMEOUT = 2.0
_ASSERTION_WAIT_INTERVAL = 0.01


@dataclass(frozen=True)
class TcpOptions:
    tls: bool = False
    certs: bool = False
    invalid_tls_handshake_first: bool = False
    client_send_delay: float = 0.0
    client_split_payload_at: int | None = None
    client_split_send_delay: float = 0.0
    inter_connection_delay: float = 0.0
    mode: str = "client"  # "client" sends data; "server" receives data
    payload: str = "foo\n"
    payloads: list[str] | None = None


@dataclass(frozen=True)
class TcpAssertions:
    server_received_contains: str | None = None
    client_received_contains: str | None = None
    server_sent_contains: str | None = None


@dataclass
class _TcpState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    server_received: bytearray = field(default_factory=bytearray)
    client_received: bytearray = field(default_factory=bytearray)
    server_sent: bytearray = field(default_factory=bytearray)


@fixture(options=TcpOptions, assertions=TcpAssertions)
def tcp() -> FixtureHandle:
    opts = current_options("tcp")
    if opts.mode not in {"client", "server"}:
        raise RuntimeError("tcp fixture option `mode` must be one of: client, server")
    port = find_free_port()
    endpoint = f"{_HOST}:{port}"
    stop_event = threading.Event()
    state = _TcpState()
    payload = opts.payload.encode()
    payloads = [entry.encode() for entry in opts.payloads] if opts.payloads else None
    temp_dir = Path(tempfile.mkdtemp(prefix="tcp-")) if opts.tls or opts.certs else None
    cert_path: Path | None = None
    key_path: Path | None = None
    ca_path: Path | None = None
    cert_and_key_path: Path | None = None
    if opts.tls or opts.certs:
        assert temp_dir is not None
        cert_path, key_path, ca_path, cert_and_key_path = generate_self_signed_cert(
            temp_dir, _COMMON_NAME
        )
    server_capture_path: str | None = None
    client_capture_path: str | None = None
    if opts.mode == "server":
        fd, server_capture_path = tempfile.mkstemp(prefix="tcp-server-", suffix=".log")
        os.close(fd)
    else:
        fd, client_capture_path = tempfile.mkstemp(prefix="tcp-client-", suffix=".log")
        os.close(fd)

    if opts.mode == "client":
        worker = threading.Thread(
            target=_run_client_worker,
            kwargs={
                "port": port,
                "stop_event": stop_event,
                "state": state,
                "payload": payload,
                "payloads": payloads,
                "tls": opts.tls,
                "ca_path": ca_path,
                "capture_path": client_capture_path,
                "invalid_tls_handshake_first": opts.invalid_tls_handshake_first,
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
                "port": port,
                "stop_event": stop_event,
                "state": state,
                "payload": payload,
                "tls": opts.tls,
                "capture_path": server_capture_path,
                "cert_path": cert_path,
                "key_path": key_path,
            },
            daemon=True,
        )
    worker.start()

    env: dict[str, str] = {"TCP_ENDPOINT": endpoint}
    if server_capture_path is not None:
        env["TCP_FILE"] = server_capture_path
    if client_capture_path is not None:
        env["TCP_CLIENT_FILE"] = client_capture_path
    if opts.tls or opts.certs:
        assert cert_path is not None
        assert key_path is not None
        assert ca_path is not None
        assert cert_and_key_path is not None
        env.update(
            {
                "TCP_CERTFILE": str(cert_path),
                "TCP_KEYFILE": str(key_path),
                "TCP_CAFILE": str(ca_path),
                "TCP_CERTKEYFILE": str(cert_and_key_path),
            }
        )

    def _assert_test(
        *,
        test: Path,
        assertions: TcpAssertions | dict[str, Any],
        **_: Any,
    ) -> None:
        if isinstance(assertions, dict):
            assertions = TcpAssertions(**assertions)
        deadline = time.monotonic() + _ASSERTION_WAIT_TIMEOUT
        while True:
            with state.lock:
                server_received = state.server_received.decode(
                    "utf-8", errors="replace"
                )
                client_received = state.client_received.decode(
                    "utf-8", errors="replace"
                )
                server_sent = state.server_sent.decode("utf-8", errors="replace")
            missing_message = None
            if (
                assertions.server_received_contains is not None
                and assertions.server_received_contains not in server_received
            ):
                missing_message = (
                    f"{test.name}: expected fixture server capture to contain "
                    f"{assertions.server_received_contains!r}, got {server_received!r}"
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
            raise RuntimeError("tcp fixture worker did not stop within 2 seconds")
        if server_capture_path is not None and os.path.exists(server_capture_path):
            os.remove(server_capture_path)
        if client_capture_path is not None and os.path.exists(client_capture_path):
            os.remove(client_capture_path)
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)

    return FixtureHandle(
        env=env,
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )


def _send_invalid_tls_handshake(port: int, stop_event: threading.Event) -> None:
    while not stop_event.is_set():
        try:
            with socket.create_connection((_HOST, port), timeout=1) as sock:
                sock.sendall(b"\x16\x03\x03\x00\x01\x01")
                return
        except (ConnectionRefusedError, OSError):
            time.sleep(_CLIENT_RETRY_DELAY)


def _run_client_worker(
    port: int,
    stop_event: threading.Event,
    state: _TcpState,
    payload: bytes,
    payloads: list[bytes] | None,
    tls: bool,
    ca_path: Path | None,
    capture_path: str | None,
    invalid_tls_handshake_first: bool,
    send_delay: float,
    split_payload_at: int | None,
    split_send_delay: float,
    inter_connection_delay: float,
) -> None:
    context: ssl.SSLContext | None = None
    if tls:
        assert ca_path is not None
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.check_hostname = False
        context.load_verify_locations(cafile=ca_path)
    if invalid_tls_handshake_first:
        _send_invalid_tls_handshake(port, stop_event)
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
            raw_sock = socket.create_connection((_HOST, port), timeout=1)
        except (ConnectionRefusedError, OSError):
            time.sleep(_CLIENT_RETRY_DELAY)
            continue
        try:
            with raw_sock:
                sock: socket.socket | ssl.SSLSocket
                if context is not None:
                    sock = context.wrap_socket(raw_sock, server_hostname=_COMMON_NAME)
                else:
                    sock = raw_sock
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
                    if context is None:
                        try:
                            sock.shutdown(socket.SHUT_WR)
                        except OSError:
                            pass
                    idle_deadline = time.monotonic() + 2
                    with (
                        open(capture_path, "ab")
                        if capture_path
                        else open(os.devnull, "wb")
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
        except (ssl.SSLError, OSError):
            pass
        current_payload = None
        if inter_connection_delay > 0:
            stop_event.wait(inter_connection_delay)
        time.sleep(_CLIENT_RETRY_DELAY)


def _run_server_worker(
    port: int,
    stop_event: threading.Event,
    state: _TcpState,
    payload: bytes,
    tls: bool,
    capture_path: str | None,
    cert_path: Path | None,
    key_path: Path | None,
) -> None:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((_HOST, port))
        server.listen(1)
        server.settimeout(0.1)
        while not stop_event.is_set():
            try:
                conn, _ = server.accept()
            except socket.timeout:
                continue
            try:
                if tls:
                    assert cert_path is not None
                    assert key_path is not None
                    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                    ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
                    conn = ctx.wrap_socket(conn, server_side=True)
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
            except (ssl.SSLError, OSError):
                pass
