from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import current_options

from ._utils import find_free_port

_HOST = "127.0.0.1"
_CLIENT_RETRY_DELAY = 0.01
_CLIENT_STOP_CHECK_INTERVAL = 1024
_CLIENT_SEND_BUFFER_SIZE = 8 * 1024 * 1024
_ASSERTION_WAIT_TIMEOUT = 2.0
_ASSERTION_WAIT_INTERVAL = 0.01


@dataclass(frozen=True)
class UdpOptions:
    mode: str = "server"  # "server" receives data; "client" sends data
    payload: str = "foo"
    payload_hex: str | None = None
    payload_sequence_hex: list[str] = field(default_factory=list)
    interval: float = _CLIENT_RETRY_DELAY
    initial_delay: float = 0.0
    source_port: int = 0


@dataclass(frozen=True)
class UdpAssertions:
    server_received_contains: str | None = None


@dataclass
class _UdpState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    server_received: bytearray = field(default_factory=bytearray)


@fixture(options=UdpOptions, assertions=UdpAssertions)
def udp() -> FixtureHandle:
    opts = current_options("udp")
    if opts.mode not in {"client", "server"}:
        raise RuntimeError("udp fixture option `mode` must be one of: client, server")
    if opts.interval < 0:
        raise RuntimeError("udp fixture option `interval` must be non-negative")
    if opts.initial_delay < 0:
        raise RuntimeError("udp fixture option `initial_delay` must be non-negative")
    if opts.source_port < 0 or opts.source_port > 65535:
        raise RuntimeError("udp fixture option `source_port` must be in [0, 65535]")
    port = find_free_port(sock_type=socket.SOCK_DGRAM)
    endpoint = f"{_HOST}:{port}"
    stop_event = threading.Event()
    state = _UdpState()
    server_sock: socket.socket | None = None
    if opts.mode == "server":
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server_sock.bind((_HOST, port))
        server_sock.settimeout(0.2)
        worker = threading.Thread(
            target=_run_server_worker,
            kwargs={
                "sock": server_sock,
                "stop_event": stop_event,
                "state": state,
            },
            daemon=True,
        )
    else:
        payloads = _client_payloads(opts)
        worker = threading.Thread(
            target=_run_client_worker,
            kwargs={
                "port": port,
                "stop_event": stop_event,
                "payloads": payloads,
                "interval": opts.interval,
                "initial_delay": opts.initial_delay,
                "source_port": opts.source_port,
            },
            daemon=True,
        )
    worker.start()

    def _assert_test(
        *,
        test: Path,
        assertions: UdpAssertions | dict[str, Any],
        **_: Any,
    ) -> None:
        if isinstance(assertions, dict):
            assertions = UdpAssertions(**assertions)
        deadline = time.monotonic() + _ASSERTION_WAIT_TIMEOUT
        while True:
            with state.lock:
                server_received = state.server_received.decode(
                    "utf-8", errors="replace"
                )
            missing_message = None
            if (
                assertions.server_received_contains is not None
                and assertions.server_received_contains not in server_received
            ):
                missing_message = (
                    f"{test.name}: expected fixture server capture to contain "
                    f"{assertions.server_received_contains!r}, got {server_received!r}"
                )
            if missing_message is None:
                return
            if time.monotonic() >= deadline:
                raise AssertionError(missing_message)
            time.sleep(_ASSERTION_WAIT_INTERVAL)

    def _teardown() -> None:
        stop_event.set()
        if server_sock is not None:
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as wakeup:
                wakeup.sendto(b"", (_HOST, port))
        worker.join(timeout=2)
        if server_sock is not None:
            server_sock.close()
        if worker.is_alive():
            raise RuntimeError("udp fixture worker did not stop within 2 seconds")

    return FixtureHandle(
        env={"UDP_ENDPOINT": endpoint},
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )


def _run_server_worker(
    sock: socket.socket,
    stop_event: threading.Event,
    state: _UdpState,
) -> None:
    while not stop_event.is_set():
        try:
            chunk, _ = sock.recvfrom(65536)
        except socket.timeout:
            continue
        except OSError:
            break
        with state.lock:
            state.server_received.extend(chunk)


def _run_client_worker(
    port: int,
    stop_event: threading.Event,
    payloads: list[bytes],
    interval: float,
    initial_delay: float,
    source_port: int,
) -> None:
    if initial_delay > 0 and stop_event.wait(initial_delay):
        return
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, _CLIENT_SEND_BUFFER_SIZE)
        if source_port:
            sock.bind((_HOST, source_port))
        sock.connect((_HOST, port))
        if interval == 0:
            _run_burst_client_worker(sock, stop_event, payloads)
            return
        while not stop_event.is_set():
            for payload in payloads:
                if stop_event.is_set():
                    return
                _send_payload(sock, payload)
                if stop_event.wait(interval):
                    return


def _run_burst_client_worker(
    sock: socket.socket,
    stop_event: threading.Event,
    payloads: list[bytes],
) -> None:
    sent_since_stop_check = 0
    while not stop_event.is_set():
        for payload in payloads:
            _send_payload(sock, payload)
            sent_since_stop_check += 1
            if sent_since_stop_check >= _CLIENT_STOP_CHECK_INTERVAL:
                sent_since_stop_check = 0
                if stop_event.is_set():
                    return


def _send_payload(sock: socket.socket, payload: bytes) -> None:
    if not payload:
        return
    try:
        sock.send(payload)
    except OSError:
        pass


def _client_payloads(opts: UdpOptions) -> list[bytes]:
    if opts.payload_sequence_hex:
        return [bytes.fromhex(payload) for payload in opts.payload_sequence_hex]
    if opts.payload_hex is not None:
        return [bytes.fromhex(opts.payload_hex)]
    return [opts.payload.encode()]
