from __future__ import annotations

import socket
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture

from ._utils import find_free_port

_HOST = "127.0.0.1"
_ASSERTION_WAIT_TIMEOUT = 2.0
_ASSERTION_WAIT_INTERVAL = 0.01


@dataclass(frozen=True)
class UdpOptions:
    pass


@dataclass(frozen=True)
class UdpAssertions:
    server_received_contains: str | None = None


@dataclass
class _UdpState:
    lock: threading.Lock = field(default_factory=threading.Lock)
    server_received: bytearray = field(default_factory=bytearray)


@fixture(options=UdpOptions, assertions=UdpAssertions)
def udp() -> FixtureHandle:
    port = find_free_port(sock_type=socket.SOCK_DGRAM)
    endpoint = f"{_HOST}:{port}"
    stop_event = threading.Event()
    state = _UdpState()
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((_HOST, port))
    sock.settimeout(0.2)
    worker = threading.Thread(
        target=_run_server_worker,
        kwargs={
            "sock": sock,
            "stop_event": stop_event,
            "state": state,
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
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as wakeup:
            wakeup.sendto(b"", (_HOST, port))
        worker.join(timeout=2)
        sock.close()
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
