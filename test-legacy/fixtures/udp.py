from __future__ import annotations

import os
import socket
import tempfile
import threading
import time
from typing import Iterator

from tenzir_test import fixture


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@fixture(name="udp_source")
def udp_source() -> Iterator[dict[str, str]]:
    port = _find_free_port()
    endpoint = f"127.0.0.1:{port}"
    stop_event = threading.Event()

    def run() -> None:
        payload = b'{"foo":42}\n'
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            while not stop_event.is_set():
                sock.sendto(payload, ("127.0.0.1", port))
                time.sleep(0.05)

    worker = threading.Thread(target=run, daemon=True)
    worker.start()

    try:
        yield {"UDP_SOURCE_ENDPOINT": endpoint}
    finally:
        stop_event.set()
        worker.join(timeout=1)


@fixture(name="udp_sink")
def udp_sink() -> Iterator[dict[str, str]]:
    port = _find_free_port()
    endpoint = f"127.0.0.1:{port}"
    stop_event = threading.Event()
    fd, path = tempfile.mkstemp(prefix="udp-sink-", suffix=".log")
    os.close(fd)

    def run() -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock, open(
            path, "w", encoding="utf-8"
        ) as fh:
            sock.bind(("127.0.0.1", port))
            sock.settimeout(0.1)
            while not stop_event.is_set():
                try:
                    data, _ = sock.recvfrom(65535)
                except socket.timeout:
                    continue
                except OSError:
                    break
                fh.write(data.decode("utf-8", errors="replace"))
                fh.flush()

    worker = threading.Thread(target=run, daemon=True)
    worker.start()

    try:
        yield {
            "UDP_SINK_ENDPOINT": endpoint,
            "UDP_SINK_FILE": path,
        }
    finally:
        stop_event.set()
        worker.join(timeout=1)
        if os.path.exists(path):
            os.remove(path)
