from __future__ import annotations

import os
import socket
import tempfile
import threading
import time
from typing import Dict, Tuple

from tenzir_test import startup, teardown


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


_SOURCE_STATE: Dict[str, Tuple[threading.Thread, threading.Event]] = {}
_SINK_STATE: Dict[str, Tuple[threading.Thread, threading.Event, str]] = {}


@startup("udp_source", replace=True)
def udp_source() -> dict[str, str]:
    port = _find_free_port()
    endpoint = f"127.0.0.1:{port}"
    stop_event = threading.Event()

    def run() -> None:
        payload = b'{"foo":42}\n'
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
            while not stop_event.is_set():
                sock.sendto(payload, ("127.0.0.1", port))
                time.sleep(0.05)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    _SOURCE_STATE[endpoint] = (thread, stop_event)
    return {"UDP_SOURCE_ENDPOINT": endpoint}


@teardown("udp_source")
def stop_udp_source(env: dict[str, str]) -> None:
    endpoint = env.get("UDP_SOURCE_ENDPOINT")
    if not endpoint:
        return
    thread, stop_event = _SOURCE_STATE.pop(endpoint, (None, None))
    if thread is None or stop_event is None:
        return
    stop_event.set()
    thread.join(timeout=1)


@startup("udp_sink", replace=True)
def udp_sink() -> dict[str, str]:
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

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    _SINK_STATE[endpoint] = (thread, stop_event, path)
    return {
        "UDP_SINK_ENDPOINT": endpoint,
        "UDP_SINK_FILE": path,
    }


@teardown("udp_sink")
def stop_udp_sink(env: dict[str, str]) -> None:
    endpoint = env.get("UDP_SINK_ENDPOINT")
    if not endpoint:
        return
    thread, stop_event, path = _SINK_STATE.pop(endpoint, (None, None, None))
    if thread is None or stop_event is None:
        return
    stop_event.set()
    thread.join(timeout=1)
    if path and os.path.exists(path):
        os.remove(path)
