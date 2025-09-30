from __future__ import annotations

import os
import shutil
import socket
import subprocess
import ssl
import tempfile
import threading
import time
from pathlib import Path
from typing import Dict, Tuple

from tenzir_test import startup, teardown

_HOST = "127.0.0.1"
_COMMON_NAME = "tenzir-node.example.org"


def _find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _generate_self_signed_cert(temp_dir: Path) -> tuple[Path, Path, Path]:
    key_path = temp_dir / "server-key.pem"
    cert_path = temp_dir / "server-cert.pem"
    ca_path = temp_dir / "ca.pem"
    cmd = [
        "openssl",
        "req",
        "-x509",
        "-newkey",
        "rsa:2048",
        "-keyout",
        str(key_path),
        "-out",
        str(cert_path),
        "-days",
        "1",
        "-nodes",
        "-subj",
        f"/CN={_COMMON_NAME}",
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    shutil.copy(cert_path, ca_path)
    return cert_path, key_path, ca_path


_TLS_STATE: Dict[str, Tuple[threading.Thread, threading.Event, Path]] = {}
_SINK_STATE: Dict[str, Tuple[threading.Thread, threading.Event, str]] = {}


@startup("tcp_tls_source", replace=True)
def tcp_tls_source() -> dict[str, str]:
    port = _find_free_port()
    temp_dir = Path(tempfile.mkdtemp(prefix="tcp-tls-"))
    cert_path, key_path, ca_path = _generate_self_signed_cert(temp_dir)
    endpoint = f"tcp://{_HOST}:{port}"
    stop_event = threading.Event()

    def _send_payload() -> None:
        deadline = time.time() + 10
        context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
        context.check_hostname = False
        context.load_verify_locations(cafile=ca_path)
        while not stop_event.is_set() and time.time() < deadline:
            try:
                with socket.create_connection((_HOST, port), timeout=1) as raw_sock:
                    with context.wrap_socket(
                        raw_sock, server_hostname=_COMMON_NAME
                    ) as tls_sock:
                        tls_sock.sendall(b"foo\n")
                        try:
                            tls_sock.shutdown(socket.SHUT_WR)
                            while not stop_event.is_set():
                                chunk = tls_sock.recv(1024)
                                if not chunk:
                                    break
                        except OSError:
                            pass
                    break
            except (ConnectionRefusedError, ssl.SSLError, OSError):
                time.sleep(0.1)
        stop_event.set()

    thread = threading.Thread(target=_send_payload, daemon=True)
    thread.start()
    _TLS_STATE[endpoint] = (thread, stop_event, temp_dir)
    return {
        "TCP_TLS_ENDPOINT": endpoint,
        "TCP_TLS_CERTFILE": str(cert_path),
        "TCP_TLS_KEYFILE": str(key_path),
        "TCP_TLS_CAFILE": str(ca_path),
    }


@teardown("tcp_tls_source")
def stop_tcp_tls_source(env: dict[str, str]) -> None:
    endpoint = env.get("TCP_TLS_ENDPOINT")
    if not endpoint:
        return
    thread, stop_event, temp_dir = _TLS_STATE.pop(endpoint, (None, None, None))
    if stop_event is not None:
        stop_event.set()
    if thread is not None:
        thread.join(timeout=1)
    if temp_dir is not None and temp_dir.exists():
        shutil.rmtree(temp_dir, ignore_errors=True)


@startup("tcp_sink", replace=True)
def tcp_sink() -> dict[str, str]:
    port = _find_free_port()
    endpoint = f"tcp://{_HOST}:{port}"
    stop_event = threading.Event()
    fd, path = tempfile.mkstemp(prefix="tcp-sink-", suffix=".log")
    os.close(fd)

    def _serve() -> None:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server, open(
            path, "wb"
        ) as fh:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((_HOST, port))
            server.listen(1)
            server.settimeout(0.1)
            while not stop_event.is_set():
                try:
                    conn, _ = server.accept()
                except socket.timeout:
                    continue
                with conn:
                    while True:
                        data = conn.recv(4096)
                        if not data:
                            break
                        fh.write(data)
                        fh.flush()
                break

    thread = threading.Thread(target=_serve, daemon=True)
    thread.start()
    _SINK_STATE[endpoint] = (thread, stop_event, path)
    return {
        "TCP_SINK_ENDPOINT": endpoint,
        "TCP_SINK_FILE": path,
    }


@teardown("tcp_sink")
def stop_tcp_sink(env: dict[str, str]) -> None:
    endpoint = env.get("TCP_SINK_ENDPOINT")
    if not endpoint:
        return
    thread, stop_event, path = _SINK_STATE.pop(endpoint, (None, None, None))
    if stop_event is not None:
        stop_event.set()
    if thread is not None:
        thread.join(timeout=1)
    if path and os.path.exists(path):
        os.remove(path)
