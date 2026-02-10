from __future__ import annotations

import os
import socket
import ssl
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import current_options

from ._utils import find_free_port, generate_self_signed_cert

_HOST = "127.0.0.1"
_COMMON_NAME = "tenzir-node.example.org"


@dataclass(frozen=True)
class TcpOptions:
    tls: bool = False
    mode: str = "client"  # "client" sends data; "server" receives data


@fixture(options=TcpOptions)
def tcp() -> Iterator[dict[str, str]]:
    opts = current_options("tcp")
    port = find_free_port()
    endpoint = f"{_HOST}:{port}"
    stop_event = threading.Event()

    if opts.mode == "client":
        yield from _run_client(opts, port, endpoint, stop_event)
    else:
        yield from _run_server(opts, port, endpoint, stop_event)


def _run_client(
    opts: TcpOptions,
    port: int,
    endpoint: str,
    stop_event: threading.Event,
) -> Iterator[dict[str, str]]:
    temp_dir = Path(tempfile.mkdtemp(prefix="tcp-")) if opts.tls else None

    if opts.tls:
        assert temp_dir is not None
        cert_path, key_path, ca_path, cert_and_key_path = generate_self_signed_cert(
            temp_dir, _COMMON_NAME
        )

    def _send_payload() -> None:
        if opts.tls:
            context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
            context.check_hostname = False
            context.load_verify_locations(cafile=ca_path)
        while not stop_event.is_set():
            try:
                with socket.create_connection((_HOST, port), timeout=1) as raw_sock:
                    if opts.tls:
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
                    else:
                        raw_sock.sendall(b"foo\n")
                        try:
                            raw_sock.shutdown(socket.SHUT_WR)
                            while not stop_event.is_set():
                                chunk = raw_sock.recv(1024)
                                if not chunk:
                                    break
                        except OSError:
                            pass
            except (ConnectionRefusedError, ssl.SSLError, OSError):
                time.sleep(0.1)
                continue
            time.sleep(0.1)

    worker = threading.Thread(target=_send_payload, daemon=True)
    worker.start()

    env: dict[str, str] = {"TCP_ENDPOINT": endpoint}
    if opts.tls:
        env.update(
            {
                "TCP_CERTFILE": str(cert_path),
                "TCP_KEYFILE": str(key_path),
                "TCP_CAFILE": str(ca_path),
                "TCP_CERTKEYFILE": str(cert_and_key_path),
            }
        )

    try:
        yield env
    finally:
        stop_event.set()
        worker.join(timeout=1)
        if temp_dir is not None:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)


def _run_server(
    opts: TcpOptions,
    port: int,
    endpoint: str,
    stop_event: threading.Event,
) -> Iterator[dict[str, str]]:
    temp_dir = Path(tempfile.mkdtemp(prefix="tcp-")) if opts.tls else None
    fd, path = tempfile.mkstemp(prefix="tcp-server-", suffix=".log")
    os.close(fd)

    if opts.tls:
        assert temp_dir is not None
        cert_path, key_path, ca_path, cert_and_key_path = generate_self_signed_cert(
            temp_dir, _COMMON_NAME
        )

    def _serve() -> None:
        with (
            socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server,
            open(path, "wb") as fh,
        ):
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((_HOST, port))
            server.listen(1)
            server.settimeout(0.1)
            while not stop_event.is_set():
                try:
                    conn, _ = server.accept()
                except socket.timeout:
                    continue
                if opts.tls:
                    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
                    ctx.load_cert_chain(certfile=cert_path, keyfile=key_path)
                    conn = ctx.wrap_socket(conn, server_side=True)
                with conn:
                    while True:
                        data = conn.recv(4096)
                        if not data:
                            break
                        fh.write(data)
                        fh.flush()
                break

    worker = threading.Thread(target=_serve, daemon=True)
    worker.start()

    env: dict[str, str] = {
        "TCP_ENDPOINT": endpoint,
        "TCP_FILE": path,
    }
    if opts.tls:
        env.update(
            {
                "TCP_CERTFILE": str(cert_path),
                "TCP_KEYFILE": str(key_path),
                "TCP_CAFILE": str(ca_path),
                "TCP_CERTKEYFILE": str(cert_and_key_path),
            }
        )

    try:
        yield env
    finally:
        stop_event.set()
        worker.join(timeout=1)
        if os.path.exists(path):
            os.remove(path)
        if temp_dir is not None:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)
