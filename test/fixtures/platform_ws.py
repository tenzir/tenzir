from __future__ import annotations

import base64
import hashlib
import shutil
import socket
import ssl
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import FixtureUnavailable, current_options

from ._utils import find_free_port

try:
    import trustme
except ImportError:
    trustme = None  # type: ignore[assignment]

_HOST = "127.0.0.1"
_COMMON_NAME = "localhost"
_WS_MAGIC = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"


@dataclass(frozen=True)
class PlatformWsOptions:
    tls: bool = True


def _generate_mtls_material(temp_dir: Path) -> dict[str, Path]:
    ca = trustme.CA()
    server_cert = ca.issue_cert(_COMMON_NAME)
    client_cert = ca.issue_cert("tenzir-client")
    ca_cert_path = temp_dir / "ca-cert.pem"
    server_cert_path = temp_dir / "server-cert.pem"
    server_key_path = temp_dir / "server-key.pem"
    client_cert_path = temp_dir / "client-cert.pem"
    client_key_path = temp_dir / "client-key.pem"
    ca.cert_pem.write_to_path(str(ca_cert_path))
    server_cert.cert_chain_pems[0].write_to_path(str(server_cert_path))
    server_cert.private_key_pem.write_to_path(str(server_key_path))
    client_cert.cert_chain_pems[0].write_to_path(str(client_cert_path))
    client_cert.private_key_pem.write_to_path(str(client_key_path))
    return {
        "ca_cert": ca_cert_path,
        "server_cert": server_cert_path,
        "server_key": server_key_path,
        "client_cert": client_cert_path,
        "client_key": client_key_path,
    }


def _read_http_headers(conn: ssl.SSLSocket) -> str:
    data = b""
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            break
        data += chunk
        if len(data) > 64 * 1024:
            break
    return data.decode("latin-1", errors="replace")


def _parse_sec_websocket_key(headers: str) -> str | None:
    for line in headers.split("\r\n"):
        if ":" not in line:
            continue
        name, value = line.split(":", 1)
        if name.strip().lower() == "sec-websocket-key":
            return value.strip()
    return None


def _build_upgrade_response(sec_key: str) -> bytes:
    accept = base64.b64encode(
        hashlib.sha1(f"{sec_key}{_WS_MAGIC}".encode("ascii")).digest()
    ).decode("ascii")
    response = (
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        f"Sec-WebSocket-Accept: {accept}\r\n"
        "x-tenzir-accepted: node-response,node-register-response,node-secret-response\r\n"
        "\r\n"
    )
    return response.encode("ascii")


@fixture(options=PlatformWsOptions)
def platform_ws() -> Iterator[dict[str, str]]:
    opts = current_options("platform_ws")
    if not opts.tls:
        raise FixtureUnavailable("platform_ws fixture currently supports only tls=true")
    if trustme is None:
        raise FixtureUnavailable("trustme package not installed")
    port = find_free_port()
    endpoint = f"wss://{_COMMON_NAME}:{port}/production"
    stop_event = threading.Event()
    temp_dir = Path(tempfile.mkdtemp(prefix="platform-ws-"))
    result_file = temp_dir / "result.txt"
    result_file.write_text("pending\n", encoding="utf-8")
    tls = _generate_mtls_material(temp_dir)

    def _serve() -> None:
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(
            certfile=str(tls["server_cert"]),
            keyfile=str(tls["server_key"]),
        )
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(cafile=str(tls["ca_cert"]))
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
            server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server.bind((_HOST, port))
            server.listen(1)
            server.settimeout(0.2)
            while not stop_event.is_set():
                try:
                    raw_conn, _ = server.accept()
                except socket.timeout:
                    continue
                try:
                    with context.wrap_socket(raw_conn, server_side=True) as conn:
                        peer_cert = conn.getpeercert()
                        if not peer_cert:
                            result_file.write_text("no-client-cert\n", encoding="utf-8")
                            return
                        headers = _read_http_headers(conn)
                        sec_key = _parse_sec_websocket_key(headers)
                        if sec_key is None:
                            result_file.write_text(
                                "no-websocket-key\n", encoding="utf-8"
                            )
                            return
                        conn.sendall(_build_upgrade_response(sec_key))
                        result_file.write_text(
                            "client-cert-authenticated\n", encoding="utf-8"
                        )
                        while not stop_event.is_set():
                            time.sleep(0.1)
                        return
                except ssl.SSLError as e:
                    result_file.write_text(f"tls-failed: {e}\n", encoding="utf-8")
                    return
                except Exception as e:
                    result_file.write_text(f"server-error: {e}\n", encoding="utf-8")
                    return

    worker = threading.Thread(target=_serve, daemon=True)
    worker.start()
    try:
        yield {
            "PLATFORM_WS_ENDPOINT": endpoint,
            "PLATFORM_WS_CACERT": str(tls["ca_cert"]),
            "PLATFORM_WS_CERTFILE": str(tls["client_cert"]),
            "PLATFORM_WS_KEYFILE": str(tls["client_key"]),
            "PLATFORM_WS_RESULT_FILE": str(result_file),
        }
    finally:
        stop_event.set()
        worker.join(timeout=1)
        shutil.rmtree(temp_dir, ignore_errors=True)
