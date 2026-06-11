"""HTTP CONNECT proxy fixture for outbound proxy tests."""

from __future__ import annotations

import select
import socket
import ssl
import tempfile
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable

from ._utils import generate_self_signed_cert

_HOST = "127.0.0.1"
_TARGET_HOST = "tenzir-http-proxy-target.test"
_PROXY_AUTH = "Basic dGVuemlyOnByb3h5"


class _BackendHandler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:  # noqa: N802
        payload = b'{"proxied":true}\n'
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, *_: object) -> None:
        return


def _recv_headers(conn: socket.socket) -> str:
    data = bytearray()
    while b"\r\n\r\n" not in data:
        chunk = conn.recv(4096)
        if not chunk:
            break
        data.extend(chunk)
    return data.decode("iso-8859-1", errors="replace")


def _tunnel(client: socket.socket, upstream: socket.socket) -> None:
    sockets = [client, upstream]
    while sockets:
        readable, _, _ = select.select(sockets, [], [], 0.2)
        for sock in readable:
            try:
                data = sock.recv(65536)
            except OSError:
                return
            if not data:
                return
            peer = upstream if sock is client else client
            try:
                peer.sendall(data)
            except OSError:
                return


@fixture(name="http_connect_proxy")
def http_connect_proxy() -> FixtureHandle:
    temp_dir = Path(tempfile.mkdtemp(prefix="http-connect-proxy-tls-"))
    try:
        cert_path, key_path, ca_path, _cert_and_key_path = generate_self_signed_cert(
            temp_dir,
            common_name=_TARGET_HOST,
            san_entries=[f"DNS:{_TARGET_HOST}"],
        )
    except Exception as exc:
        raise FixtureUnavailable(f"failed to generate TLS certificate: {exc}") from exc
    backend = ThreadingHTTPServer((_HOST, 0), _BackendHandler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
    backend.socket = context.wrap_socket(backend.socket, server_side=True)
    backend_thread = threading.Thread(target=backend.serve_forever, daemon=True)
    backend_thread.start()
    proxy = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    proxy.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    proxy.bind((_HOST, 0))
    proxy.listen()
    proxy.settimeout(0.2)
    stop = threading.Event()
    errors: list[str] = []
    saw_connect = threading.Event()
    backend_port = backend.server_address[1]
    proxy_port = proxy.getsockname()[1]

    def proxy_worker() -> None:
        while not stop.is_set():
            try:
                client, _ = proxy.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            with client:
                headers = _recv_headers(client)
                lines = headers.splitlines()
                request_line = lines[0] if lines else ""
                if request_line != f"CONNECT {_TARGET_HOST}:{backend_port} HTTP/1.1":
                    errors.append(f"unexpected CONNECT request line: {request_line!r}")
                    return
                header_map = {}
                for line in lines[1:]:
                    if ":" in line:
                        key, value = line.split(":", 1)
                        header_map[key.lower()] = value.strip()
                if header_map.get("proxy-authorization") != _PROXY_AUTH:
                    errors.append(
                        "unexpected Proxy-Authorization header: "
                        f"{header_map.get('proxy-authorization')!r}"
                    )
                    return
                saw_connect.set()
                with socket.create_connection((_HOST, backend_port)) as upstream:
                    client.sendall(b"HTTP/1.1 200 Connection Established\r\n\r\n")
                    _tunnel(client, upstream)

    proxy_thread = threading.Thread(target=proxy_worker, daemon=True)
    proxy_thread.start()

    def teardown() -> None:
        stop.set()
        proxy.close()
        backend.shutdown()
        proxy_thread.join(timeout=1)
        backend_thread.join(timeout=1)
        backend.server_close()
        for path in temp_dir.iterdir():
            path.unlink()
        temp_dir.rmdir()
        if not saw_connect.is_set():
            errors.append("proxy did not receive a CONNECT request")
        if errors:
            raise AssertionError("; ".join(errors))

    return FixtureHandle(
        env={
            "HTTP_PROXY_FIXTURE_PROXY": f"http://tenzir:proxy@{_HOST}:{proxy_port}",
            "TENZIR_HTTP_PROXY": f"http://tenzir:proxy@{_HOST}:{proxy_port}",
            "HTTP_PROXY": f"http://tenzir:proxy@{_HOST}:{proxy_port}",
            "HTTPS_PROXY": f"http://tenzir:proxy@{_HOST}:{proxy_port}",
            "http_proxy": f"http://tenzir:proxy@{_HOST}:{proxy_port}",
            "https_proxy": f"http://tenzir:proxy@{_HOST}:{proxy_port}",
            "NO_PROXY": "",
            "no_proxy": "",
            "HTTP_PROXY_FIXTURE_URL": f"https://{_TARGET_HOST}:{backend_port}/",
            "HTTP_PROXY_FIXTURE_CAFILE": str(ca_path),
        },
        teardown=teardown,
    )
