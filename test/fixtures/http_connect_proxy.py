"""HTTP CONNECT proxy fixture for outbound proxy tests."""

from __future__ import annotations

import select
import shutil
import socket
import ssl
import tempfile
import threading
from dataclasses import dataclass
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from tenzir_test import FixtureHandle, current_options, fixture
from tenzir_test.fixtures import FixtureUnavailable

from ._utils import generate_self_signed_cert

_HOST = "127.0.0.1"
_TARGET_HOST = "tenzir-http-proxy-target.test"
_FORWARD_TARGET_HOST = "tenzir-http-forward-proxy-target.test"
_PROXY_AUTH = "Basic dGVuemlyOnByb3h5"


@dataclass(frozen=True)
class ConnectProxyOptions:
    # Address the target at the default HTTPS port, so that its URL and the
    # diagnostics that name it don't contain the backend's varying port. The
    # proxy tunnels such connections to the backend.
    default_port: bool = False
    # Replaces the names in the target's certificate, e.g., to serve a
    # certificate that does not match the target host.
    target_san: list[str] | None = None
    # Serve the proxy itself over TLS with a self-signed certificate that
    # clients do not trust. The fixture then expects a failed TLS handshake
    # and no CONNECT request.
    proxy_tls: bool = False
    # Require and advertise proxy credentials. Without them, clients use
    # pooled CONNECT tunnels instead of one-shot connections.
    proxy_auth: bool = True
    # With `proxy_tls`, give clients a CA bundle without certificates, so that
    # they cannot even set up TLS for the proxy. The fixture then expects no
    # connection at all.
    invalid_ca_bundle: bool = False


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


@fixture(name="http_connect_proxy", options=ConnectProxyOptions)
def http_connect_proxy() -> FixtureHandle:
    options = current_options("http_connect_proxy")
    if not isinstance(options, ConnectProxyOptions):
        options = ConnectProxyOptions()
    temp_dir = Path(tempfile.mkdtemp(prefix="http-connect-proxy-tls-"))
    try:
        cert_path, key_path, ca_path, _cert_and_key_path = generate_self_signed_cert(
            temp_dir,
            common_name=_TARGET_HOST,
            san_entries=options.target_san or [f"DNS:{_TARGET_HOST}"],
        )
    except Exception as exc:
        raise FixtureUnavailable(f"failed to generate TLS certificate: {exc}") from exc
    proxy_context: ssl.SSLContext | None = None
    if options.proxy_tls:
        (temp_dir / "proxy").mkdir()
        proxy_cert, proxy_key, _, _ = generate_self_signed_cert(
            temp_dir / "proxy", common_name=_HOST, san_entries=[f"IP:{_HOST}"]
        )
        proxy_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        proxy_context.load_cert_chain(certfile=str(proxy_cert), keyfile=str(proxy_key))
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
    saw_rejected_tls = threading.Event()
    saw_connection = threading.Event()
    backend_port = backend.server_address[1]
    proxy_port = proxy.getsockname()[1]
    target_port = 443 if options.default_port else backend_port
    target_url = (
        f"https://{_TARGET_HOST}/"
        if options.default_port
        else f"https://{_TARGET_HOST}:{backend_port}/"
    )
    failure_log_path = temp_dir / "client-failure.log"

    def proxy_worker() -> None:
        while not stop.is_set():
            try:
                client, _ = proxy.accept()
            except TimeoutError:
                continue
            except OSError:
                return
            saw_connection.set()
            if proxy_context is not None:
                client.settimeout(5)
                try:
                    client = proxy_context.wrap_socket(client, server_side=True)
                except (ssl.SSLError, OSError):
                    # The client rejected the untrusted proxy certificate.
                    saw_rejected_tls.set()
                    client.close()
                    continue
                errors.append("client accepted the untrusted proxy certificate")
            with client:
                headers = _recv_headers(client)
                lines = headers.splitlines()
                request_line = lines[0] if lines else ""
                if request_line != f"CONNECT {_TARGET_HOST}:{target_port} HTTP/1.1":
                    errors.append(f"unexpected CONNECT request line: {request_line!r}")
                    return
                header_map = {}
                for line in lines[1:]:
                    if ":" in line:
                        key, value = line.split(":", 1)
                        header_map[key.lower()] = value.strip()
                if (
                    options.proxy_auth
                    and header_map.get("proxy-authorization") != _PROXY_AUTH
                ):
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
        if options.invalid_ca_bundle:
            if saw_connection.is_set():
                errors.append("client connected without a usable CA bundle")
        elif options.proxy_tls:
            if not saw_rejected_tls.is_set():
                errors.append("client did not attempt a TLS handshake with the proxy")
        elif not saw_connect.is_set():
            errors.append("proxy did not receive a CONNECT request")
            if failure_log_path.exists():
                errors.append(failure_log_path.read_text(encoding="utf-8"))
        shutil.rmtree(temp_dir, ignore_errors=True)
        if errors:
            raise AssertionError("; ".join(errors))

    ca_bundle = ca_path
    if options.invalid_ca_bundle:
        ca_bundle = temp_dir / "invalid-ca.pem"
        ca_bundle.write_text("not a certificate\n", encoding="utf-8")
    proxy_scheme = "https" if options.proxy_tls else "http"
    credentials = "tenzir:proxy@" if options.proxy_auth else ""
    proxy_url = f"{proxy_scheme}://{credentials}{_HOST}:{proxy_port}"
    return FixtureHandle(
        env={
            "HTTP_PROXY_FIXTURE_PROXY": proxy_url,
            "TENZIR_HTTPS_PROXY": proxy_url,
            "HTTP_PROXY": proxy_url,
            "HTTPS_PROXY": proxy_url,
            "http_proxy": proxy_url,
            "https_proxy": proxy_url,
            "NO_PROXY": "",
            "no_proxy": "",
            "HTTP_PROXY_FIXTURE_URL": target_url,
            # Give clients a CA bundle that doesn't trust the proxy, also where
            # the system has none, such as in build sandboxes.
            **({"SSL_CERT_FILE": str(ca_bundle)} if options.proxy_tls else {}),
            "HTTP_PROXY_FIXTURE_CAFILE": str(ca_path),
            "HTTP_PROXY_FIXTURE_FAILURE_LOG": str(failure_log_path),
        },
        teardown=teardown,
    )


@fixture(name="http_forward_proxy")
def http_forward_proxy() -> FixtureHandle:
    proxy = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    proxy.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    proxy.bind((_HOST, 0))
    proxy.listen()
    proxy.settimeout(0.2)
    stop = threading.Event()
    errors: list[str] = []
    saw_forward = threading.Event()
    proxy_port = proxy.getsockname()[1]
    target_port = 39871

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
                expected = f"GET http://{_FORWARD_TARGET_HOST}:{target_port}/ HTTP/1.1"
                if request_line != expected:
                    errors.append(f"unexpected forward request line: {request_line!r}")
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
                saw_forward.set()
                payload = b'{"proxied":true}\n'
                response = (
                    b"HTTP/1.1 200 OK\r\n"
                    b"Content-Type: application/json\r\n"
                    + f"Content-Length: {len(payload)}\r\n".encode("ascii")
                    + b"\r\n"
                    + payload
                )
                client.sendall(response)

    proxy_thread = threading.Thread(target=proxy_worker, daemon=True)
    proxy_thread.start()

    def teardown() -> None:
        stop.set()
        proxy.close()
        proxy_thread.join(timeout=1)
        if not saw_forward.is_set():
            errors.append("proxy did not receive a forward request")
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
            "HTTP_PROXY_FIXTURE_URL": f"http://{_FORWARD_TARGET_HOST}:{target_port}/",
        },
        teardown=teardown,
    )
