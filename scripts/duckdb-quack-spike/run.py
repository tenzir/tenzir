#!/usr/bin/env python3
"""Run the C API probe over HTTP and verified TLS on loopback only."""

import argparse
import http.client
import http.server
import os
from pathlib import Path
import socket
import ssl
import subprocess
import tempfile
import threading


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("probe", type=Path)
    args = parser.parse_args()
    probe = args.probe.resolve()
    with tempfile.TemporaryDirectory(prefix="quack-spike-") as tmp:
        root = Path(tmp)
        env = dict(os.environ, HOME=tmp)
        # Quack cannot accept an already-bound socket. Keep the reserve/release
        # window narrow; the probe reports a bind error instead of silently
        # connecting to an unrelated server.
        with socket.socket() as sock:
            sock.bind(("127.0.0.1", 0))
            backend = sock.getsockname()[1]
        http_result = subprocess.run([str(probe), str(backend)], env=env, timeout=60)
        cert = root / "cert.pem"
        key = root / "key.pem"
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-keyout",
                str(key),
                "-out",
                str(cert),
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=IP:127.0.0.1,DNS:localhost",
            ],
            check=True,
            capture_output=True,
            timeout=30,
        )

        class Proxy(http.server.BaseHTTPRequestHandler):
            def handle(self):
                try:
                    super().handle()
                except ConnectionResetError:
                    # The negative certificate test deliberately drops its TLS
                    # connection before sending a request.
                    pass

            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"]))
                connection = http.client.HTTPConnection(
                    "127.0.0.1", backend, timeout=30
                )
                try:
                    connection.request(
                        "POST",
                        self.path,
                        body,
                        {
                            "Content-Type": "application/duckdb",
                        },
                    )
                    response = connection.getresponse()
                    data = response.read()
                    self.send_response(response.status)
                    self.send_header("Content-Type", "application/duckdb")
                    self.send_header("Content-Length", str(len(data)))
                    self.end_headers()
                    self.wfile.write(data)
                finally:
                    connection.close()

            def log_message(self, *_):
                pass

        with http.server.ThreadingHTTPServer(("127.0.0.1", 0), Proxy) as server:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(cert, key)
            server.socket = context.wrap_socket(server.socket, server_side=True)
            thread = threading.Thread(target=server.serve_forever)
            thread.start()
            try:
                tls = subprocess.run(
                    [str(probe), str(backend), str(server.server_port), str(cert)],
                    env=env,
                    timeout=60,
                )
            finally:
                server.shutdown()
                thread.join()
        if http_result.returncode != 0 or tls.returncode != 0:
            raise SystemExit(1)
        if list(root.rglob("*.duckdb_extension")):
            raise RuntimeError("the probe unexpectedly installed an extension")
        print("PASS: HTTP and TLS; trusted certificate accepted, untrusted rejected")
        raise SystemExit(0)


if __name__ == "__main__":
    main()
