# runner: python
# timeout: 120

"""A TLS server whose CA file fails to load reports an error instead of hanging.

The HTTP server loads its TLS files only while it starts, and a failure there
used to leave the server hanging. The server now sets up TLS before it starts.
"""

from __future__ import annotations

import os
import shlex
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path


def run(pipeline: str, env: dict[str, str] | None = None) -> None:
    try:
        result = subprocess.run(
            [*shlex.split(os.environ["TENZIR_BINARY"]), "--nova=true", pipeline],
            capture_output=True,
            text=True,
            timeout=60,
            env={**os.environ, **(env or {})},
        )
    except subprocess.TimeoutExpired:
        print("hung")
        return
    # The error text of OpenSSL differs between platforms.
    error = next(
        (line for line in result.stderr.splitlines() if line.startswith("error:")),
        "",
    )
    # Keep the message and its cause, but not the platform-specific details.
    message = ": ".join(part.strip() for part in error.split(":")[1:3])
    print(f"exit code {result.returncode}:", message)


def free_port() -> int:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        return probe.getsockname()[1]


def listen(pipeline: str, port: int, env: dict[str, str]) -> None:
    process = subprocess.Popen(
        [*shlex.split(os.environ["TENZIR_BINARY"]), "--nova=true", pipeline],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, **env},
    )
    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline and process.poll() is None:
            try:
                socket.create_connection(("127.0.0.1", port), timeout=1).close()
                print("listening")
                return
            except OSError:
                time.sleep(0.1)
        print(
            "not listening:",
            process.poll(),
            process.stderr.read() if process.poll() is not None else "",
        )
    finally:
        process.kill()
        process.wait()


def main() -> None:
    directory = Path(tempfile.mkdtemp(prefix="tls-setup-"))
    try:
        cert = directory / "cert.pem"
        key = directory / "key.pem"
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-keyout",
                str(key),
                "-out",
                str(cert),
            ],
            check=True,
            capture_output=True,
        )
        invalid = directory / "invalid-ca.pem"
        invalid.write_text("not a certificate\n")
        tls = f'certfile: "{cert}", keyfile: "{key}", require_client_cert: true'
        print("invalid client_ca")
        run(
            f'accept_http "127.0.0.1:0", tls={{{tls}, client_ca: "{invalid}"}} {{\n'
            "  read_json\n}"
        )
        # The default `cacert`, which comes from `SSL_CERT_FILE` without a
        # check that the file exists, plays no role for client certificates.
        print("valid client_ca and missing default cacert")
        port = free_port()
        listen(
            f'accept_http "127.0.0.1:{port}", tls={{{tls}, client_ca: "{cert}"}} {{\n'
            "  read_json\n}",
            port,
            env={"SSL_CERT_FILE": str(directory / "missing.pem")},
        )
    finally:
        shutil.rmtree(directory, ignore_errors=True)


if __name__ == "__main__":
    main()
