# runner: python
# timeout: 60

"""A graceful stop ends `accept_tcp` while a TLS handshake is pending.

The client never sends a ClientHello, so the handshake fails only after its
timeout, once the accept loop has already finished. Disabling the shutdown
grace period rules out a force-cancel.
"""

from __future__ import annotations

import os
import shlex
import signal
import socket
import subprocess
import tempfile
import time
from pathlib import Path

with tempfile.TemporaryDirectory() as tmp:
    cert = Path(tmp) / "cert.pem"
    key = Path(tmp) / "key.pem"
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
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    pipeline = f"""
accept_tcp "127.0.0.1:{port}", tls={{certfile: "{cert}", keyfile: "{key}"}} {{
  read_lines
}}
"""
    process = subprocess.Popen(
        [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", pipeline],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "TENZIR_SHUTDOWN_GRACE_PERIOD": "0s"},
    )
    try:
        deadline = time.monotonic() + 20
        while True:
            try:
                silent = socket.create_connection(("127.0.0.1", port), timeout=1)
                break
            except OSError:
                if time.monotonic() > deadline:
                    raise
                time.sleep(0.05)
        with silent:
            time.sleep(0.5)
            process.send_signal(signal.SIGINT)
            _, stderr = process.communicate(timeout=30)
        assert process.returncode == 0, (process.returncode, stderr)
        assert "TLS handshake failed" in stderr, stderr
        print("stopped after the pending handshake failed")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
