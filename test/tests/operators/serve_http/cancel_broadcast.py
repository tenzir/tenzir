# runner: python
# timeout: 60

"""Cancellation releases a broadcast blocked by a client that stops reading."""

from __future__ import annotations

import json
import os
import shlex
import signal
import socket
import subprocess
import threading
import time


def _connect(port: int) -> socket.socket:
    deadline = time.monotonic() + 20
    while True:
        client = socket.socket()
        client.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 1024)
        client.settimeout(10)
        try:
            client.connect(("127.0.0.1", port))
            return client
        except OSError:
            client.close()
            if time.monotonic() >= deadline:
                raise
            threading.Event().wait(0.05)


def _run_case() -> None:
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    pipeline = f"""
from_stdin {{ read_ndjson }}
batch 1
serve_http "127.0.0.1:{port}" {{ write_lines }}
""".strip()
    process = subprocess.Popen(
        [
            *shlex.split(os.environ["TENZIR_BINARY"]),
            "--bare-mode",
            "--console-verbosity=warning",
            "--nova=true",
            "--multi",
            pipeline,
        ],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "TENZIR_SHUTDOWN_GRACE_PERIOD": "10ms"},
    )
    worker: threading.Thread | None = None
    try:
        with _connect(port) as client:
            client.sendall(b"GET / HTTP/1.1\r\nHost: localhost\r\n\r\n")

            def write() -> None:
                assert process.stdin is not None
                payload = json.dumps({"data": "x" * (1024 * 1024)}) + "\n"
                try:
                    while True:
                        process.stdin.write(payload)
                        process.stdin.flush()
                except BrokenPipeError:
                    pass
                finally:
                    try:
                        process.stdin.close()
                    except BrokenPipeError:
                        pass

            worker = threading.Thread(target=write, daemon=True)
            worker.start()
            received = bytearray()
            while b"\r\n\r\n" not in received:
                chunk = client.recv(4096)
                assert chunk, received
                received.extend(chunk)
            assert received.startswith(b"HTTP/1.1 200"), received
            # Do not consume the body: the tiny receive window backpressures
            # the broadcast, with more input still waiting in the producer.
            assert worker.is_alive(), "producer finished before cancellation"
            process.send_signal(signal.SIGINT)
            process.wait(timeout=20)
            worker.join(timeout=5)
            assert not worker.is_alive(), "producer did not stop"
            process.stdin = None
            stdout, stderr = process.communicate(timeout=5)
            assert process.returncode != 0, (process.returncode, stdout, stderr)
            assert "pipeline was aborted before completion" in stderr, stderr
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        if worker is not None:
            worker.join(timeout=5)


for _ in range(3):
    _run_case()
print("cancelled backpressured broadcast")
