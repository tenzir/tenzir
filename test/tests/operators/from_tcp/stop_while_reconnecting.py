# runner: python
# timeout: 60

"""A graceful stop ends `from_tcp`'s attempts to reconnect.

The sleeping parser keeps draining the first connection, so the stop arrives
while `from_tcp` retries. Disabling the shutdown grace period rules out a
force-cancel.
"""

from __future__ import annotations

import os
import queue
import shlex
import signal
import socket
import subprocess
import threading
import time

pipeline_template = """
from_tcp "127.0.0.1:{port}" {{
  shell "cat; sleep 5"
  read_lines
}}
"""


def stream_lines(stream, sink: queue.Queue[str], log: list[str]) -> None:
    for line in stream:
        log.append(line)
        sink.put(line)


def wait_for(sink: queue.Queue[str], needle: str) -> None:
    deadline = time.monotonic() + 20
    while needle not in sink.get(timeout=max(0.0, deadline - time.monotonic())):
        pass


server = socket.create_server(("127.0.0.1", 0))
server.settimeout(20)
port = server.getsockname()[1]
process = subprocess.Popen(
    [
        *shlex.split(os.environ["TENZIR_BINARY"]),
        "--bare-mode",
        pipeline_template.format(port=port),
    ],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    env={**os.environ, "TENZIR_SHUTDOWN_GRACE_PERIOD": "0s"},
)
stdout_lines: queue.Queue[str] = queue.Queue()
stderr_lines: queue.Queue[str] = queue.Queue()
stdout: list[str] = []
stderr: list[str] = []
readers = [
    threading.Thread(
        target=stream_lines, args=(process.stdout, stdout_lines, stdout), daemon=True
    ),
    threading.Thread(
        target=stream_lines, args=(process.stderr, stderr_lines, stderr), daemon=True
    ),
]
for reader in readers:
    reader.start()
try:
    peer, _ = server.accept()
    with peer:
        peer.sendall(b"hello-before-stop\n")
    # Refuse connections, so that `from_tcp` keeps retrying.
    server.close()
    wait_for(stdout_lines, "hello-before-stop")
    wait_for(stderr_lines, "failed to connect")
    process.send_signal(signal.SIGINT)
    server = socket.create_server(("127.0.0.1", port))
    server.settimeout(0.1)
    late_connections = 0
    while process.poll() is None:
        try:
            late, _ = server.accept()
        except socket.timeout:
            continue
        late.close()
        late_connections += 1
    process.wait(timeout=20)
    for reader in readers:
        reader.join(timeout=5)
    assert process.returncode == 0, (process.returncode, "".join(stderr))
    assert "hello-before-stop" in "".join(stdout), stdout
    assert late_connections == 0, f"{late_connections} connection(s) after stop"
    print("stopped reconnecting after the stop request")
finally:
    server.close()
    if process.poll() is None:
        process.kill()
        process.wait(timeout=5)
