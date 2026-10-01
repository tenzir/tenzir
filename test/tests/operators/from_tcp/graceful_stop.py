# runner: python
# timeout: 60

"""A graceful stop ends `from_tcp` while its peer keeps the connection open.

Disabling the shutdown grace period rules out a force-cancel.
"""

from __future__ import annotations

import os
import queue
import shlex
import signal
import socket
import subprocess
import threading

with socket.create_server(("127.0.0.1", 0)) as server:
    server.settimeout(20)
    port = server.getsockname()[1]
    pipeline = f'from_tcp "127.0.0.1:{port}" {{ read_lines }}'
    process = subprocess.Popen(
        [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", pipeline],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "TENZIR_SHUTDOWN_GRACE_PERIOD": "0s"},
    )
    lines: queue.Queue[str] = queue.Queue()
    stdout: list[str] = []

    def read_stdout() -> None:
        assert process.stdout is not None
        for line in process.stdout:
            stdout.append(line)
            lines.put(line)

    reader = threading.Thread(target=read_stdout, daemon=True)
    reader.start()
    try:
        peer, _ = server.accept()
        with peer:
            peer.sendall(b"hello-before-stop\n")
            while "hello-before-stop" not in lines.get(timeout=20):
                pass
            process.send_signal(signal.SIGINT)
            _, stderr = process.communicate(timeout=20)
        reader.join(timeout=5)
        assert process.returncode == 0, (process.returncode, stderr)
        assert "hello-before-stop" in "".join(stdout), stdout
        assert "warning" not in stderr and "error" not in stderr, stderr
        print("stopped gracefully while the peer kept the connection open")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
