# runner: python
# timeout: 60

from __future__ import annotations

import os
import shlex
import socket
import subprocess
import time


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def _wait_for_port(port: int, timeout: int = 20) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.1):
                return
        except OSError:
            time.sleep(0.1)
    raise RuntimeError("receiver did not listen on TCP port")


def _terminate(proc: subprocess.Popen[str], timeout: int = 5) -> tuple[str, str]:
    if proc.poll() is None:
        proc.terminate()
        try:
            return proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            proc.kill()
    return proc.communicate()


def _run_or_stop(proc: subprocess.Popen[str], timeout: int) -> tuple[str, str]:
    try:
        return proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        return _terminate(proc)


tenzir = shlex.split(os.environ["TENZIR_BINARY"])
tcp_port = _free_port()
receiver_env = {
    **os.environ,
    "TENZIR_NEO": "false",
}
receiver = subprocess.Popen(
    [
        *tenzir,
        (
            f'from "tcp://0.0.0.0:{tcp_port}" {{ read_bitz }}\n'
            "head 10\n"
            "sort x\n"
            "write_ndjson\n"
        ),
    ],
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
    text=True,
    env=receiver_env,
)
try:
    _wait_for_port(tcp_port)
    sender = subprocess.run(
        [
            *tenzir,
            "--neo",
            (
                "from {}\n"
                "repeat 10\n"
                "enumerate x\n"
                "repeat 2\n"
                "deduplicate x\n"
                f'to_tcp "127.0.0.1:{tcp_port}" {{ write_bitz }}\n'
            ),
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
        check=False,
    )
    assert sender.returncode == 0, sender.stderr
    stdout, stderr = _run_or_stop(receiver, timeout=10)
    assert receiver.returncode == 0, stderr
    assert "num_bytes > 0" not in stderr, (
        "legacy receiver logged assertion `num_bytes > 0`"
    )
    assert "unexpected internal error" not in stderr, (
        "legacy receiver logged an unexpected internal error"
    )
    print(stdout, end="")
finally:
    if receiver.poll() is None:
        _terminate(receiver)
