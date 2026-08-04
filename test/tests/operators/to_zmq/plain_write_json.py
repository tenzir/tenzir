# fixtures: [{zmq: {role: subscriber, mode: bind}}]
# skip: {on: fixture-unavailable}
# timeout: 60
# runner: python
"""Verify that ``to_zmq`` delivers JSON after SUB registration."""

from __future__ import annotations

import os
import shlex
import subprocess
import time
from pathlib import Path

MARKER = '{\n  "line": "hello-from-to_zmq-plain"\n}'
DELIVERY_TIMEOUT = 20


def _terminate(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def main() -> None:
    tenzir = shlex.split(os.environ["TENZIR_BINARY"])
    capture = Path(os.environ["ZMQ_FILE"])
    pipeline = """
every 10ms {
  from {
    line: "hello-from-to_zmq-plain",
  }
}
to_zmq env("ZMQ_ENDPOINT"), encoding="json", monitor=true
"""
    process = subprocess.Popen(
        [*tenzir, "--bare-mode", "--console-verbosity=warning", pipeline],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + DELIVERY_TIMEOUT
        while time.monotonic() < deadline:
            if MARKER in capture.read_text(errors="replace"):
                print("ok: delivered JSON after subscriber registration")
                return
            if process.poll() is not None:
                _, stderr = process.communicate()
                raise AssertionError(
                    f"to_zmq exited with {process.returncode}: {stderr}"
                )
            time.sleep(0.05)
        raise AssertionError("to_zmq did not deliver JSON within 20 seconds")
    finally:
        _terminate(process)


if __name__ == "__main__":
    main()
