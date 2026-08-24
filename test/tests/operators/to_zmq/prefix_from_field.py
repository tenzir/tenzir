# fixtures: [{zmq: {role: subscriber, mode: bind}}]
# skip: {on: fixture-unavailable}
# timeout: 60
# runner: python
"""Verify that ``to_zmq`` prepends a per-event prefix from a field.

The pipeline publishes in a loop instead of sending a fixed number of
events: a PUB socket drops messages until the subscriber's subscription
has propagated, which happens after the connection handshake that
``monitor=true`` waits for. See TNZ-978.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import time
from pathlib import Path

MARKER = 'alerts/{\n  "channel": "alerts/",\n  "line": "hello-from-to_zmq-prefix"\n}'
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
    channel: "alerts/",
    line: "hello-from-to_zmq-prefix",
  }
}
to_zmq env("ZMQ_ENDPOINT"), encoding="json", prefix=channel, monitor=true
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
                print("ok: delivered prefixed JSON after subscriber registration")
                return
            if process.poll() is not None:
                _, stderr = process.communicate()
                raise AssertionError(
                    f"to_zmq exited with {process.returncode}: {stderr}"
                )
            time.sleep(0.05)
        raise AssertionError("to_zmq did not deliver prefixed JSON within 20 seconds")
    finally:
        _terminate(process)


if __name__ == "__main__":
    main()
