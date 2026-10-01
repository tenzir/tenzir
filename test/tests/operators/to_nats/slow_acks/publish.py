# runner: python
"""Verify that slow publish acknowledgments apply backpressure to to_nats.

With `_max_pending=1`, every publish waits for the acknowledgment of the
previous one. Pausing the server holds that acknowledgment back for seconds,
which must delay the publish instead of failing it.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import time

PAUSE_SECONDS = 3.0
TIMEOUT_SECONDS = 60.0


def _resolve_tenzir_binary() -> str:
    binary = os.environ.get("TENZIR_BINARY") or shutil.which("tenzir")
    if binary:
        return binary
    raise RuntimeError("tenzir executable not found")


def _run_container(*args: str) -> None:
    result = subprocess.run(
        [os.environ["NATS_CONTAINER_RUNTIME"], *args],
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"failed to run {args}\nstderr:\n{result.stderr}")


def _stream_messages() -> int:
    cmd = [
        os.environ["NATS_CONTAINER_RUNTIME"],
        "run",
        "--rm",
        "--network",
        f"container:{os.environ['NATS_CONTAINER_ID']}",
        "natsio/nats-box:0.18.0",
        "nats",
        "--server",
        "nats://127.0.0.1:4222",
        "stream",
        "info",
        os.environ["NATS_STREAM"],
        "--json",
    ]
    result = subprocess.run(cmd, text=True, capture_output=True)
    if result.returncode != 0:
        raise RuntimeError(f"failed to inspect stream\nstderr:\n{result.stderr}")
    return int(json.loads(result.stdout)["state"]["messages"])


def main() -> None:
    pipeline = """
from_stdin {
  read_lines
}
to_nats env("NATS_SUBJECT"),
        url=env("NATS_URL"),
        message=line,
        _max_pending=1
""".strip()
    proc = subprocess.Popen(
        [
            _resolve_tenzir_binary(),
            "--bare-mode",
            "--console-verbosity=warning",
            "--nova=true",
            pipeline,
        ],
        text=True,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert proc.stdin
        proc.stdin.write("message-1\n")
        proc.stdin.flush()
        # Pause only once the first publish was acknowledged.
        deadline = time.monotonic() + TIMEOUT_SECONDS
        while _stream_messages() < 1:
            if proc.poll() is not None or time.monotonic() > deadline:
                raise RuntimeError("the first message was not published")
            time.sleep(0.2)
        container_id = os.environ["NATS_CONTAINER_ID"]
        _run_container("pause", container_id)
        try:
            proc.stdin.write("message-2\nmessage-3\n")
            proc.stdin.flush()
            time.sleep(PAUSE_SECONDS)
        finally:
            _run_container("unpause", container_id)
        # Closes stdin, which ends the input of the pipeline.
        stdout, stderr = proc.communicate(timeout=TIMEOUT_SECONDS)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.communicate()
    if proc.returncode != 0:
        raise RuntimeError(
            f"to_nats failed with exit code {proc.returncode}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )
    messages = _stream_messages()
    if messages != 3:
        raise RuntimeError(f"expected 3 published messages, got {messages}")
    print("published 3 messages despite slow acknowledgments")


if __name__ == "__main__":
    main()
