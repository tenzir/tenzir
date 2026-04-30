# runner: python
"""Verify early downstream stop does not ACK messages not emitted by from_nats."""

from __future__ import annotations

import os
import shutil
import subprocess


def _resolve_tenzir_binary() -> str:
    binary = shutil.which("tenzir")
    if binary:
        return binary
    raise RuntimeError("tenzir executable not found")


def _run_nats_cli(args: list[str]) -> subprocess.CompletedProcess[str]:
    runtime = os.environ["NATS_CONTAINER_RUNTIME"]
    container_id = os.environ["NATS_CONTAINER_ID"]
    cmd = [
        runtime,
        "run",
        "--rm",
        "--network",
        f"container:{container_id}",
        "natsio/nats-box:0.18.0",
        "nats",
        "--server",
        "nats://127.0.0.1:4222",
        *args,
    ]
    return subprocess.run(cmd, text=True, capture_output=True)


def main() -> None:
    # Use single-message batches to make the downstream stop boundary coincide
    # with a NATS ACK boundary.
    pipeline = """
from_nats env("NATS_SUBJECT"),
          url=env("NATS_URL"),
          durable=env("NATS_DURABLE"),
          _batch_size=1,
          _queue_capacity=1,
          _batch_timeout=5s
head 1
select line = string(message)
""".strip()
    first = subprocess.run(
        [
            _resolve_tenzir_binary(),
            "--bare-mode",
            "--console-verbosity=warning",
            "--multi",
            "--neo",
            pipeline,
        ],
        text=True,
        capture_output=True,
    )
    if first.returncode != 0:
        raise RuntimeError(
            f"from_nats failed with exit code {first.returncode}\n"
            f"stdout:\n{first.stdout}\n"
            f"stderr:\n{first.stderr}"
        )
    if "message-0001" not in first.stdout:
        raise RuntimeError(
            "first pipeline did not emit the first message\n"
            f"stdout:\n{first.stdout}\n"
            f"stderr:\n{first.stderr}"
        )
    outputs = []
    for _ in range(2):
        remaining = _run_nats_cli(
            [
                "consumer",
                "next",
                os.environ["NATS_STREAM"],
                os.environ["NATS_DURABLE"],
                "--raw",
                "--count",
                "1",
                "--wait",
                "5s",
                "--ack",
            ]
        )
        outputs.append(remaining.stdout)
        if "message-0002" in remaining.stdout:
            break
        if remaining.returncode != 0:
            break
    remaining_output = "".join(outputs)
    if "message-0001" in remaining_output:
        raise RuntimeError(
            "message-0001 was redelivered after downstream early stop\n"
            f"stdout:\n{remaining_output}\n"
        )
    if "message-0002" not in remaining_output:
        raise RuntimeError(
            "message-0002 was not available after downstream early stop\n"
            f"stdout:\n{remaining_output}\n"
        )
    print("message-0002 remains available")


if __name__ == "__main__":
    main()
