# runner: python
"""Verify that an early downstream stop leaves no JetStream message unsettled.

`head 1` stops `from_nats` through the executor's control plane, which may
reach `from_nats` only after it emitted the next message. That message counts
as delivered and is acknowledged, even though `head` drops it. Every message
that `from_nats` did not emit must be NAKed on teardown, so that it is
immediately available again instead of after the ACK wait of 30 seconds.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess


def _resolve_tenzir_binary() -> str:
    binary = os.environ.get("TENZIR_BINARY") or shutil.which("tenzir")
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


def _ack_floor() -> int:
    info = _run_nats_cli(
        [
            "consumer",
            "info",
            os.environ["NATS_STREAM"],
            os.environ["NATS_DURABLE"],
            "--json",
        ]
    )
    if info.returncode != 0:
        raise RuntimeError(f"failed to inspect consumer\nstderr:\n{info.stderr}")
    return int(json.loads(info.stdout)["ack_floor"]["stream_seq"])


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
            "--nova=true",
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
    if "message-0001" not in first.stdout or "message-0002" in first.stdout:
        raise RuntimeError(
            "first pipeline did not emit exactly the first message\n"
            f"stdout:\n{first.stdout}\n"
            f"stderr:\n{first.stderr}"
        )
    ack_floor = _ack_floor()
    if ack_floor < 1:
        raise RuntimeError("message-0001 was not acknowledged")
    if ack_floor < 2:
        # Wait well below the ACK wait, so that only a NAKed message arrives.
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
        if "message-0002" not in remaining.stdout:
            raise RuntimeError(
                "message-0002 was neither acknowledged nor available after "
                "downstream early stop\n"
                f"stdout:\n{remaining.stdout}\n"
                f"stderr:\n{remaining.stderr}"
            )
    print("no message left unsettled")


if __name__ == "__main__":
    main()
