# runner: python
"""Verify pending ACKs are not delayed by the NATS batch timeout."""

from __future__ import annotations

import os
import shutil
import subprocess
import time


def _resolve_tenzir_binary() -> str:
    binary = shutil.which("tenzir")
    if binary:
        return binary
    raise RuntimeError("tenzir executable not found")


def _run_nats_cli(
    args: list[str], *, check: bool = True
) -> subprocess.CompletedProcess[str]:
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
    result = subprocess.run(cmd, text=True, capture_output=True)
    if check and result.returncode != 0:
        raise RuntimeError(
            "failed to run NATS CLI command\n"
            f"command: {' '.join(cmd)}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    return result


def _terminate(proc: subprocess.Popen[str]) -> tuple[str, str]:
    if proc.poll() is None:
        proc.terminate()
        try:
            return proc.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
    return proc.communicate(timeout=5)


def main() -> None:
    _run_nats_cli(
        [
            "consumer",
            "add",
            os.environ["NATS_STREAM"],
            os.environ["NATS_DURABLE"],
            "--pull",
            "--filter",
            os.environ["NATS_SUBJECT"],
            "--deliver",
            "all",
            "--ack",
            "explicit",
            "--wait",
            "100ms",
            "--max-deliver",
            "5",
            "--defaults",
        ]
    )
    pipeline = """
from_nats env("NATS_SUBJECT"),
          url=env("NATS_URL"),
          durable=env("NATS_DURABLE"),
          _batch_size=10,
          _queue_capacity=10,
          _batch_timeout=5s
select line = string(message)
discard
""".strip()
    proc = subprocess.Popen(
        [
            _resolve_tenzir_binary(),
            "--bare-mode",
            "--console-verbosity=warning",
            "--multi",
            "--neo",
            pipeline,
        ],
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    stdout = ""
    stderr = ""
    try:
        time.sleep(2)
        stdout, stderr = _terminate(proc)
    finally:
        if proc.poll() is None:
            stdout, stderr = _terminate(proc)
    if proc.returncode not in {0, -15}:
        raise RuntimeError(
            f"from_nats failed with exit code {proc.returncode}\n"
            f"stdout:\n{stdout}\n"
            f"stderr:\n{stderr}"
        )
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
            "1s",
            "--ack",
        ],
        check=False,
    )
    if "message-0001" in remaining.stdout:
        raise RuntimeError(
            "message was redelivered after the ACK grace period\n"
            f"stdout:\n{remaining.stdout}\n"
            f"stderr:\n{remaining.stderr}"
        )
    print("message acknowledged before redelivery")


if __name__ == "__main__":
    main()
