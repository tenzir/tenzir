# runner: python
"""Verify idle JetStream fetch completions do not end from_nats."""

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


def _run_nats_cli(args: list[str]) -> None:
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
    if result.returncode != 0:
        raise RuntimeError(
            "failed to run NATS CLI command\n"
            f"command: {' '.join(cmd)}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )


def _terminate(proc: subprocess.Popen[str]) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=5)


def main() -> None:
    pipeline = """
from_nats env("NATS_SUBJECT"),
          url=env("NATS_URL"),
          durable=env("NATS_DURABLE") {
  read_lines
}
head 1
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
    try:
        time.sleep(7.0)
        if proc.poll() is not None:
            stdout, stderr = proc.communicate()
            raise RuntimeError(
                "from_nats exited before a delayed message was published\n"
                f"stdout:\n{stdout}\n"
                f"stderr:\n{stderr}"
            )
        _run_nats_cli(["pub", os.environ["NATS_SUBJECT"], "late-message"])
        stdout, stderr = proc.communicate(timeout=20)
        if proc.returncode != 0:
            raise RuntimeError(
                f"from_nats failed with exit code {proc.returncode}\n"
                f"stdout:\n{stdout}\n"
                f"stderr:\n{stderr}"
            )
        print(stdout.strip())
    finally:
        _terminate(proc)


if __name__ == "__main__":
    main()
