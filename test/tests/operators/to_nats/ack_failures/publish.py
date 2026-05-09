# runner: python
"""Verify multiple async publish ACK failures are counted."""

from __future__ import annotations

import os
import shutil
import subprocess


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


def main() -> None:
    subject = f"{os.environ['NATS_SUBJECT']}.reject"
    _run_nats_cli(
        [
            "stream",
            "add",
            "TO_NATS_ACK_FAILURES_REJECT",
            "--subjects",
            subject,
            "--storage",
            "file",
            "--retention",
            "limits",
            "--discard",
            "old",
            "--max-msg-size",
            "1",
            "--defaults",
        ]
    )
    pipeline = f"""
from {{message: "one"}},
     {{message: "two"}},
     {{message: "three"}},
     {{message: "four"}},
     {{message: "five"}}
to_nats "{subject}",
        message=this.message,
        url=env("NATS_URL"),
        _max_pending=5
""".strip()
    result = subprocess.run(
        [
            _resolve_tenzir_binary(),
            "--bare-mode",
            "--console-verbosity=warning",
            "--multi",
            pipeline,
        ],
        text=True,
        capture_output=True,
    )
    expected = "5 NATS publish acknowledgments failed"
    expected_fragments = [
        expected,
        "first error: Error: message size exceeds maximum allowed",
        "JetStream error code: 10054",
    ]
    if result.returncode == 0:
        raise RuntimeError(
            "to_nats succeeded despite rejected publish acknowledgments\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    output = result.stdout + result.stderr
    missing = [fragment for fragment in expected_fragments if fragment not in output]
    if missing:
        raise RuntimeError(
            f"expected diagnostic to contain {missing!r}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    print(expected)


if __name__ == "__main__":
    main()
