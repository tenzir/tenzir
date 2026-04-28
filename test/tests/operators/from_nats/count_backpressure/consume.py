# runner: python
"""Verify count applies to accepted messages, not callback deliveries."""

from __future__ import annotations

import re
import shutil
import subprocess

EXPECTED_COUNT = 50


def _resolve_tenzir_binary() -> str:
    binary = shutil.which("tenzir")
    if binary:
        return binary
    raise RuntimeError("tenzir executable not found")


def main() -> None:
    pipeline = f"""
from_nats env("NATS_SUBJECT"),
          url=env("NATS_URL"),
          durable=env("NATS_DURABLE"),
          count={EXPECTED_COUNT},
          _batch_size=1,
          _queue_capacity=1,
          _batch_timeout=100ms
select line = string(message)
""".strip()
    result = subprocess.run(
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
        timeout=60,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"from_nats failed with exit code {result.returncode}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    messages = re.findall(r"message-\d{4}", result.stdout)
    if len(messages) != EXPECTED_COUNT or len(set(messages)) != EXPECTED_COUNT:
        raise RuntimeError(
            f"expected {EXPECTED_COUNT} unique messages\n"
            f"messages: {messages}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    print(f"received {EXPECTED_COUNT} unique messages")


if __name__ == "__main__":
    main()
