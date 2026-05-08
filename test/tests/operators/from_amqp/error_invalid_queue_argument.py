# runner: python
"""Verify unsupported queue argument values fail during pipeline compilation."""

from __future__ import annotations

import os
import shutil
import subprocess

PIPELINE = """
from_amqp "amqp://example.test/%2F",
          queue_arguments={
            "x-valid": 1,
            "x-invalid": {
              nested: true
            }
          }
""".strip()


def _resolve_tenzir_binary() -> str:
    env_val = os.environ.get("TENZIR_BINARY")
    if env_val:
        return env_val
    binary = shutil.which("tenzir")
    if binary:
        return binary
    raise RuntimeError("tenzir executable not found (set TENZIR_BINARY or add to PATH)")


def main() -> None:
    result = subprocess.run(
        [
            _resolve_tenzir_binary(),
            "--bare-mode",
            "--console-verbosity=warning",
            "--neo",
            PIPELINE,
        ],
        text=True,
        capture_output=True,
        check=False,
    )
    if result.returncode == 0:
        raise RuntimeError("from_amqp accepted an unsupported queue argument value")
    expected = "expected type `number`, `bool`, or `string` for queue argument"
    output = f"{result.stdout}\n{result.stderr}"
    if expected not in output:
        raise RuntimeError(
            "from_amqp did not report the expected queue argument diagnostic\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    print("invalid queue argument rejected")


if __name__ == "__main__":
    main()
