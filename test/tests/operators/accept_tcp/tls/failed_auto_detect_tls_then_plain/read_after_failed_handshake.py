# runner: python
"""Verify a bad TLS-looking client does not stop auto-detect TCP accepts."""

from __future__ import annotations

import os
import re
import shutil
import subprocess


def _resolve_tenzir_binary() -> str:
    binary = shutil.which("tenzir")
    if binary:
        return binary
    raise RuntimeError("tenzir executable not found")


def main() -> None:
    pipeline = r"""
let $tls = {
  certfile: env("TCP_CERTFILE"),
  keyfile: env("TCP_KEYFILE"),
  skip_peer_verification: true,
}

accept_tcp env("TCP_ENDPOINT"), tls=$tls, auto_detect_tls=true {
  read_lines
}
head 1
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
        check=False,
        env=os.environ,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"accept_tcp failed with exit code {result.returncode}\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    expected_output = '{\n  line: "foo",\n}'
    if expected_output not in result.stdout:
        raise AssertionError(
            "expected accepted plaintext output\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    combined_output = f"{result.stdout}\n{result.stderr}"
    expected_warning = re.compile(
        r"warning: TLS handshake failed.*"
        r"TLS handshake with peer 127\.0\.0\.1:\d+ failed",
        re.DOTALL,
    )
    if not expected_warning.search(combined_output):
        raise AssertionError(
            "expected failed TLS handshake warning\n"
            f"stdout:\n{result.stdout}\n"
            f"stderr:\n{result.stderr}"
        )
    print(expected_output)


if __name__ == "__main__":
    main()
