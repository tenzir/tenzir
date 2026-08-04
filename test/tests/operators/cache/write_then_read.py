# runner: python
# timeout: 60

"""Write events into a named cache and read them back in a later invocation.

This is the primary cross-pipeline use of `cache`: one pipeline populates the
cache with `mode="write"` (a sink), and a separate pipeline later retrieves the
events with `mode="read"` (a source). Both invocations target the same shared
node provided by the `node` fixture, so the cache created by the writer is still
available to the reader.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys

CACHE_ID = "write-then-read"


def _tenzir(args: list[str]) -> subprocess.CompletedProcess[str]:
    binary = shlex.split(os.environ["TENZIR_NODE_CLIENT_BINARY"])
    endpoint = os.environ["TENZIR_NODE_CLIENT_ENDPOINT"]
    cmd = [
        *binary,
        "--bare-mode",
        "--console-verbosity=warning",
        f"--endpoint={endpoint}",
        *args,
    ]
    return subprocess.run(cmd, capture_output=True, text=True, check=False)


def main() -> int:
    # Populate the cache. `mode="write"` produces no output.
    write = _tenzir([f'from {{x: 10}}, {{x: 20}} | cache "{CACHE_ID}", mode="write"'])
    if write.returncode != 0:
        _ = sys.stderr.write(write.stderr)
        return write.returncode
    if write.stdout:
        _ = sys.stderr.write(f"unexpected writer output: {write.stdout!r}\n")
        return 1
    # Read the cache back in a separate invocation.
    read = _tenzir([f'cache "{CACHE_ID}", mode="read" | sort x'])
    if read.returncode != 0:
        _ = sys.stderr.write(read.stderr)
        return read.returncode
    _ = sys.stdout.write(read.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
