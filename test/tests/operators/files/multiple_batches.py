# runner: python
"""Verify a listing continues after emitting a full batch."""

from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tempfile
from pathlib import Path


def _resolve_tenzir_binary() -> tuple[str, ...]:
    env_val = os.environ.get("TENZIR_BINARY")
    if env_val:
        return tuple(shlex.split(env_val))
    which_result = shutil.which("tenzir")
    if which_result:
        return (which_result,)
    raise RuntimeError("tenzir executable not found (set TENZIR_BINARY or add to PATH)")


def main() -> None:
    with tempfile.TemporaryDirectory(prefix="files-batches-") as tmpdir:
        root = Path(tmpdir)
        for index in range(8193):
            (root / str(index)).touch()
        result = subprocess.run(
            [
                *_resolve_tenzir_binary(),
                "--bare-mode",
                "--nova=true",
                "--console-verbosity=quiet",
                "--multi",
                f'files "{root}" | where type == "regular" | summarize count=count()',
            ],
            capture_output=True,
            text=True,
            timeout=30,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr)
        if "8193" not in result.stdout:
            raise AssertionError(f"expected 8193 files, got: {result.stdout}")
        print("8193 files")


if __name__ == "__main__":
    main()
