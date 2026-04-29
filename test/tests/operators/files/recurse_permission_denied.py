# runner: python
"""Verify unreadable directories are warned about during recursive traversal."""

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
    root = os.environ["FILES_PERMISSION_ROOT"]
    pipeline = (
        f'files "{root}", recurse=true\n'
        f'path = path.replace("{root}", "<ROOT>")\n'
        "select path, type\n"
        "sort path\n"
    )
    tenzir = _resolve_tenzir_binary()
    with tempfile.TemporaryDirectory(prefix="files-permission-") as tmpdir:
        pipeline_path = Path(tmpdir) / "pipeline.tql"
        pipeline_path.write_text(pipeline, encoding="utf-8")
        result = subprocess.run(
            [
                *tenzir,
                "--bare-mode",
                "--console-verbosity=quiet",
                "--multi",
                "--neo",
                "-f",
                str(pipeline_path),
            ],
            capture_output=True,
            text=True,
            timeout=30,
            env=os.environ.copy(),
        )
    combined = (result.stdout + result.stderr).replace(root, "<ROOT>")
    if result.returncode != 0:
        raise RuntimeError(
            f"pipeline failed with exit code {result.returncode}\n{combined}"
        )
    print(combined, end="")


if __name__ == "__main__":
    main()
