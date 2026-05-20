# runner: python
"""Verify that `watch=duration` picks up files added after the initial scan."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
import sys
from pathlib import Path


def _resolve_tenzir_binary() -> tuple[str, ...]:
    env_val = os.environ.get("TENZIR_BINARY")
    if env_val:
        return tuple(shlex.split(env_val))
    which_result = shutil.which("tenzir")
    if which_result:
        return (which_result,)
    raise RuntimeError("tenzir executable not found (set TENZIR_BINARY or add to PATH)")


def _read_event(proc: subprocess.Popen[str]) -> dict:
    """Read one JSON line from tenzir stdout, failing fast on early exit."""
    line = proc.stdout.readline()  # type: ignore[union-attr]
    if not line:
        stderr = proc.stderr.read() if proc.stderr else ""  # type: ignore[union-attr]
        raise RuntimeError(f"tenzir exited before producing event: {stderr}")
    return json.loads(line)


def main() -> None:
    tenzir = _resolve_tenzir_binary()
    watch_dir = Path(os.environ["FILE_ROOT"]) / "watch_dynamic"
    watch_dir.mkdir(parents=True, exist_ok=True)
    # Start with one file so the initial scan has something to emit.
    (watch_dir / "a.json").write_text(json.dumps({"name": "a"}) + "\n")
    # Watch at 50 ms so the second poll fires quickly enough to keep the
    # overall test well under the 30 s suite timeout.
    pipeline = (
        f'from_file "{watch_dir}/*.json", watch=50ms {{ read_json }} '
        "| select name | write_ndjson"
    )
    proc = subprocess.Popen(
        [*tenzir, "--bare-mode", "--console-verbosity=error", pipeline],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        first = _read_event(proc)
        # Drop in a new file; the watcher's next poll should pick it up.
        (watch_dir / "b.json").write_text(json.dumps({"name": "b"}) + "\n")
        second = _read_event(proc)
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=5)
    names = sorted([first["name"], second["name"]])
    print(f"names: {names}")
    assert names == ["a", "b"], f"unexpected events: {names}"
    print("ok")


if __name__ == "__main__":
    main()
