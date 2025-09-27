import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import NoReturn


def _pkg_bin_dir() -> Path:
    # Resolve to .../tenzir/bin next to this package.
    return Path(__file__).resolve().parent.parent / "bin"


def _candidate_paths(name: str) -> list[Path]:
    candidates: list[Path] = []
    bin_dir = _pkg_bin_dir()
    candidates.append(bin_dir / name)
    # Fallback to PATH if not packaged.
    path_hit = shutil.which(name)
    if path_hit:
        candidates.append(Path(path_hit))
    return candidates


def exec_binary(name: str) -> NoReturn:
    # Try packaged binary first, then PATH.
    for p in _candidate_paths(name):
        if p.is_file() and os.access(p, os.X_OK):
            os.execv(p.as_posix(), [p.name, *sys.argv[1:]])
    # Nothing found: emit a helpful error and non-zero exit.
    sys.stderr.write(
        f"{name} not found. On Linux wheels this should be bundled; otherwise ensure '{name}' is in PATH.\n"
    )
    sys.stderr.flush()
    raise SystemExit(127)
