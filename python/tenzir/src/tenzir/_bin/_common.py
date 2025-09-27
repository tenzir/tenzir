import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import NoReturn


def _pkg_bin_dir() -> Path:
    # Resolve to .../tenzir/bin next to this package.
    return Path(__file__).resolve().parent.parent / "tenzir" / "bin"


def _pkg_wheels_dir() -> Path:
    root = Path(__file__).resolve().parent.parent / "tenzir"
    return root / "share" / "tenzir" / "python"


def _candidate_paths(name: str) -> list[Path]:
    candidates: list[Path] = []
    bin_dir = _pkg_bin_dir()
    candidates.append(bin_dir / name)
    # Fallback to PATH if not packaged. We need to be careful to remove the
    # parent directory from the PATH to avoid an infinite self recursion on the
    # wrapper script generated from the python project.
    this_path = str(Path(sys.argv[0]).parent)
    cleanpath = [p for p in sys.path if p != this_path]
    path_hit = shutil.which(name, path=os.pathsep.join(cleanpath))
    if path_hit:
        candidates.append(Path(path_hit))
    return candidates


def exec_binary(name: str) -> NoReturn:
    # Try packaged binary first, then PATH.
    for p in _candidate_paths(name):
        if p.is_file() and os.access(p, os.X_OK):
            os.execv(p.as_posix(), [name, *sys.argv[1:]])
    _ = sys.stderr.write(
        f"{name} not found. On Linux wheels this should be bundled; otherwise ensure '{name}' is in PATH.\n",
    )
    sys.stderr.flush()
    raise SystemExit(127)
