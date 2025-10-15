import os
import shutil
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


def _prepare_environment() -> None:
    sites = [x for x in sys.path if x.endswith("/site-packages")]
    if "PYTHONPATH" in os.environ:
        os.environ["PYTHONPATH"] += os.pathsep + os.pathsep.join(sites)
    else:
        os.environ["PYTHONPATH"] = os.pathsep.join(sites)
    _ = os.environ.setdefault(
        "UV_PYTHON",
        sys.executable,
    )


def exec_binary(name: str) -> NoReturn:
    _prepare_environment()
    # Try packaged binary first, then PATH.
    for p in _candidate_paths("tenzir"):
        if p.is_file() and os.access(p, os.X_OK):
            os.execv(p.as_posix(), [p.name, *sys.argv[1:]])
    # Nothing found: emit a helpful error and non-zero exit.
    _ = sys.stderr.write(
        f"{name} not found. On Linux wheels this should be bundled; otherwise ensure '{name}' is in PATH.\n",
    )
    _ = sys.stderr.flush()
    raise SystemExit(127)
