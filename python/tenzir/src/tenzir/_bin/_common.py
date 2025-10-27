import os
import shutil
import sys
from pathlib import Path
from typing import NoReturn


def _pkg_bin_dir() -> Path:
    # Resolve to .../tenzir/bin next to this package.
    return Path(__file__).resolve().parent.parent / "bundled" / "bin"


def _pkg_wheels_dir() -> Path:
    root = Path(__file__).resolve().parent.parent / "bundled"
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
    wheels = _pkg_wheels_dir().glob("*.whl")
    value = f"--no-deps --offline {' '.join([str(wheel) for wheel in wheels])}"
    os.environ["TENZIR_PLUGINS__PYTHON__IMPLICIT_REQUIREMENTS"] = value


def exec_binary(name: str) -> NoReturn:
    # We don't support calling this helper outside of the generated tenzir(*) wrappers
    # from the [project.scripts] in pyproject.toml.
    _prepare_environment()
    # Try packaged binary first, then PATH.
    for p in _candidate_paths("tenzir"):
        if p.is_file() and os.access(p, os.X_OK):
            os.execv(p.as_posix(), [name, *sys.argv[1:]])
    _ = sys.stderr.write(
        f"{name} not found. On Linux wheels this should be bundled; otherwise ensure '{name}' is in PATH.\n",
    )
    _ = sys.stderr.flush()
    raise SystemExit(127)
