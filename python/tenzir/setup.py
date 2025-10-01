from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.sdist import sdist as _sdist
from typing import override

DEFAULT_ATTR = "tenzir-static"
ENV_SKIP = "TENZIR_SKIP_NIX_BUILD"
ENV_ATTR = "TENZIR_NIX_ATTR"
ENV_CMD = "TENZIR_NIX_BUILD_CMD"
PACKAGE_NAME = "tenzir"
ASSET_DIRS = ("bin", "libexec", "share")
PACKAGE_ROOT = Path(__file__).resolve().parent / "src" / PACKAGE_NAME


def _assets_present() -> bool:
    return all((PACKAGE_ROOT / entry).exists() for entry in ASSET_DIRS)


def _run_nix() -> list[Path]:
    """Run nix build and stage artefacts into the source tree."""
    if os.environ.get(ENV_SKIP):
        return []

    repo_root_env = os.environ.get("TENZIR_REPO_ROOT")
    repo_root = (
        Path(repo_root_env)
        if repo_root_env
        else Path(__file__).resolve().parents[2]
    )
    attr = os.environ.get(ENV_ATTR, DEFAULT_ATTR)
    custom_cmd = os.environ.get(ENV_CMD)

    if custom_cmd:
        args: list[str] = shlex.split(custom_cmd)
    else:
        args = [
            "nix",
            "-L",
            "build",
            "--no-link",
            "--print-out-paths",
            f"{repo_root}#{attr}^package",
        ]

    try:
        nix_completed = subprocess.run(
            args,
            check=True,
            cwd=repo_root,
            stdout=subprocess.PIPE,
        )
    except FileNotFoundError as exc:
        msg = "nix command not found; export TENZIR_SKIP_NIX_BUILD=1 to bypass"
        raise RuntimeError(
            msg,
        ) from exc

    result_path = Path(nix_completed.stdout.decode("utf-8").splitlines()[-1])
    if not result_path.exists():
        msg = f"nix build did not produce an output at {result_path}"
        raise RuntimeError(
            msg,
        )

    staged: list[Path] = []

    tarballs = list(result_path.glob("tenzir-*-static.tar.gz"))
    if not tarballs:
        msg = f"nix build output at {result_path} does not contain a tarball"
        raise RuntimeError(
            msg,
        )
    if len(tarballs) != 1:
        msg = f"nix build output at {result_path} contains more than one tarball"
        raise RuntimeError(
            msg,
        )

    with tempfile.TemporaryDirectory() as tmp_dir:
        tarball = tarballs[0]
        with tarfile.open(tarball) as archive:
            archive.extractall(tmp_dir)

        extracted_root = Path(tmp_dir) / "opt" / "tenzir"
        for child in (extracted_root / "bin").iterdir():
            if child.is_symlink():
                child.unlink()

        for src_dir in extracted_root.iterdir():
            dest_dir = PACKAGE_ROOT / src_dir.name
            _ = shutil.copytree(src_dir, dest_dir, symlinks=True)
            staged.append(dest_dir)

    return staged


def _cleanup(staged: list[Path]) -> None:
    for path in staged:
        shutil.rmtree(path, ignore_errors=True)


class BuildPy(_build_py):
    @override
    def run(self) -> None:
        staged: list[Path] = []
        if not _assets_present():
            staged = _run_nix()
        try:
            super().run()
        finally:
            _cleanup(staged)


class Sdist(_sdist):
    @override
    def run(self) -> None:
        staged: list[Path] = []
        if not _assets_present():
            staged = _run_nix()
        try:
            super().run()
        finally:
            _cleanup(staged)


_ = setup(cmdclass={"build_py": BuildPy, "sdist": Sdist})
