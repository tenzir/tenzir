from __future__ import annotations

import os
import sys
import shlex
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path
from typing import List, Tuple

from setuptools import setup
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.sdist import sdist as _sdist

DEFAULT_ATTR = "tenzir-static"
ENV_SKIP = "TENZIR_SKIP_NIX_BUILD"
ENV_ATTR = "TENZIR_NIX_ATTR"
ENV_CMD = "TENZIR_NIX_BUILD_CMD"
PACKAGE_NAME = "tenzir"
ASSET_DIRS = ("bin", "libexec", "share")


def list_files(startpath):
    for root, dirs, files in os.walk(startpath):
        level = root.replace(startpath, '').count(os.sep)
        indent = ' ' * 4 * (level)
        print('{}{}/'.format(indent, os.path.basename(root)), file=sys.stderr)
        subindent = ' ' * 4 * (level + 1)
        for f in files:
            print('{}{}'.format(subindent, f), file=sys.stderr)


def _run_nix() -> List[Tuple[Path, bool]]:
    """Run nix build and stage artefacts into the source tree.

    Returns a list of (path, existed_before) for later cleanup.
    """

    if os.environ.get(ENV_SKIP):
        return []

    root = Path(__file__).resolve().parent
    print(root, file=sys.stderr)
    list_files(str(root))

    source = root / "src" / PACKAGE_NAME
    #if (source / "bin").exists():
    if False:
        package_root = Path(__file__).resolve().parent / "src" / PACKAGE_NAME
        for relative in ASSET_DIRS:
           src_dir = source / relative
           dest_dir = package_root / relative
           existed = dest_dir.exists()
           if src_dir.exists():
               if existed:
                   shutil.rmtree(dest_dir)
               shutil.copytree(src_dir, dest_dir, symlinks=True)
               staged.append((dest_dir, existed))
               produced.add(relative)
        #pass
        #shutil.copytree(prebuilt, dest_dir, symlinks=True)
        return []

    repo_root_env = os.environ.get("TENZIR_REPO_ROOT")
    repo_root = (
        Path(repo_root_env) if repo_root_env else Path(__file__).resolve().parents[2]
    )
    attr = os.environ.get(ENV_ATTR, DEFAULT_ATTR)
    custom_cmd = os.environ.get(ENV_CMD)

    if custom_cmd:
        args: List[str] = shlex.split(custom_cmd)
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
        nix_completed = subprocess.run(args, check=True, cwd=repo_root, stdout=subprocess.PIPE)
    except FileNotFoundError as exc:
        raise RuntimeError(
            "nix command not found; export TENZIR_SKIP_NIX_BUILD=1 to bypass"
        ) from exc

    result_path = Path(nix_completed.stdout.decode("UTF-8").rstrip())
    print(f"result_path: {result_path}", file=sys.stderr)
    if not result_path.exists():
        raise RuntimeError("The nix build output path does not exist")

    package_root = Path(__file__).resolve().parent / "src" / PACKAGE_NAME
    staged: List[Tuple[Path, bool]] = []

    with tarfile.open(f'{result_path}/tenzir-5.16.0-linux-static.tar.gz') as file:
        tmpdir = tempfile.mkdtemp()
        file.extractall(tmpdir)
        dirs =(Path(tmpdir) / "opt" / "tenzir").glob("*")
        for src_dir in dirs:
            dest_dir = package_root / src_dir.name
            existed = dest_dir.exists()
            if existed:
                shutil.rmtree(dest_dir)
            shutil.copytree(src_dir, dest_dir, symlinks=True)
            staged.append((dest_dir, existed))

    return staged


def _cleanup(staged: List[Tuple[Path, bool]]) -> None:
    for path, existed in staged:
        if not existed:
            shutil.rmtree(path, ignore_errors=True)


class build_py(_build_py):
    def run(self) -> None:
        print(f"root = {self.distribution.src_root}", file=sys.stderr)
        staged = _run_nix()
        try:
            super().run()
        finally:
            _cleanup(staged)


class sdist(_sdist):
    def run(self) -> None:
        staged = _run_nix()
        try:
            super().run()
        finally:
            _cleanup(staged)
            shutil.rmtree("src/tenzir.egg-info", ignore_errors=True)



setup(cmdclass={"build_py": build_py, "sdist": sdist})
