from __future__ import annotations

import os
import shlex
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

try:
    import platform
except ImportError:  # pragma: no cover - platform is part of stdlib
    platform = None

from distutils import log
from typing import override

from setuptools import Distribution, setup
from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel
from setuptools.command.build_py import build_py as _build_py
from setuptools.command.sdist import sdist as _sdist

DEFAULT_ATTR = "tenzir-static"
ENV_SKIP = "TENZIR_SKIP_NIX_BUILD"
ENV_ATTR = "TENZIR_NIX_ATTR"
ENV_CMD = "TENZIR_NIX_BUILD_CMD"
ENV_PLAT = "TENZIR_WHEEL_PLATFORM"
ENV_SIGNED_TARBALL = "TENZIR_SIGNED_TARBALL_PATH"
PACKAGE_NAME = "tenzir"
ASSET_DIRS = ("bin", "libexec", "share")
REPO_DIR = Path(__file__).resolve().parent
PACKAGE_ROOT = REPO_DIR / "src" / PACKAGE_NAME
BUILD_DIR = REPO_DIR / "build"


def _assets_present() -> bool:
    return all((PACKAGE_ROOT / entry).exists() for entry in ASSET_DIRS)


def _extract_tarball(tarball: Path) -> None:
    """Extract a tenzir tarball and stage its contents into the source tree."""
    with tempfile.TemporaryDirectory() as tmp_dir:
        with tarfile.open(tarball) as archive:
            archive.extractall(tmp_dir, filter=tarfile.tar_filter)

        extracted_root = Path(tmp_dir) / "opt" / "tenzir"
        for child in (extracted_root / "bin").iterdir():
            if child.is_symlink():
                child.unlink()

        shutil.rmtree(PACKAGE_ROOT / "bundled", ignore_errors=True)
        _ = shutil.copytree(extracted_root, PACKAGE_ROOT / "bundled")


def _run_nix() -> list[Path]:
    """Run nix build and stage artefacts into the source tree."""
    if os.environ.get(ENV_SKIP):
        return []

    # Check for pre-built signed tarball (used in CI for macOS)
    signed_tarball_path = os.environ.get(ENV_SIGNED_TARBALL)
    if signed_tarball_path:
        tarball = Path(signed_tarball_path)
        if not tarball.exists():
            msg = f"Signed tarball not found at {tarball}"
            raise RuntimeError(msg)
        log.info(f"Using pre-built signed tarball: {tarball}")
        _extract_tarball(tarball)
        return []

    repo_root_env = os.environ.get("TENZIR_REPO_ROOT")
    repo_root = (
        Path(repo_root_env) if repo_root_env else Path(__file__).resolve().parents[2]
    )
    if not (repo_root / "flake.nix").is_file():
        msg = (
            "The staged source tree does not contain the Tenzir repository; "
            "building wheels from the sdist is not supported. "
            "Please run `uv build --wheel` from a repository checkout or set "
            "TENZIR_REPO_ROOT to the project root."
        )
        raise RuntimeError(msg)
    attr = os.environ.get(ENV_ATTR, DEFAULT_ATTR)
    custom_cmd = os.environ.get(ENV_CMD)

    if custom_cmd:
        args: list[str] = shlex.split(custom_cmd)
    else:
        args = [
            "nix",
            "--accept-flake-config",
            "--print-build-logs",
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

    _extract_tarball(tarballs[0])
    return []


def _cleanup(staged: list[Path]) -> None:
    for path in staged:
        shutil.rmtree(path, ignore_errors=True)


def _cleanup_build_dir() -> None:
    shutil.rmtree(BUILD_DIR, ignore_errors=True)


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
        try:
            super().run()
        finally:
            _cleanup_build_dir()


class BinaryDistribution(Distribution):
    """Distribution that forces a platform-specific wheel."""

    @override
    def has_ext_modules(self) -> bool:
        return True


class BdistWheel(_bdist_wheel):
    """Emit platform-specific wheels and allow overriding the tag."""

    @override
    def finalize_options(self) -> None:
        plat_override = os.environ.get(ENV_PLAT)
        if plat_override:
            self.plat_name: str | None = plat_override.replace("-", "_")
            self.plat_name_supplied: bool = True
        elif platform:
            machine = platform.machine().lower()
            system = platform.system().lower()
            if system == "linux" and machine in {"x86_64", "amd64"}:
                self.plat_name = "manylinux_2_5_x86_64"
            elif system == "linux" and machine in {"aarch64", "arm64"}:
                self.plat_name = "manylinux_2_17_aarch64"
            elif system == "darwin" and machine in {"arm64", "aarch64"}:
                self.plat_name = "macosx_11_0_arm64"
            elif system == "darwin" and machine in {"x86_64", "amd64"}:
                self.plat_name = "macosx_10_9_x86_64"
            if getattr(self, "plat_name", None):
                self.plat_name_supplied = True
        super().finalize_options()
        self.root_is_pure: bool | None = False

    @override
    def get_tag(self):
        _, _, plat_name = super().get_tag()
        return "py3", "none", plat_name

    @override
    def run(self) -> None:
        try:
            super().run()
            for n, x in enumerate(self.distribution.dist_files):
                command, pyversion, file = x
                if not file.endswith(".whl"):
                    continue
                new_file = self._retag_wheel(file)
                if not new_file:
                    continue
                self.distribution.dist_files[n] = (command, pyversion, new_file)
        finally:
            _cleanup_build_dir()

    def _retag_wheel(self, input_path: str) -> str | None:
        if os.environ.get(ENV_PLAT):
            return None
        if not platform:
            return None
        system = platform.system().lower()
        machine = platform.machine().lower()
        tag = None
        if system == "linux" and machine in {"x86_64", "amd64"}:
            tag = "manylinux_2_5_x86_64.musllinux_1_1_x86_64"
        elif system == "linux" and machine in {"aarch64", "arm64"}:
            tag = "manylinux_2_17_aarch64.musllinux_1_1_aarch64"
        if not tag:
            return None
        result = subprocess.run(
            [
                sys.executable,
                "-m",
                "wheel",
                "tags",
                input_path,
                "--remove",
                "--platform-tag",
                tag,
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        output_path = Path(input_path).parent / result.stdout.strip()
        log.info(f"retagged {input_path} to {output_path}")
        return str(output_path)


_ = setup(
    distclass=BinaryDistribution,
    cmdclass={"build_py": BuildPy, "sdist": Sdist, "bdist_wheel": BdistWheel},
)
