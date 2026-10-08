from __future__ import annotations

import os
import platform
import shutil
import subprocess
import tarfile
from pathlib import Path
from typing import override

from setuptools import Distribution, setup
from setuptools.command.bdist_wheel import bdist_wheel as _bdist_wheel
from setuptools.command.build_py import build_py as _build_py

ROOT = Path(__file__).resolve().parent
BINARY = ROOT / "src" / "tenzir_platform_cli" / "platform-cli"


class BuildPy(_build_py):
    @override
    def run(self) -> None:
        signed_tarball = os.environ.get("TENZIR_SIGNED_TARBALL_PATH")
        binary_path = os.environ.get("TENZIR_PLATFORM_CLI_PATH")
        if signed_tarball:
            # Preserve the exact signed and notarized macOS executable bytes.
            with tarfile.open(signed_tarball) as archive:
                member = archive.getmember("opt/tenzir/libexec/platform-cli")
                with archive.extractfile(member) as source, BINARY.open("wb") as dest:
                    shutil.copyfileobj(source, dest)
                BINARY.chmod(member.mode)
        elif binary_path:
            source = Path(binary_path)
            shutil.copy2(source, BINARY)
        else:
            repo_root = Path(os.environ.get("TENZIR_REPO_ROOT", ROOT.parents[1]))
            result = subprocess.run(
                [
                    "nix",
                    "--accept-flake-config",
                    "build",
                    "--no-link",
                    "--print-out-paths",
                    f"{repo_root}#platform-cli",
                ],
                cwd=repo_root,
                check=True,
                capture_output=True,
                text=True,
            )
            source = Path(result.stdout.splitlines()[-1]) / "bin" / "platform-cli"
            shutil.copy2(source, BINARY)
        try:
            super().run()
        finally:
            BINARY.unlink()


class BinaryDistribution(Distribution):
    @override
    def has_ext_modules(self) -> bool:
        return True


class BdistWheel(_bdist_wheel):
    @override
    def run(self) -> None:
        try:
            super().run()
        finally:
            shutil.rmtree(ROOT / "build", ignore_errors=True)

    @override
    def get_tag(self) -> tuple[str, str, str]:
        tag = os.environ.get("TENZIR_WHEEL_PLATFORM")
        if not tag:
            tags = {
                ("Linux", "x86_64"): "manylinux_2_5_x86_64.musllinux_1_1_x86_64",
                ("Linux", "aarch64"): "manylinux_2_17_aarch64.musllinux_1_1_aarch64",
                ("Darwin", "arm64"): "macosx_13_0_arm64",
            }
            tag = tags[platform.system(), platform.machine()]
        return "py3", "none", tag


_ = setup(
    distclass=BinaryDistribution,
    cmdclass={"build_py": BuildPy, "bdist_wheel": BdistWheel},
)
