import importlib.metadata
import os
import sys
from pathlib import Path

import pytest
import tenzir_platform_cli
from tenzir._bin import _common


class ExecutedError(Exception):
    pass


@pytest.mark.parametrize("os_name", ["linux", "darwin"])
@pytest.mark.parametrize(
    ("name", "unified", "arguments", "target", "forwarded"),
    [
        ("tenzir", "0", [], "tenzir-engine", ["tenzir"]),
        ("tenzir", "0", ["from {}"], "tenzir-engine", ["tenzir", "from {}"]),
        ("tenzir", "0", ["--help"], "tenzir-engine", ["tenzir", "--help"]),
        ("tenzir", "No", ["up", "name"], "platform-cli", ["tenzir", "up", "name"]),
        ("tenzir", "0", ["up"], "platform-cli", ["tenzir", "up"]),
        ("tenzir", "0", ["down", "name"], "tenzir-engine", ["tenzir", "down", "name"]),
        (
            "tenzir",
            "FALSE",
            ["run", "-r", "x"],
            "tenzir-engine",
            ["tenzir", "run", "-r", "x"],
        ),
        (
            "tenzir",
            "",
            ["run", "from {}"],
            "tenzir-engine",
            ["tenzir", "run", "from {}"],
        ),
        ("tenzir", "0", ["platform"], "platform-cli", ["tenzir"]),
        (
            "tenzir",
            "false",
            ["platform", "--help"],
            "platform-cli",
            ["tenzir", "--help"],
        ),
        ("tenzir", "1", ["from {}"], "platform-cli", ["tenzir", "from {}"]),
        ("tenzir", "1", [], "platform-cli", ["tenzir"]),
        ("tenzir", "true", ["--help"], "platform-cli", ["tenzir", "--help"]),
        (
            "tenzir",
            "yes",
            ["unknown", "--flag"],
            "platform-cli",
            ["tenzir", "unknown", "--flag"],
        ),
        (
            "tenzir",
            "1",
            ["platform", "--help"],
            "platform-cli",
            ["tenzir", "platform", "--help"],
        ),
        (
            "tenzir",
            "1",
            ["up", "name", "-d"],
            "platform-cli",
            ["tenzir", "up", "name", "-d"],
        ),
        (
            "tenzir",
            "1",
            ["down", "name"],
            "platform-cli",
            ["tenzir", "down", "name"],
        ),
        (
            "tenzir",
            "1",
            ["run", "-r", "from {}"],
            "platform-cli",
            ["tenzir", "run", "-r", "from {}"],
        ),
        (
            "tenzir",
            "1",
            ["run", "-f", "p.tql", "--remote"],
            "platform-cli",
            ["tenzir", "run", "-f", "p.tql", "--remote"],
        ),
        (
            "tenzir",
            "1",
            ["run", "from {}"],
            "tenzir-engine",
            ["tenzir", "run", "from {}"],
        ),
        (
            "tenzir",
            "1",
            ["run", "--", "-r"],
            "tenzir-engine",
            ["tenzir", "run", "--", "-r"],
        ),
        ("tenzir-node", "1", ["--help"], "tenzir-engine", ["tenzir-node", "--help"]),
        (
            "tenzir-ctl",
            "1",
            ["run", "-r"],
            "tenzir-engine",
            ["tenzir-ctl", "run", "-r"],
        ),
        ("tenzir-ctl", "", ["platform"], "tenzir-engine", ["tenzir-ctl", "platform"]),
        ("tenzir-rebuild", "1", [], "tenzir-engine", ["tenzir-rebuild"]),
        ("tenzir-node", "", ["up"], "tenzir-engine", ["tenzir-node", "up"]),
    ],
)
def test_wheel_dispatch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    os_name: str,
    name: str,
    unified: str,
    arguments: list[str],
    target: str,
    forwarded: list[str],
) -> None:
    binary = tmp_path / "libexec" / "tenzir-engine"
    binary.parent.mkdir()
    binary.write_text("unused")
    binary.chmod(0o755)
    monkeypatch.setattr(_common, "_pkg_bin_dir", lambda: tmp_path / "bin")
    monkeypatch.setattr(_common, "_prepare_environment", lambda: None)
    # A version that ships a node, so the table covers `tenzir-node` as well.
    monkeypatch.setattr(_common, "_v7_or_later", lambda: False)
    monkeypatch.setattr(sys, "platform", os_name)
    monkeypatch.setattr(sys, "argv", [name, *arguments])
    monkeypatch.setenv("TENZIR_UNIFIED", unified)
    monkeypatch.delenv("TENZIR_LIBEXEC", raising=False)

    def record_exec(path: str | Path, argv: list[str]) -> None:
        if target == "platform-cli":
            assert Path(path) == Path(tenzir_platform_cli.__file__).with_name(
                "platform-cli"
            )
            assert os.environ["TENZIR_LIBEXEC"] == str(binary.parent)
        else:
            assert Path(path) == binary
            assert "TENZIR_LIBEXEC" not in os.environ
        assert argv == forwarded
        raise ExecutedError

    monkeypatch.setattr(os, "execv", record_exec)
    with pytest.raises(ExecutedError):
        _common.exec_binary(name)


@pytest.mark.parametrize(
    ("version", "ships_node"),
    [("6.22.0", True), ("7.0.0", False), ("7.1.0rc1", False), (None, True)],
)
def test_tenzir_node_only_before_v7(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    tmp_path: Path,
    version: str | None,
    ships_node: bool,
) -> None:
    def package_version(name: str) -> str:
        assert name == "tenzir"
        if version is None:
            raise importlib.metadata.PackageNotFoundError(name)
        return version

    binary = tmp_path / "libexec" / "tenzir-engine"
    binary.parent.mkdir()
    binary.write_text("unused")
    binary.chmod(0o755)
    monkeypatch.setattr(_common, "_pkg_bin_dir", lambda: tmp_path / "bin")
    monkeypatch.setattr(_common, "_prepare_environment", lambda: None)
    monkeypatch.setattr(importlib.metadata, "version", package_version)
    monkeypatch.setattr(sys, "argv", ["tenzir-node"])

    def record_exec(path: str | Path, argv: list[str]) -> None:
        assert Path(path) == binary
        assert argv == ["tenzir-node"]
        raise ExecutedError

    monkeypatch.setattr(os, "execv", record_exec)
    if ships_node:
        with pytest.raises(ExecutedError):
            _common.exec_binary("tenzir-node")
    else:
        with pytest.raises(SystemExit) as exit_info:
            _common.exec_binary("tenzir-node")
        assert exit_info.value.code == 1
        assert "ship no node" in capsys.readouterr().err


@pytest.mark.parametrize("unified", [None, ""])
@pytest.mark.parametrize(
    ("version", "expected"),
    [("6.22.0", False), ("7.0.0", True), ("7.1.0rc1", True), (None, False)],
)
def test_unified_default(
    monkeypatch: pytest.MonkeyPatch,
    unified: str | None,
    version: str | None,
    expected: bool,
) -> None:
    def package_version(name: str) -> str:
        assert name == "tenzir"
        if version is None:
            raise importlib.metadata.PackageNotFoundError(name)
        return version

    monkeypatch.setattr(importlib.metadata, "version", package_version)
    if unified is None:
        monkeypatch.delenv("TENZIR_UNIFIED", raising=False)
    else:
        monkeypatch.setenv("TENZIR_UNIFIED", unified)
    assert _common._unified_mode() is expected
