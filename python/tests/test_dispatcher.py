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
        ("tenzir", "", [], "tenzir-engine", ["tenzir"]),
        ("tenzir", "", ["from {}"], "tenzir-engine", ["tenzir", "from {}"]),
        ("tenzir", "0", ["--help"], "tenzir-engine", ["tenzir", "--help"]),
        ("tenzir", "No", ["up", "name"], "platform-cli", ["tenzir", "up", "name"]),
        ("tenzir", "", ["up"], "platform-cli", ["tenzir", "up"]),
        ("tenzir", "", ["down", "name"], "tenzir-engine", ["tenzir", "down", "name"]),
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
        ("tenzir", "", ["platform"], "platform-cli", ["tenzir"]),
        (
            "tenzir",
            "false",
            ["platform", "--help"],
            "platform-cli",
            ["tenzir", "--help"],
        ),
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
