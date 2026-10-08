import os
import sys
from pathlib import Path

import pytest
from tenzir._bin import _common


class ExecutedError(Exception):
    pass


@pytest.mark.parametrize("os_name", ["linux", "darwin"])
@pytest.mark.parametrize(
    ("name", "unified", "arguments", "target", "forwarded"),
    [
        ("tenzir", "", ["platform", "--help"], "platform-cli", ["--help"]),
        ("tenzir", "0", ["platform"], "platform-cli", []),
        ("tenzir", "FALSE", ["platform", "--version"], "platform-cli", ["--version"]),
        ("tenzir", "No", ["from {}"], "engine", ["tenzir", "from {}"]),
        ("tenzir", "", [], "engine", ["tenzir"]),
        ("tenzir", "1", [], "platform-cli", []),
        ("tenzir", "true", ["--help"], "platform-cli", ["--help"]),
        ("tenzir", "yes", ["unknown", "--flag"], "platform-cli", ["unknown", "--flag"]),
        ("tenzir", "1", ["platform", "--help"], "platform-cli", ["platform", "--help"]),
        ("tenzir", "1", ["run", "from {}"], "engine", ["tenzir", "from {}"]),
        ("tenzir", "1", ["up", "--help"], "engine", ["tenzir-up", "--help"]),
        ("tenzir-node", "1", ["--help"], "engine", ["tenzir-node", "--help"]),
        ("tenzir-ctl", "1", ["status"], "engine", ["tenzir-ctl", "status"]),
        ("tenzir-rebuild", "1", [], "engine", ["tenzir-rebuild"]),
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
    binary = tmp_path / "libexec" / "engine"
    binary.parent.mkdir()
    binary.write_text("unused")
    binary.chmod(0o755)
    monkeypatch.setattr(_common, "_pkg_bin_dir", lambda: tmp_path / "bin")
    monkeypatch.setattr(_common, "_prepare_environment", lambda: None)
    monkeypatch.setattr(sys, "platform", os_name)
    monkeypatch.setattr(sys, "argv", [name, *arguments])
    monkeypatch.setenv("TENZIR_UNIFIED", unified)

    def record_exec(path: str | Path, argv: list[str]) -> None:
        assert Path(path).name == target
        expected = (
            ["platform-cli", *forwarded] if target == "platform-cli" else forwarded
        )
        assert argv == expected
        raise ExecutedError

    monkeypatch.setattr(os, "execv", record_exec)
    with pytest.raises(ExecutedError):
        _common.exec_binary(name)
