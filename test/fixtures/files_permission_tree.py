"""Local filesystem fixture with an unreadable child directory."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable

RELATIVE_ROOT = Path("tests/operators/files/permission-root")


def _make_writable(path: Path) -> None:
    if path.exists():
        path.chmod(0o700)


@fixture
def files_permission_tree() -> FixtureHandle:
    """Create a deterministic local tree for recursive `files` tests."""
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        raise FixtureUnavailable("root can traverse chmod 000 directories")
    test_root = Path(__file__).resolve().parents[1]
    root = test_root / RELATIVE_ROOT
    blocked = root / "blocked"
    _make_writable(blocked)
    shutil.rmtree(root, ignore_errors=True)
    (root / "after").mkdir(parents=True)
    (root / "ok").mkdir()
    (root / "after" / "data.json").write_text("{}\n")
    (root / "ok" / "data.json").write_text("{}\n")
    blocked.mkdir()
    blocked.chmod(0)

    def _teardown() -> None:
        _make_writable(blocked)
        shutil.rmtree(root, ignore_errors=True)

    return FixtureHandle(
        env={"FILES_PERMISSION_ROOT": str(RELATIVE_ROOT)},
        teardown=_teardown,
    )
