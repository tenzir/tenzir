"""Local filesystem fixture with unreadable and dangling entries."""

from __future__ import annotations

import os
import shutil
from pathlib import Path

from tenzir_test import FixtureHandle, fixture
from tenzir_test.fixtures import FixtureUnavailable

FIXTURE_ROOT = Path("/tmp/tenzir-files-permission-root")


def _make_writable(path: Path) -> None:
    if path.exists():
        path.chmod(0o700)


@fixture
def files_permission_tree() -> FixtureHandle:
    """Create a deterministic local tree for recursive `files` tests."""
    if hasattr(os, "geteuid") and os.geteuid() == 0:
        raise FixtureUnavailable("root can traverse chmod 000 directories")
    root = FIXTURE_ROOT
    blocked = root / "blocked"
    _make_writable(blocked)
    shutil.rmtree(root, ignore_errors=True)
    (root / "after").mkdir(parents=True)
    (root / "ok").mkdir()
    (root / "after" / "data.json").write_text("{}\n")
    (root / "ok" / "data.json").write_text("{}\n")
    try:
        (root / "dangling").symlink_to("missing-target")
    except OSError as err:
        shutil.rmtree(root, ignore_errors=True)
        raise FixtureUnavailable(f"symlink creation failed: {err}") from err
    blocked.mkdir()
    blocked.chmod(0)

    def _teardown() -> None:
        _make_writable(blocked)
        shutil.rmtree(root, ignore_errors=True)

    return FixtureHandle(
        env={"FILES_PERMISSION_ROOT": str(root)},
        teardown=_teardown,
    )
