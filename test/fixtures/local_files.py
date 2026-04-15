"""Local filesystem fixture for from_file operator integration testing.

Creates a temporary directory with test files for testing the from_file operator.

Environment variables yielded:
- FILE_ROOT: Absolute path to the temporary directory containing test files.

Assertions payload accepted under ``assertions.fixtures.local_files``:
- state: unchanged | removed | renamed
"""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture

logger = logging.getLogger(__name__)

# Test data layout: maps relative path to its JSON content.
TEST_FILES: dict[str, dict[str, str]] = {
    "single/data.json": {"filename": "single/data.json"},
    "multi/data-001.json": {"filename": "multi/data-001.json"},
    "multi/data-002.json": {"filename": "multi/data-002.json"},
    "nested/subdir-a/data.json": {"filename": "nested/subdir-a/data.json"},
    "nested/subdir-b/data.json": {"filename": "nested/subdir-b/data.json"},
    "lifecycle/remove-target.json": {"filename": "lifecycle/remove-target.json"},
    "lifecycle/rename-target.json": {"filename": "lifecycle/rename-target.json"},
}


@dataclass(frozen=True)
class LocalFilesAssertions:
    """Assertions controlling post-test verification for local files."""

    state: str = "unchanged"

    def __post_init__(self) -> None:
        if self.state not in {"unchanged", "removed", "renamed"}:
            raise TypeError(
                "local_files fixture assertion `state` must be one of: "
                "unchanged, removed, renamed"
            )


def _extract_assertions(
    raw: LocalFilesAssertions | dict[str, Any],
) -> LocalFilesAssertions:
    if isinstance(raw, LocalFilesAssertions):
        return raw
    if isinstance(raw, dict):
        return LocalFilesAssertions(**raw)
    raise TypeError(
        "local_files fixture assertions must be a mapping or "
        "LocalFilesAssertions instance"
    )


def _setup_files(root: Path) -> None:
    """Create test files under *root*."""
    for rel_path, content in TEST_FILES.items():
        file_path = root / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(json.dumps(content) + "\n")
    logger.info("Local test files created under %s", root)


def _verify_post_test(root: Path, assertions: LocalFilesAssertions) -> None:
    """Verify file state after a test completes."""
    if assertions.state == "removed":
        target = root / "lifecycle/remove-target.json"
        if target.exists():
            raise RuntimeError(
                "lifecycle/remove-target.json still exists after state=removed"
            )
        logger.info("Verified: lifecycle/remove-target.json was removed")
        return

    if assertions.state == "renamed":
        target = root / "lifecycle/rename-target.json"
        renamed = root / "lifecycle/rename-target.json.done"
        if target.exists():
            raise RuntimeError(
                "lifecycle/rename-target.json still exists after state=renamed"
            )
        if not renamed.exists():
            raise RuntimeError(
                "lifecycle/rename-target.json.done not found after state=renamed"
            )
        logger.info("Verified: lifecycle/rename-target.json was renamed to .done")
        return

    # Default behavior: verify all original files are still present.
    for rel_path in TEST_FILES:
        if not (root / rel_path).exists():
            raise RuntimeError(
                f"{rel_path} is missing after tests (expected no modifications)"
            )
    logger.info("Verified: all test files are still present")


@fixture(assertions=LocalFilesAssertions)
def local_files() -> FixtureHandle:
    """Create temporary test files and return fixture handle with assertions."""
    # Resolve symlinks so that FILE_ROOT matches the canonical paths returned
    # by the Arrow filesystem. On macOS, /tmp is a symlink to /private/tmp.
    root = Path(tempfile.mkdtemp(prefix="tenzir-test-local-files-")).resolve()

    try:
        _setup_files(root)
    except Exception:
        shutil.rmtree(root, ignore_errors=True)
        raise

    def _assert_test(
        *,
        test: Path,
        assertions: LocalFilesAssertions | dict[str, Any],
        **_: Any,
    ) -> None:
        assertion_config = _extract_assertions(assertions)
        try:
            _verify_post_test(root, assertion_config)
        except RuntimeError as exc:
            raise AssertionError(f"{test.name}: {exc}") from exc

    def _teardown() -> None:
        shutil.rmtree(root, ignore_errors=True)
        logger.info("Cleaned up temp directory: %s", root)

    return FixtureHandle(
        env={"FILE_ROOT": str(root)},
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
