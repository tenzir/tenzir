"""Local filesystem fixture for from_file operator integration testing.

Creates a temporary directory with test files for testing the from_file operator.

Environment variables yielded:
- FILE_ROOT: Absolute path to the temporary directory containing test files.

Options:
- verify_remove (bool): After tests, assert that lifecycle/remove-target.json
  was deleted.
- verify_rename (bool): After tests, assert that lifecycle/rename-target.json
  was moved to lifecycle/rename-target.json.done.

When neither option is set, the fixture verifies that all originally created
test files are still present after tests complete.
"""

from __future__ import annotations

import json
import logging
import shutil
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator

from tenzir_test import fixture
from tenzir_test.fixtures import current_options

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
class LocalFilesOptions:
    """Options controlling post-test verification for local file fixtures."""

    verify_remove: bool = False
    verify_rename: bool = False


def _setup_files(root: Path) -> None:
    """Create test files under *root*."""
    for rel_path, content in TEST_FILES.items():
        file_path = root / rel_path
        file_path.parent.mkdir(parents=True, exist_ok=True)
        file_path.write_text(json.dumps(content) + "\n")
    logger.info("Local test files created under %s", root)


def _verify_post_test(root: Path, opts: LocalFilesOptions) -> None:
    """Verify file state after tests complete."""
    if opts.verify_remove:
        target = root / "lifecycle/remove-target.json"
        if target.exists():
            raise RuntimeError(
                "lifecycle/remove-target.json still exists after remove=true"
            )
        logger.info("Verified: lifecycle/remove-target.json was removed")
        return

    if opts.verify_rename:
        target = root / "lifecycle/rename-target.json"
        renamed = root / "lifecycle/rename-target.json.done"
        if target.exists():
            raise RuntimeError("lifecycle/rename-target.json still exists after rename")
        if not renamed.exists():
            raise RuntimeError(
                "lifecycle/rename-target.json.done not found after rename"
            )
        logger.info("Verified: lifecycle/rename-target.json was renamed to .done")
        return

    # No lifecycle options — verify all original files are still present.
    for rel_path in TEST_FILES:
        if not (root / rel_path).exists():
            raise RuntimeError(
                f"{rel_path} is missing after tests (expected no modifications)"
            )
    logger.info("Verified: all test files are still present")


@fixture(options=LocalFilesOptions)
def local_files() -> Iterator[dict[str, str]]:
    """Create temporary test files and yield the root directory path."""
    opts = current_options("local_files")
    root = Path(tempfile.mkdtemp(prefix="tenzir-test-local-files-"))

    try:
        _setup_files(root)

        yield {
            "FILE_ROOT": str(root),
        }

        _verify_post_test(root, opts)
    finally:
        shutil.rmtree(root, ignore_errors=True)
        logger.info("Cleaned up temp directory: %s", root)
