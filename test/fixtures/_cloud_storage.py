"""Shared helpers for cloud storage fixture testing (S3, GCS, ABS).

Provides common test data definitions, assertions, and post-test verification
logic that can be reused across MinIO (S3), fake-gcs-server (GCS), and
Azurite (ABS) fixtures.

Each backend fixture is responsible for:
- Starting and stopping its own containers
- Uploading test data (using TEST_FILES)
- Providing a ``file_exists`` callback for post-test verification
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Callable

logger = logging.getLogger(__name__)

# Bucket names shared across all cloud storage fixtures.
BUCKET = "tenzir-test"
PUBLIC_BUCKET = "tenzir-test-public"

# Test data layout: maps ``{bucket}/{key}`` to its JSON content.
# Every backend fixture should upload these files during setup.
TEST_FILES: dict[str, dict[str, str]] = {
    f"{BUCKET}/single/data.json": {"filename": "single/data.json"},
    f"{BUCKET}/multi/data-001.json": {"filename": "multi/data-001.json"},
    f"{BUCKET}/multi/data-002.json": {"filename": "multi/data-002.json"},
    f"{BUCKET}/nested/subdir-a/data.json": {"filename": "nested/subdir-a/data.json"},
    f"{BUCKET}/nested/subdir-b/data.json": {"filename": "nested/subdir-b/data.json"},
    f"{BUCKET}/lifecycle/remove-target.json": {
        "filename": "lifecycle/remove-target.json"
    },
    f"{BUCKET}/lifecycle/rename-target.json": {
        "filename": "lifecycle/rename-target.json"
    },
    f"{PUBLIC_BUCKET}/data.json": {"filename": "data.json"},
    f"{PUBLIC_BUCKET}/data-remove.json": {"filename": "data-remove.json"},
}


@dataclass(frozen=True)
class CloudStorageAssertions:
    """Assertions controlling post-test verification for cloud storage fixtures.

    Accepted states:
    - unchanged: Verify all original files are still present.
    - removed: Verify lifecycle/remove-target.json is gone.
    - renamed: Verify lifecycle/rename-target.json moved to .done.
    """

    state: str = "unchanged"

    def __post_init__(self) -> None:
        if self.state not in {"unchanged", "removed", "renamed"}:
            raise TypeError(
                "cloud storage fixture assertion `state` must be one of: "
                "unchanged, removed, renamed"
            )


def extract_assertions(
    raw: CloudStorageAssertions | dict[str, Any],
) -> CloudStorageAssertions:
    """Normalize raw fixture assertion payload into a typed dataclass."""
    if isinstance(raw, CloudStorageAssertions):
        return raw
    if isinstance(raw, dict):
        return CloudStorageAssertions(**raw)
    raise TypeError(
        "cloud storage fixture assertions must be a mapping or "
        "CloudStorageAssertions instance"
    )


def verify_post_test(
    file_exists: Callable[[str], bool],
    assertions: CloudStorageAssertions,
    bucket: str = BUCKET,
) -> None:
    """Verify bucket state after tests complete.

    Args:
        file_exists: Callback that returns True if the given key exists in
            *bucket*.  The key is relative to the bucket (e.g.
            ``"single/data.json"``).
        assertions: The fixture assertions controlling which checks to run.
        bucket: The bucket to check against.
    """
    if assertions.state == "removed":
        if file_exists("lifecycle/remove-target.json"):
            raise RuntimeError(
                "lifecycle/remove-target.json still exists after state=removed"
            )
        logger.info("Verified: lifecycle/remove-target.json was removed")
        return

    if assertions.state == "renamed":
        if file_exists("lifecycle/rename-target.json"):
            raise RuntimeError(
                "lifecycle/rename-target.json still exists after state=renamed"
            )
        if not file_exists("lifecycle/rename-target.json.done"):
            raise RuntimeError(
                "lifecycle/rename-target.json.done not found after state=renamed"
            )
        logger.info("Verified: lifecycle/rename-target.json was renamed to .done")
        return

    # Default behavior: verify all original files are still present.
    for key in TEST_FILES:
        # Keys in TEST_FILES are "{bucket}/{path}"; extract the path portion.
        parts = key.split("/", 1)
        if len(parts) != 2:
            continue
        file_bucket, file_key = parts
        if file_bucket != bucket:
            continue
        if not file_exists(file_key):
            raise RuntimeError(
                f"{key} is missing after tests (expected no modifications)"
            )
    logger.info("Verified: all test files are still present")
