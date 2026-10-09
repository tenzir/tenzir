#!/usr/bin/env python3
"""Regenerate the package sources that `package_http` serves.

The sources live under `test/inputs/packages` instead of being materialized at
run time. Tests name them in diagnostics, so their paths must be stable, and
the harness rewrites paths below the project root as relative ones. Writing
them at run time is not an option either: CI runs the suite from a read-only
store path.

Run this after changing the archives:

    python3 test/fixtures/tools/generate_package_sources.py
"""

from __future__ import annotations

import gzip
import io
import shutil
import tarfile
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent.parent / "inputs" / "packages"

# Package directories, written as plain files so they stay reviewable.
DIRECTORIES = {
    # A valid package whose id triggers a normalization warning.
    "directory/package.yaml": "id: warning-package\nname: Warning package\n",
    # The same id warning, but with a pipeline that fails to parse. A directory
    # keeps every path in the diagnostic below this root, unlike an archive,
    # which is unpacked into a temporary directory.
    "warning-error/package.yaml": "id: warning-package\nname: Warning package\n",
    "warning-error/pipelines/bad.tql": "---\nname: [\n---\nversion\n",
    # Definitions that are invalid in different ways.
    "invalid-yaml/package.yaml": "[",
    "not-record/package.yaml": "42",
}


def make_archive(files, *, compressed=True, extra=None):
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode="w:gz" if compressed else "w") as archive:
        for name, contents in files.items():
            data = contents.encode()
            entry = tarfile.TarInfo(name)
            entry.size = len(data)
            archive.addfile(entry, io.BytesIO(data))
        if extra is not None:
            archive.addfile(extra)
    return buffer.getvalue()


def archives():
    """Return the archive name to body mapping that the fixture serves."""
    files = {
        "package.yaml": (
            "id: remote_split\nname: Remote split package\n"
            "inputs:\n  message:\n    name: Message\n    type: string\n"
        ),
        "config.yaml": "inputs:\n  message: configured\n",
        "pipelines/nested/hello.tql": (
            'from {message: "{{ inputs.message }}"}\ndiscard\n'
        ),
        "operators/nested/answer.tql": "from {answer: 42}\n",
        "examples/example.tql": "version\n",
        "constants.tql": "let $answer = 42\n",
        "README.md": "Ignored by the package loader.\n",
    }
    wrapped = {
        f"repo-main-abcdef/packages/example/{name}": body
        for name, body in files.items()
    }
    derived = {
        f"derived_package/{name}": body.removeprefix("id: remote_split\n")
        for name, body in files.items()
    }
    symlink = tarfile.TarInfo("link")
    symlink.type = tarfile.SYMTYPE
    symlink.linkname = "../../outside"
    hardlink = tarfile.TarInfo("hardlink")
    hardlink.type = tarfile.LNKTYPE
    hardlink.linkname = "package.yaml"
    fifo = tarfile.TarInfo("fifo")
    fifo.type = tarfile.FIFOTYPE
    oversized = tarfile.TarInfo("large")
    oversized.size = 65 * 1024 * 1024
    valid = make_archive(wrapped)
    multiple = {
        **{f"packages/example/{name}": body for name, body in files.items()},
        "packages/other/package.yaml": "id: other\nname: Other package\n",
        "README.md": "Repository with multiple packages.\n",
    }
    return {
        "warning.tar.gz": make_archive(
            {"package.yaml": "id: warning-package\nname: Warning package\n"}
        ),
        "archive.tar.gz": valid,
        # The extensionless copy stands in for the GitLab archive endpoint and
        # checks that discovery does not depend on the file extension.
        "archive": valid,
        "archive.tar": make_archive(files, compressed=False),
        "single-root.tar.gz": make_archive(files),
        "multiple.tar.gz": make_archive(multiple),
        "multiple-wrapped.tar.gz": make_archive(
            {f"repo-main-abcdef/{name}": body for name, body in multiple.items()}
        ),
        "derived.tgz": make_archive(derived),
        "traversal.tar.gz": make_archive({"../outside": "unsafe"}),
        "absolute.tar.gz": make_archive({"/outside": "unsafe"}),
        "symlink.tar.gz": make_archive(files, extra=symlink),
        "hardlink.tar.gz": make_archive(files, extra=hardlink),
        "fifo.tar.gz": make_archive(files, extra=fifo),
        "oversized.tar.gz": gzip.compress(oversized.tobuf(), mtime=0),
        "duplicate.tar.gz": make_archive(files, extra=tarfile.TarInfo("package.yaml")),
        "missing.tar.gz": make_archive({"README.md": "no package"}),
        "ambiguous.tar.gz": make_archive(
            {
                "a/package.yaml": files["package.yaml"],
                "b/package.yaml": files["package.yaml"],
            }
        ),
        "corrupt.tar.gz": b"not an archive",
        "truncated.tar.gz": valid[: len(valid) // 2],
    }


def main() -> None:
    if ROOT.exists():
        shutil.rmtree(ROOT)
    ROOT.mkdir(parents=True)
    for name, body in archives().items():
        (ROOT / name).write_bytes(body)
    for name, body in DIRECTORIES.items():
        target = ROOT / name
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(body)
    print(f"wrote {len(archives()) + len(DIRECTORIES)} files to {ROOT}")


if __name__ == "__main__":
    main()
