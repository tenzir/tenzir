"""Provide columnar inputs for reader pushdown tests."""

# /// script
# dependencies = ["pyarrow"]
# ///

from __future__ import annotations

import subprocess
import sys
import tempfile
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from tenzir_test import current_options, fixture


@dataclass(frozen=True)
class ReadPushdownOptions:
    format: str = "parquet"

    def __post_init__(self) -> None:
        if self.format not in {"parquet", "ipc_file", "ipc_stream"}:
            raise ValueError("format must be parquet, ipc_file, or ipc_stream")


@fixture(options=ReadPushdownOptions)
def read_pushdown() -> Iterator[dict[str, str]]:
    options = current_options("read_pushdown")
    with tempfile.TemporaryDirectory(prefix="tenzir-read-pushdown-") as directory:
        root = Path(directory).resolve()
        # Keep PyArrow's native libraries out of the shared fixture process.
        # Other fixtures load native dependencies there during discovery.
        subprocess.run(
            [
                sys.executable,
                str(Path(__file__).with_name("_read_pushdown_inputs.py")),
                str(root),
                options.format,
            ],
            check=True,
        )
        yield {"READ_PUSHDOWN_ROOT": str(root)}
