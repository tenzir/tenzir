#!/usr/bin/env python3
"""Regenerate the Arrow IPC inputs for the columnar roundtrip tests.

The writers infer their output schema from the batches they receive, so the
tests need inputs whose batch boundaries and per-batch types are exact. Neither
`batch` nor a TQL literal can express that: an unresolved null column, an empty
list whose child type is still unknown, and a row count that crosses the
writers' discovery bound only arise from distinct Arrow record batches.

Concatenated IPC streams are the smallest way to pin them down, and checking
the bytes in keeps PyArrow out of the test run.

The inference cases arrive on stdin rather than through `from_file`, because a
`from_file` subpipeline emits each unresolved batch separately instead of
letting the reader refine the schema across batches. They are written next to
the tests that consume them, which is where the harness looks for `.stdin`.

Run this after changing the cases:

    python3 test/fixtures/tools/generate_roundtrip_inputs.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow as pa

TEST = Path(__file__).resolve().parent.parent.parent
ROOT = TEST / "inputs" / "feather" / "roundtrip"
STDIN = [
    TEST / "tests" / "operators" / "read_feather" / "roundtrip",
    TEST / "tests" / "operators" / "write_parquet" / "roundtrip",
]

# The row count at which the writers stop refining an unresolved column.
DISCOVERY_BOUND = 10_000

# One IPC stream per batch, so that each batch keeps its own inferred types.
BATCHES = {
    # A null column must survive until a later batch resolves it, and must not
    # lose its nulls once it does.
    "null_then_value": [[{"x": None}], [{"x": 42}], [{"x": None}]],
    "value_then_null": [[{"x": 42}], [{"x": None}], [{"x": 7}]],
    # The same, one level down, for empty lists and records with null fields.
    "nested_resolution": [
        [{"nested": {"lists": [[]], "records": [{"x": None}]}}],
        [{"nested": {"lists": [[1, 2]], "records": [{"x": 3}]}}],
        [{"nested": {"lists": [[], [None]], "records": [{"x": None}]}}],
    ],
    # EOF must flush unresolved columns without losing nulls or empty lists.
    "unresolved_at_eof": [
        [{"x": None, "nested": {"lists": [[]], "value": None}}],
        [{"x": None, "nested": {"lists": [], "value": None}}],
    ],
    # Cross the discovery bound while the schema is still unresolved.
    "unresolved_past_bound": [[{"x": None}] * DISCOVERY_BOUND, [{"x": None}]],
    # Refining a resolved column is a schema change.
    "conflict": [[{"x": 1}], [{"x": "conflict"}]],
    # So is refining an unresolved one after the discovery bound.
    "resolved_past_bound": [[{"x": None}] * DISCOVERY_BOUND, [{"x": 42}]],
}


def stream_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    return sink.getvalue().to_pybytes()


def main() -> None:
    ROOT.mkdir(parents=True, exist_ok=True)
    for name, batches in BATCHES.items():
        encoded = b"".join(stream_bytes(pa.Table.from_pylist(rows)) for rows in batches)
        for directory in STDIN:
            directory.mkdir(parents=True, exist_ok=True)
            (directory / f"inference_{name}.stdin").write_bytes(encoded)
    # Nova cannot assign `@name` or `@internal`, so the metadata enters through
    # Arrow. These keys and the JSON attributes match type::make_arrow_metadata().
    metadata = pa.table({"x": [0, 1, 2]}).replace_schema_metadata(
        {
            "TENZIR:name:0": "test.format_roundtrip",
            "TENZIR:attributes:0": json.dumps({"internal": ""}),
        }
    )
    (ROOT / "metadata.feather").write_bytes(stream_bytes(metadata))


if __name__ == "__main__":
    main()
