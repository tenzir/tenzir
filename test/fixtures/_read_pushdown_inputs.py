"""Generate columnar inputs for reader pushdown tests.

READ_PUSHDOWN_ROOT contains input, clean, and corrupt-unused files. IPC formats
also provide a store envelope; IPC streams additionally provide concatenated
schemas and trailing corruption. The fixture only generates inputs: TQL tests
and their baselines check reader behavior.
"""

# /// script
# dependencies = ["pyarrow"]
# ///

from __future__ import annotations

import sys
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def _write_table(
    path: Path, table: pa.Table, format: str, *, compressed: bool = False
) -> None:
    if format == "parquet":
        pq.write_table(table, path, row_group_size=3)
        return
    writer_factory = pa.ipc.new_file if format == "ipc_file" else pa.ipc.new_stream
    options = pa.ipc.IpcWriteOptions(compression="zstd" if compressed else None)
    with writer_factory(path, table.schema, options=options) as writer:
        writer.write_table(table, max_chunksize=3)


def _stream_bytes(table: pa.Table) -> bytes:
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table, max_chunksize=3)
    return sink.getvalue().to_pybytes()


def _setup_inputs(root: Path, format: str) -> None:
    table = pa.table(
        {
            # Neither reader supports decimal256 as an event field. Projected
            # reads must exclude this column before converting to a slice.
            "unused": pa.array([Decimal(i) for i in range(9)], pa.decimal256(50)),
            "id": pa.array(range(9), pa.int64()),
            "nested": pa.array([{"x": i + 50, "sibling": "keep"} for i in range(9)]),
            "literal.dot": pa.array(range(9), pa.int64()),
        }
    )
    _write_table(root / "input", table, format)
    clean = table.drop(["unused"]).append_column(
        "import_time", pa.array(range(100, 109))
    )
    _write_table(root / "clean", clean, format)

    corrupted = root / "corrupt-unused"
    _write_table(corrupted, table, format, compressed=True)
    damaged = bytearray(corrupted.read_bytes())
    if format == "parquet":
        offset = pq.read_metadata(corrupted).row_group(0).column(0).data_page_offset
    else:
        # The first Zstandard frame belongs to the unselected first column.
        offset = damaged.find(b"\x28\xb5\x2f\xfd")
        assert offset >= 0, "compressed column buffer not found"
    damaged[offset] = 0
    corrupted.write_bytes(damaged)

    if format == "parquet":
        return
    metadata_time = pa.array([123_000_000_000] * 9, pa.timestamp("ns"))
    event = clean.to_batches()[0].to_struct_array()
    envelope = pa.Table.from_arrays(
        [metadata_time, event],
        schema=pa.schema(
            [
                pa.field("import_time", metadata_time.type),
                pa.field(
                    "event",
                    event.type,
                    metadata={"TENZIR:name:0": "test.events"},
                ),
            ]
        ),
    )
    _write_table(root / "envelope", envelope, format)

    if format == "ipc_stream":
        first = _stream_bytes(table.slice(0, 3))
        second = table.slice(3).select(["nested", "unused", "literal.dot", "id"])
        (root / "concatenated").write_bytes(first + _stream_bytes(second))
        (root / "trailing").write_bytes(first + b"not another IPC stream")


if __name__ == "__main__":
    _setup_inputs(Path(sys.argv[1]), sys.argv[2])
