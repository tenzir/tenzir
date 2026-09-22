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
        pq.write_table(
            table.select(["unused"]), root / "unsupported-only", row_group_size=3
        )
        types = pa.table(
            {
                "id": pa.array(range(4), pa.int32()),
                "flag": pa.array([True, None, False, True]),
                "count": pa.array([1, None, 3, 4], pa.uint32()),
                "value": pa.array([1.5, None, -2.5, 0.0], pa.float32()),
                "text": pa.array(["first", None, "", "last"]),
                "bytes": pa.array([b"\x00\xff", None, b"", b"last"], pa.binary()),
                "timestamp": pa.array([0, None, 1234, 5678], pa.timestamp("ms")),
                "record": pa.array([{"x": 1}, None, {"x": None}, {"x": 4}]),
                "list": pa.array([[0, 1], None, [], [None, 4]]),
                "records": pa.array([[{"x": 1}, None], [], None, [{"x": 4}]]),
                "null": pa.nulls(4),
            }
        ).replace_schema_metadata({"TENZIR:name:0": "test.parquet"})
        pq.write_table(types, root / "types", row_group_size=2)
        # Cross the default 8192-row import batch boundary twice.
        batch_rows = 8192
        batches = pa.table(
            {
                "id": pa.array(range(2 * batch_rows + 1), pa.int32()),
                "text": pa.array(["value"] * (2 * batch_rows + 1)),
            }
        )
        pq.write_table(batches, root / "batches", row_group_size=batch_rows)
        damaged = bytearray((root / "batches").read_bytes())
        offset = (
            pq.read_metadata(root / "batches").row_group(1).column(0).data_page_offset
        )
        damaged[offset] = 0
        (root / "corrupt-lookahead").write_bytes(damaged)
        return
    _write_table(root / "unsupported-only", table.select(["unused"]), format)
    subnet_storage = pa.struct(
        [
            pa.field(
                "address",
                pa.binary(16),
                metadata={
                    "ARROW:extension:name": "tenzir.ip",
                    "ARROW:extension:metadata": "tenzir.ip",
                },
            ),
            pa.field("length", pa.uint8()),
        ]
    )
    subnet_schema = pa.schema(
        [
            pa.field(
                "subnet",
                subnet_storage,
                metadata={
                    "ARROW:extension:name": "tenzir.subnet",
                    "ARROW:extension:metadata": "tenzir.subnet",
                },
            )
        ]
    )
    for child in ["address", "length"]:
        value = {"address": bytes(16), "length": 64}
        value[child] = None
        subnets = pa.Table.from_arrays(
            [pa.array([value], type=subnet_storage)], schema=subnet_schema
        )
        _write_table(root / f"subnet-null-{child}", subnets, format)
    types = pa.table(
        {
            "id": pa.array(range(4), pa.int32()),
            "flag": pa.array([True, None, False, True]),
            "count": pa.array([1, None, 3, 4], pa.uint32()),
            "value": pa.array([1.5, None, -2.5, 0.0], pa.float32()),
            "text": pa.array(["first", None, "", "last"]),
            "bytes": pa.array([b"\x00\xff", None, b"", b"last"], pa.binary()),
            "timestamp": pa.array([0, None, 1234, 5678], pa.timestamp("ms")),
            "record": pa.array([{"x": 1}, None, {"x": None}, {"x": 4}]),
            "list": pa.array([[0, 1], None, [], [None, 4]]),
            "records": pa.array([[{"x": 1}, None], [], None, [{"x": 4}]]),
            "null": pa.nulls(4),
            "dictionary": pa.chunked_array(
                [
                    pa.DictionaryArray.from_arrays(
                        pa.array([0, 1, None], pa.int8()), pa.array(["first", None])
                    ),
                    pa.DictionaryArray.from_arrays(
                        pa.array([1], pa.int8()), pa.array(["unused", "last"])
                    ),
                ]
            ),
        }
    ).replace_schema_metadata({"TENZIR:name:0": "test.feather"})
    if format == "ipc_file":
        # IPC files require a single dictionary; streams may replace it.
        types = types.set_column(
            types.schema.get_field_index("dictionary"),
            "dictionary",
            pa.DictionaryArray.from_arrays(
                pa.array([0, 1, None, 2], pa.int8()),
                pa.array(["first", None, "last"]),
            ),
        )
    for compressed in [False, True]:
        name = "types-compressed" if compressed else "types"
        _write_table(root / name, types, format, compressed=compressed)
    _write_table(root / "empty", clean.slice(0, 0), format)
    zero_columns = pa.RecordBatch.from_struct_array(
        pa.Array.from_buffers(pa.struct([]), 4, [None])
    )
    _write_table(root / "zero-columns", pa.Table.from_batches([zero_columns]), format)
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
    # Store envelopes preserve the last import time of each batch, not the
    # event field named import_time. Exercise nulls and the historical prefix.
    historical = envelope.set_column(
        0,
        "import_time",
        pa.array([0, 1, 2, 3, 4, None, 6, 7, 8], pa.timestamp("ns")),
    )
    historical = historical.cast(
        pa.schema(
            [
                historical.schema.field(0),
                historical.schema.field(1).with_metadata(
                    {
                        "VAST:name:0": "test.historical",
                        "VAST:attributes:0": '{"internal":""}',
                    }
                ),
            ]
        )
    )
    _write_table(root / "historical", historical, format)

    if format == "ipc_stream":
        first = _stream_bytes(table.slice(0, 3))
        second = table.slice(3).select(["nested", "unused", "literal.dot", "id"])
        (root / "concatenated").write_bytes(first + _stream_bytes(second))
        (root / "trailing").write_bytes(first + b"not another IPC stream")


if __name__ == "__main__":
    _setup_inputs(Path(sys.argv[1]), sys.argv[2])
