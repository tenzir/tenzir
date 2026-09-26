"""Generate columnar inputs for reader pushdown tests.

READ_PUSHDOWN_ROOT contains input, clean, and corrupt-unused files. Parquet
also provides a copy of the input whose nested sibling field is corrupt, files
whose columns hold one value or only nulls per row group, of which one is
corrupt where the statistics suffice, and a wide file whose size dwarfs
the reader's footer read, plus a gzipped copy, so that tests can tell a
random-access scan from a whole-file read by the bytes it fetches. IPC formats also provide a store envelope; IPC
streams additionally provide concatenated schemas and trailing corruption. The
fixture only generates inputs: TQL tests and their baselines check reader
behavior.
"""

# /// script
# dependencies = ["pyarrow"]
# ///

from __future__ import annotations

import gzip
import random
import sys
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

# Rows in the wide file. With 64 bytes of incompressible payload per row, the
# file is several MiB; a projection to `id` with a small limit needs a fraction.
# The payload comes from a seeded generator so that every run produces the same
# bytes.
WIDE_ROWS = 40_000
WIDE_ROW_GROUP_SIZE = 10_000
WIDE_SEED = 1034


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
        # Damage only `nested.sibling`, so that reading `nested.x` succeeds only
        # if the reader decodes the fields of a struct selectively. The sibling
        # varies, as readers need not decode a field whose statistics show a
        # single value.
        nested = pa.array([{"x": i + 50, "sibling": f"keep{i}"} for i in range(9)])
        corrupted = root / "corrupt-nested"
        pq.write_table(
            table.set_column(table.schema.get_field_index("nested"), "nested", nested),
            corrupted,
            row_group_size=3,
        )
        metadata = pq.read_metadata(corrupted).row_group(0)
        (sibling,) = (
            metadata.column(i)
            for i in range(metadata.num_columns)
            if metadata.column(i).path_in_schema == "nested.sibling"
        )
        damaged = bytearray(corrupted.read_bytes())
        damaged[sibling.data_page_offset] = 0
        corrupted.write_bytes(damaged)
        # Columns with one value per row group, which readers may keep as
        # dictionaries, next to ones whose nulls or values vary.
        constants = pa.table(
            {
                "id": pa.array(range(9), pa.int64()),
                "group": pa.array(["a"] * 3 + ["b"] * 3 + ["c"] * 3),
                "gaps": pa.array(["x", None, "x", "x", "x", "x", None, None, None]),
                "bytes": pa.array([b"\x00"] * 9, pa.binary()),
                "category": pa.array(["same"] * 9).dictionary_encode(),
            }
        )
        pq.write_table(constants, root / "constants", row_group_size=3)

        # Columns whose statistics show a single value, or only nulls, in each
        # row group, which readers may take from the statistics instead of
        # decoding. Values change between row groups, except in `same` and
        # `none`, which readers of byte streams can take from the statistics,
        # too, as their batches may span row groups. Statistics leave out NaNs.
        def per_group(*values):
            return [value for value in values for _ in range(3)]

        record = pa.struct(
            [
                ("x", pa.int64()),
                ("c", pa.int32()),
                ("n", pa.string()),
                ("inner", pa.struct([("a", pa.string()), ("b", pa.float64())])),
            ]
        )
        statistics = pa.table(
            {
                "id": pa.array(range(9), pa.int64()),
                "flag": pa.array(per_group(True, False, True)),
                "small": pa.array(per_group(-128, 127, 0), pa.int8()),
                "count": pa.array(per_group(0, 2**32 - 1, 7), pa.uint32()),
                "big": pa.array(per_group(0, 2**64 - 1, 1), pa.uint64()),
                "text": pa.array(per_group("a", "b", "c")),
                "time": pa.array(
                    per_group(0, 1234, 5678), pa.timestamp("ms", tz="UTC")
                ),
                "duration": pa.array(per_group(1, 60, 3600), pa.duration("s")),
                "decimal": pa.array(
                    per_group(Decimal("1.50"), Decimal("-2.25"), Decimal("0.00")),
                    pa.decimal128(10, 2),
                ),
                "nan": pa.array([1.0, float("nan"), 1.0, *per_group(2.0, 3.0)]),
                "same": pa.array(["everywhere"] * 9),
                "none": pa.nulls(9, pa.int64()),
                "record": pa.array(
                    [
                        {"x": i, "c": 7, "n": None, "inner": {"a": "ab"[i // 6]}}
                        for i in range(9)
                    ],
                    record,
                ),
            }
        )
        pq.write_table(statistics, root / "statistics", row_group_size=3)
        # Damage the columns that the statistics provide in every row group, so
        # that the file reads only if no reader decodes them.
        constant = pa.table(
            {
                "id": pa.array(range(9), pa.int64()),
                "number": pa.array([42] * 9, pa.int64()),
                "same": pa.array(["everywhere"] * 9),
                "none": pa.nulls(9, pa.string()),
                "record": pa.array(
                    [{"x": i, "c": 7, "n": None} for i in range(9)],
                    pa.struct(
                        [("x", pa.int64()), ("c", pa.int32()), ("n", pa.string())]
                    ),
                ),
                "meta": pa.array([{"product": "p", "version": 1}] * 9),
            }
        )
        corrupted = root / "corrupt-constants"
        pq.write_table(constant, corrupted, row_group_size=3)
        metadata = pq.read_metadata(corrupted)
        damaged = bytearray(corrupted.read_bytes())
        for group in range(metadata.num_row_groups):
            for i in range(metadata.num_columns):
                chunk = metadata.row_group(group).column(i)
                if chunk.path_in_schema not in {"id", "record.x"}:
                    first_page = (
                        chunk.dictionary_page_offset
                        if chunk.has_dictionary_page
                        else chunk.data_page_offset
                    )
                    damaged[first_page] = 0
        corrupted.write_bytes(damaged)
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
        pq.write_table(batches, root / "single-group", row_group_size=len(batches))
        damaged = bytearray((root / "batches").read_bytes())
        offset = (
            pq.read_metadata(root / "batches").row_group(1).column(0).data_page_offset
        )
        damaged[offset] = 0
        (root / "corrupt-lookahead").write_bytes(damaged)
        payload = random.Random(WIDE_SEED)
        wide = pa.table(
            {
                "id": pa.array(range(WIDE_ROWS), pa.int64()),
                "payload": pa.array(
                    [payload.randbytes(64) for _ in range(WIDE_ROWS)], pa.binary()
                ),
            }
        )
        pq.write_table(wide, root / "wide", row_group_size=WIDE_ROW_GROUP_SIZE)
        # A fixed header timestamp keeps the copy byte-identical across runs.
        (root / "wide.gz").write_bytes(
            gzip.compress((root / "wide").read_bytes(), mtime=0)
        )
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
