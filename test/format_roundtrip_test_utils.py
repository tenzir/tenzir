"""Shared Nova writer/reader roundtrip checks for columnar formats."""

import json
import os
import shlex
import subprocess

import pyarrow as pa


SOURCE = """
from {x: 0, msg: "masked", ip: 192.0.2.9, subnet: 192.0.2.0/24,
      list: [9], nested: {ok: false}, missing: null},
     {x: 1, msg: "alpha", ip: 192.0.2.1, subnet: 192.0.2.0/24,
      list: [1, 2], nested: {ok: true}, missing: null},
     {x: 2, msg: "beta", ip: 192.0.2.2, subnet: 192.0.2.0/24,
      list: [3], nested: {ok: false}, missing: null}
where x > 0
empty = []
mixed = [x, "x"]
nested_mixed = {
  lists: [[x, "x"], ["y"]],
  records: [{value: x}, {value: "x"}]
}
"""
STRINGIFY = """
text = f"{msg}/{ip}/{subnet}/{list}/{nested}/{missing}"
"""
EXPECTED = [
    {
        "x": 1,
        "msg": "alpha",
        "ip": "192.0.2.1",
        "subnet": "192.0.2.0/24",
        "list": [1, 2],
        "nested": {"ok": True},
        "missing": None,
        "text": "alpha/192.0.2.1/192.0.2.0/24/[1,2]/{ok:true}/null",
        "empty": [],
        "mixed": ["1", "x"],
        "nested_mixed": {
            "lists": [["1", "x"], ["y"]],
            "records": [{"value": "1"}, {"value": "x"}],
        },
    },
    {
        "x": 2,
        "msg": "beta",
        "ip": "192.0.2.2",
        "subnet": "192.0.2.0/24",
        "list": [3],
        "nested": {"ok": False},
        "missing": None,
        "text": "beta/192.0.2.2/192.0.2.0/24/[3]/{ok:false}/null",
        "empty": [],
        "mixed": ["2", "x"],
        "nested_mixed": {
            "lists": [["2", "x"], ["y"]],
            "records": [{"value": "2"}, {"value": "x"}],
        },
    },
]


def run_process(pipeline: str, *, data: bytes | None = None):
    return subprocess.run(
        [
            *shlex.split(os.environ["TENZIR_BINARY"]),
            "--bare-mode",
            "--nova=true",
            pipeline,
        ],
        input=data,
        capture_output=True,
        timeout=20,
    )


def run(pipeline: str, *, data: bytes | None = None):
    result = run_process(pipeline, data=data)
    assert result.returncode == 0, (pipeline, result.stderr.decode())
    assert not result.stderr, (pipeline, result.stderr.decode())
    return result.stdout


def assert_metadata_roundtrip(writer: str, readers: list[str]) -> None:
    # Seed metadata through Arrow because Nova cannot assign @name/@internal.
    # These keys and the JSON attributes match type::make_arrow_metadata().
    table = pa.table({"x": [0, 1, 2]}).replace_schema_metadata(
        {
            "TENZIR:name:0": "test.format_roundtrip",
            "TENZIR:attributes:0": json.dumps({"internal": ""}),
        }
    )
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as ipc_writer:
        ipc_writer.write_table(table)
    encoded = run(
        "load_stdin\nread_feather\nwhere x > 0\n"
        "input_name = @name\ninput_internal = @internal\n" + writer,
        data=sink.getvalue().to_pybytes(),
    )
    assert encoded, writer
    for reader in readers:
        decoded = run(
            f"load_stdin\n{reader}\n"
            "output_name = @name\noutput_internal = @internal\n"
            "write_json compact=true",
            data=encoded,
        )
        rows = [json.loads(line) for line in decoded.splitlines()]
        assert rows == [
            {
                "x": x,
                "input_name": "test.format_roundtrip",
                "input_internal": True,
                "output_name": "test.format_roundtrip",
                "output_internal": True,
            }
            for x in [1, 2]
        ], (writer, reader, rows)


def encode_batches(batches: list[list[dict]]) -> bytes:
    # `batch` does not accept Nova events. Separate IPC streams force distinct
    # input batches, including independently inferred null/list child types.
    streams = []
    for rows in batches:
        table = pa.Table.from_pylist(rows)
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, table.schema) as ipc_writer:
            ipc_writer.write_table(table)
        streams.append(sink.getvalue().to_pybytes())
    return b"".join(streams)


def assert_schema_inference(writer: str, readers: list[str]) -> None:
    cases = [
        [[{"x": None}], [{"x": 42}], [{"x": None}]],
        [[{"x": 42}], [{"x": None}], [{"x": 7}]],
        [
            [{"nested": {"lists": [[]], "records": [{"x": None}]}}],
            [{"nested": {"lists": [[1, 2]], "records": [{"x": 3}]}}],
            [{"nested": {"lists": [[], [None]], "records": [{"x": None}]}}],
        ],
        # EOF must flush unresolved columns without losing nulls or empty lists.
        [
            [{"x": None, "nested": {"lists": [[]], "value": None}}],
            [{"x": None, "nested": {"lists": [], "value": None}}],
        ],
        # Cross the default discovery row bound while the schema is unresolved.
        [[{"x": None}] * 10_000, [{"x": None}]],
    ]
    for batches in cases:
        encoded = run(
            f"load_stdin\nread_feather\n{writer}", data=encode_batches(batches)
        )
        assert encoded, (writer, batches)
        expected = [row for batch in batches for row in batch]
        for reader in readers:
            decoded = run(
                f"load_stdin\n{reader}\nwrite_json compact=true", data=encoded
            )
            rows = [json.loads(line) for line in decoded.splitlines()]
            assert rows == expected, (writer, reader, rows, expected)
    for batches in [
        [[{"x": 1}], [{"x": "conflict"}]],
        # Refinement is no longer allowed after the discovery bound is reached.
        [[{"x": None}] * 10_000, [{"x": 42}]],
    ]:
        result = run_process(
            f"load_stdin\nread_feather\n{writer}", data=encode_batches(batches)
        )
        assert result.returncode != 0, (writer, batches, result.stderr.decode())
        assert "input schema changed" in result.stderr.decode(), (
            writer,
            result.stderr.decode(),
        )


def assert_format_roundtrip(writer: str, readers: list[str]) -> list[dict]:
    assert_schema_inference(writer, readers)
    assert_metadata_roundtrip(writer, readers)
    # These heterogeneous lists must be unified by the writer, including
    # conflicts nested inside records and lists, rather than by format strings.
    encoded = run(SOURCE + STRINGIFY + writer)
    assert encoded, writer
    for reader in readers:
        decoded = run(
            f"load_stdin\n{reader}\n"
            "stored_text = text\n" + STRINGIFY + "write_json compact=true",
            data=encoded,
        )
        rows = [json.loads(line) for line in decoded.splitlines()]
        # Exercise stringification on both sides of the Arrow boundary, not
        # just JSON printing of the decoded columns.
        assert rows == [dict(row, stored_text=row["text"]) for row in EXPECTED], (
            writer,
            reader,
            rows,
        )
    return rows
