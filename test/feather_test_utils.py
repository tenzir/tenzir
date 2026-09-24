"""Checks shared by the Nova IPC file and stream reader tests."""

import gzip
import json
import os
from pathlib import Path
import shlex
import subprocess

from format_roundtrip_test_utils import assert_format_roundtrip


def run(pipeline: str, *, nova: bool = True, data: bytes | None = None):
    command = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode"]
    command.append(f"--nova={str(nova).lower()}")
    result = subprocess.run(
        [*command, pipeline], input=data, capture_output=True, timeout=20
    )
    assert result.returncode == 0, (pipeline, result.stderr.decode())
    return result


def read(path: Path, tail: str = "", mode: str = "file"):
    source = path
    prefix = ""
    if mode == "gzip":
        source = Path(os.environ["TENZIR_TMP_DIR"]) / f"{path.name}.gz"
        source.write_bytes(gzip.compress(path.read_bytes(), mtime=0))
        prefix = "decompress_gzip\n"
    if mode != "file":
        prefix += "split_bytes 97\n"
    pipeline = (
        f"from_file {json.dumps(str(source))} {{\n{prefix}read_feather\n}}\n"
        f"{tail}\nwrite_json compact=true"
    )
    result = run(pipeline)
    assert not result.stderr, (pipeline, result.stderr.decode())
    return [json.loads(line) for line in result.stdout.splitlines()]


def assert_byte_stream():
    root = Path(os.environ["READ_PUSHDOWN_ROOT"])
    cases = [
        ("clean", "", 9),
        ("input", "where nested.x >= 54\nselect id\nhead 2", 2),
        ("input", "where nested.x >= 100\nselect id", 0),
        ("input", "select id\nhead 0", 0),
        ("corrupt-unused", "select id\nhead 2", 2),
        ("input", "select\nhead 4", 4),
        ("corrupt-unused", "select\nhead 4", 4),
        ("unsupported-only", "select\nhead 4", 4),
        ("types", "select\nhead 4", 4),
        ("types-compressed", "select\nhead 4", 4),
        ("types", "", 4),
        ("types-compressed", "", 4),
        ("types", "where id >= 2\nhead 1", 1),
        ("types", "schema = @name", 4),
        ("envelope", "where nested.x >= 54\nselect id\nhead 2", 2),
        (
            "historical",
            "select import_time\nschema = @name\ntimestamp = @import_time",
            9,
        ),
        ("empty", "", 0),
        ("empty", "select", 0),
        ("zero-columns", "", 4),
        ("zero-columns", "select\nhead 4", 4),
    ]
    if (root / "concatenated").exists():
        cases.extend(
            [
                ("concatenated", "select\nhead 5", 5),
                ("concatenated", "where id >= 2\nselect id\nhead 4", 4),
                ("trailing", "where id >= 1\nselect id\nhead 1", 1),
            ]
        )
    for name, tail, count in cases:
        expected = read(root / name, tail)
        assert len(expected) == count, (name, tail, expected)
        if name in {"input", "envelope"} and count == 2:
            assert expected == [{"id": 4}, {"id": 5}], expected
        if name == "corrupt-unused" and count == 2:
            assert expected == [{"id": 0}, {"id": 1}], expected
        if tail.startswith("select\nhead") or name == "zero-columns":
            assert expected == [{}] * count, expected
        if name in {"types", "types-compressed"} and not tail:
            columns = {key: [row[key] for row in expected] for key in expected[0]}
            assert columns == {
                "id": [0, 1, 2, 3],
                "flag": [True, None, False, True],
                "count": [1, None, 3, 4],
                "value": [1.5, None, -2.5, 0.0],
                "text": ["first", None, "", "last"],
                "bytes": ["AP8=", None, "", "bGFzdA=="],
                "timestamp": [
                    "1970-01-01T00:00:00Z",
                    None,
                    "1970-01-01T00:00:01.234Z",
                    "1970-01-01T00:00:05.678Z",
                ],
                "record": [{"x": 1}, None, {"x": None}, {"x": 4}],
                "list": [[0, 1], None, [], [None, 4]],
                "records": [[{"x": 1}, None], [], None, [{"x": 4}]],
                "null": [None] * 4,
                "dictionary": ["first", None, None, "last"],
            }, columns
        if tail == "schema = @name":
            assert all(row["schema"] == "test.feather" for row in expected)
        if name == "historical":
            times = ["1970-01-01T00:00:00.000000002Z"] * 3
            # A missing import time reads as `null`.
            times += [None] * 3
            times += ["1970-01-01T00:00:00.000000008Z"] * 3
            assert expected == [
                {
                    "import_time": 100 + i,
                    "schema": "test.historical",
                    "timestamp": timestamp,
                }
                for i, timestamp in enumerate(times)
            ], expected
        if name == "concatenated" and count == 4:
            assert expected == [{"id": i} for i in range(2, 6)], expected
        if name == "trailing":
            assert expected == [{"id": 1}], expected
        for mode in ["split", "gzip"]:
            actual = read(root / name, tail, mode)
            assert actual == expected, (name, tail, mode, expected, actual)
    print("Nova Feather decoding preserves columns and metadata across byte chunking")


def assert_predicate_diagnostics():
    path = Path(os.environ["READ_PUSHDOWN_ROOT"]) / "types"
    cases = [
        ("where null", [], ["warning: expected `bool`"]),
        (
            "where absent",
            [],
            ["warning: event does not have field", "warning: expected `bool`"],
        ),
        ("where flag", [0, 3], ["warning: expected `bool`"]),
        ("where id != 1\nwhere flag", [0, 3], []),
        ("where false\nwhere absent", [], []),
        ("where id >= 2", [2, 3], []),
        ("where id", [], ["warning: expected `bool`"]),
    ]
    for predicates, ids, warnings in cases:
        results = []
        for pushed in [True, False]:
            barrier = " " * len("head 4") if pushed else "head 4"
            pipeline = (
                f"from_file {json.dumps(str(path))} {{ read_feather }}\n"
                f"{barrier}\n{predicates}\nselect id\nwrite_json compact=true"
            )
            results.append(run(pipeline))
        pushed, standalone = results
        assert pushed.stdout == standalone.stdout, predicates
        assert pushed.stderr == standalone.stderr, (
            predicates,
            pushed.stderr.decode(),
            standalone.stderr.decode(),
        )
        rows = [json.loads(line) for line in pushed.stdout.splitlines()]
        assert rows == [{"id": value} for value in ids], (predicates, rows)
        actual = [
            line
            for line in pushed.stderr.decode().splitlines()
            if line.startswith("warning:")
        ]
        assert actual == warnings, (predicates, actual)
    print("Pushed and standalone Nova predicates preserve rows and diagnostics")


def encode_zeek(batch_size: int, *, compressed: bool = False) -> bytes:
    # Keep unported input operators and the writer out of the Nova reader process.
    writer = "write_feather"
    if compressed:
        writer += ' compression_level=10, compression_type="zstd"'
    encoded = run(
        'from_file f"{env("TENZIR_INPUTS")}/zeek/conn.log.gz" {\n'
        "  decompress_gzip\n"
        "  read_zeek_tsv\n"
        "}\n"
        f"batch {batch_size}\n{writer}",
        nova=False,
    )
    assert not encoded.stderr, encoded.stderr.decode()
    return encoded.stdout


def assert_roundtrip():
    for writer in ["write_feather", 'write_feather compression_type="zstd"']:
        assert_format_roundtrip(writer, ["read_feather", "split_bytes 1\nread_feather"])
    print("Nova Feather roundtrips columns, stringification, and nested conflicts")
