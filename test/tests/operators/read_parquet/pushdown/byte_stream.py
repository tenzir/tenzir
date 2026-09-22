# runner: python
# timeout: 120

"""Exercise Parquet decoding through file, split-byte, and compressed input."""

import gzip
import json
import os
from pathlib import Path
import shlex
import subprocess


def read(path: Path, tail: str = "", mode: str = "file", reader: str = "read_parquet"):
    source = path
    prefix = ""
    if mode == "gzip":
        source = Path(os.environ["TENZIR_TMP_DIR"]) / f"{path.name}.gz"
        source.write_bytes(gzip.compress(path.read_bytes(), mtime=0))
        prefix = "decompress_gzip\n"
    if mode != "file":
        prefix += "split_bytes 97\n"
    pipeline = (
        f"from_file {json.dumps(str(source))} {{\n{prefix}{reader}\n}}\n"
        f"{tail}\nwrite_json compact=true"
    )
    result = subprocess.run(
        [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova", pipeline],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, (pipeline, result.stderr)
    assert not result.stderr, (pipeline, result.stderr)
    return [json.loads(line) for line in result.stdout.splitlines()]


def main():
    root = Path(os.environ["READ_PUSHDOWN_ROOT"])
    inputs = Path(os.environ["TENZIR_INPUTS"]) / "parquet"
    cases = [
        (root / "clean", "", "read_parquet", 9),
        (root / "input", "where nested.x >= 54\nselect id\nhead 2", "read_parquet", 2),
        (root / "input", "where nested.x >= 100\nselect id", "read_parquet", 0),
        (root / "input", "select id\nhead 0", "read_parquet", 0),
        (root / "corrupt-unused", "select id\nhead 2", "read_parquet", 2),
        (root / "input", "select\nhead 4", "read_parquet", 4),
        (root / "corrupt-unused", "select\nhead 4", "read_parquet", 4),
        (root / "unsupported-only", "select\nhead 4", "read_parquet", 4),
        (root / "batches", "", "read_parquet", 16385),
        (root / "batches", "where id >= 8192\nselect id\nhead 3", "read_parquet", 3),
        (
            root / "corrupt-lookahead",
            "where id >= 0\nselect id\nhead 1",
            "read_parquet",
            1,
        ),
        (root / "types", "", "read_parquet", 4),
        (root / "types", "where id >= 2\nhead 1", "read_parquet", 1),
        (root / "types", "schema = @name", "read_parquet", 4),
        (inputs / "basic.parquet", "", "read_parquet", 2),
        (inputs / "maps.parquet", "", "read_parquet", 2),
        (inputs / "decimal128.parquet", "", "read_parquet", 3),
        (inputs / "decimal128.parquet", "", 'read_parquet decimal_format="float"', 3),
    ]
    for path, tail, reader, count in cases:
        expected = read(path, tail, reader=reader)
        assert len(expected) == count, (path, tail, expected)
        if path == root / "input" and count == 2:
            assert expected == [{"id": 4}, {"id": 5}]
        if path == root / "corrupt-unused" and count == 2:
            assert expected == [{"id": 0}, {"id": 1}]
        if tail == "select\nhead 4":
            assert expected == [{}] * 4
        if path == root / "batches" and not tail:
            assert expected == [{"id": i, "text": "value"} for i in range(16385)]
        if path == root / "batches" and count == 3:
            assert expected == [{"id": i} for i in range(8192, 8195)]
        if path == root / "corrupt-lookahead":
            assert expected == [{"id": 0}]
        if path == root / "types" and not tail:
            columns = {name: [row[name] for row in expected] for name in expected[0]}
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
                "null": [None, None, None, None],
            }, columns
        if tail == "schema = @name":
            assert all(row["schema"] == "test.parquet" for row in expected)
        for mode in ["split", "gzip"]:
            actual = read(path, tail, mode, reader)
            assert actual == expected, (path, tail, mode, expected, actual)
    # The filesystem source must forward events and retain file bindings
    # across independently instantiated readers. Order files only in the test.
    files = Path(os.environ["TENZIR_TMP_DIR"]) / "files"
    files.mkdir()
    for name in ["a", "b"]:
        (files / name).write_bytes((root / "types").read_bytes())
    rows = read(
        files / "*",
        "where id >= 2\nselect id, file",
        reader="read_parquet\nfile = $file.path",
    )
    assert sorted((row["id"], Path(row["file"]).name) for row in rows) == [
        (2, "a"),
        (2, "b"),
        (3, "a"),
        (3, "b"),
    ], rows
    assert len(read(files / "*", "head 5\nselect id")) == 5
    print("Parquet decoding is independent of byte chunking")


if __name__ == "__main__":
    main()
