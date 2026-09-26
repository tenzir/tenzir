# runner: python
# timeout: 120

"""Exercise Parquet decoding through file, split-byte, and compressed input."""

from concurrent.futures import ThreadPoolExecutor
import gzip
import json
import os
from pathlib import Path
import shlex
import subprocess
import tempfile

# Pipelines are independent, so run a few at a time to stay well within the
# test timeout on loaded machines.
WORKERS = 4


def run(path: Path, tail: str = "", mode: str = "file", reader: str = "read_parquet"):
    source = path
    prefix = ""
    if mode == "gzip":
        # A file per call keeps concurrent cases from sharing a copy.
        with tempfile.NamedTemporaryFile(
            dir=os.environ["TENZIR_TMP_DIR"], suffix=".gz", delete=False
        ) as copy:
            copy.write(gzip.compress(path.read_bytes(), mtime=0))
        source = Path(copy.name)
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
    return pipeline, result


def read(path: Path, tail: str = "", mode: str = "file", reader: str = "read_parquet"):
    pipeline, result = run(path, tail, mode, reader)
    assert result.returncode == 0, (pipeline, result.stderr)
    assert not result.stderr, (pipeline, result.stderr)
    return [json.loads(line) for line in result.stdout.splitlines()]


def per_group(*values):
    """The values of a column with one value per row group of three rows."""
    return [value for value in values for _ in range(3)]


def main():
    root = Path(os.environ["READ_PUSHDOWN_ROOT"])
    inputs = Path(os.environ["TENZIR_INPUTS"]) / "parquet"
    cases = [
        (root / "clean", "", "read_parquet", 9),
        (root / "input", "where nested.x >= 54\nselect id\nhead 2", "read_parquet", 2),
        (root / "input", "where nested.x >= 100\nselect id", "read_parquet", 0),
        (root / "input", "select id\nhead 0", "read_parquet", 0),
        (root / "corrupt-unused", "select id\nhead 2", "read_parquet", 2),
        (root / "corrupt-nested", "select nested.x\nhead 2", "read_parquet", 2),
        (root / "constants", "", "read_parquet", 9),
        (root / "constants", 'where group == "b"\nselect id', "read_parquet", 3),
        (root / "statistics", "", "read_parquet", 9),
        (root / "statistics", "", 'read_parquet decimal_format="float"', 9),
        (
            root / "statistics",
            'where text == "b"\nselect id, record.c',
            "read_parquet",
            3,
        ),
        (root / "statistics", "select same, none, record.inner", "read_parquet", 9),
        (root / "corrupt-constants", "", "read_parquet", 9),
        (
            root / "corrupt-constants",
            "where number == 42\nselect id, meta",
            "read_parquet",
            9,
        ),
        (root / "input", "select\nhead 4", "read_parquet", 4),
        (root / "corrupt-unused", "select\nhead 4", "read_parquet", 4),
        (root / "unsupported-only", "select\nhead 4", "read_parquet", 4),
        (root / "batches", "", "read_parquet", 16385),
        (root / "single-group", "", "read_parquet", 16385),
        (
            root / "single-group",
            "where id >= 8192\nselect id\nhead 3",
            "read_parquet",
            3,
        ),
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

    def check(case):
        path, tail, reader, count = case
        expected = read(path, tail, reader=reader)
        assert len(expected) == count, (path, tail, expected)
        if path == root / "input" and count == 2:
            assert expected == [{"id": 4}, {"id": 5}]
        if path == root / "corrupt-unused" and count == 2:
            assert expected == [{"id": 0}, {"id": 1}]
        if path == root / "constants" and not tail:
            columns = {name: [row[name] for row in expected] for name in expected[0]}
            assert columns == {
                "id": list(range(9)),
                "group": ["a"] * 3 + ["b"] * 3 + ["c"] * 3,
                "gaps": ["x", None, "x", "x", "x", "x", None, None, None],
                "bytes": ["AA=="] * 9,
                "category": ["same"] * 9,
            }, columns
        if path == root / "constants" and tail:
            assert expected == [{"id": 3}, {"id": 4}, {"id": 5}]
        if path == root / "statistics" and not tail:
            columns = {name: [row[name] for row in expected] for name in expected[0]}
            decimals = ("1.50", "-2.25", "0.00")
            if "float" in reader:
                decimals = (1.5, -2.25, 0.0)
            assert columns == {
                "id": list(range(9)),
                "flag": per_group(True, False, True),
                "small": per_group(-128, 127, 0),
                "count": per_group(0, 2**32 - 1, 7),
                "big": per_group(0, 2**64 - 1, 1),
                "text": per_group("a", "b", "c"),
                "time": per_group(
                    "1970-01-01T00:00:00Z",
                    "1970-01-01T00:00:01.234Z",
                    "1970-01-01T00:00:05.678Z",
                ),
                "duration": per_group("1s", "1min", "1h"),
                "decimal": per_group(*decimals),
                # JSON has no NaN, which the writer prints as null.
                "nan": [1.0, None, 1.0, *per_group(2.0, 3.0)],
                "same": ["everywhere"] * 9,
                "none": [None] * 9,
                "record": [
                    {"x": i, "c": 7, "n": None, "inner": {"a": "ab"[i // 6], "b": None}}
                    for i in range(9)
                ],
            }, columns
        if path == root / "statistics" and tail.startswith("where"):
            assert expected == [{"id": i, "record": {"c": 7}} for i in range(3, 6)]
        if path == root / "statistics" and tail.startswith("select"):
            assert expected == [
                {
                    "same": "everywhere",
                    "none": None,
                    "record": {"inner": {"a": "ab"[i // 6], "b": None}},
                }
                for i in range(9)
            ], expected
        if path == root / "corrupt-constants":
            # The damaged column chunks come from the statistics.
            meta = {"product": "p", "version": 1}
            if tail:
                assert expected == [{"id": i, "meta": meta} for i in range(9)]
            else:
                assert expected == [
                    {
                        "id": i,
                        "number": 42,
                        "same": "everywhere",
                        "none": None,
                        "record": {"x": i, "c": 7, "n": None},
                        "meta": meta,
                    }
                    for i in range(9)
                ], expected
        if path == root / "corrupt-nested":
            assert expected == [{"nested": {"x": 50}}, {"nested": {"x": 51}}]
        if tail == "select\nhead 4":
            assert expected == [{}] * 4
        if path.name in {"batches", "single-group"} and not tail:
            assert expected == [{"id": i, "text": "value"} for i in range(16385)]
        if path.name in {"batches", "single-group"} and count == 3:
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

    with ThreadPoolExecutor(WORKERS) as pool:
        list(pool.map(check, cases))
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
    # Failures must look the same whether the reader seeks or buffers.
    tmp = Path(os.environ["TENZIR_TMP_DIR"])
    (tmp / "empty").write_bytes(b"")
    (tmp / "not-parquet").write_bytes(b"not a parquet file")
    failures = [
        (root / "corrupt-lookahead", "where id >= 8192\nselect id\nhead 1"),
        (root / "corrupt-unused", "head 1"),
        (root / "corrupt-nested", "select nested\nhead 1"),
        (tmp / "empty", ""),
        (tmp / "not-parquet", ""),
    ]

    def check_failure(case):
        path, tail = case
        _, expected = run(path, tail)
        for mode in ["split", "gzip"]:
            _, actual = run(path, tail, mode)
            assert (actual.returncode, actual.stdout, actual.stderr) == (
                expected.returncode,
                expected.stdout,
                expected.stderr,
            ), (path, tail, mode, expected.stderr, actual.stderr)

    with ThreadPoolExecutor(WORKERS) as pool:
        list(pool.map(check_failure, failures))
    print("Parquet decoding is independent of byte chunking")


if __name__ == "__main__":
    main()
