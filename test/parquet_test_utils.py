"""Helpers for checking interoperability of Parquet readers and writers."""

import json
import os
import shlex
import subprocess


def assert_roundtrip(writer: str) -> None:
    binary = shlex.split(os.environ["TENZIR_BINARY"])
    # Use the file format as the boundary while the writer and reader use
    # different in-memory event representations.
    encoded = subprocess.run(
        [
            *binary,
            "--bare-mode",
            "--nova=false",
            'from {x: 1, msg: "alpha"}, {x: 2, msg: "beta"}\n' + writer,
        ],
        capture_output=True,
        timeout=20,
    )
    assert encoded.returncode == 0, encoded.stderr.decode()
    assert not encoded.stderr, encoded.stderr.decode()
    decoded = subprocess.run(
        [
            *binary,
            "--bare-mode",
            "--nova",
            "load_stdin\nread_parquet\nwrite_json compact=true",
        ],
        input=encoded.stdout,
        capture_output=True,
        timeout=20,
    )
    assert decoded.returncode == 0, decoded.stderr.decode()
    assert not decoded.stderr, decoded.stderr.decode()
    rows = [json.loads(line) for line in decoded.stdout.splitlines()]
    assert rows == [{"x": 1, "msg": "alpha"}, {"x": 2, "msg": "beta"}], rows
    print(f"{{\n  n: {len(rows)},\n  total: {sum(row['x'] for row in rows)},\n}}")
