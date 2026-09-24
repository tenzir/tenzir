# runner: python
# fixtures: [clickhouse]
# timeout: 120
"""Marked byte-array columns accept blobs and unsigned integer lists."""

import json
import os
import shlex
import subprocess

# /// script
# ///


def query(sql: str) -> str:
    result = subprocess.run(
        [
            os.environ["CLICKHOUSE_CONTAINER_RUNTIME"],
            "exec",
            os.environ["CLICKHOUSE_CONTAINER_ID"],
            "clickhouse-client",
            f"--password={os.environ['CLICKHOUSE_PASSWORD']}",
            "--multiquery",
            "--query",
            sql,
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    return result.stdout.strip()


def main() -> None:
    query(
        "CREATE TABLE mapped_byte_lists (id Int64, payload Array(UInt8), "
        "extra JSON COMMENT 'tenzir:catch_all') ENGINE=Memory"
    )
    program = """from {id:0,payload:[uint(0),uint(255)]},
                     {id:1,payload:"AP8=".decode_base64()},
                     {id:2,payload:[]}, {id:3,payload:[uint(256)]},
                     {id:4,payload:[uint(42)]}
to_clickhouse table="mapped_byte_lists", host=env("CLICKHOUSE_HOST"),
              port=int(env("CLICKHOUSE_PORT")),
              password=env("CLICKHOUSE_PASSWORD"), tls=false,
              mode="append", _jobs=1
"""
    result = subprocess.run(
        [*shlex.split(os.environ["TENZIR_BINARY"]), program],
        capture_output=True,
        text=True,
        timeout=60,
    )
    rows = [
        json.loads(line)
        for line in query(
            "SELECT id, payload FROM mapped_byte_lists ORDER BY id FORMAT JSONEachRow"
        ).splitlines()
    ]
    expected = [
        {"id": 0, "payload": [0, 255]},
        {"id": 1, "payload": [0, 255]},
        {"id": 2, "payload": []},
        {"id": 4, "payload": [42]},
    ]
    assert result.returncode == 0, result.stderr
    assert rows == expected, (rows, result.stderr)
    assert "value out of range" in result.stderr, result.stderr
    assert "expected `blob`" not in result.stderr, result.stderr
    print("ok")


if __name__ == "__main__":
    main()
