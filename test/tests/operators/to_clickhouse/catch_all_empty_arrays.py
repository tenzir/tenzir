# runner: python
# fixtures: [clickhouse]
# timeout: 120
"""Retain empty arrays while rejecting incompatible elements and null arrays."""

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
    failures = []
    for case, dtype, events, expected in (
        (
            "nullable_null_elements",
            "Array(Nullable(String))",
            "{id:0,payload:null}, {id:1,payload:[null]}, {id:2,payload:[]}",
            [{"id": 1, "payload": [None]}, {"id": 2, "payload": []}],
        ),
        (
            "nullable_string",
            "Array(Nullable(String))",
            "{id:0,payload:[]}, {id:1,payload:[1]}, {id:2,payload:[]}",
            [{"id": 0, "payload": []}, {"id": 2, "payload": []}],
        ),
        (
            "nullable_bool",
            "Array(Nullable(Bool))",
            "{id:0,payload:[]}, {id:1,payload:[1]}, {id:2,payload:[]}",
            [{"id": 0, "payload": []}, {"id": 2, "payload": []}],
        ),
        (
            "nullable_int64",
            "Array(Nullable(Int64))",
            '{id:0,payload:[]}, {id:1,payload:["x"]}, {id:2,payload:[]}',
            [{"id": 0, "payload": []}, {"id": 2, "payload": []}],
        ),
        (
            "null_parent",
            "Array(UInt32)",
            "{id:0,payload:null}, {id:1,payload:[uint(7)]}, {id:2,payload:[]}",
            [{"id": 1, "payload": [7]}, {"id": 2, "payload": []}],
        ),
        (
            "null_parent_nullable_elements",
            "Array(Nullable(UInt32))",
            "{id:0,payload:null}, {id:1,payload:[null,uint(7)]}, {id:2,payload:[]}",
            [{"id": 1, "payload": [None, 7]}, {"id": 2, "payload": []}],
        ),
        (
            "null_inner_array",
            "Array(Array(UInt32))",
            "{id:0,payload:[null]}, {id:1,payload:[[uint(7)]]}, {id:2,payload:[[]]}",
            [{"id": 1, "payload": [[7]]}, {"id": 2, "payload": [[]]}],
        ),
        (
            "numeric",
            "Array(UInt32)",
            '{id:0,payload:[]}, {id:1,payload:["x"]}, {id:2,payload:[]}',
            [{"id": 0, "payload": []}, {"id": 2, "payload": []}],
        ),
        (
            "nullable",
            "Array(Nullable(UInt32))",
            '{id:0,payload:[]}, {id:1,payload:["x"]}, {id:2,payload:[]}',
            [{"id": 0, "payload": []}, {"id": 2, "payload": []}],
        ),
        (
            "nested",
            "Array(Array(UInt32))",
            '{id:0,payload:[]}, {id:1,payload:[["x"]]}, {id:2,payload:[[]]}',
            [{"id": 0, "payload": []}, {"id": 2, "payload": [[]]}],
        ),
    ):
        table = f"surviving_empty_{case}"
        query(
            f"CREATE TABLE {table} (id Int64, payload {dtype}, "
            "extra JSON COMMENT 'tenzir:catch_all') ENGINE=Memory"
        )
        # Unroll one list so empty and incompatible rows share the element type.
        program = f'''from {{events:[{events}]}}
unroll events
this = events
to_clickhouse table="{table}", host=env("CLICKHOUSE_HOST"),
              port=int(env("CLICKHOUSE_PORT")),
              password=env("CLICKHOUSE_PASSWORD"), tls=false,
              mode="append", _jobs=1
'''
        result = subprocess.run(
            [*shlex.split(os.environ["TENZIR_BINARY"]), program],
            capture_output=True,
            text=True,
            timeout=60,
        )
        rows = [
            json.loads(line)
            for line in query(
                f"SELECT id, payload FROM {table} ORDER BY id FORMAT JSONEachRow"
            ).splitlines()
        ]
        if (
            result.returncode != 0
            or rows != expected
            or "warning:" not in result.stderr
            or "failed to add column" in result.stderr
        ):
            failures.append(
                dict(
                    case=case, expected=expected, actual=rows, diagnostics=result.stderr
                )
            )
    assert not failures, json.dumps(failures, indent=2)
    print("ok")


if __name__ == "__main__":
    main()
