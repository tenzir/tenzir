# runner: python
# fixtures: [clickhouse]
# timeout: 120
"""Nullable array elements must retain numeric range validation."""

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
    for nullable in (False, True):
        table = f"checked_array_{int(nullable)}"
        dtype = "Nullable(UInt32)" if nullable else "UInt32"
        query(f"CREATE TABLE {table} (id Int64, payload Array({dtype})) ENGINE=Memory")
        valid = (
            "[uint(0), uint(4294967295), null]"
            if nullable
            else "[uint(0), uint(4294967295)]"
        )
        nullable_events = (
            ", {id:3,payload:[]}, {id:4,payload:[null]}" if nullable else ""
        )
        program = f'''from {{id:0,payload:{valid}}}, {{id:1,payload:[uint(4294967296)]}}, {{id:2,payload:[uint(42)]}}{nullable_events}
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
                f"SELECT * FROM {table} ORDER BY id FORMAT JSONEachRow"
            ).splitlines()
        ]
        expected = [
            {"id": 0, "payload": [0, 4294967295] + ([None] if nullable else [])},
            {"id": 2, "payload": [42]},
        ]
        if nullable:
            expected.append({"id": 3, "payload": []})
            expected.append({"id": 4, "payload": [None]})
        if (
            result.returncode != 0
            or rows != expected
            or "value out of range" not in result.stderr
        ):
            failures.append(
                dict(
                    table=table,
                    expected=expected,
                    actual=rows,
                    exit_code=result.returncode,
                    diagnostics=result.stderr,
                )
            )
    assert not failures, json.dumps(failures, indent=2)
    print("ok")


if __name__ == "__main__":
    main()
