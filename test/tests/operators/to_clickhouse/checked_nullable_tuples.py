# runner: python
# fixtures: [clickhouse]
# timeout: 120
"""Fully nullable tuples must retain numeric range validation."""

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
    for nested in (False, True):
        table = f"checked_tuple_{int(nested)}"
        dtype = "Tuple(n Nullable(UInt32))"
        if nested:
            dtype = f"Tuple(inner {dtype})"
        query(f"CREATE TABLE {table} (id Int64, payload {dtype}) ENGINE=Memory")

        def payload(value: str) -> str:
            record = "{n:" + value + "}"
            return "{inner:" + record + "}" if nested else record

        events = ", ".join(
            "{id:" + str(i) + ",payload:" + payload(value) + "}"
            for i, value in enumerate(
                ("uint(0)", "uint(4294967296)", "uint(4294967295)", "null")
            )
        )
        events += ", {id:4,payload:null}"
        program = f'''from {events}
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
        field = "payload.inner.n" if nested else "payload.n"
        rows = [
            json.loads(line)
            for line in query(
                f"SELECT id, {field} AS n FROM {table} ORDER BY id FORMAT JSONEachRow"
            ).splitlines()
        ]
        expected = [
            {"id": 0, "n": 0},
            {"id": 2, "n": 4294967295},
            {"id": 3, "n": None},
            {"id": 4, "n": None},
        ]
        assert result.returncode == 0, result.stderr
        assert rows == expected, (table, rows, result.stderr)
        assert "value out of range" in result.stderr, result.stderr
    print("ok")


if __name__ == "__main__":
    main()
