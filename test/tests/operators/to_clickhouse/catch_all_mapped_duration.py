# runner: python
# fixtures: [clickhouse]
# timeout: 120
"""Adding a catch-all must preserve duration writes as Int64 nanoseconds."""

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
        values = [("1s", 1000000000), ("-1ns", -1), ("0ns", 0)]
        if nullable:
            values.append(("null", None))
        for catch_all in (False, True):
            table = f"durations_{int(nullable)}_{int(catch_all)}"
            dtype = "Nullable(Int64)" if nullable else "Int64"
            extra = ", extra JSON COMMENT 'tenzir:catch_all'" if catch_all else ""
            query(
                f"CREATE TABLE {table} (id Int64, payload {dtype}{extra}) ENGINE=Memory"
            )
            events = ", ".join(
                f"{{id:{i},payload:{expression}}}"
                for i, (expression, _) in enumerate(values)
            )
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
            rows = [
                json.loads(line)
                for line in query(
                    f"SELECT * FROM {table} ORDER BY id FORMAT JSONEachRow"
                ).splitlines()
            ]
            expected = [
                dict(id=i, payload=value, **({"extra": {}} if catch_all else {}))
                for i, (_, value) in enumerate(values)
            ]
            if result.returncode != 0 or rows != expected or result.stderr:
                failures.append(
                    dict(
                        table=table,
                        actual=rows,
                        expected=expected,
                        diagnostics=result.stderr,
                        exit_code=result.returncode,
                    )
                )
    assert not failures, json.dumps(failures, indent=2)
    print("ok")


if __name__ == "__main__":
    main()
