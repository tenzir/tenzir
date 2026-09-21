# runner: python
# fixtures: [clickhouse]
# timeout: 120
"""Adding a catch-all must preserve blob writes to Array(UInt8)."""

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
    for case, expression, payload in (
        ("empty", 'b""', []),
        ("bytes", 'b"abc"', [97, 98, 99]),
        ("boundaries", '"AH+A/w==".decode_base64()', [0, 127, 128, 255]),
    ):
        for catch_all in (False, True):
            table = f"mapped_{case}_{int(catch_all)}"
            extra = ", extra JSON COMMENT 'tenzir:catch_all'" if catch_all else ""
            query(f"CREATE TABLE {table} (payload Array(UInt8){extra}) ENGINE=Memory")
            program = f'''from {{payload:{expression}}}
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
                    f"SELECT * FROM {table} FORMAT JSONEachRow"
                ).splitlines()
            ]
            expected = {"payload": payload}
            if catch_all:
                expected["extra"] = {}
            # Successful insertion alone misses values redirected to extra.
            # Check every case before reporting failures so both table variants
            # and all input shapes are exercised even before the fix.
            if result.returncode != 0 or rows != [expected] or result.stderr:
                failures.append(
                    {
                        "table": table,
                        "expected": expected,
                        "actual": rows,
                        "exit_code": result.returncode,
                        "diagnostics": result.stderr,
                    }
                )
    assert not failures, json.dumps(failures, indent=2)
    print("ok")


if __name__ == "__main__":
    main()
