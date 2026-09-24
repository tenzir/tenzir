# runner: python
# fixtures: [clickhouse]
# timeout: 120
"""Separate JSON paths must not share numeric precision checks."""

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
        (
            "fields",
            "{items:[{large:9007199254740993,ratio:1.5}]}",
            {"items": [{"large": 9007199254740993, "ratio": 1.5}]},
        ),
        (
            "nested",
            "{items:[{a:{value:9007199254740993},b:{value:1.5}}]}",
            {"items": [{"a": {"value": 9007199254740993}, "b": {"value": 1.5}}]},
        ),
        (
            "lists",
            "{items:[{large:[9007199254740993],ratio:[1.5]}]}",
            {"items": [{"large": [9007199254740993], "ratio": [1.5]}]},
        ),
        (
            "dotted",
            '{items:[{"a.b":9007199254740993,a:{b:1.5}}]}',
            {"items": [{"a.b": 9007199254740993, "a": {"b": 1.5}}]},
        ),
    ):
        for catch_all in (False, True):
            if case == "dotted" and not catch_all:
                continue
            table = f"mapped_{case}_{int(catch_all)}"
            extra = ", extra JSON COMMENT 'tenzir:catch_all'" if catch_all else ""
            query(f"CREATE TABLE {table} (payload JSON{extra}) ENGINE=Memory")
            program = f'''from {{payload:{expression}, unmapped:{expression}}}
to_clickhouse table="{table}", host=env("CLICKHOUSE_HOST"),
              port=int(env("CLICKHOUSE_PORT")),
              password=env("CLICKHOUSE_PASSWORD"), tls=false,
              mode="append", _jobs=1
'''
            if not catch_all:
                program = program.replace(
                    "\nto_clickhouse", "\nselect payload\nto_clickhouse"
                )
            result = subprocess.run(
                [*shlex.split(os.environ["TENZIR_BINARY"]), program],
                capture_output=True,
                text=True,
                timeout=60,
            )
            rows = [
                json.loads(line)
                for line in query(
                    f"SELECT * FROM {table} SETTINGS json_type_escape_dots_in_keys=1 FORMAT JSONEachRow"
                ).splitlines()
            ]
            expected = {"payload": payload}
            if catch_all:
                expected["extra"] = {"unmapped": payload}
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
