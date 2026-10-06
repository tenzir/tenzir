# runner: python
"""Verify that nulls got the column defaults, including for list and JSON
columns, and that the event with a null for a column without a default was
dropped."""

import os
import subprocess

# /// script
# ///


def ch_query(sql: str) -> str:
    runtime = os.environ["CLICKHOUSE_CONTAINER_RUNTIME"]
    container = os.environ["CLICKHOUSE_CONTAINER_ID"]
    password = os.environ["CLICKHOUSE_PASSWORD"]
    result = subprocess.run(
        [
            runtime,
            "exec",
            container,
            "clickhouse-client",
            f"--password={password}",
            f"--query={sql}",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    return result.stdout.strip()


def main() -> None:
    rows = ch_query("SELECT id, lit, expr, name FROM test_null_default ORDER BY id")
    print(rows.replace("\t", " "))
    assert rows.splitlines() == [
        "1\t1\t1\tone",
        "2\t7\t20\tid-2",
        "3\t7\t30\tid-3",
        "4\t7\t40\tid-4",
    ], f"unexpected rows: {rows!r}"
    rows = ch_query(
        "SELECT id, arrayStringConcat(tags, ','), toString(meta) "
        "FROM test_null_default ORDER BY id"
    )
    print(rows.replace("\t", " "))
    assert rows.splitlines() == [
        '1\ta\t{"k":"v"}',
        '2\tx\t{"k":"d"}',
        '3\tx\t{"k":"d"}',
        '4\tx\t{"k":"d"}',
    ], f"unexpected rows: {rows!r}"
    rows = ch_query(
        "SELECT class_uid, activity_id, severity_id, "
        "arrayStringConcat(tags, ','), toString(event) "
        "FROM test_null_default_catch_all ORDER BY time"
    )
    print(rows.replace("\t", " "))
    assert rows.splitlines() == [
        '4001\t0\t1\tx\t{"message":"a"}',
        '4001\t0\t1\tx\t{"message":"b"}',
        '0\t3\t0\tx\t{"message":"c"}',
    ], f"unexpected rows: {rows!r}"
    rows = ch_query("SELECT id, req FROM test_null_required ORDER BY id")
    print(rows.replace("\t", " "))
    assert rows == "1\t1", f"unexpected rows: {rows!r}"
    print("ok")


if __name__ == "__main__":
    main()
