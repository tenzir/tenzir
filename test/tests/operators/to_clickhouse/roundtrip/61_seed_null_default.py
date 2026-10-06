# runner: python
"""Seed tables whose non-nullable columns have literal and expression defaults,
including list and JSON columns, with and without a catch-all column, and a
table with a column without a default."""

import os
import subprocess

# /// script
# ///


def main() -> None:
    runtime = os.environ["CLICKHOUSE_CONTAINER_RUNTIME"]
    container = os.environ["CLICKHOUSE_CONTAINER_ID"]
    password = os.environ["CLICKHOUSE_PASSWORD"]
    sql = """
DROP TABLE IF EXISTS test_null_default;
CREATE TABLE test_null_default (
    id Int64,
    lit Int64 DEFAULT 7,
    expr Int64 DEFAULT id * 10,
    name String DEFAULT concat('id-', toString(id)),
    tags Array(Nullable(String)) DEFAULT ['x'],
    meta JSON DEFAULT '{"k":"d"}'
) ENGINE = MergeTree ORDER BY id;
DROP TABLE IF EXISTS test_null_default_catch_all;
CREATE TABLE test_null_default_catch_all (
    time DateTime64(9, 'UTC'),
    class_uid Int32 DEFAULT 0,
    activity_id Int32 DEFAULT 0,
    severity_id Int32 DEFAULT class_uid % 1000,
    tags Array(Nullable(String)) DEFAULT ['x'],
    event JSON COMMENT 'tenzir:catch_all'
) ENGINE = MergeTree ORDER BY (class_uid, time);
DROP TABLE IF EXISTS test_null_required;
CREATE TABLE test_null_required (
    id Int64,
    req Int64
) ENGINE = MergeTree ORDER BY id;
"""
    subprocess.run(
        [
            runtime,
            "exec",
            container,
            "clickhouse-client",
            f"--password={password}",
            "--multiquery",
            f"--query={sql}",
        ],
        capture_output=True,
        text=True,
        check=True,
    )


if __name__ == "__main__":
    main()
