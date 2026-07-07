# runner: python
"""Seed tables with defaulted columns: an unsupported-typed column (`IPv4`) with
and without a DEFAULT, and a supported-typed non-nullable column with a
DEFAULT."""

import os
import subprocess

# /// script
# ///


def main() -> None:
    runtime = os.environ["CLICKHOUSE_CONTAINER_RUNTIME"]
    container = os.environ["CLICKHOUSE_CONTAINER_ID"]
    password = os.environ["CLICKHOUSE_PASSWORD"]
    sql = """
DROP TABLE IF EXISTS test_default_col;
CREATE TABLE test_default_col (
    id Int64,
    tag IPv4 DEFAULT toIPv4('1.2.3.4')
) ENGINE = MergeTree ORDER BY id;
DROP TABLE IF EXISTS test_no_default_col;
CREATE TABLE test_no_default_col (
    id Int64,
    tag IPv4
) ENGINE = MergeTree ORDER BY id;
DROP TABLE IF EXISTS test_supported_default;
CREATE TABLE test_supported_default (
    id Int64,
    n String DEFAULT 'def'
) ENGINE = MergeTree ORDER BY id;
DROP TABLE IF EXISTS test_all_default;
CREATE TABLE test_all_default (
    a Int64 DEFAULT 1,
    b String DEFAULT 'x'
) ENGINE = MergeTree ORDER BY a;
DROP TABLE IF EXISTS test_generated;
CREATE TABLE test_generated (
    id Int64,
    m Int64 MATERIALIZED id * 2,
    al Int64 ALIAS id + 1
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
