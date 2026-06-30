# runner: python
"""Seed a table with LowCardinality and scaled DateTime64 columns to append to."""

import os
import subprocess

# /// script
# ///


def main() -> None:
    runtime = os.environ["CLICKHOUSE_CONTAINER_RUNTIME"]
    container = os.environ["CLICKHOUSE_CONTAINER_ID"]
    password = os.environ["CLICKHOUSE_PASSWORD"]
    sql = """
DROP TABLE IF EXISTS test_lc_dt;
CREATE TABLE test_lc_dt (
    id Int64,
    name LowCardinality(String),
    ts DateTime64(3, 'UTC')
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
