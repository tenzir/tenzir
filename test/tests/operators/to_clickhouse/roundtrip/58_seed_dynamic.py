# runner: python
"""Seed two tables for the dynamic-table test: one with a JSON column, one with
a plain String column. `to_clickhouse` must resolve the target per row and
serialize the JSON column only for the table that actually has one."""

import os
import subprocess

# /// script
# ///


def main() -> None:
    runtime = os.environ["CLICKHOUSE_CONTAINER_RUNTIME"]
    container = os.environ["CLICKHOUSE_CONTAINER_ID"]
    password = os.environ["CLICKHOUSE_PASSWORD"]
    sql = """
SET allow_experimental_json_type = 1;
DROP TABLE IF EXISTS dyn_a;
DROP TABLE IF EXISTS dyn_b;
CREATE TABLE dyn_a (kind String, payload JSON) ENGINE = MergeTree ORDER BY tuple();
CREATE TABLE dyn_b (kind String, payload String) ENGINE = MergeTree ORDER BY tuple();
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
