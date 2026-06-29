# runner: python
"""Seed a ClickHouse table with a Nullable(JSON) column."""

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
DROP TABLE IF EXISTS test_json_nullable;
CREATE TABLE test_json_nullable (id Int64, payload Nullable(JSON))
  ENGINE = MergeTree ORDER BY id;
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
