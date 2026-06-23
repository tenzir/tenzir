# runner: python
"""Seed a ClickHouse table with alias and materialized columns."""

import os
import subprocess

# /// script
# ///


def main() -> None:
    runtime = os.environ["CLICKHOUSE_CONTAINER_RUNTIME"]
    container = os.environ["CLICKHOUSE_CONTAINER_ID"]
    password = os.environ["CLICKHOUSE_PASSWORD"]
    ddl = """
DROP TABLE IF EXISTS fc_generated_columns;
CREATE TABLE fc_generated_columns (
  id UInt64,
  base String,
  alias_col String ALIAS concat(base, '-alias'),
  materialized_col UInt64 MATERIALIZED id + 1
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
            f"--query={ddl}",
        ],
        capture_output=True,
        text=True,
        check=True,
    )
    subprocess.run(
        [
            runtime,
            "exec",
            container,
            "clickhouse-client",
            f"--password={password}",
            "--query=INSERT INTO fc_generated_columns (id, base) VALUES (1, 'hello')",
        ],
        capture_output=True,
        text=True,
        check=True,
    )


if __name__ == "__main__":
    main()
