# runner: python
"""Verify the basic table has 10k rows with expected schema."""

import os
import subprocess

# /// script
# ///


def ch_query(sql: str) -> str:
    runtime = os.environ["CLICKHOUSE_CONTAINER_RUNTIME"]
    container = os.environ["CLICKHOUSE_CONTAINER_ID"]
    password = os.environ["CLICKHOUSE_PASSWORD"]
    result = subprocess.run(
        [runtime, "exec", container, "clickhouse-client",
         f"--password={password}", f"--query={sql}"],
        capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


def main() -> None:
    print(ch_query("SELECT count() FROM test_basic"))
    print(ch_query(
        "DESCRIBE test_basic SETTINGS describe_compact_output=1"
    ))


if __name__ == "__main__":
    main()
