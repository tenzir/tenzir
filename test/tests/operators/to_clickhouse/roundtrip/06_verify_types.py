# runner: python
"""Verify the types table schema and data."""

import os
import subprocess

# /// script
# ///


def main() -> None:
    runtime = os.environ["CLICKHOUSE_CONTAINER_RUNTIME"]
    container = os.environ["CLICKHOUSE_CONTAINER_ID"]
    password = os.environ["CLICKHOUSE_PASSWORD"]

    def query(sql: str) -> str:
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

    # Print schema
    schema = query("DESCRIBE test_types SETTINGS describe_compact_output=1")
    print(schema)
    print("---")
    # Print data
    data = query("SELECT * FROM test_types")
    print(data)


if __name__ == "__main__":
    main()
