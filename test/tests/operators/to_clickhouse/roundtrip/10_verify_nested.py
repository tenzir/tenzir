# runner: python
"""Verify 5k nested rows and Tuple schema."""

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
    total = int(ch_query("SELECT count() FROM test_nested"))
    print(f"total={total}")
    assert total == 5000, f"expected 5000, got {total}"
    schema = ch_query("DESCRIBE test_nested SETTINGS describe_compact_output=1")
    print(schema)
    # The meta column should be a Tuple with nested location Tuple
    assert "Tuple" in schema
    print("ok")


if __name__ == "__main__":
    main()
