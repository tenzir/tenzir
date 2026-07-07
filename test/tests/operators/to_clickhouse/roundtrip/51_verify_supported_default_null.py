# runner: python
"""Verify the event carrying a null for the non-nullable `n` column was dropped."""

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
    # Unchanged from the previous step: the null-carrying event was dropped.
    total = int(ch_query("SELECT count() FROM test_supported_default"))
    print(f"total={total}")
    assert total == 3, f"expected 3, got {total}"
    dropped = int(ch_query("SELECT count() FROM test_supported_default WHERE id = 5"))
    print(f"dropped={dropped}")
    assert dropped == 0, f"expected 0 rows with id = 5, got {dropped}"
    print("ok")


if __name__ == "__main__":
    main()
