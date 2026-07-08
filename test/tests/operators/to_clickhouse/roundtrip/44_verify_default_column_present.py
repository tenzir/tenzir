# runner: python
"""Verify events that provided the unsupported-typed column were dropped."""

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
    # Still only the 3 rows from the earlier append; the 2 events that provided
    # `tag` were dropped.
    total = int(ch_query("SELECT count() FROM test_default_col"))
    print(f"total={total}")
    assert total == 3, f"expected 3, got {total}"
    dropped = int(ch_query("SELECT count() FROM test_default_col WHERE id >= 10"))
    print(f"dropped={dropped}")
    assert dropped == 0, f"expected 0 rows with id >= 10, got {dropped}"
    print("ok")


if __name__ == "__main__":
    main()
