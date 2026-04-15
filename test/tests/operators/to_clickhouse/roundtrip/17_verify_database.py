# runner: python
"""Verify the database was created and the table has 1000 rows."""

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
    total = int(ch_query("SELECT count() FROM testdb.test_qualified"))
    print(f"total={total}")
    assert total == 1000, f"expected 1000, got {total}"
    print("ok")


if __name__ == "__main__":
    main()
