# runner: python
"""Verify that nulls for the non-nullable `n` column got its default."""

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
    total = int(ch_query("SELECT count() FROM test_supported_default"))
    print(f"total={total}")
    assert total == 6, f"expected 6, got {total}"
    rows = ch_query(
        "SELECT id, n FROM test_supported_default WHERE id >= 5 ORDER BY id"
    )
    print(rows.replace("\t", " "))
    assert rows == "5\tdef\n6\tdef\n7\ty", f"unexpected rows: {rows!r}"
    print("ok")


if __name__ == "__main__":
    main()
