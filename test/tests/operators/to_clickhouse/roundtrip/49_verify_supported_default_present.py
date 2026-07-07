# runner: python
"""Verify a provided value for the defaulted `n` column is written as given."""

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
    assert total == 3, f"expected 3, got {total}"
    present_n = ch_query("SELECT n FROM test_supported_default WHERE id = 3")
    print(f"present_n={present_n}")
    assert present_n == "x", f"expected 'x', got {present_n!r}"
    print("ok")


if __name__ == "__main__":
    main()
