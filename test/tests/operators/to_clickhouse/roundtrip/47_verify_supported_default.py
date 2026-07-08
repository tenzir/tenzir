# runner: python
"""Verify the omitted non-nullable `n` column was filled with its DEFAULT."""

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
    assert total == 2, f"expected 2, got {total}"
    default_n = int(ch_query("SELECT countIf(n = 'def') FROM test_supported_default"))
    print(f"default_n={default_n}")
    assert default_n == 2, f"expected 2 default values, got {default_n}"
    print("ok")


if __name__ == "__main__":
    main()
