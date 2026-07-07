# runner: python
"""Verify provided values for generated columns were ignored, not written."""

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
    total = int(ch_query("SELECT count() FROM test_generated"))
    print(f"count={total}")
    assert total == 2, f"expected 2, got {total}"
    # The provided 999 values are ignored; ClickHouse computes m = id * 2 and
    # al = id + 1 from id = 20.
    m = int(ch_query("SELECT m FROM test_generated WHERE id = 20"))
    print(f"m={m}")
    assert m == 40, f"expected m=40, got {m}"
    al = int(ch_query("SELECT al FROM test_generated WHERE id = 20"))
    print(f"al={al}")
    assert al == 21, f"expected al=21, got {al}"
    print("ok")


if __name__ == "__main__":
    main()
