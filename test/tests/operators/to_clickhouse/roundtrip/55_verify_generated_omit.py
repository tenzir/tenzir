# runner: python
"""Verify the omitted generated columns were computed by ClickHouse."""

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
    # m is MATERIALIZED as id * 2, al is ALIAS as id + 1.
    m = int(ch_query("SELECT m FROM test_generated WHERE id = 10"))
    print(f"m={m}")
    assert m == 20, f"expected m=20, got {m}"
    al = int(ch_query("SELECT al FROM test_generated WHERE id = 10"))
    print(f"al={al}")
    assert al == 11, f"expected al=11, got {al}"
    print("ok")


if __name__ == "__main__":
    main()
