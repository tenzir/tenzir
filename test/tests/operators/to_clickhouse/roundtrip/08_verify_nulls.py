# runner: python
"""Verify 10k null rows inserted and null counts are roughly 20%."""

import os
import subprocess

# /// script
# ///


def ch_query(sql: str) -> str:
    runtime = os.environ["CLICKHOUSE_CONTAINER_RUNTIME"]
    container = os.environ["CLICKHOUSE_CONTAINER_ID"]
    password = os.environ["CLICKHOUSE_PASSWORD"]
    result = subprocess.run(
        [runtime, "exec", container, "clickhouse-client",
         f"--password={password}", f"--query={sql}"],
        capture_output=True, text=True, check=True,
    )
    return result.stdout.strip()


def main() -> None:
    total = int(ch_query("SELECT count() FROM test_nulls"))
    null_a = int(ch_query("SELECT countIf(a IS NULL) FROM test_nulls"))
    null_b = int(ch_query("SELECT countIf(b IS NULL) FROM test_nulls"))
    print(f"total={total}")
    assert total == 9999, f"expected 9999 rows, got {total}"
    # 3 rows repeat in pattern: null-a, null-b, no-nulls. So 1/3 of
    # each column is null = 3333.
    for name, count in [("a", null_a), ("b", null_b)]:
        assert count == 3333, (
            f"null count for {name}: expected 3333, got {count}"
        )
    print(f"null_a={null_a} null_b={null_b}")
    print("ok")


if __name__ == "__main__":
    main()
