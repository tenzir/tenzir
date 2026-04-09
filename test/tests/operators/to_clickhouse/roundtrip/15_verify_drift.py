# runner: python
"""Verify 10k rows after schema drift, with y=NULL for appended rows."""

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
    total = int(ch_query("SELECT count() FROM test_drift"))
    print(f"total={total}")
    assert total == 10000, f"expected 10000, got {total}"
    null_y = int(ch_query("SELECT countIf(y IS NULL) FROM test_drift"))
    print(f"null_y={null_y}")
    assert null_y == 5000, f"expected 5000 NULL y values, got {null_y}"
    non_null_y = int(ch_query("SELECT countIf(y IS NOT NULL) FROM test_drift"))
    print(f"non_null_y={non_null_y}")
    assert non_null_y == 5000, f"expected 5000 non-NULL y values, got {non_null_y}"
    print("ok")


if __name__ == "__main__":
    main()
