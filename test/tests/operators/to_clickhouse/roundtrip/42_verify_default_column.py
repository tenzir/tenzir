# runner: python
"""Verify the omitted `tag` column was filled with its ClickHouse DEFAULT."""

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
    total = int(ch_query("SELECT count() FROM test_default_col"))
    print(f"total={total}")
    assert total == 3, f"expected 3, got {total}"
    default_tag = int(
        ch_query("SELECT countIf(tag = toIPv4('1.2.3.4')) FROM test_default_col")
    )
    print(f"default_tag={default_tag}")
    assert default_tag == 3, f"expected 3 default tags, got {default_tag}"
    print("ok")


if __name__ == "__main__":
    main()
