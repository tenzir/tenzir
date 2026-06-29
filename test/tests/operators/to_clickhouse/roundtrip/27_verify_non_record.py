# runner: python
"""Verify a non-record value is stored as an empty object in a JSON column."""

import json
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
    total = int(ch_query("SELECT count() FROM test_json"))
    assert total == 4, f"expected 4, got {total}"
    raw = ch_query("SELECT toString(payload) FROM test_json WHERE id = 3")
    assert json.loads(raw) == {}, raw
    print("ok")


if __name__ == "__main__":
    main()
