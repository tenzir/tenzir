# runner: python
"""Verify 5k list rows with Array schema."""

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
    total = int(ch_query("SELECT count() FROM test_lists"))
    print(f"total={total}")
    assert total == 5000, f"expected 5000, got {total}"
    schema = ch_query("DESCRIBE test_lists SETTINGS describe_compact_output=1")
    print(schema)
    assert "Array" in schema
    avg_tags = float(ch_query("SELECT avg(length(tags)) FROM test_lists"))
    print(f"avg_tags_length={avg_tags:.1f}")
    assert avg_tags > 1.0, f"avg tags length too low: {avg_tags}"
    print("ok")


if __name__ == "__main__":
    main()
