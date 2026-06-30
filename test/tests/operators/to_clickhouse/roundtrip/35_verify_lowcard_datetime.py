# runner: python
"""Verify values round-trip into LowCardinality and scaled DateTime64 columns."""

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
    total = int(ch_query("SELECT count() FROM test_lc_dt"))
    assert total == 3, f"expected 3, got {total}"
    # The column types are unchanged by the append.
    schema = ch_query("DESCRIBE test_lc_dt SETTINGS describe_compact_output=1")
    assert "LowCardinality(String)" in schema, schema
    assert "DateTime64(3, 'UTC')" in schema, schema
    rows = ch_query(
        "SELECT id, name, toString(ts) FROM test_lc_dt ORDER BY id"
    ).splitlines()
    parsed = {}
    for row in rows:
        rid, name, ts = row.split("\t")
        parsed[int(rid)] = (name, ts)
    # Nanosecond precision is truncated to the column's millisecond scale.
    assert parsed[0] == ("alpha", "2024-01-01 12:00:00.123"), parsed[0]
    assert parsed[1] == ("beta", "2024-06-15 08:30:00.000"), parsed[1]
    assert parsed[2] == ("alpha", "2024-12-31 23:59:59.999"), parsed[2]
    print("ok")


if __name__ == "__main__":
    main()
