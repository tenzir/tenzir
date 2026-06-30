# runner: python
"""Verify `LowCardinality` columns over string and numeric types round-trip."""

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
    total = int(ch_query("SELECT count() FROM test_low_cardinality"))
    assert total == 3, f"expected 3, got {total}"
    schema = ch_query(
        "DESCRIBE test_low_cardinality SETTINGS describe_compact_output=1"
    )
    # Non-primary columns are created as `Nullable`, wrapped in `LowCardinality`.
    assert "LowCardinality(Nullable(String))" in schema, schema
    assert "LowCardinality(Nullable(Int64))" in schema, schema
    assert "LowCardinality(Nullable(Float64))" in schema, schema
    rows = ch_query(
        "SELECT id, name, code, score FROM test_low_cardinality ORDER BY id"
    ).splitlines()
    parsed = {int(r.split("\t")[0]): tuple(r.split("\t")[1:]) for r in rows}
    assert parsed[0] == ("alpha", "7", "1.5"), parsed[0]
    assert parsed[1] == ("beta", "7", "2.5"), parsed[1]
    assert parsed[2] == ("alpha", "9", "1.5"), parsed[2]
    print("ok")


if __name__ == "__main__":
    main()
