# runner: python
"""Verify Nullable(JSON) stores records as JSON and nulls as SQL NULL."""

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
    total = int(ch_query("SELECT count() FROM test_json_nullable"))
    assert total == 2, f"expected 2, got {total}"
    schema = ch_query("DESCRIBE test_json_nullable SETTINGS describe_compact_output=1")
    assert "Nullable(JSON)" in schema, schema
    rows = ch_query(
        "SELECT id, isNull(payload), toString(payload) "
        "FROM test_json_nullable ORDER BY id"
    ).splitlines()
    parsed = {}
    for row in rows:
        rid, is_null, raw = row.split("\t", 2)
        parsed[int(rid)] = (is_null == "1", raw)
    assert parsed[0] == (False, '{"a":1,"b":[2,3]}'), parsed[0]
    assert json.loads(parsed[0][1]) == {"a": 1, "b": [2, 3]}
    assert parsed[1][0] is True, parsed[1]
    print("ok")


if __name__ == "__main__":
    main()
