# runner: python
"""Verify structured values round-trip into a ClickHouse JSON column."""

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
    assert total == 3, f"expected 3, got {total}"
    schema = ch_query("DESCRIBE test_json SETTINGS describe_compact_output=1")
    assert "JSON" in schema, schema
    rows = ch_query(
        "SELECT id, toString(payload) FROM test_json ORDER BY id"
    ).splitlines()
    payloads = {}
    for row in rows:
        rid, raw = row.split("\t", 1)
        payloads[int(rid)] = json.loads(raw)
    assert payloads[0] == {"a": 1, "b": [2, 3], "c": "x"}, payloads[0]
    # ClickHouse's JSON type does not store null-valued paths, so the `c: null`
    # field we send is dropped on the server side.
    assert payloads[1] == {"a": 5, "b": []}, payloads[1]
    assert payloads[2] == {}, payloads[2]
    print("ok")


if __name__ == "__main__":
    main()
