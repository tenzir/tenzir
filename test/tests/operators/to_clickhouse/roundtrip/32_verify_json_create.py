# runner: python
"""Verify `json=` creates JSON columns, including ones absent from the input."""

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
    total = int(ch_query("SELECT count() FROM test_json_create"))
    assert total == 2, f"expected 2, got {total}"
    # The `payload` and `extra` columns must be JSON; `extra` was created even
    # though it never appeared in the input.
    schema = {}
    for line in ch_query(
        "DESCRIBE test_json_create SETTINGS describe_compact_output=1"
    ).splitlines():
        name, type_ = line.split("\t")[:2]
        schema[name] = type_
    assert schema.get("id") == "Int64", schema
    assert schema.get("payload") == "JSON", schema
    assert schema.get("extra") == "JSON", schema
    rows = ch_query(
        "SELECT id, toString(payload), toString(extra) FROM test_json_create "
        "ORDER BY id"
    ).splitlines()
    parsed = {}
    for row in rows:
        rid, payload, extra = row.split("\t", 2)
        parsed[int(rid)] = (json.loads(payload), json.loads(extra))
    assert parsed[0] == ({"a": 1, "b": [2, 3]}, {}), parsed[0]
    assert parsed[1] == ({"a": 5}, {}), parsed[1]
    print("ok")


if __name__ == "__main__":
    main()
