# runner: python
"""Verify the dynamic-table routing: records land as JSON objects in the table
with a JSON column, and strings land verbatim in the table with a String column.
This locks in that JSON-ness is resolved per concrete target table."""

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
    # dyn_a: JSON column, records serialized to objects.
    a_total = int(ch_query("SELECT count() FROM dyn_a"))
    assert a_total == 2, f"expected 2 in dyn_a, got {a_total}"
    a_schema = ch_query("DESCRIBE dyn_a SETTINGS describe_compact_output=1")
    assert "JSON" in a_schema, a_schema
    a_rows = ch_query("SELECT toString(payload) FROM dyn_a").splitlines()

    # Dicts are not orderable, so canonicalize each object to a sorted-key JSON
    # string and compare those instead of sorting the dicts directly.
    def canonical(obj: object) -> str:
        return json.dumps(obj, sort_keys=True)

    a_objs = sorted(canonical(json.loads(r)) for r in a_rows)
    expected = sorted(canonical(o) for o in [{"x": 1}, {"y": [1, 2]}])
    assert a_objs == expected, a_objs

    # dyn_b: String column, strings stored verbatim.
    b_total = int(ch_query("SELECT count() FROM dyn_b"))
    assert b_total == 2, f"expected 2 in dyn_b, got {b_total}"
    b_rows = sorted(ch_query("SELECT payload FROM dyn_b").splitlines())
    assert b_rows == ["hello", "world"], b_rows
    print("ok")


if __name__ == "__main__":
    main()
