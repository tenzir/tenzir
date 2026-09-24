# runner: python
# fixtures: [clickhouse]
# timeout: 120
"""Preserve literal dotted JSON keys separately from nested paths."""

import json
import os
import shlex
import subprocess

# /// script
# ///


def query(sql: str) -> str:
    result = subprocess.run(
        [
            os.environ["CLICKHOUSE_CONTAINER_RUNTIME"],
            "exec",
            os.environ["CLICKHOUSE_CONTAINER_ID"],
            "clickhouse-client",
            f"--password={os.environ['CLICKHOUSE_PASSWORD']}",
            "--multiquery",
            "--query",
            sql,
        ],
        capture_output=True,
        text=True,
        check=True,
        timeout=30,
    )
    return result.stdout.strip()


def main() -> None:
    query("""
CREATE TABLE dotted_keys (
  id Int64, `src.port` Int32, payload JSON,
  extra JSON COMMENT 'tenzir:catch_all'
) ENGINE=MergeTree ORDER BY id;
""")
    events = [
        {"id": 1, "a.b": 1},
        {"id": 2, "a": {"b": 2}},
        {"id": 3, "a.b": 3, "a": {"b": 4}},
        {"id": 4, "payload": {"a.b": 5, "a": {"b": 6}}},
        {"id": 5, "src.port": 7, "src": {"port": 443}},
        {"id": 6, "items": [{"a.b": 8}, {"a": {"b": 9}}]},
    ]
    program = """
from_stdin { read_json }
to_clickhouse table="dotted_keys",
               host=env("CLICKHOUSE_HOST"), port=int(env("CLICKHOUSE_PORT")),
               password=env("CLICKHOUSE_PASSWORD"), tls=false,
               mode="append", _jobs=2, max_batch_rows=2
"""
    result = subprocess.run(
        [*shlex.split(os.environ["TENZIR_BINARY"]), program],
        input="".join(json.dumps(event) + "\n" for event in events),
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "warning:" not in result.stderr, result.stderr
    rows = [
        json.loads(line)
        for line in query(
            "SELECT * FROM dotted_keys ORDER BY id "
            "SETTINGS json_type_escape_dots_in_keys=1 FORMAT JSONEachRow"
        ).splitlines()
    ]
    expected = [
        {"id": 1, "src.port": 0, "payload": {}, "extra": {"a.b": 1}},
        {"id": 2, "src.port": 0, "payload": {}, "extra": {"a": {"b": 2}}},
        {"id": 3, "src.port": 0, "payload": {}, "extra": {"a.b": 3, "a": {"b": 4}}},
        {"id": 4, "src.port": 0, "payload": {"a.b": 5, "a": {"b": 6}}, "extra": {}},
        {"id": 5, "src.port": 443, "payload": {}, "extra": {"src.port": 7}},
        {
            "id": 6,
            "src.port": 0,
            "payload": {},
            "extra": {"items": [{"a.b": 8}, {"a": {"b": 9}}]},
        },
    ]
    assert rows == expected, rows
    query("OPTIMIZE TABLE dotted_keys FINAL")
    program = """
from_clickhouse sql="SELECT * FROM dotted_keys ORDER BY id SETTINGS json_type_escape_dots_in_keys=1",
                host=env("CLICKHOUSE_HOST"), port=int(env("CLICKHOUSE_PORT")),
                password=env("CLICKHOUSE_PASSWORD"), tls=false
payload = payload.parse_json(raw=true)
extra = extra.parse_json(raw=true)
write_ndjson
"""
    result = subprocess.run(
        [*shlex.split(os.environ["TENZIR_BINARY"]), program],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "warning:" not in result.stderr, result.stderr
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    # Tenzir gives records within a list a common schema, filling missing
    # fields with null. Their literal dotted names must remain distinct.
    expected[5]["extra"]["items"] = [
        {"a.b": 8, "a": None},
        {"a.b": None, "a": {"b": 9}},
    ]
    assert rows == expected, rows
    print("ok")


if __name__ == "__main__":
    main()
