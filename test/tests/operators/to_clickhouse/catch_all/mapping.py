# runner: python
"""Check marked destinations against stored SQL values, including empty defaults."""

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
    )
    return result.stdout.strip()


def run(
    events: str,
    table: str,
    *,
    fail: str | None = None,
    jobs: int = 1,
    warning: str | None = None,
) -> None:
    program = f"""
from {events}
to_clickhouse table={table},
              host=env("CLICKHOUSE_HOST"),
              port=int(env("CLICKHOUSE_PORT")),
              password=env("CLICKHOUSE_PASSWORD"), tls=false,
              mode="append", _jobs={jobs}, max_batch_rows=2
"""
    result = subprocess.run(
        [*shlex.split(os.environ["TENZIR_BINARY"]), program],
        capture_output=True,
        text=True,
        timeout=60,
    )
    if fail is not None:
        assert result.returncode != 0, result.stdout
        assert fail in result.stderr, result.stderr
    else:
        assert result.returncode == 0, result.stderr
        if warning is not None:
            assert warning in result.stderr, result.stderr


def main() -> None:
    # Neither table has any default kind or expression. The marker appears
    # first in one description and last in the other.
    query("""
CREATE TABLE sa_first (
  extra JSON COMMENT 'tenzir:catch_all', id Int64,
  `src.port` UInt32, n UInt8, payload JSON
) ENGINE=MergeTree ORDER BY id;
CREATE TABLE sa_last (
  id Int64, `src.port` UInt32, n UInt8, payload JSON,
  extra JSON COMMENT 'tenzir:catch_all'
) ENGINE=MergeTree ORDER BY id;
CREATE TABLE sa_plain (id Int64, payload String) ENGINE=MergeTree ORDER BY id;
""")
    for table in ("sa_first", "sa_last"):
        run(
            '{id:1, src:{port:uint(443), label:"https"}, n:uint(255), payload:{x:1}, other:[1,2]},'
            '{id:2, src:{port:uint(4294967295), label:"max"}, n:uint(256), payload:{y:"z"}, other:[3]},'
            '{id:3, src:{port:"bad", label:"bad"}, n:uint(1), payload:{x:2}, extra:"input"}',
            json.dumps(table),
            jobs=2,
            warning="value out of range",
        )
        rows = [
            json.loads(line)
            for line in query(
                f"SELECT * FROM {table} ORDER BY id FORMAT JSONEachRow"
            ).splitlines()
        ]
        assert rows == [
            {
                "id": 1,
                "src.port": 443,
                "n": 255,
                "payload": {"x": 1},
                "extra": {"src": {"label": "https"}, "other": [1, 2]},
            }
        ], rows

    # One dynamic expression resolves to both marked and unmarked destinations.
    run(
        '{id:4,destination:"sa_plain",payload:"plain"},'
        '{id:5,destination:"sa_last",payload:{x:5}}',
        "destination",
        jobs=2,
    )
    assert query("SELECT payload FROM sa_plain WHERE id=4") == "plain"
    assert query("SELECT count() FROM sa_last WHERE id=5") == "1"

    query("""
CREATE TABLE sa_invalid (id Int64, extra String COMMENT 'tenzir:catch_all')
ENGINE=Memory;
""")
    run("{id:1}", '"sa_invalid"', fail="invalid ClickHouse catch-all")
    assert query("SELECT count() FROM sa_invalid") == "0"
    # Null object fields are absent in the remainder, including within arrays.
    run(
        "{id:6, other:null, nested:{missing:null}, "
        'loggers:[{name:null,uid:"first"},{name:null}], '
        "endpoints:[[{hostname:null,port:443}]]}",
        '"sa_last"',
    )
    row = json.loads(query("SELECT * FROM sa_last WHERE id=6 FORMAT JSONEachRow"))
    assert row["n"] == 0 and row["src.port"] == 0, row
    assert row["extra"] == {
        "loggers": [{"uid": "first"}, {}],
        "endpoints": [[{"port": 443}]],
    }, row
    # Keep the OCSF table example in the operator reference executable.
    query("""
CREATE DATABASE IF NOT EXISTS security;
CREATE TABLE security.events (
  time DateTime64(9), class_uid UInt32,
  class_name LowCardinality(String), severity_id UInt8,
  message Nullable(String), `src_endpoint.ip` Nullable(IPv6),
  unmapped JSON, event JSON COMMENT 'tenzir:catch_all'
) ENGINE=MergeTree ORDER BY (time, class_uid);
""")
    run(
        '{time:2026-09-07T10:00:00Z,class_uid:uint(4001),class_name:"Network Activity",'
        'severity_id:uint(1),message:null,src_endpoint:{ip:ip("2001:db8::1"),port:443},'
        'unmapped:{source_field:"x"},x:"y"}',
        '"security.events"',
    )
    row = json.loads(query("SELECT * FROM security.events FORMAT JSONEachRow"))
    assert row["class_uid"] == 4001 and row["severity_id"] == 1, row
    assert row["class_name"] == "Network Activity" and row["message"] is None, row
    assert row["src_endpoint.ip"] == "2001:db8::1", row
    assert row["unmapped"] == {"source_field": "x"}, row
    assert row["event"] == {"src_endpoint": {"port": 443}, "x": "y"}, row
    print("ok")


if __name__ == "__main__":
    main()
