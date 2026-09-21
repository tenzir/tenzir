# runner: python
# fixtures: [clickhouse]
# timeout: 120
"""Adding a catch-all must not redirect serialized JSON from a mapped column."""

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
CREATE TABLE json_plain (id Int64, payload JSON) ENGINE=Memory;
CREATE TABLE json_catch_all (
  id Int64, payload JSON, extra JSON COMMENT 'tenzir:catch_all'
) ENGINE=Memory;
""")
    for table in ("json_plain", "json_catch_all"):
        program = f"""
from {{id:1, payload:r#"{{"action":"login"}}"#}}
to_clickhouse table="{table}",
              host=env("CLICKHOUSE_HOST"), port=int(env("CLICKHOUSE_PORT")),
              password=env("CLICKHOUSE_PASSWORD"), tls=false,
              mode="append", _jobs=1
"""
        result = subprocess.run(
            [*shlex.split(os.environ["TENZIR_BINARY"]), program],
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert result.returncode == 0, result.stderr
        rows = [
            json.loads(line)
            for line in query(f"SELECT * FROM {table} FORMAT JSONEachRow").splitlines()
        ]
        expected = {"id": 1, "payload": {"action": "login"}}
        if table == "json_catch_all":
            expected["extra"] = {}
        # Check stored data before diagnostics: successful insertion alone
        # misses the regression where payload becomes {} and moves to extra.
        assert rows == [expected], (table, rows, expected, result.stderr)
        assert "warning:" not in result.stderr, result.stderr
    # An invalid document remains mapped and is rejected by ClickHouse.
    # It must not be redirected into extra and acknowledged as an insertion.
    program = """
from {id:10,payload:r#"{"broken":invalid}"#}
to_clickhouse table="json_catch_all",
              host=env("CLICKHOUSE_HOST"), port=int(env("CLICKHOUSE_PORT")),
              password=env("CLICKHOUSE_PASSWORD"), tls=false,
              mode="append", _jobs=1
"""
    result = subprocess.run(
        [*shlex.split(os.environ["TENZIR_BINARY"]), program],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode != 0, result.stderr
    assert query("SELECT count() FROM json_catch_all WHERE id=10") == "0"
    assert "Cannot parse JSON object" in result.stderr, result.stderr
    assert "preserved" not in result.stderr, result.stderr
    print("ok")


if __name__ == "__main__":
    main()
