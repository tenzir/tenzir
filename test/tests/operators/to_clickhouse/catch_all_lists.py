# runner: python
# fixtures: [clickhouse]
# timeout: 180
"""Check catch-all null and list semantics through writes and structured reads."""

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
    no_warnings: bool = False,
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
        if no_warnings:
            assert "warning:" not in result.stderr, result.stderr


def main() -> None:
    # Exercise each documented omission/preservation case through native writes,
    # SQL storage, native reads, and structured JSON parsing.
    cases = [
        ("{name:null}", {}),
        ("{parent:{name:null}}", {}),
        ("{items:[]}", {"items": []}),
        ("{items:[null]}", {"items": []}),
        ('{items:["a",null,"b"]}', {"items": ["a", "b"]}),
        ("{items:[true,null,false]}", {"items": [True, False]}),
        ("{items:[1,null,2]}", {"items": [1, 2]}),
        ("{items:[null,1,null,2,null]}", {"items": [1, 2]}),
        ("{items:[{name:null},null]}", {"items": [{}]}),
        (
            '{items:[{name:"a"},null,{name:"b"}]}',
            {"items": [{"name": "a"}, {"name": "b"}]},
        ),
        (
            "{items:[[],[null],[1,null,2]]}",
            {"items": [[], [], [1, 2]]},
        ),
        ("{items:[[],null,[null]]}", {"items": [[], []]}),
    ]
    query("""
CREATE TABLE ca_list_contract (extra JSON COMMENT 'tenzir:catch_all')
ENGINE=MergeTree ORDER BY tuple();
""")
    for event, expected in cases:
        query("TRUNCATE TABLE ca_list_contract")
        run(event, '"ca_list_contract"', no_warnings=True)
        stored = json.loads(
            query("SELECT toJSONString(extra) FROM ca_list_contract FORMAT TSVRaw")
        )
        assert stored == expected, (event, stored, expected)
        program = """
from_clickhouse table="ca_list_contract",
                host=env("CLICKHOUSE_HOST"), port=int(env("CLICKHOUSE_PORT")),
                password=env("CLICKHOUSE_PASSWORD"), tls=false
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
        assert json.loads(result.stdout) == {"extra": expected}, result.stdout

    # A mixed list is coerced upstream with a diagnostic. The writer cannot
    # recover the original types from the resulting list of strings.
    result = subprocess.run(
        [
            *shlex.split(os.environ["TENZIR_BINARY"]),
            'from {input:r#"[1, "two", null, true]"#} '
            "| parsed = input.parse_json(raw=true) | write_ndjson",
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    assert "type mismatch between list elements" in result.stderr, result.stderr
    assert json.loads(result.stdout)["parsed"] == ["1", "two", None, "true"]
    print("ok")


if __name__ == "__main__":
    main()
