# runner: python
# timeout: 300
"""Assert generated LRQ queries and windows, and SentinelOne's semantics."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
from pathlib import Path

CAPTURE = Path(os.environ["S1_FIXTURE_LRQ_CAPTURE_FILE"])
BINARY = shlex.split(os.environ.get("TENZIR_BINARY", "tenzir run"))
START = "2024-01-01T00:00:00Z"
END = "2024-01-02T00:00:00Z"


def calls() -> list[dict]:
    return [json.loads(line) for line in CAPTURE.read_text().splitlines() if line]


def run(
    tail: str = "",
    *,
    query: str | None = None,
    scope: str = "pushdown_empty",
    expected: str | list[str] | None = None,
    start: str = START,
    end: str = END,
    args: str | None = None,
    warnings: bool = False,
) -> list[dict]:
    if args is None:
        args = f", start={START}, end={END}"
    native = "" if query is None else f", query={json.dumps(query)}"
    pipeline = f"""
from_sentinelone_data_lake env("S1_FIXTURE_URL"),
  token="test-token-s1-12345",
  account_ids=[{json.dumps(scope)}]{native}{args}
{tail}
write_ndjson
"""
    offset = len(calls())
    result = subprocess.run(
        [
            *BINARY,
            "--bare-mode",
            "--nova=true",
            "--console-verbosity=warning",
            pipeline,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (pipeline, result.stderr)
    if not warnings:
        assert not result.stderr, (pipeline, result.stderr)
    requests = calls()[offset:]
    assert not [r for r in requests if "error" in r], requests
    posts = [r for r in requests if r["method"] == "POST"]
    if expected is None:
        expected = query if query is not None else ""
    queries = expected if isinstance(expected, list) else [expected]
    # A PowerQuery starts with `|` or with the user's query, and a LOG read
    # sends its filter.
    expected_requests = [
        ("PQ" if query is not None or q.startswith("|") else "LOG", q) for q in queries
    ]
    actual = [
        (
            r["payload"]["queryType"],
            r["payload"]["pq"]["query"]
            if r["payload"]["queryType"] == "PQ"
            else r["payload"]["log"]["filter"],
        )
        for r in posts
    ]
    assert actual == expected_requests, requests
    for post in posts:
        payload = post["payload"]
        assert payload["startTime"] == start, payload
        assert payload["endTime"] == end, payload
        deleted = [
            r for r in requests if r["method"] == "DELETE" and r["id"] == post["id"]
        ]
        assert len(deleted) == 1, requests
    return [json.loads(line) for line in result.stdout.splitlines()]


def main() -> None:
    atoms = [
        ('x == "hello"', '(x == "hello")'),
        (r'x == "a\"b\\c"', r'(x == "a\"b\\c")'),
        ("x == 42", "(x == 42)"),
        ("x == true", "(x == true)"),
        ("x == false", "(x == false)"),
        ("x in [true, false]", "(x in (true, false))"),
        ("x > 9007199254740992", "(x > 9007199254740992)"),
        ("x > -42", "(x > -42)"),
        ("x >= 1.5", "(x >= 1.5)"),
        ("x < 42", "(x < 42)"),
        ("42 >= x", "(42 >= x)"),
        ('x < "ascii"', '(x < "ascii")'),
        ('x in ["a", 1]', '(x in ("a", 1))'),
        ('x.starts_with("pre")', '(x starts_with:matchcase "pre")'),
        ('x.ends_with("post")', '(x ends_with:matchcase "post")'),
        ("event._id2 == 42", "(event._id2 == 42)"),
        ("x == 9007199254740992", "(x == 9007199254740992)"),
        # SentinelOne's `!=` also matches events without the field.
        ("x != 42", "(x != 42)"),
        ("not (x == 42)", "!((x == 42))"),
        ("x == null", "!(x = *)"),
        ("x != null", "(x = *)"),
        ('x > "é"', '(x > "é")'),
        ("x == y", "(x == y)"),
    ]
    for predicate, fragment in atoms:
        run(f"where {predicate}", expected=fragment)
    for predicate in [
        "x",
        "not x",
        "x in []",
        "x in [1, null]",
        "x == 9007199254740993",
        'x == "a\\nb"',
        'x.starts_with("")',
        '"needle" in x',
        '"" in x',
        'x.starts_with("a", ignore_case=true)',
        'x.match_regex("a")',
        "x == 192.0.2.1",
        "x in 192.0.2.0/24",
        "x.length_bytes() > 0",
        "x + 1 > 0",
    ]:
        # The predicate stays in Tenzir, which reads every event.
        run(f"where {predicate}", expected="")
    # Command names, such as `group`, still name fields.
    run('where this["group"] == 1', expected="(group == 1)")
    # A filter that stays in Tenzir keeps the filters after it there, too.
    run(
        'where (x == 1 or (y == "a" and z > 2)) and x.length_bytes() > 0',
        expected='((x == 1) or ((y == "a") and (z > 2)))',
    )
    run(
        "where x == 1 and x.length_bytes() > 0 and y == 2",
        expected="(x == 1)",
    )
    run("where x == 1 or x.length_bytes() > 0", expected="")
    run("head 7", expected="")
    run("head 0", expected=[])
    run("where x == 1\nhead 7", expected="(x == 1)")
    run("where x != 1\nhead 7", expected="(x != 1)")
    # A select of whole events is a PowerQuery that names its columns.
    run(
        "select x, y",
        expected="| columns __tenzir_column_0 = x, __tenzir_column_1 = y "
        "| limit 1600000",
    )
    run("head 7\nselect x", expected="")
    # Do not suppress the original query merely because its explicit window is
    # empty: an aggregate can still produce rows without any input events.
    run(
        query="pushdown_empty | group n=count()",
        end=START,
        args=f", start={START}, end={START}",
    )

    lower = "2024-01-01T12:00:00.123456789Z"
    upper = "2024-01-01T13:00:00.123456789Z"
    run(
        f"where timestamp >= {lower} and timestamp < {upper}",
        expected="(timestamp >= 1704110400123456789) and "
        "(timestamp < 1704114000123456789)",
        start="2024-01-01T12:00:00.122Z",
        end="2024-01-01T13:00:00.125Z",
    )
    run(
        "where timestamp > 2024-01-01T12:00:00Z and timestamp <= 2024-01-01T13:00:00Z",
        expected="(timestamp > 1704110400000000000) and "
        "(timestamp <= 1704114000000000000)",
        start="2024-01-01T11:59:59.999Z",
        end="2024-01-01T13:00:00.002Z",
    )
    run(
        "where 2024-01-01T12:00:00Z <= timestamp and 2024-01-01T13:00:00Z > timestamp",
        expected="(1704110400000000000 <= timestamp) and "
        "(1704114000000000000 > timestamp)",
        start="2024-01-01T11:59:59.999Z",
        end="2024-01-01T13:00:00.002Z",
    )
    run(
        "where timestamp >= 2023-01-01 and timestamp < 2025-01-01",
        expected="(timestamp >= 1672531200000000000) and "
        "(timestamp < 1735689600000000000)",
    )
    run(
        "where timestamp >= 2023-01-01 and timestamp < 2025-01-01",
        expected="(timestamp >= 1672531200000000000) and "
        "(timestamp < 1735689600000000000)",
        start="2022-12-31T23:59:59.999Z",
        args=f", end={END}",
    )
    run(
        "where timestamp < 2024-01-01T12:00:00Z or timestamp > 2024-01-01T13:00:00Z",
        expected="((timestamp < 1704110400000000000) or "
        "(timestamp > 1704114000000000000))",
    )
    run(
        "where timestamp > 2024-01-01T13:00:00Z and timestamp < 2024-01-01T12:00:00Z",
        expected=[],
    )
    run(
        f"where timestamp >= {lower}",
        expected="(timestamp >= 1704110400123456789)",
        start=lower,
        args=f", start={lower}, end={END}",
    )
    run(
        f"where timestamp <= {upper}",
        expected="(timestamp <= 1704114000123456789)",
        end=upper,
        args=f", start={START}, end={upper}",
    )

    # SentinelOne compares numeric strings with numbers before `group`, unlike
    # TQL, so the read gets the event whose `n` is the text "42".
    tail = "where n == 42 and addr in 192.0.2.0/24\nhead 2\nselect id"
    pushed = run(
        tail,
        scope="pushdown_semantics",
        expected="(n == 42)",
        warnings=True,
    )
    # A preceding head keeps the filters in Tenzir.
    local = run(
        "head 1000\n" + tail,
        scope="pushdown_semantics",
        expected="",
        warnings=True,
    )
    assert pushed == [{"id": 1}, {"id": 2}], pushed
    assert local == [{"id": 1}, {"id": 4}], local
    tail = "where n == 42 and missing != null"
    pushed = run(
        tail,
        scope="pushdown_semantics",
        warnings=True,
        expected="(n == 42) and (missing = *)",
    )
    local = run(
        "head 1000\n" + tail,
        scope="pushdown_semantics",
        warnings=True,
        expected="",
    )
    assert pushed == local == [], (pushed, local)

    # A bare field searches the full text in SentinelOne, so it stays in
    # Tenzir, with or without type inference.
    for raw, ids in [(False, [1, 2]), (True, [1])]:
        args = f", start={START}, end={END}, raw={str(raw).lower()}"
        pushed = run(
            "where flag\nselect id",
            scope="pushdown_booleans",
            args=args,
            warnings=raw,
            expected="",
        )
        local = run(
            "head 1000\nwhere flag\nselect id",
            scope="pushdown_booleans",
            args=args,
            warnings=True,
            expected="",
        )
        assert pushed == local == [{"id": i} for i in ids], (pushed, local)

    # Exploratory reads retain every parsed attribute, even when neither
    # the filter nor a downstream operator names it. Selects use PQ.
    dns = 'where event.type == "DNS Resolved"'
    pushed = '(event.type == "DNS Resolved")'
    rows = run(dns, scope="pushdown_projection", expected=pushed)
    assert [(r["message"], r["event"]) for r in rows] == [
        ("resolved", {"type": "DNS Resolved"})
    ] * 2, rows
    assert [r["account"]["id"] for r in rows] == ["a1", "a2"], rows
    assert all("dataSource" in r for r in rows), rows
    columns = (
        "| columns __tenzir_column_0 = event.type, __tenzir_column_1 = account.id "
        "| limit 1600000"
    )
    rows = run(
        dns + "\nselect event.type, account.id",
        scope="pushdown_projection",
        expected=f"| filter {pushed} {columns}",
    )
    assert rows == [
        {"event": {"type": "DNS Resolved"}, "account": {"id": i}} for i in ("a1", "a2")
    ], rows
    # A predicate that stays in Tenzir keeps the select there, too.
    rows = run(
        'where event.type.match_regex("^DNS")\nselect account.id',
        scope="pushdown_projection",
        expected="",
    )
    assert rows == [{"account": {"id": i}} for i in ("a1", "a2")], rows
    # `dataSource` resembles the `datasource` command, but names a field.
    source = 'where dataSource.name == "SentinelOne"'
    rows = run(
        source + "\nselect dataSource.name, account.id",
        scope="pushdown_projection",
        expected='| filter (dataSource.name == "SentinelOne") '
        "| columns __tenzir_column_0 = dataSource.name, "
        "__tenzir_column_1 = account.id | limit 1600000",
    )
    assert rows == [
        {"dataSource": {"name": "SentinelOne"}, "account": {"id": i}}
        for i in ("a1", "a2")
    ], rows
    rows = run(
        "select dataSource.name",
        scope="pushdown_projection",
        expected="| columns __tenzir_column_0 = dataSource.name | limit 1600000",
    )
    assert [r["dataSource"]["name"] for r in rows] == [
        "SentinelOne",
        "Other",
        "SentinelOne",
    ], rows
    print("ok")


if __name__ == "__main__":
    main()
