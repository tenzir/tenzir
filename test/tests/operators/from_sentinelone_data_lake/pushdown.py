# runner: python
# timeout: 300
"""Assert generated LRQ queries/windows and local enforcement of prefilters."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import subprocess
from pathlib import Path

CAPTURE = Path(os.environ["S1_FIXTURE_LRQ_CAPTURE_FILE"])
BINARY = shlex.split(
    os.environ.get("TENZIR_BINARY", shutil.which("tenzir") or "tenzir")
)
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
    fallback: bool = False,
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
        expected = query if query is not None else "| filter (1 == 1)"
    queries = expected if isinstance(expected, list) else [expected]
    mode = "PQ" if query is not None or "select " in tail else "LOG"

    def log_filter(pq: str) -> str:
        # LOG initial-search predicates have the same spelling, but no PQ
        # columns/limit stages and no constant-true filter.
        if not pq.startswith("| filter ") or pq == "| filter (1 == 1)":
            return ""
        return pq.removeprefix("| filter ").split(" | columns ")[0]

    expected_requests = [(mode, q if mode == "PQ" else log_filter(q)) for q in queries]
    if fallback:
        expected_requests.append(("LOG", log_filter(queries[0])))
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
        ('x == "hello"', '(x) == ("hello")'),
        (r'x == "a\"b\\c"', r'(x) == ("a\"b\\c")'),
        ("x == 42", "(x) == (42)"),
        ("x == true", '((x) == (true)) or ((x) == ("true"))'),
        ("x == false", '((x) == (false)) or ((x) == ("false"))'),
        ("x", '((x) == (true)) or ((x) == ("true"))'),
        ("x in [true, false]", '(x) in (true, "true", false, "false")'),
        ("x > 9007199254740992", "(x) >= (9007199254740992)"),
        ("x > -42", "(x) > (-42)"),
        ("x >= 1.5", "(x) >= (1.5)"),
        ("x < 42", "(x) < (42)"),
        ("42 >= x", "(x) <= (42)"),
        ('x < "ascii"', '(x) < ("ascii")'),
        ('x in ["a", 1]', '(x) in ("a", 1)'),
        ('x.starts_with("pre")', '(x) starts_with:matchcase("pre")'),
        ('x.ends_with("post")', '(x) ends_with:matchcase("post")'),
        ('"needle" in x', '(x) contains:matchcase("needle")'),
        ("event._id2 == 42", "(event._id2) == (42)"),
        ("x == 9007199254740992", "(x) == (9007199254740992)"),
    ]
    # The default response lacks the filtered field, so request it as well.
    columns = "| columns timestamp, message"
    for predicate, fragment in atoms:
        field = "event._id2" if "event" in predicate else "x"
        run(f"where {predicate}", expected=f"| filter ({fragment}) {columns}, {field}")
    for predicate in [
        "x != 42",
        "not (x == 42)",
        "x == null",
        "x != null",
        "not x",
        "x in []",
        "x in [1, null]",
        "x == 9007199254740993",
        'x > "é"',
        'x == "a\\nb"',
        'x.starts_with("")',
        '"" in x',
        'x.starts_with("a", ignore_case=true)',
        'x.match_regex("a")',
        "x == 192.0.2.1",
        "x in 192.0.2.0/24",
        "x.length_bytes() > 0",
        "x + 1 > 0",
    ]:
        # Local-only predicates still need their field in the response.
        run(f"where {predicate}", expected=f"{columns}, x")
    run("where x == y", expected=f"{columns}, x, y")
    # Command names, such as `group`, still name fields.
    run(
        'where this["group"] == 1',
        expected=f"| filter ((group) == (1)) {columns}, group",
    )
    run(
        'where (x == 1 or (y == "a" and z > 2)) and x.length_bytes() > 0',
        expected='| filter (((x) == (1)) or (((y) == ("a")) and ((z) > (2)))) '
        f"{columns}, x, y, z",
    )
    run(
        "where x == 1 and x.length_bytes() > 0 and y == 2",
        expected=f"| filter ((x) == (1)) and ((y) == (2)) {columns}, x, y",
    )
    run("where x == 1 or x.length_bytes() > 0", expected=f"{columns}, x")
    run("head 7", expected="| limit 7")
    run("head 0", expected=[])
    run("where x == 1\nhead 7", expected=f"| filter ((x) == (1)) {columns}, x")
    run("where x != 1\nhead 7", expected=f"{columns}, x")
    # A downstream projection replaces the default columns.
    run("select x, y", expected="| columns timestamp, y, x")
    run("head 7\nselect x", expected="| columns timestamp, x | limit 7")
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
        start="2024-01-01T12:00:00.123Z",
        end="2024-01-01T13:00:00.124Z",
    )
    run(
        "where timestamp > 2024-01-01T12:00:00Z and timestamp <= 2024-01-01T13:00:00Z",
        start="2024-01-01T12:00:00Z",
        end="2024-01-01T13:00:00.001Z",
    )
    run(
        "where 2024-01-01T12:00:00Z <= timestamp and 2024-01-01T13:00:00Z > timestamp",
        start="2024-01-01T12:00:00Z",
        end="2024-01-01T13:00:00Z",
    )
    run("where timestamp >= 2023-01-01 and timestamp < 2025-01-01")
    run(
        "where timestamp >= 2023-01-01 and timestamp < 2025-01-01",
        start="2023-01-01T00:00:00Z",
        args=f", end={END}",
    )
    run("where timestamp < 2024-01-01T12:00:00Z or timestamp > 2024-01-01T13:00:00Z")
    run(
        "where timestamp > 2024-01-01T13:00:00Z and timestamp < 2024-01-01T12:00:00Z",
        expected=[],
    )
    run(f"where timestamp >= {lower}", start=lower, args=f", start={lower}, end={END}")
    run(f"where timestamp <= {upper}", end=upper, args=f", start={START}, end={upper}")

    tail = "where n == 42 and addr in 192.0.2.0/24\nhead 2\nselect id"
    pushed = run(
        tail,
        scope="pushdown_semantics",
        expected="| filter ((n) == (42)) | columns timestamp, n, addr, id",
        warnings=True,
    )
    # A preceding head is a filter barrier, giving the purely local evaluation.
    local = run(
        "head 1000\n" + tail,
        scope="pushdown_semantics",
        expected="| columns timestamp, id, n, addr | limit 1000",
        warnings=True,
        fallback=True,
    )
    assert pushed == local == [{"id": 1}, {"id": 4}], (pushed, local)
    tail = "where n == 42 and missing != null"
    pushed = run(
        tail,
        scope="pushdown_semantics",
        warnings=True,
        expected="| filter ((n) == (42)) | columns timestamp, message, n, missing",
    )
    local = run(
        "head 1000\n" + tail,
        scope="pushdown_semantics",
        warnings=True,
        expected="| limit 1000",
    )
    assert pushed == local == [], (pushed, local)

    for raw, ids in [(False, [1, 2]), (True, [1])]:
        args = f", start={START}, end={END}, raw={str(raw).lower()}"
        pushed = run(
            "where flag\nselect id",
            scope="pushdown_booleans",
            args=args,
            warnings=raw,
            expected='| filter (((flag) == (true)) or ((flag) == ("true"))) '
            "| columns timestamp, flag, id",
        )
        local = run(
            "head 1000\nwhere flag\nselect id",
            scope="pushdown_booleans",
            args=args,
            warnings=True,
            expected="| columns timestamp, id, flag | limit 1000",
            fallback=True,
        )
        assert pushed == local == [{"id": i} for i in ids], (pushed, local)

    # Exploratory reads retain every parsed attribute, even when neither
    # the filter nor a downstream operator names it. Restricted reads use PQ.
    dns = 'where event.type == "DNS Resolved"'
    pushed = '| filter ((event.type) == ("DNS Resolved"))'
    rows = run(
        dns,
        scope="pushdown_projection",
        expected=pushed + " | columns timestamp, message, event.type",
    )
    assert [(r["message"], r["event"]) for r in rows] == [
        ("resolved", {"type": "DNS Resolved"})
    ] * 2, rows
    assert [r["account"]["id"] for r in rows] == ["a1", "a2"], rows
    assert all("dataSource" in r for r in rows), rows
    rows = run(
        dns + "\nselect event.type, account.id",
        scope="pushdown_projection",
        expected=pushed + " | columns timestamp, event.type, account.id",
    )
    assert rows == [
        {"event": {"type": "DNS Resolved"}, "account": {"id": i}} for i in ("a1", "a2")
    ], rows
    # A local-only predicate needs its field as much as a pushed one.
    rows = run(
        'where event.type.match_regex("^DNS")\nselect account.id',
        scope="pushdown_projection",
        expected="| columns timestamp, event.type, account.id",
    )
    assert rows == [{"account": {"id": i}} for i in ("a1", "a2")], rows
    # `dataSource` resembles the `datasource` command, but names a field.
    source = 'where dataSource.name == "SentinelOne"'
    rows = run(
        source + "\nselect dataSource.name, account.id",
        scope="pushdown_projection",
        expected='| filter ((dataSource.name) == ("SentinelOne")) '
        "| columns timestamp, dataSource.name, account.id",
    )
    assert rows == [
        {"dataSource": {"name": "SentinelOne"}, "account": {"id": i}}
        for i in ("a1", "a2")
    ], rows
    rows = run(
        "select dataSource.name",
        scope="pushdown_projection",
        expected="| columns timestamp, dataSource.name",
    )
    assert [r["dataSource"]["name"] for r in rows] == [
        "SentinelOne",
        "Other",
        "SentinelOne",
    ], rows
    print("ok")


if __name__ == "__main__":
    main()
