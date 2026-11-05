# runner: python
# timeout: 180
"""Build a source request from TQL without query/start/end arguments."""

from __future__ import annotations

import json
import os
import re
import shlex
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

CAPTURE = Path(os.environ["S1_FIXTURE_LRQ_CAPTURE_FILE"])
BINARY = shlex.split(os.environ.get("TENZIR_BINARY", "tenzir run"))
START = "2024-01-01T00:00:00Z"
END = "2024-01-01T00:00:02Z"
WINDOW = f"where timestamp >= {START} and timestamp < {END}"


def calls() -> list[dict]:
    return [json.loads(line) for line in CAPTURE.read_text().splitlines() if line]


def run(
    tail: str,
    *,
    query: str | re.Pattern[str] | None,
    args: str = "",
    prefix: str = "",
    window: tuple[str | None, str | None] | None = None,
    error: str = "",
) -> tuple[list[dict], list[dict]]:
    pipeline = f"""
{prefix}
from_sentinelone_data_lake env("S1_FIXTURE_URL"),
  token="test-token-s1-12345"{args}
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
        text=True,
        capture_output=True,
        timeout=30,
    )
    if error:
        assert result.returncode != 0 and error in result.stderr, (pipeline, result)
        assert not result.stdout, result.stdout
    else:
        assert result.returncode == 0 and not result.stderr, (pipeline, result)
    requests = calls()[offset:]
    assert not [r for r in requests if "error" in r], requests
    posts = [r for r in requests if r["method"] == "POST"]
    pattern = query.pattern if isinstance(query, re.Pattern) else query
    expected_mode = "PQ" if pattern and pattern.startswith("|") else "LOG"
    assert all(r["payload"]["queryType"] == expected_mode for r in posts), requests
    sent = [
        r["payload"]["pq"]["query"]
        if expected_mode == "PQ"
        else r["payload"]["log"]["filter"]
        for r in posts
    ]
    if isinstance(query, re.Pattern):
        assert len(sent) == 1 and query.fullmatch(sent[0]), requests
    else:
        assert sent == ([] if query is None else [query]), requests
    if query is None:
        assert not requests, requests
    if window:
        for post in posts:
            for key, expected in zip(("startTime", "endTime"), window):
                if expected is not None:
                    assert datetime.fromisoformat(
                        post["payload"][key]
                    ) == datetime.fromisoformat(expected), post
    launched = {r["id"] for r in posts if r["status"] == 200}
    deleted = {r["id"] for r in requests if r["method"] == "DELETE"}
    assert launched == deleted, requests
    return [json.loads(line) for line in result.stdout.splitlines()], posts


def main() -> None:
    # The window of the filters replaces missing arguments, rather than being
    # silently intersected with today's default 24-hour interval. The end of
    # the request moves past the millisecond that holds the inclusive bound.
    window = "(timestamp >= 1704067200000000000) and (timestamp < 1704067202000000000)"
    pq_window = (
        "| filter (timestamp >= 1704067200000000000) "
        "| filter (timestamp < 1704067202000000000)"
    )
    end = "2024-01-01T00:00:02.002Z"
    # Whole events go through LOG, which pages, so the select stays in Tenzir
    # after a limit.
    rows, _ = run(
        WINDOW + "\nwhere event_id == 42\nhead 1\nselect event_id",
        query=f"{window} and (event_id == 42)",
        window=("2023-12-31T23:59:59.999Z", end),
    )
    assert rows == [{"event_id": 42}], rows
    rows, _ = run(
        WINDOW + '\nwhere message.starts_with("login")\nselect event_id',
        query=f'{pq_window} | filter (message starts_with:matchcase "login") '
        "| columns __tenzir_column_0 = event_id | limit 1600000",
        window=("2023-12-31T23:59:59.999Z", end),
    )
    assert rows == [{"event_id": 42}], rows
    rows, _ = run(
        WINDOW
        + '\nwhere event_id > 40 and message.match_regex("attempt$")\nhead 1\nselect event_id',
        query=f"{window} and (event_id > 40)",
        window=("2023-12-31T23:59:59.999Z", end),
    )
    assert rows == [{"event_id": 42}], rows
    select = "| columns __tenzir_column_0 = event_id | limit 1600000"
    rows, _ = run(
        WINDOW + "\nselect event_id",
        query=f"{pq_window} {select}",
        window=("2023-12-31T23:59:59.999Z", end),
    )
    assert rows == [{"event_id": 42}, {"event_id": 43}], rows
    rows, _ = run(
        "where timestamp >= $end - 2s and timestamp < $end\nselect event_id",
        prefix=f"let $end = {END}",
        query=f"{pq_window} {select}",
        window=("2023-12-31T23:59:59.999Z", end),
    )
    assert rows == [{"event_id": 42}, {"event_id": 43}], rows
    _, posts = run(
        "where timestamp >= $end - 1h and timestamp < $end",
        prefix="let $end = now()",
        query=re.compile(r"\(timestamp >= \d+\) and \(timestamp < \d+\)"),
    )
    body = posts[0]["payload"]
    span = datetime.fromisoformat(body["endTime"]) - datetime.fromisoformat(
        body["startTime"]
    )
    assert timedelta(hours=1) <= span < timedelta(hours=1, milliseconds=4), body
    # Infer either endpoint separately; materialize the remaining default.
    run(
        f"where timestamp >= {START}",
        query="(timestamp >= 1704067200000000000)",
        window=("2023-12-31T23:59:59.999Z", None),
    )
    run(
        f"where timestamp < {END}",
        query="(timestamp < 1704067202000000000)",
        window=("2023-12-31T00:00:02.002Z", end),
    )
    # Explicit arguments stay hard boundaries. They can also constrain a scan
    # whose TQL predicates are evaluated entirely in Tenzir.
    rows, _ = run(
        WINDOW + "\nselect event_id",
        args=f", start=2024-01-01T00:00:00.5Z, end={END}",
        query=f"{pq_window} {select}",
        window=("2024-01-01T00:00:00.5Z", END),
    )
    assert rows == [{"event_id": 43}], rows
    rows, _ = run(
        'where message.match_regex("attempt$")\nselect event_id',
        args=f", start={START}, end={END}",
        query="",
    )
    assert rows == [{"event_id": 42}], rows
    # A supported non-time predicate or a limit uses the documented defaults.
    for tail, query in [
        ("where event_id == 42", "(event_id == 42)"),
        ("head 1", ""),
    ]:
        _, posts = run(tail, query=query)
        body = posts[0]["payload"]
        assert datetime.fromisoformat(body["endTime"]) - datetime.fromisoformat(
            body["startTime"]
        ) == timedelta(hours=24), body
    # An oversized predicate stays in Tenzir, and so does everything after it.
    oversized = json.dumps("a" * 10000)
    tail = f"where message == {oversized} and event_id == 42"
    run(tail, query=None, error="cannot construct a SentinelOne query from TQL")
    rows, _ = run(
        WINDOW + "\n" + tail + "\nhead 1",
        query=window,
        window=("2023-12-31T23:59:59.999Z", end),
    )
    assert rows == [], rows
    run("head 0", query=None)
    run(f"where timestamp >= {END} and timestamp < {START}", query=None)
    # The request window rounds outward, and the filter decides.
    rows, _ = run(
        f"where timestamp >= {START} and timestamp < {START}",
        query="(timestamp >= 1704067200000000000) and "
        "(timestamp < 1704067200000000000)",
        window=("2023-12-31T23:59:59.999Z", "2024-01-01T00:00:00.002Z"),
    )
    assert rows == [], rows
    run(
        f"where timestamp >= {START} or event_id != 42",
        query="((timestamp >= 1704067200000000000) or (event_id != 42))",
    )
    for tail in [
        "",
        "select event_id",
        'where message.match_regex("attempt$")',
        'where message.match_regex("attempt$")\nhead 1',
        'where message == "' + "a" * 10000 + '"',
    ]:
        run(tail, query=None, error="cannot construct a SentinelOne query from TQL")
    # Fields that PowerQuery cannot name stay in Tenzir, and so does
    # everything after them. An empty fixture checks planning without
    # missing-field warnings.
    empty = ', account_ids=["pushdown_empty"]'
    for field in ('this["a.b"]', 'this["not"]', 'this["a-b"]'):
        run(WINDOW + f"\nwhere {field} == 1", args=empty, query=window)
        run(
            f"where {field} == 1 and event_id == 42",
            args=empty,
            query=None,
            error="cannot construct a SentinelOne query from TQL",
        )
        run(f"head 1\nselect {field}", args=empty, query="")
    for predicate in (
        "this[key] == 1",
        "event[key] == 1",
        '"event_id" in this.keys()',
    ):
        run(
            f"where {predicate} and event_id == 42\nhead 1\nselect event_id",
            args=empty,
            query=None,
            error="cannot construct a SentinelOne query from TQL",
        )
        run(WINDOW + f"\nwhere {predicate}", args=empty, query=window)
    fields = ["field_" + str(i) + "x" * 500 for i in range(20)]
    run("head 1\nselect " + ", ".join(fields), args=empty, query="")
    for predicate, query in (
        (
            'event.type == "DNS"',
            (
                f'{pq_window} | filter (event.type == "DNS") '
                "| columns __tenzir_column_0 = event | limit 1600000"
            ),
        ),
        ('event.type.match_regex("DNS")', window),
    ):
        run(
            WINDOW + f"\nwhere {predicate}\nselect event",
            args=empty,
            query=query,
        )
    # A generated query has no fallback. Even parser errors reported as 500
    # must result in exactly one launch, and cleanup on polls.
    for field in ("launch_error", "poll_error"):
        run(
            f"where {field} == 1",
            query=f"({field} == 1)",
            error="SentinelOne rejected the generated query",
        )
    print("ok")


if __name__ == "__main__":
    main()
