# runner: python
# timeout: 180
"""Transparent complete-event discovery, projected PQ, and LOG continuation."""

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


def run(
    tail: str = "",
    *,
    scope: str = "hybrid_discovery",
    modes: list[str] | None = None,
    args: str = "",
    error: str = "",
) -> tuple[list[dict], list[dict]]:
    offset = len(CAPTURE.read_text().splitlines())
    pipeline = f"""
from_sentinelone_data_lake env("S1_FIXTURE_URL"),
  token="test-token-s1-12345",
  account_ids=[{json.dumps(scope)}],
  start=2024-01-01T00:00:00Z, end=2024-01-02T00:00:00Z{args}
{tail}
write_ndjson
"""
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
        assert result.returncode > 0 and error in result.stderr, result
    else:
        assert result.returncode == 0 and not result.stderr, (pipeline, result)
    calls = [json.loads(line) for line in CAPTURE.read_text().splitlines()[offset:]]
    assert not [call for call in calls if "error" in call], calls
    posts = [call for call in calls if call["method"] == "POST"]
    if modes is not None:
        assert [call["payload"]["queryType"] for call in posts] == modes, calls
    assert {call["id"] for call in posts} == {
        call["id"] for call in calls if call["method"] == "DELETE"
    }, calls
    for post in posts:
        payload = post["payload"]
        assert payload["tenant"] is False and payload["accountIds"] == [scope], payload
        assert payload["startTime"] == "2024-01-01T00:00:00Z", payload
        assert payload["endTime"] == "2024-01-02T00:00:00Z", payload
        if payload["queryType"] == "LOG":
            assert "pq" not in payload, payload
            assert 2 <= payload["log"]["limit"] <= 1000, payload
    return [json.loads(line) for line in result.stdout.splitlines()], posts


def main() -> None:
    for tail in (
        "",
        'where event.type == "DNS Resolved"',
        "where event != null",
        'where severity == 3 and threadId == "worker"',
    ):
        rows, posts = run(tail, modes=["LOG"])
        assert [row["id"] for row in rows] == [0, 1, 2], rows
        assert rows[0]["event"] == {
            "type": "DNS Resolved",
            "dns": {"request": "host-0"},
            "extra": "kept",
        }, rows
        assert rows[0]["items"] == [1, {"nested": False}], rows
        assert rows[0]["flag"] is True, rows
        assert rows[0]["timestamp"] == "2024-01-01T00:00:00Z", rows
        if tail == "where event != null":
            assert posts[0]["payload"]["log"]["filter"] == "", posts
    rows, _ = run("select id", modes=["PQ"])
    assert rows == [{"id": i} for i in range(3)], rows
    rows, _ = run("select id", scope="hybrid_partial", modes=["PQ", "LOG"])
    assert rows == [{"id": i} for i in range(3)], rows
    rows, posts = run(scope="hybrid_inline", modes=["LOG"])
    assert [row["event_id"] for row in rows] == [42, 43], rows
    # No GET is necessary when the launch already carries a completed result.
    all_calls = [json.loads(line) for line in CAPTURE.read_text().splitlines()]
    assert not [
        call
        for call in all_calls
        if call["method"] == "GET" and call["id"] == posts[0]["id"]
    ], all_calls
    # A null parent column cannot distinguish an absent field from a subtree.
    rows, _ = run("select event", modes=["PQ", "LOG"])
    assert [row["event"]["dns"]["request"] for row in rows] == [
        f"host-{i}" for i in range(3)
    ], rows
    rows, _ = run('where event.type == "DNS Resolved"\nselect event', modes=["LOG"])
    assert all(row["event"]["extra"] == "kept" for row in rows), rows
    rows, _ = run("select items", modes=["PQ", "LOG"])
    assert rows == [{"items": [1, {"nested": False}]}] * 3, rows
    rows, _ = run("where this[key] == 2\nselect id", modes=["LOG"])
    assert rows == [{"id": 2}], rows
    rows, _ = run('where "items" in this.keys()\nselect id', modes=["LOG"])
    assert rows == [{"id": i} for i in range(3)], rows
    rows, _ = run("head 1", args=", raw=true", modes=["LOG"])
    assert rows[0]["flag"] == "true" and rows[0]["items"][1]["nested"] == "false", rows
    assert rows[0]["timestamp"] == "2024-01-01T00:00:00Z", rows

    for raw in ("false", "true"):
        rows, _ = run(
            'where type_of(a).kind == "float" and b > 0 and c < 0',
            scope="hybrid_numeric_floats",
            args=f", raw={raw}",
            modes=["LOG"],
        )
        # JSON renders non-finite numbers as null. The predicate above checks
        # their actual types and infinity signs before serialization.
        assert len(rows) == 1 and all(
            rows[0][key] is None for key in ("a", "b", "c")
        ), rows
        for scope in ("hybrid_numeric_positive", "hybrid_numeric_negative"):
            rows, _ = run(
                scope=scope,
                args=f", raw={raw}",
                modes=["LOG"],
                error="JSON integer does not fit into 64 bits",
            )
            assert not rows, rows

    # No projection means full discovery. Every timestamp ties, including
    # the boundary, and the deliberately inaccurate estimate is ignored.
    rows, posts = run(scope="hybrid_dense", modes=["LOG", "LOG"])
    assert [row["id"] for row in rows] == list(range(1005)), rows
    assert posts[1]["payload"]["log"]["cursor"] == "match-999", posts
    # A saturated projected PQ response must not silently truncate the read.
    rows, _ = run("select id", scope="hybrid_dense", modes=["PQ", "LOG", "LOG"])
    assert rows == [{"id": i} for i in range(1005)], rows
    # Optional nested projections must tolerate missing and null parents,
    # including after a saturated PQ result falls back to paged LOG reads.
    rows, _ = run(
        "select id, event?.type?, dataSource?.name?",
        scope="hybrid_mixed",
        modes=["PQ", "LOG", "LOG"],
    )
    assert rows == [
        {
            "id": i,
            "event": {"type": "DNS Resolved" if i % 3 == 0 else None},
            "dataSource": {"name": "SentinelOne" if i % 3 == 0 else None},
        }
        for i in range(1005)
    ], rows
    tail = 'where event.dns.request.match_regex("^host-100[0-4]$")\nhead 3'
    rows, posts = run(tail, scope="hybrid_dense")
    assert [row["id"] for row in rows] == [1000, 1001, 1002], rows
    assert all(post["payload"]["queryType"] == "LOG" for post in posts), posts
    assert len(posts) > 2, posts
    rows, _ = run(
        tail + "\nselect id", scope="hybrid_dense", modes=["PQ", *["LOG"] * len(posts)]
    )
    assert rows == [{"id": i} for i in (1000, 1001, 1002)], rows

    rows, _ = run(scope="hybrid_retry", modes=["LOG", "LOG"])
    assert len(rows) == 1005, len(rows)
    rows, _ = run(
        scope="hybrid_timeout",
        args=", timeout=2s",
        modes=["LOG", "LOG"],
        error="query timed out",
    )
    assert len(rows) <= 1000, len(rows)
    rows, _ = run(
        scope="hybrid_bad_boundary",
        modes=["LOG", "LOG"],
        error="LOG continuation did not repeat the boundary match",
    )
    assert len(rows) <= 1000, len(rows)
    rows, _ = run(
        scope="hybrid_bad_values",
        modes=["LOG"],
        error="missing or invalid LOG match values",
    )
    assert not rows, rows
    rows, _ = run(
        scope="hybrid_partial_log",
        modes=["LOG"],
        error="SentinelOne returned an incomplete LOG result",
    )
    assert not rows, rows
    print("ok")


if __name__ == "__main__":
    main()
