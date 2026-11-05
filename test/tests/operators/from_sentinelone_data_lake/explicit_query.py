# runner: python
# timeout: 300
"""Append the stages after an explicit PowerQuery, keeping its request window."""

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
LATE = "2024-01-01T00:16:40Z"


def calls() -> list[dict]:
    return [json.loads(line) for line in CAPTURE.read_text().splitlines() if line]


def run(
    tail: str = "",
    *,
    query: str = "pushdown_capped",
    start: str = START,
    end: str = END,
    raw: bool = False,
    expected: str | None = None,
) -> list[dict]:
    offset = len(calls())
    pipeline = f"""
from_sentinelone_data_lake env("S1_FIXTURE_URL"),
  token="test-token-s1-12345",
  query={json.dumps(query)}, start={start}, end={end}, raw={str(raw).lower()}
{tail}
select id
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
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0 and not result.stderr, (pipeline, result)
    requests = calls()[offset:]
    assert not [r for r in requests if "error" in r], requests
    posts = [r for r in requests if r["method"] == "POST"]
    assert len(posts) == 1, requests
    post = posts[0]
    if expected is None:
        expected = f"{query} | columns __tenzir_column_0 = id | limit 1600000"
    assert post["payload"]["pq"]["query"] == expected, requests
    assert post["payload"]["startTime"] == start, requests
    assert post["payload"]["endTime"] == end, requests
    deletes = [r for r in requests if r["method"] == "DELETE"]
    assert len(deletes) == 1 and deletes[0]["id"] == post["id"], requests
    return [json.loads(line) for line in result.stdout.splitlines()]


def main() -> None:
    # The stages after the query run in SentinelOne, before its cap, and the
    # appended limit lifts the default cap of 1,000 rows.
    rows = [{"id": i} for i in range(1005)]
    columns = " | columns __tenzir_column_0 = id | limit 1600000"
    for raw in (False, True):
        assert run(raw=raw) == rows
        assert (
            run(
                "where id >= 995",
                raw=raw,
                expected="pushdown_capped | filter (id >= 995)" + columns,
            )
            == rows[995:]
        )
        # The request window stays as it is, while the stages compare with
        # SentinelOne's own timestamp.
        late = "1704068200000000000"
        assert (
            run(
                f"where timestamp >= {LATE}",
                raw=raw,
                expected=f"pushdown_capped | filter (timestamp >= {late})" + columns,
            )
            == rows[1000:]
        )
        assert (
            run(
                f"where timestamp >= {LATE} and timestamp < 2024-01-01T00:16:45Z",
                raw=raw,
                expected=f"pushdown_capped | filter (timestamp >= {late}) | "
                "filter (timestamp < 1704068205000000000)" + columns,
            )
            == rows[1000:]
        )
        assert run(
            "where timestamp < 2024-01-01T00:00:05Z",
            query="pushdown_capped_reverse",
            raw=raw,
            expected="pushdown_capped_reverse | filter (timestamp < "
            "1704067205000000000)" + columns,
        ) == [{"id": i} for i in range(1000, 1005)]
        # A filter or a select must not follow a limit of whole events, so
        # only the limit runs in SentinelOne.
        assert (
            run("head 1001", raw=raw, expected="pushdown_capped | limit 1001")
            == rows[:1001]
        )
        assert run("head 1", raw=raw, expected="pushdown_capped | limit 1") == rows[:1]
    assert run(query="pushdown_capped | filter ((id) >= (995))") == rows[995:]
    assert run(start=LATE) == rows[1000:]
    assert run(query="pushdown_capped_reverse", end="2024-01-01T00:00:05Z") == [
        {"id": i} for i in range(1000, 1005)
    ]
    assert run(query="pushdown_capped | limit 1001") == rows[:1001]
    print("ok")


if __name__ == "__main__":
    main()
