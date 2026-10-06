# runner: python
# timeout: 300
"""Keep an explicit PowerQuery's capped result and request window unchanged."""

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
) -> list[dict]:
    offset = len(calls())
    pipeline = f"""
from_sentinelone_data_lake env("S1_FIXTURE_URL"),
  token=secret("test-token-s1-12345", _literal=true),
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
    assert post["payload"]["pq"]["query"] == query, requests
    assert post["payload"]["startTime"] == start, requests
    assert post["payload"]["endTime"] == end, requests
    deletes = [r for r in requests if r["method"] == "DELETE"]
    assert len(deletes) == 1 and deletes[0]["id"] == post["id"], requests
    return [json.loads(line) for line in result.stdout.splitlines()]


def main() -> None:
    capped = [{"id": i} for i in range(1000)]
    for raw in (False, True):
        assert run(raw=raw) == capped
        # Filtering before the cap would expose five additional matching rows.
        assert run("where id >= 995", raw=raw) == capped[995:]
        # Narrowing either bound would also admit rows outside the original cap.
        assert run(f"where timestamp >= {LATE}", raw=raw) == []
        assert (
            run(
                f"where timestamp >= {LATE} and timestamp < 2024-01-01T00:16:45Z",
                raw=raw,
            )
            == []
        )
        assert (
            run(
                "where timestamp < 2024-01-01T00:00:05Z",
                query="pushdown_capped_reverse",
                raw=raw,
            )
            == []
        )
        # A remote limit above 1,000 must not raise the implicit source cap.
        assert run("head 1001", raw=raw) == capped
        assert run("head 1", raw=raw) == capped[:1]
    # Controls prove that the fixture distinguishes the old unsafe rewrites.
    assert run(query="pushdown_capped | filter ((id) >= (995))") == [
        {"id": i} for i in range(995, 1005)
    ]
    assert run(start=LATE) == [{"id": i} for i in range(1000, 1005)]
    assert run(query="pushdown_capped_reverse", end="2024-01-01T00:00:05Z") == [
        {"id": i} for i in range(1000, 1005)
    ]
    assert run(query="pushdown_capped | limit 1001") == [{"id": i} for i in range(1001)]
    print("ok")


if __name__ == "__main__":
    main()
