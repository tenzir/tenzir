# runner: python
"""Assert the LRQ wire contract, retries, cleanup, and graceful shutdown."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import signal
import subprocess
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

CAPTURE = Path(os.environ["S1_FIXTURE_LRQ_CAPTURE_FILE"])
BINARY = shlex.split(
    os.environ.get("TENZIR_BINARY", shutil.which("tenzir") or "tenzir")
)


def calls() -> list[dict]:
    return [json.loads(line) for line in CAPTURE.read_text().splitlines() if line]


def command(query: str, arguments: str = "", tail: str = "write_ndjson") -> list[str]:
    pipeline = f"""
from_sentinelone_data_lake env("S1_FIXTURE_URL"),
  token=secret("test-token-s1-12345", _literal=true),
  query={json.dumps(query)}{arguments}
{tail}
"""
    return [
        *BINARY,
        "--bare-mode",
        "--nova=true",
        "--console-verbosity=warning",
        pipeline,
    ]


def run(
    query: str,
    arguments: str = "",
    tail: str = "write_ndjson",
    error: str = "",
    *,
    ambiguous_launch: bool = False,
):
    offset = len(calls())
    result = subprocess.run(
        command(query, arguments, tail), capture_output=True, text=True, timeout=30
    )
    if error:
        assert result.returncode != 0, result
        assert error in result.stderr, result.stderr
        assert not result.stdout, result.stdout
    else:
        assert result.returncode == 0, result.stderr
        assert not result.stderr, result.stderr
    requests = calls()[offset:]
    assert not [call for call in requests if "error" in call], requests
    launched = [call for call in requests if call["method"] == "POST" and "id" in call]
    assert len(launched) == 1, requests
    if ambiguous_launch:
        # The server created one query, but never returned its ID. The client
        # must neither replay the launch nor try to poll/delete an unknown ID.
        assert requests == launched, requests
    else:
        assert requests[-1]["method"] == "DELETE", requests
        assert requests[-1]["id"] == launched[0]["id"], requests
    return result, requests, launched[0]["payload"]


def main() -> None:
    for timeout in ("0s", "-1s"):
        offset = len(calls())
        result = subprocess.run(
            command("select_basic", f", timeout={timeout}"),
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode != 0, result
        assert "`timeout` must be greater than zero" in result.stderr, result.stderr
        assert not result.stdout, result.stdout
        assert len(calls()) == offset, calls()[offset:]

    # Invalid delays must stop rejected launches, not wrap to a negative sleep
    # or fall back to a shorter backoff that replays the POST.
    for query in (
        "select_launch_retry_unsigned_max",
        "select_launch_retry_signed_overflow",
        "select_launch_retry_beyond_unsigned",
        "select_launch_retry_invalid",
    ):
        offset = len(calls())
        result = subprocess.run(
            command(query), capture_output=True, text=True, timeout=10
        )
        requests = calls()[offset:]
        assert len(requests) == 1, requests
        assert requests[0]["method"] == "POST" and requests[0]["status"] == 429, (
            requests
        )
        assert "id" not in requests[0] and "error" not in requests[0], requests
        assert result.returncode != 0, result
        assert "invalid `Retry-After` response header" in result.stderr, result.stderr
        assert not result.stdout, result.stdout

    _, requests, _ = run(
        "select_poll_retry_overflow", error="invalid `Retry-After` response header"
    )
    assert [r["method"] for r in requests] == ["POST", "GET", "DELETE"], requests

    before = datetime.now(timezone.utc)
    result, requests, payload = run("select_basic", ", timeout=10s")
    after = datetime.now(timezone.utc)
    end = datetime.fromisoformat(payload["endTime"])
    start = datetime.fromisoformat(payload["startTime"])
    assert before <= end <= after, payload
    assert end - start == timedelta(hours=24), payload
    assert payload["tenant"] is True and "accountIds" not in payload, payload
    assert payload["pq"] == {"query": "select_basic", "resultType": "TABLE"}, payload
    assert [r["method"] for r in requests] == ["POST", "GET", "GET", "GET", "DELETE"]
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    assert [row["event_id"] for row in rows] == [42, 43], rows
    assert [r["step"] for r in requests if r["method"] == "GET"] == [0, 1, 2]

    _, _, payload = run(
        "select_empty",
        ", start=2024-01-01T00:00:00.000000001Z, "
        'end=2024-01-02T00:00:00.123456789Z, account_ids=["a", "b"]',
    )
    assert payload["startTime"] == "2024-01-01T00:00:00.000000001Z", payload
    assert payload["endTime"] == "2024-01-02T00:00:00.123456789Z", payload
    assert payload["tenant"] is False and payload["accountIds"] == ["a", "b"], payload
    _, _, payload = run("select_empty", ", end=2024-01-02")
    assert payload["startTime"] == "2024-01-01T00:00:00Z", payload
    assert payload["endTime"] == "2024-01-02T00:00:00Z", payload
    _, _, payload = run("select_empty", ", start=2024-01-01")
    assert payload["startTime"] == "2024-01-01T00:00:00Z", payload

    result, requests, _ = run("select_retry")
    polls = [r for r in requests if r["method"] == "GET"]
    assert [r["status"] for r in polls] == [429, 200, 200, 200], requests
    assert polls[1]["time"] - polls[0]["time"] >= 1, polls
    assert len(result.stdout.splitlines()) == 2, result.stdout
    run("select_transport_retry")
    _, requests, _ = run("select_launch_retry")
    launches = [r for r in requests if r["method"] == "POST"]
    assert [r["status"] for r in launches] == [429, 200], requests
    assert launches[1]["time"] - launches[0]["time"] >= 1, launches
    result, requests, _ = run("select_launch_long_retry")
    launches = [r for r in requests if r["method"] == "POST"]
    assert [r["status"] for r in launches] == [429, 200], requests
    assert launches[1]["time"] - launches[0]["time"] >= 20, launches
    assert len(result.stdout.splitlines()) == 2, result.stdout

    # No query exists while a rejected launch waits. Even an oversized header
    # must wait for the operator timeout, without retrying early or deleting.
    for query in ("select_launch_retry_timeout", "select_launch_retry_overflow"):
        offset = len(calls())
        result = subprocess.run(
            command(query, ", timeout=2500ms"),
            capture_output=True,
            text=True,
            timeout=10,
        )
        assert result.returncode != 0, result
        assert "query timed out" in result.stderr, result.stderr
        assert not result.stdout, result.stdout
        requests = calls()[offset:]
        assert len(requests) == 1, requests
        assert requests[0]["method"] == "POST" and requests[0]["status"] == 429, (
            requests
        )
        assert "id" not in requests[0] and "error" not in requests[0], requests
        assert 2 <= time.monotonic() - requests[0]["time"] < 8, requests

    run(
        "select_launch_transport_error",
        error="failed to make http request",
        ambiguous_launch=True,
    )
    run(
        "select_launch_server_error",
        error="erroneous http status: 503",
        ambiguous_launch=True,
    )
    result, _, _ = run("select_expired", error='{"code": "not_found"}')
    assert "expired or was killed" in result.stderr, result.stderr
    _, requests, _ = run("select_slow_retry", error="retry delay exceeds")
    assert len([r for r in requests if r["method"] == "GET"]) == 1, requests
    _, requests, _ = run("select_delete_retry")
    assert [r["status"] for r in requests if r["method"] == "DELETE"] == [503, 200]
    run("select_poll_error", error='{"error": "invalid PowerQuery"}')
    _, requests, _ = run("select_retry_exhausted", error="temporarily unavailable")
    polls = [r for r in requests if r["method"] == "GET"]
    assert len(polls) == 5, requests
    for previous, current, delay in zip(polls, polls[1:], (0.5, 1, 2, 4)):
        assert current["time"] - previous["time"] >= delay, polls

    # Neither unknown totals nor stalled positive totals can keep renewing
    # the lease forever. Polling backoff and in-flight requests share the budget.
    for query in (
        "select_running",
        "select_stalled",
        "select_deadline_retry",
        "select_hung_poll",
    ):
        _, requests, _ = run(query, ", timeout=2500ms", error="query timed out")
        polls = [r for r in requests if r["method"] == "GET"]
        assert polls, requests
        elapsed = requests[-1]["time"] - requests[0]["time"]
        assert 2 <= elapsed < 8, requests
        if query in {"select_deadline_retry", "select_hung_poll"}:
            # Do not retry early to fit Retry-After inside the remaining time.
            assert len(polls) == 1, polls
        else:
            expected_step = 1 if query == "select_stalled" else 0
            assert all(r["step"] == expected_step for r in polls), polls

    # A forced upstream stop from head must still find the query deleted.
    result, _, _ = run("select_batch_boundary", tail="head 1\nwrite_ndjson")
    assert result.stdout == '{"n":0}\n', result.stdout

    # Signal only once the fixture has observed a poll, not after a guessed
    # startup delay. This is an intentionally unfinished query.
    offset = len(calls())
    process = subprocess.Popen(
        command("select_running"),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        deadline = time.monotonic() + 20
        while not any(r["method"] == "GET" for r in calls()[offset:]):
            assert process.poll() is None, process.communicate()
            assert time.monotonic() < deadline, calls()[offset:]
            time.sleep(0.05)
        process.send_signal(signal.SIGTERM)
        stdout, stderr = process.communicate(timeout=20)
        assert process.returncode == 0, stderr
        assert not stdout, stdout
        assert stderr.strip() == (
            "initiating graceful shutdown... (repeat to terminate immediately)"
        ), stderr
        assert [r["method"] for r in calls()[offset:]][-1] == "DELETE", calls()[offset:]
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()
    print("ok")


if __name__ == "__main__":
    main()
