# runner: python
# timeout: 60

"""First-page retries against /serve-multi via the explicit initial token.

/serve-multi requires a continuation token on every request; the well-known
nil UUID addresses the first page. Because every page—including the first—has
a token, a client that lost a response can always retry the same request and
receive the cached batch again instead of an "unknown continuation token"
error.

Flow against the static pipelines serve_multi_retry_a (n: 1..4) and
serve_multi_retry_b (n: 5..8):
  1. Initial poll with the nil UUID for both streams -> the first two events
     of each stream and a fresh continuation token per stream.
  2. Repeat the exact same request -> identical events and identical tokens,
     served from the retry cache.
  3. Continue with the tokens from step 1 -> the remaining events, completion.
  4. Omitting the continuation token is rejected with an error that names the
     initial token.

Tokens are runtime values, so we drive the REST API directly from Python and
assert only on the stable `n` payloads and the completion state.
"""

from __future__ import annotations

import json
import os
import shlex
import socket
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Mapping
from pathlib import Path

API = "/api/v0"

NIL_TOKEN = "00000000-0000-0000-0000-000000000000"
STREAMS = ("serve_multi_retry_a", "serve_multi_retry_b")


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _post(url: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body or {}).encode()
    req = urllib.request.Request(
        f"{url}{API}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=30) as r:
            return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")


def _wait_for_api(url: str, timeout: int = 30) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status, _ = _post(url, "/ping")
            if status == 200:
                return
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(0.2)
    raise RuntimeError("REST API did not come up")


def start_web_server(
    env: Mapping[str, str],
) -> tuple[subprocess.Popen[str], str]:
    binary = shlex.split(env["TENZIR_NODE_CLIENT_BINARY"])
    tenzir_ctl = str(Path(binary[0]).with_name("tenzir-ctl"))
    endpoint = env["TENZIR_NODE_CLIENT_ENDPOINT"]
    port = _free_port()
    proc = subprocess.Popen(
        [
            tenzir_ctl,
            "--bare-mode",
            "--console-verbosity=warning",
            f"--endpoint={endpoint}",
            "web",
            "server",
            "--mode=dev",
            "--bind=127.0.0.1",
            f"--port={port}",
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    api_url = f"http://127.0.0.1:{port}"
    _wait_for_api(api_url)
    return proc, api_url


def _payloads(resp: dict) -> list[int]:
    """The `n` field of each returned event, in order."""
    return [e["data"]["n"] for e in resp.get("events", [])]


def _multi(
    api: str, requests: list[dict], min_events: int, max_events: int
) -> tuple[int, dict]:
    body = {
        "requests": requests,
        "max_events": max_events,
        "min_events": min_events,
        "timeout": "5s",
        "schema": "never",
    }
    return _post(api, "/serve-multi", body)


def _drain(api: str, serve_id: str, token: str | None) -> tuple[list[int], str]:
    """Follow continuation tokens until the stream completes."""
    events: list[int] = []
    state = "running"
    for _ in range(10):
        if not token:
            break
        status, resp = _multi(
            api, [{"serve_id": serve_id, "continuation_token": token}], 1, 4
        )
        assert status == 200, f"drain of {serve_id} failed ({status}): {resp}"
        entry = resp[serve_id]
        events += _payloads(entry)
        state = entry["state"]
        token = entry["next_continuation_token"]
    assert not token, f"{serve_id} did not complete"
    return events, state


def main() -> None:
    proc, api = start_web_server(os.environ)
    try:
        initial = [
            {"serve_id": s, "continuation_token": NIL_TOKEN} for s in STREAMS
        ]
        # 1. The initial poll addresses the first page with the nil UUID.
        status, first = _multi(api, initial, 4, 4)
        assert status == 200, f"initial poll failed ({status}): {first}"
        for s in STREAMS:
            print(f"{s}-first: {_payloads(first[s])}")
            assert first[s]["next_continuation_token"], (
                f"expected a continuation token: {first}"
            )

        # 2. Re-sending the identical request replays the cached first page.
        status, retry = _multi(api, initial, 4, 4)
        assert status == 200, f"retry poll failed ({status}): {retry}"
        events_match = all(
            _payloads(retry[s]) == _payloads(first[s]) for s in STREAMS
        )
        tokens_match = all(
            retry[s]["next_continuation_token"]
            == first[s]["next_continuation_token"]
            for s in STREAMS
        )
        print(f"retry-events-match: {events_match}")
        print(f"retry-tokens-match: {tokens_match}")
        assert events_match and tokens_match, f"retry diverged: {retry}"

        # 3. The tokens from the first response remain valid after the retry.
        for s in STREAMS:
            events, state = _drain(api, s, first[s]["next_continuation_token"])
            print(f"{s}-rest: {events}")
            print(f"{s}-final-state: {state}")

        # 4. Omitting the continuation token is a client error.
        status, err = _multi(api, [{"serve_id": STREAMS[0]}], 1, 4)
        error = err.get("error", "")
        print(f"missing-token-rejected: {'continuation_token' in error}")
        assert "continuation_token" in error, f"expected an error: {err}"
    finally:
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.wait()


if __name__ == "__main__":
    main()
