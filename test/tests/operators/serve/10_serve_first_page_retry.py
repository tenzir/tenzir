# runner: python
# timeout: 60

"""Retrying the first page of /serve must replay it, even after completion.

The first page of a serve is addressed by the well-known nil UUID token, which
/serve fills in when the continuation token is omitted. When a pipeline's
entire output fits into the first response, the pipeline completes right away;
a client that lost that response must be able to retry the first page and
receive the events again instead of an empty "completed" response.

Flow against the static `serve_first_page` pipeline (four events n: 1..4):
  1. Poll without a token and max_events=4 -> all four events.
  2. Poll without a token again -> the same events from the retry cache.
  3. Poll with the explicit nil UUID -> the same events; the omitted token is
     just shorthand for it.
  4. If the stream still carries a token, follow it to completion.

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


def _serve(api: str, token: str | None, min_events: int = 4) -> tuple[int, dict]:
    body: dict = {
        "serve_id": "serve_first_page",
        "max_events": 4,
        "min_events": min_events,
        "timeout": "5s",
        "schema": "never",
    }
    if token is not None:
        body["continuation_token"] = token
    return _post(api, "/serve", body)


def _matches(resp: dict, first: dict) -> bool:
    return (
        _payloads(resp) == _payloads(first)
        and resp["next_continuation_token"] == first["next_continuation_token"]
    )


def main() -> None:
    proc, api = start_web_server(os.environ)
    try:
        # 1. The first poll returns the pipeline's entire output.
        status, first = _serve(api, None)
        assert status == 200, f"first poll failed ({status}): {first}"
        print(f"first-events: {_payloads(first)}")

        # 2. Retrying without a token replays the cached first page.
        status, retry = _serve(api, None)
        assert status == 200, f"retry failed ({status}): {retry}"
        print(f"retry-matches-first: {_matches(retry, first)}")
        assert _matches(retry, first), f"retry diverged: {retry}"

        # 3. The explicit nil UUID addresses the same page.
        status, explicit = _serve(api, NIL_TOKEN)
        assert status == 200, f"explicit retry failed ({status}): {explicit}"
        print(f"explicit-nil-matches-first: {_matches(explicit, first)}")
        assert _matches(explicit, first), f"explicit retry diverged: {explicit}"

        # 4. Depending on shutdown timing the first poll may complete right
        # away or still carry a token; follow it to the end either way.
        token = first["next_continuation_token"]
        state = first["state"]
        extra: list[int] = []
        for _ in range(10):
            if not token:
                break
            status, resp = _serve(api, token, min_events=1)
            assert status == 200, f"follow-up poll failed ({status}): {resp}"
            extra += _payloads(resp)
            state = resp["state"]
            token = resp["next_continuation_token"]
        print(f"extra-events: {extra}")
        print(f"final-state: {state}")
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
