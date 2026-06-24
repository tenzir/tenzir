# runner: python
# timeout: 60

"""Retrying a poll after a serve pipeline completed must not error.

The web is lossy and clients may cancel and re-issue polls. When a serve
pipeline exhausts, the serve-manager keeps the last delivered batch around for
the retention window, so a client that lost (or cancelled) the response to its
final poll can retry with the same continuation token and get the final batch
again instead of an "expired serve id" error.

Flow against the static `serve_retry` pipeline (four events `n: 1..4`):
  1. Poll with max_events=2 -> first two events, a non-empty continuation token.
  2. Poll with that token -> last two events, empty token (completed).
  3. Retry step 2 with the *same* token -> the last two events again, completed.

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
from pathlib import Path

API = "/api/v0"


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


def start_web_server(env: dict[str, str]) -> tuple[subprocess.Popen[str], str]:
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


def _serve(api: str, token: str | None) -> tuple[int, dict]:
    body: dict = {
        "serve_id": "serve_retry",
        "max_events": 2,
        "min_events": 2,
        "timeout": "5s",
        "schema": "never",
    }
    if token is not None:
        body["continuation_token"] = token
    return _post(api, "/serve", body)


def main() -> None:
    proc, api = start_web_server(os.environ)
    try:
        # 1. First poll drains the first two events and yields a token.
        status, first = _serve(api, None)
        assert status == 200, f"first poll failed ({status}): {first}"
        token = first["next_continuation_token"]
        assert token, f"expected a continuation token: {first}"
        print(f"first-events: {_payloads(first)}")
        print(f"first-state: {first['state']}")

        # 2. Second poll drains the final two events and completes.
        status, final = _serve(api, token)
        assert status == 200, f"second poll failed ({status}): {final}"
        print(f"final-events: {_payloads(final)}")
        print(f"final-state: {final['state']}")
        assert not final["next_continuation_token"], f"expected completion: {final}"

        # 3. Retrying the completing poll with the same token must re-deliver
        # the final batch and report completion, not an "expired serve id"
        # error.
        status, retry = _serve(api, token)
        assert status == 200, f"retry failed ({status}): {retry}"
        assert "error" not in retry, f"retry returned an error: {retry}"
        print(f"retry-events: {_payloads(retry)}")
        print(f"retry-state: {retry['state']}")
        assert _payloads(retry) == _payloads(final), (
            f"retry payloads {_payloads(retry)} != final {_payloads(final)}"
        )
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
