# runner: python
# timeout: 120

"""Polling /serve before the serve operator registered must not fail.

A pipeline reports "running" before its operators finished starting, so a
client that launches a pipeline and immediately polls /serve can reach the
serve-manager before the serve operator registered its id. Such first-page
polls are parked until the operator registers (or the timeout fires) instead
of failing with an unknown-serve-id error.

Flow, launching hidden //neo pipelines through /pipeline/launch:
  1. Poll /serve for an id with no pipeline at all, then launch the pipeline
     while the poll is parked -> the poll delivers the pipeline's events.
  2. Poll /serve-multi for a just-launched stream and a never-registered one
     -> the response arrives as soon as the live stream has events; the
     never-registered stream reports an empty running first page.
  3. Poll /serve for a never-registered id -> the unknown-serve-id error
     arrives only after the requested timeout.

Tokens are runtime values, so we drive the REST API directly from Python and
assert only on the stable `n` payloads and the completion state.
"""

from __future__ import annotations

import json
import os
import shlex
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
import uuid
from collections.abc import Mapping
from pathlib import Path

API = "/api/v0"

NIL_TOKEN = "00000000-0000-0000-0000-000000000000"

# Serve ids must be unique per attempt: a timed-out attempt leaks its hidden
# pipelines until their ttl, and a retry reusing the same serve id would race
# the leaked registration.
SUFFIX = uuid.uuid4().hex[:8]


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


def _wait_for_api(proc: subprocess.Popen[str], url: str, timeout: int = 60) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            _, stderr = proc.communicate()
            raise RuntimeError(
                f"web server exited with {proc.returncode}: {stderr[-2000:]}"
            )
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
    try:
        _wait_for_api(proc, api_url)
    except Exception:
        if proc.poll() is None:
            proc.kill()
            proc.wait()
        raise
    return proc, api_url


def _launch(api: str, name: str, definition: str, serve_id: str) -> str:
    """Launches a hidden //neo pipeline ending in `serve` and returns its id."""
    status, body = _post(
        api,
        "/pipeline/launch",
        {
            "definition": f"//neo\n{definition}",
            "name": name,
            "hidden": True,
            "serve_id": serve_id,
            "ttl": "5m",
            "autostart": {"created": True},
        },
    )
    assert status == 200, f"launch of {name} failed ({status}): {body}"
    pipeline_id = str(body.get("id", ""))
    assert pipeline_id, f"launch of {name} returned no id: {body}"
    return pipeline_id


def _delete_pipeline(api: str, pipeline_id: str) -> None:
    if pipeline_id:
        _post(api, "/pipeline/delete", {"id": pipeline_id})


def _payloads(resp: dict) -> list[int]:
    """The `n` field of each returned event, in order."""
    return [e["data"]["n"] for e in resp.get("events", [])]


def _serve(api: str, serve_id: str, token: str | None, **kwargs) -> tuple[int, dict]:
    body: dict = {
        "serve_id": serve_id,
        "max_events": 4,
        "min_events": 4,
        "timeout": "10s",
        "schema": "never",
        **kwargs,
    }
    if token is not None:
        body["continuation_token"] = token
    return _post(api, "/serve", body)


def _drain(api: str, serve_id: str, token: str | None) -> str:
    """Follows the continuation token until the stream completes."""
    state = "completed"
    for _ in range(10):
        if not token:
            break
        status, resp = _serve(api, serve_id, token, min_events=1, timeout="5s")
        assert status == 200, f"drain poll failed ({status}): {resp}"
        state = resp["state"]
        token = resp["next_continuation_token"]
    return state


def check_parked_poll_delivers(api: str) -> None:
    """A first-page poll parked before registration delivers the events."""
    serve_id = f"park_deliver_{SUFFIX}"
    result: list[tuple[int, dict]] = []

    def poll() -> None:
        result.append(_serve(api, serve_id, None))

    poller = threading.Thread(target=poll)
    poller.start()
    # Give the poll time to reach the serve-manager and park; only then launch
    # the pipeline whose serve operator resolves it.
    time.sleep(1.0)
    pipeline_id = _launch(
        api,
        "park-deliver",
        "from {n: 1}, {n: 2}, {n: 3}, {n: 4}",
        serve_id,
    )
    try:
        poller.join(timeout=30)
        assert not poller.is_alive(), "parked poll did not return"
        status, resp = result[0]
        assert status == 200, f"parked poll failed ({status}): {resp}"
        assert "next_continuation_token" in resp, f"parked poll errored: {resp}"
        print(f"parked-events: {_payloads(resp)}")
        state = _drain(api, serve_id, resp["next_continuation_token"])
        print(f"parked-final-state: {state}")
    finally:
        _delete_pipeline(api, pipeline_id)


def check_multi_flush_pending(api: str) -> None:
    """A group poll is not held back by a never-registered serve id."""
    live_id = f"park_live_{SUFFIX}"
    ghost_id = f"park_ghost_{SUFFIX}"
    pipeline_id = _launch(
        api,
        "park-live",
        "from {n: 5}, {n: 6}, {n: 7}, {n: 8}",
        live_id,
    )
    try:
        start = time.monotonic()
        status, resp = _post(
            api,
            "/serve-multi",
            {
                "requests": [
                    {"serve_id": live_id, "continuation_token": NIL_TOKEN},
                    {"serve_id": ghost_id, "continuation_token": NIL_TOKEN},
                ],
                "max_events": 8,
                "min_events": 1,
                "timeout": "10s",
                "schema": "never",
            },
        )
        elapsed = time.monotonic() - start
        assert status == 200, f"multi poll failed ({status}): {resp}"
        assert live_id in resp and ghost_id in resp, f"bad response: {resp}"
        live = resp[live_id]
        ghost = resp[ghost_id]
        print(f"multi-live-events: {_payloads(live)}")
        print(f"multi-ghost-events: {_payloads(ghost)}")
        print(f"multi-ghost-state: {ghost['state']}")
        token_is_initial = ghost["next_continuation_token"] == NIL_TOKEN
        print(f"multi-ghost-token-is-initial: {token_is_initial}")
        print(f"multi-returned-before-timeout: {elapsed < 8.0}")
        _drain(api, live_id, live["next_continuation_token"])
    finally:
        _delete_pipeline(api, pipeline_id)


def check_unknown_id_fails_after_timeout(api: str) -> None:
    """A poll for a never-registered id errors only after the timeout."""
    start = time.monotonic()
    status, resp = _serve(api, f"park_never_{SUFFIX}", None, timeout="2s", min_events=1)
    elapsed = time.monotonic() - start
    error = json.dumps(resp)
    print(f"never-waited-for-timeout: {elapsed >= 1.5}")
    print(f"never-mentions-unknown-id: {'unknown serve id' in error}")
    print(f"never-has-events: {'events' in resp}")


def main() -> None:
    proc, api = start_web_server(os.environ)
    try:
        check_parked_poll_delivers(api)
        check_multi_flush_pending(api)
        check_unknown_id_fails_after_timeout(api)
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
