# runner: python
# timeout: 60

"""Reproduce TNZ-668: every loses batches when subpipeline is slow.

A managed pipeline uses ``every 1s { delay ... | import }`` where the
delay makes the subpipeline outlive the window boundary.  While the old
subpipeline is still alive, ``every`` cannot start the next one, so
events arriving in that gap are dropped.

The test publishes 120 events at 20ms intervals (~2.4s), spanning
multiple ``every 1s`` windows.  A 1.5s delay inside the subpipeline
ensures it outlives the window by 500ms, creating a gap where the bug
drops events.  A loss-free run delivers all 120 events to storage.
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

# ---------------------------------------------------------------------------
# REST API helpers (shared with shutdown.py)
# ---------------------------------------------------------------------------

API = "/api/v0"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _terminate(proc: subprocess.Popen[str], timeout: int = 20) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


def _post(url: str, path: str, body: dict | None = None) -> tuple[int, dict]:
    data = json.dumps(body or {}).encode()
    req = urllib.request.Request(
        f"{url}{API}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")


def _wait_for_api(url: str, timeout: int = 20) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            status, _ = _post(url, "/ping")
            if status == 200:
                return
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(0.1)
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


def create_pipeline(
    api_url: str,
    definition: str,
    *,
    name: str | None = None,
) -> str:
    body: dict = {
        "definition": f"//neo\n{definition}",
        "autostart": {"created": True},
    }
    if name is not None:
        body["name"] = name
    status, resp = _post(api_url, "/pipeline/create", body)
    assert status == 200, f"/pipeline/create failed ({status}): {resp}"
    pid = resp.get("id", "")
    assert pid, f"no pipeline id in response: {resp}"
    return pid


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

node = acquire_fixture("node")
web_proc = None

try:
    node.start()
    web_proc, api = start_web_server(node.env)
    tenzir = Executor.from_env(node.env)

    # Subscriber: every 1s, delay events by 1.5s (outliving the 1s window
    # by 500ms), then import.  The delay simulates a slow sink like
    # to_http with retries.
    create_pipeline(
        api,
        'subscribe "tpc.raw"\n'
        "every 1s {\n"
        "  this = {...this, _ts: 2025-01-01T00:00:01.5}\n"
        "  delay _ts, start=2025-01-01T00:00:00, speed=1.0\n"
        "  drop _ts\n"
        '  @name = "every-loss"\n'
        "  import\n"
        "}",
        name="every-loss-repro",
    )

    # Publish 120 events at 20ms intervals (~2.4s total), spanning
    # multiple `every 1s` windows.
    result = tenzir.run(
        'every 20ms { from {} }\nhead 120\nenumerate seq\npublish "tpc.raw"\n'
    )
    assert result.returncode == 0, f"publish failed: {result.stderr.decode()}"

    # Poll until the import count stabilises (no new events for 1s).
    count = 0
    prev_count = -1
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        time.sleep(0.5)
        r = tenzir.run(
            "export\n"
            'where @name == "every-loss"\n'
            "summarize count=count()\n"
            "write_ndjson\n"
        )
        if r.returncode == 0:
            try:
                count = json.loads(r.stdout.decode().strip()).get("count", 0)
            except (json.JSONDecodeError, ValueError):
                pass
        if count > 0 and count == prev_count:
            break
        prev_count = count

    if count != 120:
        print(f"FAIL: expected 120 events, got {count}")
        raise SystemExit(1)

    print(f"ok: all 120 events delivered")

finally:
    if web_proc:
        _terminate(web_proc)
    node.stop()
