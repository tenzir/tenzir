# runner: python
# timeout: 60

"""Reproduce TNZ-668: every loses batches when subpipeline is slow.

A managed pipeline uses ``every 250ms { delay ... | import }`` where the
delay makes the subpipeline outlive the window boundary. While the old
subpipeline is still alive, ``every`` must apply backpressure rather than
drop a later batch.

The test confirms that the subscriber has registered, then publishes a
one-event batch, waits for the window to close, and publishes a separate
120-event batch. The 2s delay leaves a wide gap where the latter batch must be
retained. A loss-free run imports every sequence number exactly once.
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

    # Subscriber: every 250ms, delay events by 2s (outliving the window by
    # 1.75s), then import. The delay simulates a slow sink like
    # to_http with retries.
    create_pipeline(
        api,
        'subscribe "tpc.raw"\n'
        "fork {\n"
        "  where seq == -2\n"
        '  @name = "every-loss-ready"\n'
        "  import\n"
        "}\n"
        "where seq != -2\n"
        "every 250ms {\n"
        "  this = {...this, _ts: 2025-01-01T00:00:02}\n"
        "  delay _ts, start=2025-01-01T00:00:00, speed=1.0\n"
        "  drop _ts\n"
        '  @name = "every-loss"\n'
        "  import\n"
        "}",
        name="every-loss-repro",
    )

    # Wait for a probe to reach the immediate fork branch. This proves that
    # the same subscriber that feeds `every` has registered before the
    # one-shot seed starts the timing-sensitive scenario.
    ready_count = 0
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        probe = tenzir.run('from {seq: -2}\npublish "tpc.raw"\n')
        assert probe.returncode == 0, f"probe publish failed: {probe.stderr.decode()}"
        ready = tenzir.run(
            "export\n"
            'where @name == "every-loss-ready"\n'
            "summarize count=count()\n"
            "write_ndjson\n"
        )
        assert ready.returncode == 0, (
            f"readiness export failed: {ready.stderr.decode()}"
        )
        ready_count = json.loads(ready.stdout.decode().strip()).get("count", 0)
        if ready_count:
            break
        time.sleep(0.1)
    assert ready_count, "subscriber did not register within 10 seconds"

    # Finalizing the first publisher flushes a distinct batch immediately.
    # When the second publisher starts, `every` has already closed its first
    # window but its slow subpipeline is still running.
    seed = tenzir.run('from {seq: -1}\npublish "tpc.raw"\n')
    assert seed.returncode == 0, f"seed publish failed: {seed.stderr.decode()}"
    time.sleep(1)
    result = tenzir.run(
        'from {}\nrepeat 120\nenumerate seq\nbatch 120\npublish "tpc.raw"\n'
    )
    assert result.returncode == 0, f"publish failed: {result.stderr.decode()}"

    # Wait for the expected terminal state, not for a temporarily unchanged
    # count while a delayed subpipeline is still running.
    count = 0
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        r = tenzir.run(
            "export\n"
            'where @name == "every-loss"\n'
            "summarize count=count()\n"
            "write_ndjson\n"
        )
        assert r.returncode == 0, f"export failed: {r.stderr.decode()}"
        count = json.loads(r.stdout.decode().strip()).get("count", 0)
        if count == 121:
            break
        time.sleep(0.2)

    assert count == 121, f"timed out waiting for 121 events, got {count}"
    rows = tenzir.run('export\nwhere @name == "every-loss"\nsort seq\nwrite_ndjson\n')
    assert rows.returncode == 0, f"export failed: {rows.stderr.decode()}"
    assert [row["seq"] for row in map(json.loads, rows.stdout.splitlines())] == [
        -1,
        *range(120),
    ]

    print("ok: all batches delivered exactly once")

finally:
    if web_proc:
        _terminate(web_proc)
    node.stop()
