# runner: python
# timeout: 60

"""Reproduce TNZ-668: every + to_http buffer_all=true loses batches.

A managed pipeline uses ``every 1s { to_http ... buffer_all=true }`` to
POST events.  The target returns 503 twice then 200, so the first batch
retries for ~400 ms.  Batches that arrive while the first one is still
retrying must be buffered and delivered, not dropped.

The test publishes 8 batches of 10 events (seq 0..79) at 120ms
intervals.  A loss-free run delivers all 80 events.  The bug causes
``every`` to discard batches that arrive while the subpipeline is busy
retrying, so fewer events arrive at the HTTP server.
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
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

# ---------------------------------------------------------------------------
# REST API helpers
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
# Recording HTTP target (retry-503 behaviour)
# ---------------------------------------------------------------------------


class HttpServerRecorder:
    """Thread-safe collector for HTTP request bodies."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.attempts = 0
        self.bodies: list[str] = []

    def handle(self, body: str) -> int:
        """Record a request body.  Return the HTTP status code to send."""
        with self.lock:
            self.attempts += 1
            self.bodies.append(body)
            # First 2 requests → 503; all subsequent → 200.
            if self.attempts <= 2:
                return 503
            return 200

    def all_events(self) -> list[dict]:
        """Parse NDJSON from all *successful* bodies (status 200)."""
        with self.lock:
            events: list[dict] = []
            for i, body in enumerate(self.bodies):
                if i < 2:
                    continue  # skip the 503 bodies
                for line in body.splitlines():
                    line = line.strip()
                    if line:
                        events.append(json.loads(line))
            return events


def _start_target(recorder: HttpServerRecorder) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", 0))
            body = self.rfile.read(length).decode() if length else ""
            status = recorder.handle(body)
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            if status == 503:
                payload = b'{"error":"service-unavailable"}\n'
            else:
                payload = b'{"ok":true}\n'
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_: object) -> None:
            return

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

node = acquire_fixture("node")
web_proc = None
target = None
recorder = HttpServerRecorder()

try:
    node.start()
    web_proc, api = start_web_server(node.env)
    tenzir = Executor.from_env(node.env)

    target = _start_target(recorder)
    target_port = target.server_address[1]
    target_url = f"http://127.0.0.1:{target_port}/status/retry-503"

    # Subscriber pipeline: every 1s { to_http (retry) }.
    create_pipeline(
        api,
        f'subscribe "tpc.raw"\n'
        f"every 1s {{\n"
        f'  to_http "{target_url}", method="POST", timeout=1s,\n'
        f"          max_retry_count=40, retry_delay=200ms,\n"
        f"          buffer_all=true {{\n"
        f"    write_ndjson\n"
        f"  }}\n"
        f"}}",
        name="to-http-loss-repro",
    )

    result = tenzir.run("""
        every 20ms { from {} } | head 120 | enumerate seq
        publish "tpc.raw"
        """)
    assert result.returncode == 0, f"publish failed: {result.stderr.decode()}"

    # Wait for the `every 1s` window to close and retries to finish.
    time.sleep(5)

    # Verify all 80 events were delivered.
    events = recorder.all_events()
    received_seqs = sorted(e["seq"] for e in events)
    expected_seqs = list(range(120))

    if received_seqs != expected_seqs:
        missing = sorted(set(expected_seqs) - set(received_seqs))
        print(
            f"FAIL: lost {len(missing)} events (seq {missing}), "
            f"got {len(received_seqs)}/120, "
            f"total POST attempts: {recorder.attempts}"
        )
        raise SystemExit(1)

    print(f"ok: all 120 events delivered, total POST attempts: {recorder.attempts}")

finally:
    if target:
        target.shutdown()
    if web_proc:
        _terminate(web_proc)
    node.stop()
