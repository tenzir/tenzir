# runner: python
# timeout: 60

"""Exercise the stateful multiserve membership endpoints.

The `multiserve_id` is a server-generated random UUID, so we cannot drive
add/remove/status from a static `.tql` test: each `api` call body must be a
compile-time constant and cannot reference a runtime id. Instead we drive the
REST API directly from Python, capture the id at runtime, and assert only on
the stable parts of each response (sorted member serve ids and their schema
overrides). The volatile `multiserve_id` and `continuation_token` fields are
never printed.

Flow:
  1. POST /multiserve (create) seeded with two members -> capture id.
  2. POST /multiserve/status -> two members.
  3. POST /multiserve/add a third member with a schema override -> three members.
  4. POST /multiserve/remove one member -> two members.
  5. POST /multiserve/status against an unknown id -> error body.
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


def _members(resp: dict) -> list[dict]:
    """Return members reduced to their stable fields, sorted by serve id.

    The `continuation_token` is volatile, so we only keep `serve_id` and
    `schema` (the override, which is null when unset).
    """
    out = []
    for m in resp.get("members", []):
        out.append({"serve_id": m["serve_id"], "schema": m["schema"]})
    return sorted(out, key=lambda m: m["serve_id"])


def main() -> None:
    proc, api = start_web_server(os.environ)
    try:
        # 1. Create a session seeded with two members.
        status, resp = _post(
            api,
            "/multiserve",
            {
                "serve_ids": [
                    {"serve_id": "multiserve_1"},
                    {"serve_id": "multiserve_2", "schema": "never"},
                ],
                "timeout": "1s",
                "min_events": 0,
                "max_events": 0,
            },
        )
        assert status == 200, f"create failed ({status}): {resp}"
        assert "multiserve_id" in resp, f"no multiserve_id: {resp}"
        assert set(resp.get("results", {})) == {
            "multiserve_1",
            "multiserve_2",
        }, f"unexpected results keys: {resp}"
        mid = resp["multiserve_id"]
        print("create-results-keys: ['multiserve_1', 'multiserve_2']")

        # 2. Status reflects the seeded members.
        status, resp = _post(api, "/multiserve/status", {"multiserve_id": mid})
        assert status == 200, f"status failed ({status}): {resp}"
        print(f"status-after-create: {_members(resp)}")

        # 3. Add a third member with a schema override.
        status, resp = _post(
            api,
            "/multiserve/add",
            {
                "multiserve_id": mid,
                "serve_id": "multiserve_3",
                "schema": "exact",
            },
        )
        assert status == 200, f"add failed ({status}): {resp}"
        print(f"members-after-add: {_members(resp)}")

        # 4. Remove a member; membership shrinks.
        status, resp = _post(
            api,
            "/multiserve/remove",
            {"multiserve_id": mid, "serve_id": "multiserve_1"},
        )
        assert status == 200, f"remove failed ({status}): {resp}"
        print(f"members-after-remove: {_members(resp)}")

        # 5. Unknown session id yields an error response. The web server wraps
        # handler-level errors as HTTP 200 with an `error` body (the non-200
        # path is reserved for auth/parse failures), so we assert on the body.
        status, resp = _post(
            api, "/multiserve/status", {"multiserve_id": "does-not-exist"}
        )
        print(f"unknown-id-is-error: {'error' in resp}")
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
