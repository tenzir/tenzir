# runner: python
# timeout: 90

"""`partitions`, `fields` and `schemas` report on the catalog.

Those event batches have no CAF inspector, so the catalog can only hand them
to a caller in its own process. The node therefore runs with `tenzir.nova:
true` (see `tenzir-node.yaml`) and the pipelines under test are deployed into
it rather than run from the client, which is a separate process.

`to_stdout` is the only sink that accepts them today, so the assertions read
the node's stdout. `01_seed.tql` has already populated the catalog.
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
        with urllib.request.urlopen(req, timeout=10) as r:
            return r.status, json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read().decode() or "{}")


def _wait_for_api(url: str, timeout: int = 20) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if _post(url, "/ping")[0] == 200:
                return
        except (urllib.error.URLError, ConnectionError, OSError):
            pass
        time.sleep(0.1)
    raise RuntimeError("REST API did not come up")


def _start_web_server(env: dict[str, str]) -> tuple[subprocess.Popen[str], str]:
    binary = shlex.split(env["TENZIR_NODE_CLIENT_BINARY"])
    tenzir_ctl = str(Path(binary[0]).with_name("tenzir-ctl"))
    port = _free_port()
    proc = subprocess.Popen(
        [
            tenzir_ctl,
            "--bare-mode",
            "--console-verbosity=warning",
            f"--endpoint={env['TENZIR_NODE_CLIENT_ENDPOINT']}",
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


def _node_stdout(env: dict[str, str]) -> Path:
    return Path(env["TENZIR_NODE_STDOUT_LOG"])


def _deploy(api_url: str, definition: str, name: str) -> None:
    status, resp = _post(
        api_url,
        "/pipeline/create",
        {"definition": definition, "name": name, "autostart": {"created": True}},
    )
    assert status == 200, f"/pipeline/create failed ({status}): {resp}"
    # A pipeline that fails to start reports that in the body, not the status.
    assert "error" not in resp, f"{name} failed to start: {resp['error']}"


def _await_output(log: Path, needles: list[str], timeout: int = 30) -> str:
    deadline = time.monotonic() + timeout
    text = ""
    while time.monotonic() < deadline:
        text = log.read_text() if log.exists() else ""
        if all(needle in text for needle in needles):
            return text
        time.sleep(0.2)
    raise AssertionError(f"missing {needles!r} in node stdout:\n{text}")


# `test.yaml` declares the node fixture for this suite, so the harness has
# already started it and injected its endpoint and log paths here. That is the
# node `01_seed.tql` imported into, so do not start a second one.
env = dict(os.environ)

log = _node_stdout(env)
web_proc, api_url = _start_web_server(env)
try:
    # `select` has no implementation for this model yet, so use `drop`.
    _deploy(
        api_url,
        "partitions\nwhere not internal\n"
        "drop uuid, memusage, diskusage, min_import_time, max_import_time, "
        "schema_id, store, indexes, sketches, approx_bytes, version, internal\n"
        "to_stdout",
        "catalog-partitions",
    )
    text = _await_output(
        log,
        ['schema: "count.small"', 'schema: "count.large"', "events: 10", "events: 90"],
    )

    _deploy(
        api_url,
        'fields\nwhere schema == "count.small"\n'
        "drop schema_id, path, index, type\nto_stdout",
        "catalog-fields",
    )
    _await_output(log, ['field: "index"'])

    _deploy(api_url, "schemas\nto_stdout", "catalog-schemas")
    _await_output(log, ['name: "count.small"'])
finally:
    web_proc.terminate()
    web_proc.wait(timeout=20)
