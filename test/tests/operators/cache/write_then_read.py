# runner: python
# timeout: 60

"""Write events into a named cache and read them from another node pipeline."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

API_PREFIX = "/api/v0"
CACHE_ID = "write-then-read"


def _post(base_url: str, path: str, body: dict[str, Any]) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{base_url}{API_PREFIX}{path}",
        data=json.dumps(body).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=10) as response:
        result = json.load(response)
    assert isinstance(result, dict), result
    return result


def _wait_for_server(base_url: str, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline and process.poll() is None:
        try:
            result = _post(base_url, "/pipeline/list", {})
            if isinstance(result.get("pipelines"), list):
                return
        except (urllib.error.URLError, TimeoutError):
            pass
        time.sleep(0.1)
    raise RuntimeError("web server did not become ready")


def _wait_for_stop(base_url: str, pipeline_id: str) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        result = _post(base_url, "/pipeline/update", {"id": pipeline_id})
        pipeline = result.get("pipeline")
        assert isinstance(pipeline, dict), result
        state = pipeline.get("state")
        if state in ("completed", "stopped"):
            return
        assert state != "failed", pipeline
        time.sleep(0.1)
    raise RuntimeError(f"pipeline {pipeline_id} did not stop (last state: {state})")


def _start_pipeline(base_url: str, name: str, definition: str) -> str:
    result = _post(
        base_url,
        "/pipeline/create",
        {"name": name, "definition": definition},
    )
    pipeline_id = result.get("id")
    assert isinstance(pipeline_id, str), result
    result = _post(
        base_url,
        "/pipeline/update",
        {"id": pipeline_id, "action": "start"},
    )
    assert isinstance(result.get("pipeline"), dict), result
    return pipeline_id


def _run_pipeline(base_url: str, name: str, definition: str) -> str:
    pipeline_id = _start_pipeline(base_url, name, definition)
    _wait_for_stop(base_url, pipeline_id)
    return pipeline_id


def _stop_pipeline(base_url: str, pipeline_id: str) -> None:
    result = _post(
        base_url,
        "/pipeline/update",
        {"id": pipeline_id, "action": "stop"},
    )
    assert isinstance(result.get("pipeline"), dict), result
    _wait_for_stop(base_url, pipeline_id)


def _ctl_binary() -> list[str]:
    binary = shlex.split(os.environ["TENZIR_NODE_CLIENT_BINARY"])
    sibling = Path(binary[0]).with_name("tenzir-ctl")
    if sibling.is_file():
        return [str(sibling)]
    fallback = shutil.which("tenzir-ctl")
    assert fallback, "could not find tenzir-ctl"
    return [fallback]


def main() -> int:
    endpoint = os.environ["TENZIR_NODE_CLIENT_ENDPOINT"]
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    base_url = f"http://127.0.0.1:{port}"
    command = [
        *_ctl_binary(),
        "--bare-mode",
        "--nova=true",
        "--console-verbosity=warning",
        f"--endpoint={endpoint}",
        "web",
        "server",
        "--mode=dev",
        "--bind=127.0.0.1",
        f"--port={port}",
    ]
    server = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    pipeline_ids: list[str] = []
    try:
        _wait_for_server(base_url, server)
        pipeline_ids.append(
            _run_pipeline(
                base_url,
                "cache-writer",
                f'from {{x: 10}}, {{x: 20}} | cache "{CACHE_ID}", mode="write"',
            )
        )
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "events.json"
            pipeline_ids.append(
                _run_pipeline(
                    base_url,
                    "cache-reader",
                    f'cache "{CACHE_ID}", mode="read" | sort x | '
                    f"to_file {json.dumps(str(output))} {{ write_tql }}",
                )
            )
            _ = sys.stdout.write(output.read_text())
            readwrite_output = Path(directory) / "readwrite.json"
            pipeline_ids.append(
                _run_pipeline(
                    base_url,
                    "cache-readwrite",
                    "from {x: 2}, {x: 1}, {x: 3} | "
                    'cache "readwrite-roundtrip" | sort x | '
                    f"to_file {json.dumps(str(readwrite_output))} "
                    "{ write_ndjson }",
                )
            )
            assert readwrite_output.read_text().splitlines() == [
                '{"x":1}',
                '{"x":2}',
                '{"x":3}',
            ]
            delayed_output = Path(directory) / "delayed.json"
            pipeline_ids.append(
                _run_pipeline(
                    base_url,
                    "delayed-cache-writer",
                    "from {x: 42} | delay 2s | "
                    'cache "delayed-first-write", mode="write", '
                    "write_timeout=1s",
                )
            )
            pipeline_ids.append(
                _run_pipeline(
                    base_url,
                    "delayed-cache-reader",
                    'cache "delayed-first-write", mode="read" | '
                    f"to_file {json.dumps(str(delayed_output))} "
                    "{ write_ndjson }",
                )
            )
            assert delayed_output.read_text() == '{"x":42}\n'
        waiting_writer = _start_pipeline(
            base_url,
            "waiting-cache-writer",
            'from {x: 1} | delay 1h | cache "waiting-reader", mode="write"',
        )
        pipeline_ids.append(waiting_writer)
        waiting_reader = _start_pipeline(
            base_url,
            "waiting-cache-reader",
            'cache "waiting-reader", mode="read" | discard',
        )
        pipeline_ids.append(waiting_reader)
        _stop_pipeline(base_url, waiting_reader)
        _stop_pipeline(base_url, waiting_writer)
    finally:
        for pipeline_id in pipeline_ids:
            try:
                _post(base_url, "/pipeline/delete", {"id": pipeline_id})
            except (urllib.error.URLError, TimeoutError):
                pass
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
