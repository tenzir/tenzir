# runner: python
"""Regression coverage for timed-out pipeline creation retaining an occupied ID."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

API_PREFIX = "/api/v0"
PIPELINE_ID = "repro/p1"


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def _decode_json_or_empty(payload: str) -> dict[str, Any]:
    if not payload.strip():
        return {}
    try:
        value = json.loads(payload)
    except json.JSONDecodeError:
        return {"_raw": payload}
    if isinstance(value, dict):
        return value
    return {"_value": value}


def _request_json(
    method: str,
    url: str,
    body: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    payload = None
    headers: dict[str, str] = {}
    if body is not None:
        payload = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(url, data=payload, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            text = response.read().decode("utf-8", errors="replace")
            return int(response.status), _decode_json_or_empty(text)
    except urllib.error.HTTPError as exc:
        text = exc.read().decode("utf-8", errors="replace")
        return int(exc.code), _decode_json_or_empty(text)


def _post_api(
    base_url: str,
    path: str,
    payload: dict[str, Any] | None = None,
) -> tuple[int, dict[str, Any]]:
    return _request_json("POST", f"{base_url}{API_PREFIX}{path}", payload or {})


def _resolve_node_binary() -> list[str]:
    env_val = os.environ.get("TENZIR_NODE_BINARY")
    if env_val:
        return shlex.split(env_val)
    which_result = shutil.which("tenzir-node")
    if which_result:
        return [which_result]
    raise RuntimeError(
        "tenzir-node executable not found (set TENZIR_NODE_BINARY or add to PATH)"
    )


def _resolve_client_binary(node_binary: list[str]) -> list[str]:
    # Always use tenzir-ctl (not tenzir) since we need the `web server` subcommand.
    # Don't use TENZIR_NODE_CLIENT_BINARY from the test framework — it points to
    # `tenzir` which is the pipeline runner, not the control client.
    executable = Path(node_binary[0])
    candidate = executable.with_name("tenzir-ctl")
    if candidate.is_file():
        return [str(candidate)]
    fallback = shutil.which("tenzir-ctl")
    if fallback:
        return [fallback]
    raise RuntimeError(
        "tenzir-ctl executable not found (add to PATH or place next to tenzir-node)"
    )


def _wait_for_process_socket(
    host: str, port: int, process: subprocess.Popen[str], timeout: float
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if process.poll() is not None:
            break
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(0.2)
            if sock.connect_ex((host, port)) == 0:
                return
        time.sleep(0.1)
    raise RuntimeError(f"process did not listen on {host}:{port} in time")


def _wait_for_server(base_url: str, process: subprocess.Popen[str]) -> None:
    deadline = time.monotonic() + 20
    last_status: int | None = None
    last_error: str | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            break
        try:
            status, _ = _request_json("POST", f"{base_url}{API_PREFIX}/ping", {})
            last_status = status
            if status == 200:
                return
        except urllib.error.URLError as exc:
            last_error = str(exc)
        time.sleep(0.2)
    details: list[str] = []
    if process.poll() is not None:
        details.append(f"process exited with code {process.returncode}")
        stderr_out = process.stderr.read() if process.stderr else ""
        if stderr_out:
            details.append(f"stderr: {stderr_out[-500:]}")
    if last_status is not None:
        details.append(f"last /ping status: {last_status}")
    if last_error:
        details.append(f"last /ping error: {last_error}")
    suffix = f"; {'; '.join(details)}" if details else ""
    raise RuntimeError(f"web server did not become ready{suffix}")


def _terminate(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _wait_for_pipeline(base_url: str, pipeline_id: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        status, body = _post_api(base_url, "/pipeline/list")
        if status == 200:
            pipelines = body.get("pipelines")
            if isinstance(pipelines, list):
                for pipeline in pipelines:
                    if isinstance(pipeline, dict) and pipeline.get("id") == pipeline_id:
                        return
        time.sleep(0.1)
    raise RuntimeError(f"pipeline {pipeline_id} did not appear in time")


@contextmanager
def _running_node() -> Iterator[tuple[subprocess.Popen[str], Path, str]]:
    node_binary = _resolve_node_binary()
    endpoint_port = _free_port()
    endpoint = f"127.0.0.1:{endpoint_port}/tcp"
    with tempfile.TemporaryDirectory(prefix="pm-timeout-repro-") as tmpdir:
        tmp_path = Path(tmpdir)
        log_path = tmp_path / "node.log"
        env = os.environ.copy()
        env.update(
            {
                "TENZIR_ENDPOINT": endpoint,
                "TENZIR_STATE_DIRECTORY": str(tmp_path / "state"),
                "TENZIR_CACHE_DIRECTORY": str(tmp_path / "cache"),
                "TENZIR_LOG_FILE": "/dev/stderr",
                "TENZIR_PIPELINE_MANAGER_TEST_REQUEST_TIMEOUT_MS": "200",
                "TENZIR_PIPELINE_MANAGER_TEST_CREATE_DELAY_MS": "500",
                "TENZIR_PIPELINE_MANAGER_TEST_DELAY_ID": PIPELINE_ID,
                "TENZIR_PIPELINE_MANAGER_TEST_FORCE_CREATE_ID": PIPELINE_ID,
            }
        )
        command = [*node_binary, f"--endpoint={endpoint}", "--console-verbosity=debug"]
        with open(log_path, "w+b") as log:
            process = subprocess.Popen(
                command,
                env=env,
                stdout=log,
                stderr=log,
                text=False,
            )
            try:
                _wait_for_process_socket("127.0.0.1", endpoint_port, process, 20.0)
                yield process, log_path, endpoint
            finally:
                _terminate(process)


@contextmanager
def _running_web_server(endpoint: str) -> Iterator[str]:
    node_binary = _resolve_node_binary()
    client_binary = _resolve_client_binary(node_binary)
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    command = [
        *client_binary,
        "--bare-mode",
        "--console-verbosity=warning",
        f"--endpoint={endpoint}",
        "web",
        "server",
        "--mode=dev",
        "--bind=127.0.0.1",
        f"--port={port}",
    ]
    # Use a clean env to avoid inheriting TENZIR_ENDPOINT etc. from the test
    # framework, which would cause tenzir-ctl to connect to the wrong node.
    env = {k: v for k, v in os.environ.items() if not k.startswith("TENZIR_")}
    env["PATH"] = os.environ.get("PATH", "")
    process = subprocess.Popen(
        command,
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_server(base_url, process)
        yield base_url
    finally:
        _terminate(process)


def _launch_payload() -> dict[str, Any]:
    return {
        "name": "timeout-duplicate-id",
        "definition": "from {x: 1}",
        "cache_id": "timeout-duplicate-id-cache",
    }


def main() -> None:
    with _running_node() as (_node, log_path, endpoint):
        with _running_web_server(endpoint) as base_url:
            first_status, first_body = _post_api(
                base_url, "/pipeline/launch", _launch_payload()
            )
            _wait_for_pipeline(base_url, PIPELINE_ID, timeout=5.0)
            second_status, second_body = _post_api(
                base_url, "/pipeline/launch", _launch_payload()
            )
            time.sleep(2.0)

        logs = log_path.read_text(encoding="utf-8", errors="replace")
        print(f"first response: {first_status} {first_body}")
        print(f"second response: {second_status} {second_body}")
        assert "pipeline manager request" in logs, logs
        assert "timed out" in logs, logs
        duplicate_error = "requested id is not available"
        assert duplicate_error in logs or duplicate_error in str(second_body), logs
        print("timed-out-create-retains-occupied-id: ok")


if __name__ == "__main__":
    main()
