# runner: python
"""Integration coverage for neo pipeline-manager launch/update behavior."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import socket
import subprocess
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any

API_PREFIX = "/api/v0"


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


def _delete_pipeline(base_url: str, pipeline_id: str) -> None:
    status, _ = _post_api(base_url, "/pipeline/delete", {"id": pipeline_id})
    if status not in (200, 500):
        raise AssertionError(f"unexpected delete status {status} for {pipeline_id}")


def _wait_for_server(base_url: str, process: subprocess.Popen[str]) -> None:
    deadline = time.time() + 20
    last_status: int | None = None
    last_error: str | None = None
    while time.time() < deadline:
        if process.poll() is not None:
            break
        try:
            status, _ = _request_json("POST", f"{base_url}{API_PREFIX}/ping", {})
            last_status = status
            if status == 200:
                return
        except urllib.error.URLError as exc:
            # The socket may not be open yet while the web server starts.
            last_error = str(exc)
        time.sleep(0.2)
    stderr = ""
    if process.poll() is not None and process.stderr:
        try:
            stderr = process.stderr.read()
        except Exception:
            stderr = ""
    details: list[str] = []
    if last_status is not None:
        details.append(f"last /ping status: {last_status}")
    if last_error:
        details.append(f"last /ping error: {last_error}")
    if stderr.strip():
        details.append(f"stderr: {stderr.strip()}")
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


def _resolve_client_binary() -> list[str]:
    binary = shlex.split(os.environ["TENZIR_NODE_CLIENT_BINARY"])
    executable = Path(binary[0])
    candidate = executable.with_name("tenzir-ctl")
    if candidate.is_file():
        return [str(candidate)]
    fallback = shutil.which("tenzir-ctl")
    if fallback:
        return [fallback]
    # If no tenzir-ctl exists, keep the fixture-provided client as a fallback.
    return binary


@contextmanager
def _running_web_server() -> Iterator[str]:
    binary = _resolve_client_binary()
    endpoint = os.environ["TENZIR_NODE_CLIENT_ENDPOINT"]
    port = _free_port()
    base_url = f"http://127.0.0.1:{port}"
    command = [
        *binary,
        "--bare-mode",
        "--console-verbosity=warning",
        f"--endpoint={endpoint}",
        "web",
        "server",
        "--mode=dev",
        "--bind=127.0.0.1",
        f"--port={port}",
    ]
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        _wait_for_server(base_url, process)
        yield base_url
    finally:
        _terminate(process)


def _check_launch_compile_diagnostics(base_url: str) -> None:
    status, body = _post_api(
        base_url,
        "/pipeline/launch",
        {
            "definition": "//neo\nthis_operator_does_not_exist",
            "name": "neo-launch-compile-diags",
        },
    )
    assert status < 500, {"status": status, "body": body}
    diagnostics = body.get("diagnostics")
    assert isinstance(diagnostics, list) and diagnostics, body
    error_text = str(body.get("error", ""))
    assert "pipeline has no sink" not in error_text, body
    messages = [
        str(diagnostic.get("message", ""))
        for diagnostic in diagnostics
        if isinstance(diagnostic, dict)
    ]
    assert all("pipeline has no sink" not in message for message in messages), messages
    print("neo-launch-compile-diagnostics: ok")


def _check_sink_id_escaping(base_url: str) -> None:
    cache_id = 'cache id: "quoted" \\ backslash \n newline'
    serve_id = 'serve id: "quoted" \\ backslash \n newline'
    created_id = ""
    try:
        status, body = _post_api(
            base_url,
            "/pipeline/launch",
            {
                "definition": "//neo\nfrom {x: 1}",
                "name": "neo-launch-escaped-sink-ids",
                "cache_id": cache_id,
                "serve_id": serve_id,
            },
        )
        assert status == 200, body
        created_id = str(body.get("id", ""))
        assert created_id, body

        status, listed = _post_api(base_url, "/pipeline/list")
        assert status == 200, listed
        pipelines = listed.get("pipelines")
        assert isinstance(pipelines, list), listed
        match = None
        for pipeline in pipelines:
            if isinstance(pipeline, dict) and pipeline.get("id") == created_id:
                match = pipeline
                break
        assert match is not None, listed
        definition = str(match.get("definition", ""))
        assert "cache " in definition and "serve " in definition, definition
        print("neo-launch-sink-id-escaping: ok")
    finally:
        if created_id:
            _delete_pipeline(base_url, created_id)


def _check_update_parse_compatibility(base_url: str) -> None:
    legacy_definition = "from {x: 1} | discard"
    legacy_updated_definition = "from {x: 2} | discard"
    neo_updated_definition = "//neo\nfrom {x: 3} | discard"
    created_id = ""
    try:
        status, body = _post_api(
            base_url,
            "/pipeline/create",
            {
                "definition": legacy_definition,
                "name": "update-parse-compatibility",
            },
        )
        assert status == 200, body
        created_id = str(body.get("id", ""))
        assert created_id, body

        status, body = _post_api(
            base_url,
            "/pipeline/update",
            {"id": created_id, "definition": legacy_updated_definition},
        )
        assert status == 200, body
        assert (
            body.get("pipeline", {}).get("definition") == legacy_updated_definition
        ), body

        status, body = _post_api(
            base_url,
            "/pipeline/update",
            {"id": created_id, "definition": neo_updated_definition},
        )
        assert status == 200, body
        assert body.get("pipeline", {}).get("definition") == neo_updated_definition, (
            body
        )
        print("update-accepts-legacy-and-neo-definitions: ok")
    finally:
        if created_id:
            _delete_pipeline(base_url, created_id)


def main() -> None:
    with _running_web_server() as base_url:
        _check_launch_compile_diagnostics(base_url)
        _check_sink_id_escaping(base_url)
        _check_update_parse_compatibility(base_url)


if __name__ == "__main__":
    main()
