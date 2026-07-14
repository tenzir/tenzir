# runner: python
"""Integration coverage for neo pipeline-manager launch/update behavior."""

from __future__ import annotations

import json
import os
import shlex
import shutil
import socket
import subprocess
import threading
import time
import urllib.error
import urllib.request
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any, NamedTuple

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
    for diagnostic in diagnostics:
        assert isinstance(diagnostic, dict), body
        rendered = diagnostic.get("rendered")
        assert isinstance(rendered, str) and "this_operator_does_not_exist" in rendered
        annotations = diagnostic.get("annotations")
        assert isinstance(annotations, list) and annotations, diagnostic
        for annotation in annotations:
            assert isinstance(annotation, dict), diagnostic
            source = annotation.get("source")
            assert isinstance(source, dict), annotation
            assert isinstance(source.get("begin"), int), annotation
            assert isinstance(source.get("end"), int), annotation
            assert "[" not in json.dumps(source), annotation
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


def _wait_for_pipeline_state(
    base_url: str,
    pipeline_id: str,
    expected_state: str,
    *,
    timeout: float = 10,
) -> dict[str, Any]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        # Query the pipeline directly instead of using /pipeline/list, as the
        # latter omits hidden pipelines.
        status, body = _post_api(base_url, "/pipeline/update", {"id": pipeline_id})
        assert status == 200, body
        pipeline = body.get("pipeline")
        assert isinstance(pipeline, dict), body
        if pipeline.get("state") == expected_state:
            return pipeline
        time.sleep(0.1)
    raise AssertionError(
        f"pipeline {pipeline_id} did not reach state {expected_state!r} within {timeout}s"
    )


def _wait_for_pipeline_stopped_or_deleted(
    base_url: str,
    pipeline_id: str,
    *,
    timeout: float = 10,
) -> None:
    # Hidden pipelines are automatically deleted once they stop, so a
    # successful stop manifests either as the `stopped` state or as the
    # pipeline no longer existing.
    deadline = time.time() + timeout
    while time.time() < deadline:
        status, body = _post_api(base_url, "/pipeline/update", {"id": pipeline_id})
        pipeline = body.get("pipeline")
        if status != 200 or not isinstance(pipeline, dict):
            # The pipeline no longer exists.
            return
        if pipeline.get("state") == "stopped":
            return
        time.sleep(0.1)
    raise AssertionError(f"pipeline {pipeline_id} did not stop within {timeout}s")


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


class _ServeResult(NamedTuple):
    status: int
    body: dict[str, Any]


@contextmanager
def _background_serve_request(
    base_url: str, serve_id: str
) -> Iterator[tuple[threading.Event, list[_ServeResult], list[BaseException]]]:
    """Poll `/serve` on a background thread.

    Yields `(done_event, results, errors)`. After the context exits, exactly
    one of `results` or `errors` will be populated (once `done_event` is set).
    """
    done = threading.Event()
    results: list[_ServeResult] = []
    errors: list[BaseException] = []

    def worker() -> None:
        try:
            # `/serve` long-polls, but it may hand back an early empty page
            # (with a continuation token and state `running`) before any events
            # or a terminal state. Keep polling until we either observe events
            # or the pipeline terminates (`next_continuation_token` is null),
            # so `done` is set only when the stream has genuinely ended. This
            # avoids a race where an early empty page looks like the serve
            # request returned before the pipeline was stopped.
            token: str | None = None
            while True:
                payload: dict[str, Any] = {
                    "serve_id": serve_id,
                    "timeout": "10s",
                    "min_events": 1,
                    "max_events": 1,
                    "schema": "never",
                }
                if token is not None:
                    payload["continuation_token"] = token
                status, response = _post_api(base_url, "/serve", payload)
                if status != 200 or not isinstance(response, dict):
                    results.append(_ServeResult(status, response))
                    return
                events = response.get("events") or []
                token = response.get("next_continuation_token")
                state = response.get("state")
                if events or token is None or state != "running":
                    results.append(_ServeResult(status, response))
                    return
                # Guard against a tight loop if the endpoint returns empty
                # pages without honoring the long-poll timeout.
                time.sleep(0.05)
        except BaseException as exc:  # pragma: no cover - surfaced below
            errors.append(exc)
        finally:
            done.set()

    thread = threading.Thread(target=worker, daemon=True)
    thread.start()
    try:
        yield done, results, errors
    finally:
        thread.join(timeout=0)


def _assert_serve_completed_after_stop(
    base_url: str,
    created_id: str,
    serve_id: str,
    done: threading.Event,
    results: list[_ServeResult],
    errors: list[BaseException],
) -> dict[str, Any]:
    """Stop `created_id` and assert the background `/serve` completes cleanly."""
    time.sleep(0.5)
    assert not done.is_set(), "serve returned before pipeline stop"
    status, body = _post_api(
        base_url,
        "/pipeline/update",
        {"id": created_id, "action": "stop"},
    )
    assert status == 200, body
    assert done.wait(5), "serve did not terminate promptly after stop"
    assert not errors, errors
    assert len(results) == 1, results
    result = results[0]
    assert result.status == 200, result
    assert isinstance(result.body, dict), result
    assert result.body.get("state") == "completed", result.body
    _wait_for_pipeline_stopped_or_deleted(base_url, created_id)
    return result.body


def _check_every_stop_terminates_serve(base_url: str) -> None:
    created_id = ""
    try:
        status, body = _post_api(
            base_url,
            "/pipeline/launch",
            {
                "definition": (
                    "//neo\n"
                    "every 500ms {\n"
                    "  pipeline::list\n"
                    "  where not hidden\n"
                    "  sort id\n"
                    "  summarize pipelines = this.collect()\n"
                    # Discard all events so that the long-polling /serve
                    # request below stays blocked until the pipeline stops.
                    "  where false\n"
                    "}\n"
                ),
                "name": "neo-every-stop",
                "hidden": True,
                "serve_id": "neo-every-stop",
                "ttl": "60s",
                "autostart": {"created": True},
            },
        )
        assert status == 200, body
        created_id = str(body.get("id", ""))
        assert created_id, body
        _wait_for_pipeline_state(base_url, created_id, "running")
        with _background_serve_request(base_url, "neo-every-stop") as (
            done,
            results,
            errors,
        ):
            _assert_serve_completed_after_stop(
                base_url, created_id, "neo-every-stop", done, results, errors
            )
        print("every-stop-terminates-serve: ok")
    finally:
        if created_id:
            _delete_pipeline(base_url, created_id)


def _check_hidden_diagnostics_stop_terminates_serve(base_url: str) -> None:
    created_id = ""
    try:
        status, body = _post_api(
            base_url,
            "/pipeline/launch",
            {
                "definition": (
                    "//neo\n"
                    'diagnostics live=true, retro=true | where pipeline_id == "'
                    '00000000-0000-0000-0000-000000000000"'
                ),
                "name": "neo-hidden-diagnostics-stop",
                "hidden": True,
                "serve_id": "neo-hidden-diagnostics-stop",
                "ttl": "60s",
                "autostart": {"created": True},
            },
        )
        assert status == 200, body
        created_id = str(body.get("id", ""))
        assert created_id, body
        _wait_for_pipeline_state(base_url, created_id, "running")
        with _background_serve_request(base_url, "neo-hidden-diagnostics-stop") as (
            done,
            results,
            errors,
        ):
            response = _assert_serve_completed_after_stop(
                base_url,
                created_id,
                "neo-hidden-diagnostics-stop",
                done,
                results,
                errors,
            )
        events = response.get("events")
        assert isinstance(events, list) and not events, response
        print("hidden-diagnostics-stop-terminates-serve: ok")
    finally:
        if created_id:
            _delete_pipeline(base_url, created_id)


def _check_full_buffer_stop_terminates_serve(base_url: str) -> None:
    # Regression test: a pipeline ending in `serve` whose buffer is full (the
    # operator is throttled inside its `put`) and that has no client draining
    # it must still stop promptly. Previously the operator blocked its own main
    # loop awaiting the throttling `put`, so the graceful-stop signal was never
    # delivered, the serve-manager never dropped its buffer, and the pipeline
    # hung until the shutdown watchdog force-killed it.
    created_id = ""
    try:
        status, body = _post_api(
            base_url,
            "/pipeline/launch",
            {
                # Produce far more than the default serve buffer (1024 events)
                # so the operator gets throttled with no client draining it.
                "definition": ("//neo\nfrom {x: 1}\nrepeat 100000\n"),
                "name": "neo-full-buffer-stop",
                "hidden": True,
                "serve_id": "neo-full-buffer-stop",
                "ttl": "60s",
                "autostart": {"created": True},
            },
        )
        assert status == 200, body
        created_id = str(body.get("id", ""))
        assert created_id, body
        _wait_for_pipeline_state(base_url, created_id, "running")
        # Give the pipeline a moment to fill the serve buffer and throttle,
        # without ever polling `/serve`.
        time.sleep(1.0)
        status, body = _post_api(
            base_url,
            "/pipeline/update",
            {"id": created_id, "action": "stop"},
        )
        assert status == 200, body
        _wait_for_pipeline_stopped_or_deleted(base_url, created_id, timeout=10)
        print("full-buffer-stop-terminates-serve: ok")
    finally:
        if created_id:
            _delete_pipeline(base_url, created_id)


def _check_force_stop_terminates_pipeline(base_url: str) -> None:
    # A `force-stop` skips the graceful drain and terminates a running pipeline
    # immediately. Here we launch a continuously-running pipeline and assert it
    # stops after a force-stop request.
    created_id = ""
    try:
        status, body = _post_api(
            base_url,
            "/pipeline/launch",
            {
                "definition": ("//neo\nfrom {x: 1}\nrepeat\n"),
                "name": "neo-force-stop",
                "hidden": True,
                "serve_id": "neo-force-stop",
                "ttl": "60s",
                "autostart": {"created": True},
            },
        )
        assert status == 200, body
        created_id = str(body.get("id", ""))
        assert created_id, body
        _wait_for_pipeline_state(base_url, created_id, "running")
        status, body = _post_api(
            base_url,
            "/pipeline/update",
            {"id": created_id, "action": "force-stop"},
        )
        assert status == 200, body
        _wait_for_pipeline_stopped_or_deleted(base_url, created_id, timeout=10)
        print("force-stop-terminates-pipeline: ok")
    finally:
        if created_id:
            _delete_pipeline(base_url, created_id)


def _check_invalid_action_rejected(base_url: str) -> None:
    # Unknown actions must still be rejected so that clients get clear feedback.
    created_id = ""
    try:
        status, body = _post_api(
            base_url,
            "/pipeline/create",
            {
                "definition": "from {x: 1} | discard",
                "name": "invalid-action",
            },
        )
        assert status == 200, body
        created_id = str(body.get("id", ""))
        assert created_id, body
        status, body = _post_api(
            base_url,
            "/pipeline/update",
            {"id": created_id, "action": "stahp"},
        )
        # The web layer surfaces the pipeline manager's rejection as an error
        # body rather than a distinct HTTP status.
        assert body.get("error"), {"status": status, "body": body}
        print("invalid-action-rejected: ok")
    finally:
        if created_id:
            _delete_pipeline(base_url, created_id)


def main() -> None:
    with _running_web_server() as base_url:
        _check_launch_compile_diagnostics(base_url)
        _check_sink_id_escaping(base_url)
        _check_update_parse_compatibility(base_url)
        _check_every_stop_terminates_serve(base_url)
        _check_hidden_diagnostics_stop_terminates_serve(base_url)
        _check_full_buffer_stop_terminates_serve(base_url)
        _check_force_stop_terminates_pipeline(base_url)
        _check_invalid_action_rejected(base_url)


if __name__ == "__main__":
    main()
