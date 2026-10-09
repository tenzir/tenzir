"""Platform fixture that receives standalone pipeline telemetry.

The fixture serves platform discovery and the buffered telemetry endpoint, and
answers the first telemetry request with 503 to exercise retries. It exports:

- **PLATFORM_TELEMETRY_URL** - Platform URL to pass to ``--platform``.
- **PLATFORM_TELEMETRY_KEY_FILE** - Key file to pass to ``--key-file``.
- **PLATFORM_TELEMETRY_DIR** - Directory in which the fixture creates
  ``running-<pipeline-id>`` once a run reports the ``running`` state.

The ``assert_test`` hook checks the received batches after the test ran.
"""

from __future__ import annotations

import json
import shutil
import tempfile
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any

from tenzir_test import FixtureHandle, fixture

_HOST = "127.0.0.1"
_KEY = "fixture-key"
_TELEMETRY_PATH = "/api/deployments/test-deployment/telemetry"


def _check(requests: list[tuple[str, dict[str, Any]]], errors: list[str]) -> None:
    assert not errors, errors
    assert len(requests) >= 2, requests
    unique: dict[tuple[str, int], tuple[str, dict[str, Any]]] = {}
    for body, batch in requests:
        identity = (batch["boot"], batch["seq"])
        if identity in unique:
            assert unique[identity][0] == body, "retry mutated its batch"
        unique[identity] = (body, batch)
    assert len(unique) < len(requests), "503 did not trigger a retry"
    batches = [batch for _, batch in requests]
    short = [batch for _, batch in unique.values()]
    sources = [
        flow
        for batch in short
        for sample in batch["pipelines"]
        if sample["pipelineId"] == "short"
        for flow in sample["sources"]
    ]
    assert sum(flow["events"] for flow in sources) == 2, sources
    assert sum(flow["bytes"] for flow in sources) == 6, sources
    assert all(flow["connector"] == "from_stdin" for flow in sources), sources
    diagnostics = [d for batch in batches for d in batch["diagnostics"]]
    compile_errors = [d for d in diagnostics if d["pipelineId"] == "compile-failure"]
    assert compile_errors and compile_errors[0]["severity"] == "error"
    assert compile_errors[0]["rendered"] and compile_errors[0]["annotations"]
    runtime_errors = [d for d in diagnostics if d["pipelineId"] == "runtime-failure"]
    assert any(d["severity"] == "error" for d in runtime_errors), runtime_errors
    abort_errors = [d for d in diagnostics if d["pipelineId"] == "aborted"]
    assert any(
        d["severity"] == "error"
        and "pipeline was aborted before completion" in d["message"]
        for d in abort_errors
    ), abort_errors
    warnings = [d for d in diagnostics if d["pipelineId"] == "warnings"]
    assert sum(d["occurrences"] for d in warnings) == 2, warnings
    states = {(run["pipelineId"], run["state"]) for b in batches for run in b["runs"]}
    for expected in [
        ("short", "finished"),
        ("compile-failure", "failed"),
        ("runtime-failure", "failed"),
        ("stopped", "stopped"),
        ("aborted", "failed"),
    ]:
        assert expected in states, (expected, states)
    assert all(not batch["operators"] for batch in batches)
    assert all(run["run"] == 7 for batch in batches for run in batch["runs"])


@fixture(name="platform_telemetry")
def platform_telemetry() -> FixtureHandle:
    directory = Path(tempfile.mkdtemp(prefix="platform-telemetry-"))
    key_file = directory / "key"
    key_file.write_text(f"{_KEY}\n")
    # Wire batches are heterogeneous nested JSON; the checks validate the
    # fields this fixture relies on instead of duplicating the Platform schema.
    requests: list[tuple[str, dict[str, Any]]] = []
    errors: list[str] = []
    lock = threading.Lock()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: object) -> None:
            pass

        def do_GET(self) -> None:
            if self.path == "/failed.json":
                self.send_response(400)
                self.end_headers()
                return
            if self.path != "/.well-known/tenzir-platform":
                self.send_response(404)
                self.end_headers()
                return
            body = json.dumps({"apiUrl": f"http://{_HOST}:{server.server_port}/api"})
            self.send_response(200)
            self.end_headers()
            self.wfile.write(body.encode())

        def do_POST(self) -> None:
            body = self.rfile.read(int(self.headers["content-length"])).decode()
            problems = []
            if self.path != _TELEMETRY_PATH:
                problems.append(f"unexpected path {self.path}")
            if self.headers["x-tenzir-deployment-key"] != _KEY:
                problems.append("missing deployment key")
            if self.headers["content-type"] != "application/json":
                problems.append("unexpected content type")
            with lock:
                errors.extend(problems)
                retry = not requests
                if not problems:
                    batch = json.loads(body)
                    requests.append((body, batch))
                    for run in batch["runs"]:
                        if run["state"] == "running":
                            (directory / f"running-{run['pipelineId']}").touch()
            self.send_response(503 if retry else 204)
            self.end_headers()

    server = ThreadingHTTPServer((_HOST, 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def _assert_test(**_: Any) -> None:
        with lock:
            _check(list(requests), list(errors))

    def _teardown() -> None:
        server.shutdown()
        server.server_close()
        thread.join()
        shutil.rmtree(directory, ignore_errors=True)

    return FixtureHandle(
        env={
            "PLATFORM_TELEMETRY_URL": f"http://{_HOST}:{server.server_port}",
            "PLATFORM_TELEMETRY_KEY_FILE": str(key_file),
            "PLATFORM_TELEMETRY_DIR": str(directory),
        },
        teardown=_teardown,
        hooks={"assert_test": _assert_test},
    )
