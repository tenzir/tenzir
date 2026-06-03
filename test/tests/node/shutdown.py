# runner: python
# timeout: 120

"""Verify no data is lost when a node receives SIGTERM.

Phase 1: managed import pipeline -> SIGTERM -> restart -> verify export.
Phase 2: managed pub/sub+import -> SIGTERM -> restart -> verify export.
Phase 3: two sequential publishers + subscriber -> SIGTERM -> restart
         -> verify events from both publishers survived.
Phase 4: publisher batches via sort, then publishes -> SIGTERM -> restart
         -> verify all events in correct order survived.
Phase 5: source `every` with slow subpipeline -> SIGTERM -> restart
         -> verify shutdown does not spawn an extra subpipeline.
Phase 6: streaming input into `every` with slow subpipeline -> SIGTERM
         -> restart -> verify shutdown still spawns follow-up subpipelines.

Adapted from the pubsub/drain test pattern.  Uses managed pipelines
(created via the REST API) so that all work runs inside the node and
the SIGTERM shutdown path drains them properly.
"""

from __future__ import annotations

import json
import os
import shlex
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

# ---------------------------------------------------------------------------
# REST API helpers (the only part not covered by the node fixture)
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
        time.sleep(0.2)
    raise RuntimeError("REST API did not come up")


# ---------------------------------------------------------------------------
# Web server + managed pipeline helpers
# ---------------------------------------------------------------------------


def start_web_server(env: dict[str, str]) -> tuple[subprocess.Popen[str], str]:
    """Start tenzir-ctl web server and return (process, api_base_url)."""
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
    autostart: bool = True,
) -> str:
    """Create a managed pipeline via /pipeline/create.  Returns the id."""
    body: dict = {
        "definition": f"//neo\n{definition}",
        "autostart": {"created": autostart},
    }
    if name is not None:
        body["name"] = name
    status, resp = _post(api_url, "/pipeline/create", body)
    assert status == 200, f"/pipeline/create failed ({status}): {resp}"
    pid = resp.get("id", "")
    assert pid, f"no pipeline id in response: {resp}"
    return pid


# ---------------------------------------------------------------------------
# Query helpers
# ---------------------------------------------------------------------------


def export_ndjson(tenzir: Executor, pipeline: str) -> list[dict]:
    r = tenzir.run(pipeline)
    assert r.returncode == 0, f"export failed: {r.stderr.decode()}"
    stdout = r.stdout.decode().strip()
    return [json.loads(line) for line in stdout.splitlines() if line.strip()]


def export_summary(tenzir: Executor, schema: str, field: str) -> dict:
    r = tenzir.run(
        f"export\n"
        f'where @name == "{schema}"\n'
        f"summarize count=count(), lo=min({field}), hi=max({field})\n"
        f"write_ndjson\n"
    )
    assert r.returncode == 0, f"export failed: {r.stderr.decode()}"
    return json.loads(r.stdout.decode().strip())


def wait_for_count(
    tenzir: Executor, schema: str, expected: int, timeout: int = 30
) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        r = tenzir.run(
            f"export\n"
            f'where @name == "{schema}"\n'
            f"summarize count=count()\n"
            f"write_ndjson\n"
        )
        if r.returncode == 0:
            try:
                if json.loads(r.stdout.decode().strip()).get("count") == expected:
                    return
            except (json.JSONDecodeError, ValueError):
                pass
        time.sleep(0.5)
    raise AssertionError(f"timed out waiting for {expected} events in {schema}")


# ---------------------------------------------------------------------------
# Node lifecycle helpers
# ---------------------------------------------------------------------------


class NodeController:
    def __init__(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="shutdown-node-")
        self.root = Path(self.temp_dir.name)
        self.state_dir = self.root / "state"
        self.cache_dir = self.root / "cache"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.proc: subprocess.Popen[str] | None = None
        self.env: dict[str, str] = {}

    def start(self) -> dict[str, str]:
        pid_lock = self.state_dir / "pid.lock"
        pid_lock.unlink(missing_ok=True)
        node_binary = shlex.split(os.environ["TENZIR_NODE_BINARY"])
        env = os.environ.copy()
        cmd = [
            *node_binary,
            "--bare-mode",
            "--console-verbosity=warning",
            f"--state-directory={self.state_dir}",
            f"--cache-directory={self.cache_dir}",
            "--endpoint=localhost:0",
            "--print-endpoint",
            "--no-autostart",  # When restarting, don't resume pipelines
        ]
        if package_dirs := env.get("TENZIR_PACKAGE_DIRS"):
            cmd.append(f"--package-dirs={package_dirs}")
        self.proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
            env=env,
            cwd=Path(__file__).parent,
            start_new_session=True,
        )
        assert self.proc.stdout is not None
        endpoint = self.proc.stdout.readline().strip()
        if not endpoint:
            returncode = self.proc.poll()
            stderr = ""
            if self.proc.stderr is not None:
                stderr = self.proc.stderr.read()
            detail = []
            if returncode is not None:
                detail.append(f"exit code {returncode}")
            if stderr:
                detail.append(f"stderr:\n{stderr}")
            raise RuntimeError(
                "failed to obtain endpoint from tenzir-node "
                f"({'; '.join(detail) if detail else 'no additional diagnostics available'})"
            )
        self.env = {
            "TENZIR_NODE_CLIENT_ENDPOINT": endpoint,
            "TENZIR_NODE_CLIENT_BINARY": os.environ["TENZIR_BINARY"],
            "TENZIR_NODE_CLIENT_TIMEOUT": os.environ.get("TENZIR_TIMEOUT", "120"),
            "TENZIR_NODE_STATE_DIRECTORY": str(self.state_dir),
            "TENZIR_NODE_CACHE_DIRECTORY": str(self.cache_dir),
        }
        return self.env

    def stop(self) -> None:
        if self.proc is not None:
            _terminate(self.proc)
            if self.proc.stdout is not None:
                self.proc.stdout.close()
            if self.proc.stderr is not None:
                self.proc.stderr.close()
            self.proc = None
        self.env = {}

    def cleanup(self) -> None:
        self.stop()
        self.temp_dir.cleanup()


# ---------------------------------------------------------------------------
# Test
# ---------------------------------------------------------------------------

node = NodeController()
web_proc = None


def stop_and_restart() -> Executor:
    global web_proc
    if web_proc:
        _terminate(web_proc)
        web_proc = None
    node.stop()
    node.start()
    return Executor.from_env(node.env)


try:
    # --- Phase 1: managed import survives SIGTERM -------------------------

    node.start()
    web_proc, api = start_web_server(node.env)
    tenzir = Executor.from_env(node.env)

    create_pipeline(
        api,
        'from {}\nrepeat 100\nenumerate index\n@name = "shutdown-phase1"\nimport',
        name="phase1-import",
    )
    wait_for_count(tenzir, "shutdown-phase1", 100)
    tenzir = stop_and_restart()
    data = export_summary(tenzir, "shutdown-phase1", "index")
    assert data == {"count": 100, "lo": 0, "hi": 99}, f"phase1: {data}"
    print("phase1-import-survives-sigterm: ok")

    # --- Phase 2: managed pubsub drain survives SIGTERM -------------------

    web_proc, api = start_web_server(node.env)
    tenzir = Executor.from_env(node.env)

    create_pipeline(
        api,
        'subscribe "shutdown-drain"\n@name = "shutdown-phase2"\nimport',
        name="phase2-subscriber",
    )
    time.sleep(1)  # Let subscriber attach (same pattern as pubsub/drain).
    create_pipeline(
        api,
        'from {}\nrepeat 100\nenumerate x\npublish "shutdown-drain"',
        name="phase2-publisher",
    )
    wait_for_count(tenzir, "shutdown-phase2", 100)
    tenzir = stop_and_restart()
    data = export_summary(tenzir, "shutdown-phase2", "x")
    assert data == {"count": 100, "lo": 0, "hi": 99}, f"phase2: {data}"
    print("phase2-pubsub-drain-survives-sigterm: ok")

    # --- Phase 3: sequential publishers survive SIGTERM -------------------
    # Adapted from pubsub/sequential: two publishers publish to the same
    # topic one after another; the subscriber must see events from both.

    web_proc, api = start_web_server(node.env)
    tenzir = Executor.from_env(node.env)

    create_pipeline(
        api,
        'subscribe "shutdown-sequential"\n@name = "shutdown-phase3"\nimport',
        name="phase3-subscriber",
    )
    time.sleep(1)  # Let subscriber attach.
    create_pipeline(
        api,
        'every 10ms {\n  from {id: "pub-a"}\n}\nhead 5\npublish "shutdown-sequential"',
        name="phase3-publisher-a",
    )
    time.sleep(1)  # Sequential: publisher B starts after A.
    create_pipeline(
        api,
        'from {id: "pub-b"}\npublish "shutdown-sequential"',
        name="phase3-publisher-b",
    )
    wait_for_count(tenzir, "shutdown-phase3", 6)
    tenzir = stop_and_restart()
    rows = export_ndjson(
        tenzir,
        "export\n"
        'where @name == "shutdown-phase3"\n'
        "sort id\n"
        "deduplicate id\n"
        "write_ndjson\n",
    )
    assert rows == [{"id": "pub-a"}, {"id": "pub-b"}], f"phase3: {rows}"
    print("phase3-sequential-publishers-survive-sigterm: ok")

    # --- Phase 4: batched publish after sort survives SIGTERM -------------
    # Adapted from pubsub/finalize_drain: the publisher accumulates events
    # through a blocking operator (sort), then publishes the sorted batch.

    web_proc, api = start_web_server(node.env)
    tenzir = Executor.from_env(node.env)

    create_pipeline(
        api,
        'subscribe "shutdown-finalize"\n@name = "shutdown-phase4"\nimport',
        name="phase4-subscriber",
    )
    time.sleep(1)  # Let subscriber attach.
    create_pipeline(
        api,
        "every 50ms {\n"
        "  from {x: 3}, {x: 1}, {x: 2}\n"
        "}\n"
        "head 9\n"
        "sort x\n"
        'publish "shutdown-finalize"',
        name="phase4-publisher",
    )
    wait_for_count(tenzir, "shutdown-phase4", 9)
    tenzir = stop_and_restart()
    rows = export_ndjson(
        tenzir,
        'export\nwhere @name == "shutdown-phase4"\nsort x\nwrite_ndjson\n',
    )
    assert rows == [{"x": 1}] * 3 + [{"x": 2}] * 3 + [{"x": 3}] * 3, f"phase4: {rows}"
    print("phase4-batched-publish-survives-sigterm: ok")

    # --- Phase 5: source every stops respawning during shutdown ----------
    # The first subpipeline takes longer than the interval. During graceful
    # shutdown, `every` must drain the in-flight subpipeline, but must not
    # start another one after stop was requested.

    web_proc, api = start_web_server(node.env)
    tenzir = Executor.from_env(node.env)

    create_pipeline(
        api,
        "every 200ms {\n"
        "  from {x: 1, _ts: 1970-01-01T00:00:02}\n"
        "  delay _ts, start=1970-01-01T00:00:00, speed=1.0\n"
        "  drop _ts\n"
        '  @name = "shutdown-phase5"\n'
        "  import\n"
        "}",
        name="phase5-every-source",
    )
    time.sleep(0.5)  # Let the timer elapse while the first subpipeline runs.
    tenzir = stop_and_restart()
    data = export_summary(tenzir, "shutdown-phase5", "x")
    assert data == {"count": 1, "lo": 1, "hi": 1}, f"phase5: {data}"
    print("phase5-every-source-stops: ok")

    # --- Phase 6: sink every keeps respawning during shutdown ------------
    # Unlike the source case above, `every` with table-slice input must keep
    # spawning follow-up subpipelines while draining buffered input.

    web_proc, api = start_web_server(node.env)
    tenzir = Executor.from_env(node.env)

    create_pipeline(
        api,
        """
        from {}
        repeat 100
        enumerate seq
        d = 1970-01-01T00:00 + 23ms * seq
        delay d
        drop d
        every 50ms {
          @name = "shutdown-phase6"
          import
        }
        """,
        name="phase6-every-transform",
    )
    time.sleep(1)  # Let it run (but not fully)
    tenzir = stop_and_restart()
    data = export_summary(tenzir, "shutdown-phase6", "seq")
    assert data == {"count": 100, "lo": 0, "hi": 99}, f"phase6: {data}"
    print("phase6-every-transform-drains: ok")

finally:
    if web_proc:
        _terminate(web_proc)
    node.cleanup()
