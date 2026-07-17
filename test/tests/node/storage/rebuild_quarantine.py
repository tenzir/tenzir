# runner: python
# timeout: 120

"""Verify that rebuild quarantines partitions with corrupt store files.

Reproduces the incident scenario: a partition's feather store file on disk
is overwritten with garbage. Previously, every rebuild that touched the
partition failed, the rebuilder retried it forever, and stale continuations
of the failed run crashed the node (SIGSEGV / `num_total == 0` assertion).

Phase 1: seed two schemas, restart, and corrupt one schema's store file.
Phase 2: `rebuild --all` succeeds, quarantines exactly the corrupt
         partition, and rebuilds the healthy one.
Phase 3: the healthy data is still exportable and the node is alive.
Phase 4: a second `rebuild --all` does not retry the quarantined partition.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import tempfile
import time
from pathlib import Path


def _terminate(proc: subprocess.Popen[str], timeout: int = 20) -> None:
    if proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


class NodeController:
    def __init__(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="rebuild-quarantine-")
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
        # Keep rebuilds under explicit test control.
        env["TENZIR_AUTOMATIC_REBUILD"] = "0"
        cmd = [
            *node_binary,
            "--bare-mode",
            "--console-verbosity=warning",
            f"--state-directory={self.state_dir}",
            f"--cache-directory={self.cache_dir}",
            "--endpoint=localhost:0",
            "--print-endpoint",
            "--no-autostart",
        ]
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
            stderr = self.proc.stderr.read() if self.proc.stderr else ""
            raise RuntimeError(
                f"failed to obtain endpoint from tenzir-node "
                f"(exit code {returncode}; stderr:\n{stderr})"
            )
        self.env = {
            "TENZIR_NODE_CLIENT_ENDPOINT": endpoint,
            "TENZIR_NODE_CLIENT_BINARY": os.environ["TENZIR_BINARY"],
            "TENZIR_NODE_CLIENT_TIMEOUT": os.environ.get("TENZIR_TIMEOUT", "120"),
            "TENZIR_NODE_STATE_DIRECTORY": str(self.state_dir),
            "TENZIR_NODE_CACHE_DIRECTORY": str(self.cache_dir),
        }
        return self.env

    def alive(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

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


def run_ctl(node: NodeController, *args: str) -> subprocess.CompletedProcess[str]:
    binary = shlex.split(node.env["TENZIR_NODE_CLIENT_BINARY"])
    tenzir_ctl = str(Path(binary[0]).with_name("tenzir-ctl"))
    endpoint = node.env["TENZIR_NODE_CLIENT_ENDPOINT"]
    return subprocess.run(
        [
            tenzir_ctl,
            "--bare-mode",
            "--console-verbosity=warning",
            f"--endpoint={endpoint}",
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )


def export_count(tenzir: Executor, schema: str) -> int:
    r = tenzir.run(
        f'export\nwhere @name == "{schema}"\nsummarize count=count()\nwrite_ndjson\n'
    )
    assert r.returncode == 0, f"export failed: {r.stderr.decode()}"
    import json

    return json.loads(r.stdout.decode().strip()).get("count", 0)


def partition_uuids(tenzir: Executor, schema: str) -> list[str]:
    r = tenzir.run(
        f'partitions\nwhere schema == "{schema}"\nselect uuid\nwrite_ndjson\n'
    )
    assert r.returncode == 0, f"partitions failed: {r.stderr.decode()}"
    import json

    return [
        json.loads(line)["uuid"]
        for line in r.stdout.decode().splitlines()
        if line.strip()
    ]


node = NodeController()

try:
    # --- Phase 1: seed data and corrupt one partition's store file --------

    node.start()
    tenzir = Executor.from_env(node.env)
    for name in ("bad", "good"):
        r = tenzir.run(
            f"from {{}}\nrepeat 100\nenumerate index\n"
            f'@name = "quarantine.{name}"\nimport\n'
        )
        assert r.returncode == 0, f"import failed: {r.stderr.decode()}"
    # Restart so the active partitions get persisted and show up in the
    # catalog.
    node.stop()
    node.start()
    tenzir = Executor.from_env(node.env)
    bad_uuids = partition_uuids(tenzir, "quarantine.bad")
    assert len(bad_uuids) == 1, f"expected one bad partition: {bad_uuids}"
    good_uuids = partition_uuids(tenzir, "quarantine.good")
    assert len(good_uuids) == 1, f"expected one good partition: {good_uuids}"
    node.stop()
    store_file = node.state_dir / "archive" / f"{bad_uuids[0]}.feather"
    assert store_file.exists(), f"store file not found: {store_file}"
    store_file.write_bytes(b"this is not an apache feather or arrow ipc file")
    print("phase1-corrupt-store-file: ok")

    # --- Phase 2: rebuild succeeds and quarantines the corrupt partition --

    node.start()
    tenzir = Executor.from_env(node.env)
    r = run_ctl(node, "rebuild", "--all")
    assert r.returncode == 0, f"rebuild failed: {r.stderr}"
    assert node.alive(), "node died during rebuild"
    r = run_ctl(node, "rebuild", "show")
    assert r.returncode == 0, f"rebuild show failed: {r.stderr}"
    assert "quarantined-size: 1" in r.stdout, f"unexpected status:\n{r.stdout}"
    assert bad_uuids[0] in r.stdout, f"quarantined uuid missing:\n{r.stdout}"
    print("phase2-rebuild-quarantines-corrupt-partition: ok")

    # --- Phase 3: healthy data is still available -------------------------

    assert export_count(tenzir, "quarantine.good") == 100
    print("phase3-healthy-data-survives: ok")

    # --- Phase 4: a second rebuild skips the quarantined partition --------

    r = run_ctl(node, "rebuild", "--all")
    assert r.returncode == 0, f"second rebuild failed: {r.stderr}"
    assert node.alive(), "node died during second rebuild"
    r = run_ctl(node, "rebuild", "show")
    assert r.returncode == 0, f"rebuild show failed: {r.stderr}"
    assert "quarantined-size: 1" in r.stdout, f"unexpected status:\n{r.stdout}"
    assert export_count(tenzir, "quarantine.good") == 100
    print("phase4-second-rebuild-skips-quarantined: ok")
finally:
    node.cleanup()
