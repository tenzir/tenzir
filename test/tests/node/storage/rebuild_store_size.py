# runner: python
# timeout: 120

"""Verify that a rebuilt partition records its store file in the catalog.

The partition transformer, which backs both rebuild and compaction, asks
every store builder to `persist` and gets the store's location and size
back. That reply used to be discarded, so a rebuilt or compacted partition
reached the catalog with an empty `store` resource: `partitions` reported a
store size of zero, and the real value only reappeared after a restart,
when the index re-stats the archive. That made `partitions` useless for
disk accounting on any long-running node, because automatic rebuild
eventually touches nearly every partition.

Phase 1: seed a schema and restart so the partition lands in the catalog
         with a store size recovered from disk.
Phase 2: rebuild it and check -- without restarting, so nothing can re-stat
         the archive -- that the catalog still knows the store's path and
         size.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import tempfile
from pathlib import Path

SCHEMA = "store_size.event"


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
        self.temp_dir = tempfile.TemporaryDirectory(prefix="rebuild-store-size-")
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
        # Keep rebuilds under explicit test control, so that the only rebuild
        # in this test is the one we trigger below.
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


def stores(tenzir: Executor) -> list[dict[str, object]]:
    """The uuid and store resource of every partition holding SCHEMA."""
    r = tenzir.run(
        f'partitions\nwhere schema == "{SCHEMA}"\n'
        "select uuid, url = store.url, size = store.size\nwrite_ndjson\n"
    )
    assert r.returncode == 0, f"partitions failed: {r.stderr.decode()}"
    return [json.loads(line) for line in r.stdout.decode().splitlines() if line.strip()]


node = NodeController()

try:
    # --- Phase 1: seed a partition and get it into the catalog ------------

    node.start()
    tenzir = Executor.from_env(node.env)
    r = tenzir.run(
        f'from {{}}\nrepeat 1000\nenumerate index\n@name = "{SCHEMA}"\nimport\n'
    )
    assert r.returncode == 0, f"import failed: {r.stderr.decode()}"
    # Restart so the active partition is persisted and appears in the catalog.
    # The index recovers store sizes by stat'ing the archive on startup, so
    # this establishes the baseline that the rebuild below must preserve.
    node.stop()
    node.start()
    tenzir = Executor.from_env(node.env)
    before = stores(tenzir)
    assert len(before) == 1, f"expected one partition, got {before}"
    assert before[0]["size"] > 0, f"baseline store size is not set: {before}"
    assert before[0]["url"], f"baseline store url is not set: {before}"
    print("phase1-baseline-store-recorded: ok")

    # --- Phase 2: rebuild, then check the store is still recorded ---------

    r = run_ctl(node, "rebuild", "--all")
    assert r.returncode == 0, f"rebuild failed: {r.stderr}"
    after = stores(tenzir)
    assert len(after) == 1, f"expected one partition after rebuild, got {after}"
    # A rebuild writes a new partition, so the uuid must have changed --
    # otherwise this phase would pass on the stale baseline synopsis.
    assert after[0]["uuid"] != before[0]["uuid"], (
        f"rebuild did not replace the partition: {before} -> {after}"
    )
    assert after[0]["size"] > 0, f"rebuilt partition has no store size: {after}"
    assert after[0]["url"], f"rebuilt partition has no store url: {after}"
    print("phase2-rebuilt-store-recorded: ok")
finally:
    node.cleanup()
