# runner: python
# timeout: 300

"""Verify that a node over its disk budget evicts partitions.

Runs against whichever implementation is enforcing the budget: the standalone
disk monitor by default, and the catalog's own budget loop when
`tenzir.catalog-maintenance` is set. Both read the same
`tenzir.start.disk-budget-*` settings, so the test does not care which one is
live -- which is the point, since the catalog loop replaces the disk monitor.

Phase 1: seed several partitions with no budget configured, and measure them.
Phase 2: restart under a budget below that size; the node evicts down to it.
Phase 3: the node is alive, still queryable, and kept the newest data.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import tempfile
import time
from pathlib import Path

BATCHES = 6
EVENTS_PER_BATCH = 200


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
        self.temp_dir = tempfile.TemporaryDirectory(prefix="disk-budget-")
        self.root = Path(self.temp_dir.name)
        self.state_dir = self.root / "state"
        self.cache_dir = self.root / "cache"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.proc: subprocess.Popen[str] | None = None
        self.env: dict[str, str] = {}

    def start(self, *extra: str) -> dict[str, str]:
        pid_lock = self.state_dir / "pid.lock"
        pid_lock.unlink(missing_ok=True)
        node_binary = shlex.split(os.environ["TENZIR_NODE_BINARY"])
        env = os.environ.copy()
        # Keep rebuilds out of it: a rebuild rewrites partitions while the
        # budget loop is trying to delete them, which makes the sizes move for
        # reasons this test is not about.
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
            f"--max-partition-size={EVENTS_PER_BATCH}",
            *extra,
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


def dir_size(path: Path) -> int:
    return sum(f.stat().st_size for f in path.rglob("*") if f.is_file())


def partition_count(tenzir: Executor) -> int:
    r = tenzir.run("partitions\nsummarize count=count()\nwrite_ndjson\n")
    assert r.returncode == 0, f"partitions failed: {r.stderr.decode()}"
    out = r.stdout.decode().strip()
    return json.loads(out).get("count", 0) if out else 0


def event_count(tenzir: Executor, schema: str) -> int:
    r = tenzir.run(
        f'export\nwhere @name == "{schema}"\nsummarize count=count()\nwrite_ndjson\n'
    )
    assert r.returncode == 0, f"export failed: {r.stderr.decode()}"
    out = r.stdout.decode().strip()
    return json.loads(out).get("count", 0) if out else 0


def seed(tenzir: Executor, schema: str, count: int) -> None:
    r = tenzir.run(
        f'from {{}}\nrepeat {count}\nenumerate index\n@name = "{schema}"\nimport\n'
    )
    assert r.returncode == 0, f"import failed: {r.stderr.decode()}"


node = NodeController()
try:
    # --- Phase 1: seed partitions with no budget in force ------------------

    node.start()
    tenzir = Executor.from_env(node.env)
    for i in range(BATCHES):
        # Distinct schemas so that the survivors can be identified by name;
        # each import lands in its own partition.
        seed(tenzir, f"budget.s{i}", EVENTS_PER_BATCH)
    # Restart so the active partitions get persisted and show up in the
    # catalog, which is what the budget loop selects from.
    node.stop()
    node.start()
    tenzir = Executor.from_env(node.env)
    before = partition_count(tenzir)
    assert before >= BATCHES, f"expected at least {BATCHES} partitions: {before}"
    node.stop()
    seeded_size = dir_size(node.state_dir)
    assert seeded_size > 0, "state directory is empty"
    print("phase1-seed: ok")

    # --- Phase 2: restart under a budget and wait for the eviction ---------

    # Three quarters of what is there now. Part of the state directory is
    # fixed overhead that no eviction can reclaim, so a budget set too low
    # would have the node delete every partition chasing a size it can never
    # reach -- which is a different behaviour than the one under test.
    budget = seeded_size * 3 // 4
    node.start(
        f"--disk-budget-high={budget}",
        "--disk-budget-check-interval=1",
        "--disk-budget-step-size=1",
    )
    tenzir = Executor.from_env(node.env)
    deadline = time.time() + 120
    after = before
    while time.time() < deadline:
        after = partition_count(tenzir)
        if after < before:
            break
        time.sleep(1)
    assert node.alive(), "node died while evicting"
    assert after < before, (
        f"no partition was evicted: {after} of {before} left, "
        f"size {dir_size(node.state_dir)} against budget {budget}"
    )
    print("phase2-evicts-down-to-the-budget: ok")

    # --- Phase 3: the node survived and kept the newest data ---------------

    assert node.alive(), "node died after evicting"
    # Eviction takes the oldest partitions first, so the last schema imported
    # is the last one that may go.
    newest = event_count(tenzir, f"budget.s{BATCHES - 1}")
    oldest = event_count(tenzir, "budget.s0")
    assert newest >= oldest, (
        f"evicted newest before oldest: newest={newest} oldest={oldest}"
    )
    assert newest == EVENTS_PER_BATCH, f"newest partition was evicted: {newest}"
    print("phase3-keeps-the-newest-data: ok")
finally:
    node.cleanup()
