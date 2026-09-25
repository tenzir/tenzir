# runner: python
# timeout: 180

"""Verify that a rebuild cannot pull a partition out from under an export.

An export asks the catalog for candidate partitions and then opens each of
them itself, so nothing but the catalog is in a position to notice that a
concurrent rebuild is about to replace those very partitions. Before the
catalog pinned candidate sets, a rebuild that committed between the lookup and
the read deleted the files an export was still about to open, and the export
silently came back short.

Phase 1: seed several partitions.
Phase 2: run `rebuild --all` while exports run against the same data; every
         export must see the full event count and the node must stay alive.
Phase 3: the rebuild really did replace every partition, and the ones it
         replaced stay gone across a restart.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import tempfile
import threading
from pathlib import Path

SCHEMA = "rebuild.race"
PARTITIONS = 8
EVENTS_PER_PARTITION = 200
TOTAL_EVENTS = PARTITIONS * EVENTS_PER_PARTITION
EXPORT_ROUNDS = 12


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
        self.temp_dir = tempfile.TemporaryDirectory(prefix="rebuild-during-export-")
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
        # Small partitions so that the seed below spreads over several of
        # them, giving the rebuild and the exports room to interleave.
        env["TENZIR_MAX_PARTITION_SIZE"] = str(EVENTS_PER_PARTITION)
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
            "TENZIR_NODE_CLIENT_TIMEOUT": os.environ.get("TENZIR_TIMEOUT", "180"),
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
        timeout=120,
    )


def export_count(tenzir: Executor) -> int:
    r = tenzir.run(
        f'export\nwhere @name == "{SCHEMA}"\nsummarize count=count()\nwrite_ndjson\n'
    )
    assert r.returncode == 0, f"export failed: {r.stderr.decode()}"
    return json.loads(r.stdout.decode().strip()).get("count", 0)


def partition_uuids(tenzir: Executor) -> set[str]:
    r = tenzir.run(
        f'partitions\nwhere schema == "{SCHEMA}"\nselect uuid\nwrite_ndjson\n'
    )
    assert r.returncode == 0, f"partitions failed: {r.stderr.decode()}"
    return {
        json.loads(line)["uuid"]
        for line in r.stdout.decode().splitlines()
        if line.strip()
    }


node = NodeController()

try:
    # --- Phase 1: seed several partitions ---------------------------------

    node.start()
    tenzir = Executor.from_env(node.env)
    for _ in range(PARTITIONS):
        r = tenzir.run(
            f"from {{}}\nrepeat {EVENTS_PER_PARTITION}\nenumerate index\n"
            f'@name = "{SCHEMA}"\nimport\n'
        )
        assert r.returncode == 0, f"import failed: {r.stderr.decode()}"
    # Restart so that every partition is persisted and in the catalog.
    node.stop()
    node.start()
    tenzir = Executor.from_env(node.env)
    seeded = partition_uuids(tenzir)
    assert len(seeded) == PARTITIONS, f"expected {PARTITIONS} partitions: {seeded}"
    assert export_count(tenzir) == TOTAL_EVENTS
    print("phase1-seed: ok")

    # --- Phase 2: export while rebuilding ---------------------------------

    counts: list[int] = []
    failures: list[str] = []

    def export_repeatedly() -> None:
        exporter = Executor.from_env(node.env)
        for _ in range(EXPORT_ROUNDS):
            try:
                counts.append(export_count(exporter))
            except Exception as exc:  # noqa: BLE001 - reported below
                failures.append(str(exc))
                return

    exporter_thread = threading.Thread(target=export_repeatedly)
    exporter_thread.start()
    rebuild = run_ctl(node, "rebuild", "start", "--all")
    exporter_thread.join()
    assert rebuild.returncode == 0, f"rebuild failed: {rebuild.stderr}"
    assert node.alive(), "node died during rebuild"
    assert not failures, f"exports failed during rebuild: {failures}"
    assert len(counts) == EXPORT_ROUNDS, f"expected {EXPORT_ROUNDS} exports: {counts}"
    short = [count for count in counts if count != TOTAL_EVENTS]
    assert not short, f"exports lost events during rebuild: {short}"
    print("phase2-export-during-rebuild: ok")

    # --- Phase 3: the inputs are gone for good ----------------------------

    rebuilt = partition_uuids(tenzir)
    assert not rebuilt & seeded, f"rebuild replaced nothing: {rebuilt & seeded}"
    assert export_count(tenzir) == TOTAL_EVENTS
    node.stop()
    node.start()
    tenzir = Executor.from_env(node.env)
    assert export_count(tenzir) == TOTAL_EVENTS
    # The replaced partitions must not come back: their files are deleted once
    # the last export that held them is done, and a tombstone marker keeps a
    # restart from regenerating a synopsis for a leftover partition file.
    restarted = partition_uuids(tenzir)
    assert restarted == rebuilt, f"partitions changed across restart: {restarted}"
    print("phase3-inputs-gone-for-good: ok")
finally:
    node.cleanup()
