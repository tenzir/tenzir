# runner: python
# timeout: 120

"""Verify the offline rebuild tool (`tenzir-rebuild`).

Phase 1: seed multiple small partitions across two schemas by importing
         with a tiny active-partition timeout.
Phase 2: `tenzir-rebuild` refuses to run while the node is alive.
Phase 3: a dry run reports the plan without modifying the state directory.
Phase 4: the real run consolidates each schema into a single partition.
Phase 5: after a restart, all events survive and the partition counts
         per schema dropped to one.
Phase 6: partitions above half the target still merge (groups stop growing
         only after crossing 0.8 x max-partition-size, so a single output
         may exceed the target), and partitions at or above the 0.8 x max
         cutoff are never rewritten.
Phase 7: a full partition that falls between two undersized partitions in
         import-time order acts as a barrier: the undersized partitions do
         not merge across it, so no merged partition spans the import-time
         range of a partition that stays in place.
Phase 8: with --max-partition-span, partitions group by the event timestamp
         field instead of import time, and merged partitions cover no more
         than the given span.
Phase 9: when run on a terminal, the interactive schema selector aborts the
         run with q, leaving the state directory untouched.
Phase 10: deselecting a schema in the interactive selector excludes it from
          the consolidation.
"""

from __future__ import annotations

import json
import os
import pty
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
        self.temp_dir = tempfile.TemporaryDirectory(prefix="offline-rebuild-")
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
        # Flush active partitions quickly so that consecutive imports create
        # separate partitions.
        env["TENZIR_ACTIVE_PARTITION_TIMEOUT"] = "250ms"
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


def run_rebuild(node: NodeController, *args: str) -> subprocess.CompletedProcess[str]:
    binary = shlex.split(os.environ["TENZIR_BINARY"])
    tenzir_rebuild = str(Path(binary[0]).with_name("tenzir-rebuild"))
    return subprocess.run(
        [
            tenzir_rebuild,
            "--bare-mode",
            "--console-verbosity=warning",
            f"--state-directory={node.state_dir}",
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )


def run_rebuild_pty(node: NodeController, keys: bytes, *args: str) -> tuple[int, str]:
    """Run tenzir-rebuild with a pseudo-terminal on stdin, feeding `keys` to
    the interactive selector."""
    binary = shlex.split(os.environ["TENZIR_BINARY"])
    tenzir_rebuild = str(Path(binary[0]).with_name("tenzir-rebuild"))
    master, slave = pty.openpty()
    try:
        proc = subprocess.Popen(
            [
                tenzir_rebuild,
                "--bare-mode",
                "--console-verbosity=warning",
                f"--state-directory={node.state_dir}",
                *args,
            ],
            stdin=slave,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        os.close(slave)
        os.write(master, keys)
        _, err = proc.communicate(timeout=60)
        return proc.returncode, err
    finally:
        os.close(master)


def export_count(tenzir: Executor, schema: str) -> int:
    r = tenzir.run(
        f'export\nwhere @name == "{schema}"\nsummarize count=count()\nwrite_ndjson\n'
    )
    assert r.returncode == 0, f"export failed: {r.stderr.decode()}"
    return json.loads(r.stdout.decode().strip()).get("count", 0)


def partition_uuids(tenzir: Executor, schema: str) -> list[str]:
    r = tenzir.run(
        f'partitions\nwhere schema == "{schema}"\nselect uuid\nwrite_ndjson\n'
    )
    assert r.returncode == 0, f"partitions failed: {r.stderr.decode()}"
    return [
        json.loads(line)["uuid"]
        for line in r.stdout.decode().splitlines()
        if line.strip()
    ]


def partition_events(tenzir: Executor, schema: str) -> list[int]:
    r = tenzir.run(
        f'partitions\nwhere schema == "{schema}"\nselect events\nwrite_ndjson\n'
    )
    assert r.returncode == 0, f"partitions failed: {r.stderr.decode()}"
    return sorted(
        json.loads(line)["events"]
        for line in r.stdout.decode().splitlines()
        if line.strip()
    )


node = NodeController()

try:
    # --- Phase 1: seed several small partitions per schema ----------------

    node.start()
    tenzir = Executor.from_env(node.env)
    rounds = 4
    for i in range(rounds):
        for name, events in (("alpha", 25), ("beta", 10)):
            r = tenzir.run(
                f"from {{}}\nrepeat {events}\nenumerate index\n"
                f'@name = "offline.{name}"\nimport\n'
            )
            assert r.returncode == 0, f"import failed: {r.stderr.decode()}"
        # Exceed the active-partition timeout so the next round starts a new
        # partition.
        time.sleep(1.0)
    alpha_before = export_count(tenzir, "offline.alpha")
    beta_before = export_count(tenzir, "offline.beta")
    assert alpha_before == rounds * 25, f"unexpected alpha count: {alpha_before}"
    assert beta_before == rounds * 10, f"unexpected beta count: {beta_before}"
    assert len(partition_uuids(tenzir, "offline.alpha")) > 1
    assert len(partition_uuids(tenzir, "offline.beta")) > 1
    print("phase1-seed-small-partitions: ok")

    # --- Phase 2: refuse to run while the node is alive -------------------

    r = run_rebuild(node)
    assert r.returncode != 0, "tenzir-rebuild ran despite a live node"
    assert "in use" in r.stderr, f"unexpected error output:\n{r.stderr}"
    print("phase2-refuse-live-state-directory: ok")

    # --- Phase 3: dry run reports the plan and changes nothing ------------

    node.stop()
    index_dir = node.state_dir / "index"
    files_before = sorted(p.name for p in index_dir.iterdir())
    r = run_rebuild(node, "--dry-run")
    assert r.returncode == 0, f"dry run failed: {r.stderr}"
    assert "would merge" in r.stderr, f"missing plan output:\n{r.stderr}"
    files_after = sorted(p.name for p in index_dir.iterdir())
    assert files_before == files_after, "dry run modified the state directory"
    print("phase3-dry-run-changes-nothing: ok")

    # --- Phase 4: consolidate ----------------------------------------------

    r = run_rebuild(node, "--parallel=3")
    assert r.returncode == 0, f"offline rebuild failed: {r.stderr}"
    assert "done: merged" in r.stderr, f"missing summary output:\n{r.stderr}"
    print("phase4-consolidate: ok")

    # --- Phase 5: all events survive with fewer partitions ----------------

    node.start()
    tenzir = Executor.from_env(node.env)
    alpha_after = export_count(tenzir, "offline.alpha")
    beta_after = export_count(tenzir, "offline.beta")
    assert alpha_after == alpha_before, f"{alpha_after} != {alpha_before}"
    assert beta_after == beta_before, f"{beta_after} != {beta_before}"
    assert len(partition_uuids(tenzir, "offline.alpha")) == 1
    assert len(partition_uuids(tenzir, "offline.beta")) == 1
    print("phase5-data-survives-consolidation: ok")

    # --- Phase 6: the 0.8 x max cutoff drives the grouping ----------------

    # Seed three 60-event partitions and one 90-event partition. With
    # --max-partition-size=100 the undersized cutoff is 80: the 60-event
    # partitions must merge even though any pair exceeds the target (the
    # group stops growing only after crossing 80, so the output holds 120
    # events), while the 90-event partition is not undersized and must stay
    # untouched.
    for _ in range(3):
        r = tenzir.run(
            'from {}\nrepeat 60\nenumerate index\n@name = "offline.pair"\nimport\n'
        )
        assert r.returncode == 0, f"import failed: {r.stderr.decode()}"
        time.sleep(1.0)
    r = tenzir.run(
        'from {}\nrepeat 90\nenumerate index\n@name = "offline.full"\nimport\n'
    )
    assert r.returncode == 0, f"import failed: {r.stderr.decode()}"
    time.sleep(1.0)
    assert partition_events(tenzir, "offline.pair") == [60, 60, 60]
    assert partition_events(tenzir, "offline.full") == [90]
    # Additionally seed a schema whose import-time order interleaves a full
    # partition between two undersized ones: the full partition acts as a
    # barrier, so the undersized partitions before and after it must not
    # merge across it into a partition with an overlapping time range.
    for events in (60, 90, 60):
        r = tenzir.run(
            f"from {{}}\nrepeat {events}\nenumerate index\n"
            '@name = "offline.mid"\nimport\n'
        )
        assert r.returncode == 0, f"import failed: {r.stderr.decode()}"
        time.sleep(1.0)
    assert partition_events(tenzir, "offline.mid") == [60, 60, 90]
    node.stop()
    r = run_rebuild(node, "--max-partition-size=100")
    assert r.returncode == 0, f"offline rebuild failed: {r.stderr}"
    node.start()
    tenzir = Executor.from_env(node.env)
    assert partition_events(tenzir, "offline.pair") == [60, 120]
    assert partition_events(tenzir, "offline.full") == [90]
    print("phase6-undersized-cutoff-drives-grouping: ok")

    # --- Phase 7: a full partition amid undersized ones is a barrier ------

    assert partition_events(tenzir, "offline.mid") == [60, 60, 90]
    mid_count = export_count(tenzir, "offline.mid")
    assert mid_count == 210, f"unexpected mid count: {mid_count}"
    print("phase7-full-partition-is-a-barrier: ok")

    # --- Phase 8: --max-partition-span cuts by the timestamp field --------

    # Seed four partitions whose `timestamp` field falls into two separate
    # hours, in import order 00:05, 00:45, 01:20, 01:50. With a span of one
    # hour the first two and the last two merge, but nothing merges across
    # the hour boundary (00:05 to 01:20 already exceeds the span).
    for ts in ("00:05", "00:45", "01:20", "01:50"):
        r = tenzir.run(
            f"from {{timestamp: 2026-08-01T{ts}:00}}\nrepeat 30\n"
            'enumerate index\n@name = "offline.window"\nimport\n'
        )
        assert r.returncode == 0, f"import failed: {r.stderr.decode()}"
        time.sleep(1.0)
    assert partition_events(tenzir, "offline.window") == [30, 30, 30, 30]
    node.stop()
    r = run_rebuild(node, "--max-partition-size=100", "--max-partition-span=1h")
    assert r.returncode == 0, f"offline rebuild failed: {r.stderr}"
    assert "cut by timestamp" in r.stderr, f"missing cut field:\n{r.stderr}"
    node.start()
    tenzir = Executor.from_env(node.env)
    assert partition_events(tenzir, "offline.window") == [60, 60]
    window_count = export_count(tenzir, "offline.window")
    assert window_count == 120, f"unexpected window count: {window_count}"
    print("phase8-span-cuts-by-timestamp-field: ok")

    # --- Phase 9: the interactive selector aborts with q ------------------

    # Seed one more mergeable schema for the selector phases. It sorts first
    # in the selector, before offline.window and the internal metrics.
    for _ in range(2):
        r = tenzir.run(
            'from {}\nrepeat 30\nenumerate index\n@name = "offline.aselect"\nimport\n'
        )
        assert r.returncode == 0, f"import failed: {r.stderr.decode()}"
        time.sleep(1.0)
    assert partition_events(tenzir, "offline.aselect") == [30, 30]
    node.stop()
    rc, err = run_rebuild_pty(node, b"q\n", "--max-partition-size=100")
    assert rc == 0, f"selector abort failed: {err}"
    assert "select the schemas" in err, f"missing selector output:\n{err}"
    assert "aborted" in err, f"missing abort message:\n{err}"
    node.start()
    tenzir = Executor.from_env(node.env)
    assert partition_events(tenzir, "offline.aselect") == [30, 30]
    assert partition_events(tenzir, "offline.window") == [60, 60]
    print("phase9-selector-aborts: ok")

    # --- Phase 10: deselecting a schema excludes it from the run ----------

    node.stop()
    # Space deselects the first entry (offline.aselect), enter starts.
    rc, err = run_rebuild_pty(node, b" \n", "--max-partition-size=100")
    assert rc == 0, f"selector run failed: {err}"
    assert "consolidating" in err, f"missing selection summary:\n{err}"
    node.start()
    tenzir = Executor.from_env(node.env)
    assert partition_events(tenzir, "offline.aselect") == [30, 30]
    assert partition_events(tenzir, "offline.window") == [120]
    print("phase10-selector-narrows-plan: ok")
finally:
    node.cleanup()
