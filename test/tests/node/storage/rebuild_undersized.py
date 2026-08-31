# runner: python
# timeout: 120

"""Verify that rebuilding several tiny partitions reduces their file count."""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import tempfile
import threading
from datetime import UTC, datetime
from pathlib import Path

SCHEMA = "rebuild.undersized"

# Import times come from the node's clock. Pick a fixed-offset zone that puts
# the test in hour two of a four-hour bucket, leaving at least an hour before
# the next boundary even when the test starts near the end of the hour.
FIXED_OFFSET_TIMEZONES = ("Etc/GMT", "Etc/GMT-1", "Etc/GMT-2", "Etc/GMT-3")
TEST_TIMEZONE = FIXED_OFFSET_TIMEZONES[(2 - datetime.now(UTC).hour) % 4]


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
    def __init__(
        self,
        memory_budget: str = "0",
        merge_margin: str = "0.6",
        timezone: str = TEST_TIMEZONE,
    ) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(prefix="rebuild-undersized-")
        self.state_dir = Path(self.temp_dir.name) / "state"
        self.cache_dir = Path(self.temp_dir.name) / "cache"
        self.memory_budget = memory_budget
        self.merge_margin = merge_margin
        self.timezone = timezone
        self.state_dir.mkdir(parents=True)
        self.cache_dir.mkdir(parents=True)
        self.proc: subprocess.Popen[str] | None = None
        self.env: dict[str, str] = {}

    def start(self) -> None:
        (self.state_dir / "pid.lock").unlink(missing_ok=True)
        env = os.environ.copy()
        env["TENZIR_AUTOMATIC_REBUILD"] = "0"
        env["TENZIR_REBUILD_MERGE_MARGIN"] = self.merge_margin
        env["TENZIR_REBUILD_TIMEZONE"] = self.timezone
        # Exercise the explicit unlimited mode instead of relying on the
        # machine-dependent automatic memory estimate.
        env["TENZIR_REBUILD_MEMORY_BUDGET"] = self.memory_budget
        cmd = [
            *shlex.split(os.environ["TENZIR_NODE_BINARY"]),
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
    return subprocess.run(
        [
            str(Path(binary[0]).with_name("tenzir-ctl")),
            "--bare-mode",
            "--console-verbosity=warning",
            f"--endpoint={node.env['TENZIR_NODE_CLIENT_ENDPOINT']}",
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=60,
    )


def partition_count(tenzir: Executor) -> int:
    result = tenzir.run(
        f'partitions\nwhere schema == "{SCHEMA}"\nsummarize count=count()\nwrite_ndjson\n'
    )
    assert result.returncode == 0, f"partitions failed: {result.stderr.decode()}"
    return json.loads(result.stdout.decode())["count"]


def startup_error(node: NodeController, expected: str) -> str:
    try:
        try:
            node.start()
        except RuntimeError as error:
            return str(error)
        assert node.proc is not None
        proc = node.proc
        assert proc.stderr is not None
        diagnostics: list[str] = []
        found = threading.Event()

        def collect_diagnostics() -> None:
            assert proc.stderr is not None
            for line in proc.stderr:
                diagnostics.append(line)
                if expected in line:
                    found.set()

        reader = threading.Thread(target=collect_diagnostics, daemon=True)
        reader.start()
        found.wait(timeout=20)
        _terminate(proc)
        reader.join(timeout=20)
        return "".join(diagnostics)
    finally:
        node.cleanup()


invalid_node = NodeController("-1")
message = startup_error(invalid_node, "rebuild-memory-budget must not be negative")
assert "rebuild-memory-budget must not be negative" in message
print("negative-memory-budget-rejected: ok")

invalid_node = NodeController(merge_margin="1.1")
message = startup_error(invalid_node, "rebuild-merge-margin must be between 0 and 1")
assert "rebuild-merge-margin must be between 0 and 1" in message
print("invalid-merge-margin-rejected: ok")

invalid_node = NodeController(timezone="Not/A_Timezone")
message = startup_error(invalid_node, "failed to resolve")
assert "failed to resolve" in message, message
assert "tenzir.rebuild-timezone 'Not/A_Timezone'" in message, message
assert "not found in timezone database" in message, message
print("invalid-timezone-rejected: ok")

node = NodeController()

try:
    # Restart after every import so each event is persisted in its own tiny
    # partition instead of joining the active partition for this schema.
    for index in range(6):
        node.start()
        tenzir = Executor.from_env(node.env)
        result = tenzir.run(f'from {{index: {index}}}\n@name = "{SCHEMA}"\nimport\n')
        assert result.returncode == 0, f"import failed: {result.stderr.decode()}"
        node.stop()
    node.start()
    tenzir = Executor.from_env(node.env)
    assert partition_count(tenzir) == 6
    result = run_ctl(node, "rebuild", "--undersized")
    assert result.returncode == 0, f"rebuild failed: {result.stderr}"
    assert partition_count(tenzir) == 1
    print("six-tiny-partitions-merged: ok")
finally:
    node.cleanup()
