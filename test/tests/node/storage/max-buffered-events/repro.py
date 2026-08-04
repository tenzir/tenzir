# fixtures: [node]
# timeout: 180

from __future__ import annotations

import json
import os
import select
import subprocess

NUM_ROUNDS = 8
ROUNDS_PER_WAVE = 1
ROWS_PER_IMPORT = 100
EXPORTS_PER_ROUND = 2
COMMAND_TIMEOUT = 30
READY_ROUND = -1


class Executor:
    def __init__(self) -> None:
        self._binary = os.environ["TENZIR_PYTHON_FIXTURE_BINARY"]
        self._endpoint = os.environ.get("TENZIR_PYTHON_FIXTURE_ENDPOINT")

    def command(self, pipeline: str) -> list[str]:
        cmd = [
            self._binary,
            "--bare-mode",
            "--console-verbosity=warning",
        ]
        if self._endpoint:
            cmd.append(f"--endpoint={self._endpoint}")
        cmd.append(pipeline)
        return cmd

    def run(self, pipeline: str) -> subprocess.CompletedProcess[bytes]:
        return subprocess.run(
            self.command(pipeline),
            timeout=COMMAND_TIMEOUT,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )


def make_import_pipeline(round_index: int) -> str:
    return f"""
from {{round: {round_index}}}
repeat {ROWS_PER_IMPORT}
enumerate x
batch {ROWS_PER_IMPORT}
import
"""


def make_export_pipeline(round_index: int) -> str:
    return f"""
export live=true, retro=true
where round == {READY_ROUND} or round == {round_index}
deduplicate round
head 2
to_stdout {{
  write_ndjson
}}
"""


def stop(process: subprocess.Popen[bytes]) -> None:
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


def wait_for_ready(process: subprocess.Popen[bytes]) -> None:
    assert process.stdout is not None
    ready, _, _ = select.select([process.stdout], [], [], COMMAND_TIMEOUT)
    if not ready:
        stop(process)
        _, stderr = process.communicate()
        raise AssertionError(
            f"export did not observe readiness sentinel: {stderr.decode()}"
        )
    line = process.stdout.readline()
    if not line:
        _, stderr = process.communicate()
        raise AssertionError(
            f"export stopped before readiness sentinel: {stderr.decode()}"
        )
    assert json.loads(line)["round"] == READY_ROUND, line.decode()


def assert_export(process: subprocess.Popen[bytes], round_index: int) -> None:
    stdout, stderr = process.communicate(timeout=COMMAND_TIMEOUT)
    assert process.returncode == 0, stderr.decode()
    assert [json.loads(line) for line in stdout.splitlines()] == [
        {"round": round_index, "x": 0}
    ], stdout.decode()


def run_round(round_index: int) -> None:
    executor = Executor()
    rounds = [round_index * 10 + offset for offset in range(ROUNDS_PER_WAVE)]
    exports = [
        (
            current_round,
            subprocess.Popen(
                executor.command(make_export_pipeline(current_round)),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            ),
        )
        for current_round in rounds
        for _ in range(EXPORTS_PER_ROUND)
    ]
    try:
        for _, export in exports:
            wait_for_ready(export)
        for current_round in rounds:
            executor.run(make_import_pipeline(current_round))
        for current_round, export in exports:
            assert_export(export, current_round)
    finally:
        for _, export in exports:
            stop(export)


def main() -> None:
    # This stored sentinel confirms each export is live before its import starts.
    Executor().run(make_import_pipeline(READY_ROUND))
    for round_index in range(NUM_ROUNDS):
        run_round(round_index)
    print("ok")


if __name__ == "__main__":
    main()
