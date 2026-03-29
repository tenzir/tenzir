# fixtures: [node]
# timeout: 180

from __future__ import annotations

import os
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor


class Executor:
    def __init__(self) -> None:
        self._binary = os.environ["TENZIR_PYTHON_FIXTURE_BINARY"]
        self._endpoint = os.environ.get("TENZIR_PYTHON_FIXTURE_ENDPOINT")
        self._remaining_timeout = float(os.environ.get("TENZIR_PYTHON_FIXTURE_TIMEOUT", "180"))

    def run(self, pipeline: str) -> subprocess.CompletedProcess[bytes]:
        cmd = [
            self._binary,
            "--bare-mode",
            "--console-verbosity=warning",
            "--multi",
        ]
        if self._endpoint:
            cmd.append(f"--endpoint={self._endpoint}")
        cmd.append(pipeline)
        started = time.monotonic()
        result = subprocess.run(
            cmd,
            timeout=max(self._remaining_timeout, 1.0),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        self._remaining_timeout = max(0.0, self._remaining_timeout - (time.monotonic() - started))
        return result


def make_import_pipeline(round_index: int) -> str:
    return f"""
from {{round: {round_index}}}
repeat 500
enumerate x
batch 500
import
"""


def make_export_pipeline(round_index: int) -> str:
    return f"""
export
where round == {round_index}
head 1
write_lines
"""


def run_round(round_index: int) -> None:
    executor = Executor()
    import_processes: list[subprocess.Popen[bytes]] = []
    rounds = [round_index * 10 + offset for offset in range(2)]
    for current_round in rounds:
        import_processes.append(
            subprocess.Popen(
                [
                    executor._binary,
                    "--bare-mode",
                    "--console-verbosity=warning",
                    "--multi",
                    *([f"--endpoint={executor._endpoint}"] if executor._endpoint else []),
                    make_import_pipeline(current_round),
                ],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        )
    # Start exports while the imports are still flushing their batches.
    time.sleep(0.01)
    with ThreadPoolExecutor(max_workers=4) as pool:
        futures = [
            pool.submit(executor.run, make_export_pipeline(current_round))
            for current_round in rounds
            for _ in range(2)
        ]
        for future in futures:
            try:
                future.result()
            except subprocess.CalledProcessError as error:
                print(f"round {round_index} export failed", file=sys.stderr)
                if error.stdout:
                    print(error.stdout.decode(), file=sys.stderr)
                if error.stderr:
                    print(error.stderr.decode(), file=sys.stderr)
                raise
    for current_round, import_process in zip(rounds, import_processes, strict=True):
        stdout, stderr = import_process.communicate(timeout=30)
        if import_process.returncode != 0:
            print(f"round {current_round} import failed", file=sys.stderr)
            if stdout:
                print(stdout.decode(), file=sys.stderr)
            if stderr:
                print(stderr.decode(), file=sys.stderr)
            raise subprocess.CalledProcessError(
                import_process.returncode,
                import_process.args,
                stdout,
                stderr,
            )


def main() -> None:
    for round_index in range(20):
        run_round(round_index)
    print("ok")


if __name__ == "__main__":
    main()
