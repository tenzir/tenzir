import subprocess
from pathlib import Path
import os
import sys
import time


class Executor:
    def __init__(self):
        self.binary = os.environ["TENZIR_PYTHON_FIXTURE_BINARY"]
        self.endpoint = os.environ.get("TENZIR_PYTHON_FIXTURE_ENDPOINT")
        self.remaining_timeout = float(os.environ.get("TENZIR_PYTHON_FIXTURE_TIMEOUT"))

    def run(
        self, source: str, desired_timeout: float = None, mirror: bool = False
    ) -> subprocess.CompletedProcess[bytes]:
        cmd = [
            self.binary,
            "--bare-mode",
            "--console-verbosity=warning",
            "--multi",
        ]
        if self.endpoint is not None:
            cmd.append(f"--endpoint={self.endpoint}")
        cmd.append(source)
        start = time.process_time()
        timeout = min(self.remaining_timeout, desired_timeout or self.remaining_timeout)
        res = subprocess.run(cmd, timeout=timeout, capture_output=True)
        end = time.process_time()
        used_time = end - start
        self.remaining_timeout = max(0, self.remaining_timeout - used_time)
        if mirror:
            if res.stdout:
                print(res.stdout.decode())
            if res.stderr:
                print(res.stderr.decode(), file=sys.stderr)
        return res
