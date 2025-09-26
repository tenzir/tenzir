# fixtures: [node]

from __future__ import annotations

import json
import os
import subprocess
import sys
import time


class Executor:
    def __init__(self) -> None:
        self._binary = os.environ["TENZIR_PYTHON_FIXTURE_BINARY"]
        self._endpoint = os.environ.get("TENZIR_PYTHON_FIXTURE_ENDPOINT")
        self._remaining_timeout = float(os.environ.get("TENZIR_PYTHON_FIXTURE_TIMEOUT", "30"))

    def run(self, pipeline: str, mirror: bool = False) -> subprocess.CompletedProcess[bytes]:
        cmd = [
            self._binary,
            "--bare-mode",
            "--console-verbosity=warning",
            "--multi",
        ]
        if self._endpoint:
            cmd.append(f"--endpoint={self._endpoint}")
        cmd.append(pipeline)
        start = time.process_time()
        result = subprocess.run(
            cmd,
            timeout=self._remaining_timeout,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=True,
        )
        elapsed = time.process_time() - start
        self._remaining_timeout = max(0.0, self._remaining_timeout - elapsed)
        if mirror:
            if result.stdout:
                print(result.stdout.decode())
            if result.stderr:
                print(result.stderr.decode(), file=sys.stderr)
        return result


def parse(value: bytes) -> dict[str, object]:
    return json.loads(value.decode())


def make_serve_query(serve_id: str, continuation_token: str, max_events: int) -> str:
    return f"""api "/serve", {{serve_id: "{serve_id}", continuation_token: "{continuation_token}", timeout: "5s", max_events: {max_events}, schema: "never" }} | write_ndjson"""


def test(serve_id: str, max_events: int):
    executor = Executor()
    token = ""
    print(f"testing {serve_id} with max {max_events}")
    while True:
        res = executor.run(make_serve_query(serve_id, token, max_events))
        parsed = parse(res.stdout)
        token = parsed.get("next_continuation_token", "")
        events = parsed.get("events")
        for e in events:
            print(e.get("data"))
        if not token:
            break


def main():
    test("serve-1", 1)
    test("serve-2", 5)


if __name__ == "__main__":
    main()
