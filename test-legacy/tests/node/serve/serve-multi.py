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


def parse(value: bytes):
    return json.loads(value.decode())


def make_serve_multi_query(continuation_tokens: dict[str, str], max_events: int) -> str:
    res = """api "/serve-multi", { requests: ["""
    for sid, token in continuation_tokens.items():
        res += f"""{{ serve_id:"{sid}", continuation_token:"{token}" }},"""
    res += f"""], timeout: "5s", max_events: {max_events}, schema: "never" }} | write_ndjson"""
    return res


def test() -> None:
    executor = Executor()
    continuation_tokens = {"serve-1": "", "serve-2": ""}
    for idx in range(0, 4):
        print(f"query {idx}")
        query = make_serve_multi_query(continuation_tokens, 3)
        result = executor.run(query)
        if result.stderr:
            print(result.stderr.decode(), file=sys.stderr)
            break
        parsed = parse(result.stdout)
        for sid in list(continuation_tokens.keys()):
            print(f"id: {sid}")
            data = parsed[sid]
            next_token = data.get("next_continuation_token", "")
            if not next_token:
                continuation_tokens.pop(sid)
            else:
                continuation_tokens[sid] = next_token
            for event in data.get("events", []):
                print(event.get("data"))
        if not continuation_tokens:
            break
        print()


def main() -> None:
    test()


if __name__ == "__main__":
    main()
