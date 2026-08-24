# runner: python
# timeout: 20

from __future__ import annotations

import json
import os
import select
import shlex
import subprocess


def assert_streams(payload: bytes, expected: object) -> None:
    pipeline = 'from_stdin { read_avro schema=r#""string""# } | head 1 | write_ndjson'
    process = subprocess.Popen(
        [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", pipeline],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert process.stdin is not None
        assert process.stdout is not None
        process.stdin.write(payload)
        process.stdin.flush()
        readable, _, _ = select.select([process.stdout], [], [], 5)
        assert readable, "read_avro produced no output before EOF"
        line = process.stdout.readline()
        assert json.loads(line) == expected, line
    finally:
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=5)


def main() -> None:
    assert_streams(b"\x0ahello", {"value": "hello"})
    print("streams_before_eof: true")


if __name__ == "__main__":
    main()
