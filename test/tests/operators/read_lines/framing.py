# timeout: 120

from __future__ import annotations

import base64
import json
import os
import select
import shlex
import subprocess


BINARY = [*shlex.split(os.environ["TENZIR_BINARY"]), "--nova=true"]


def read(payload: bytes, chunk_size: int, jobs: int, options: str = ""):
    args = [options] if options else []
    if jobs:
        args.append(f"_jobs={jobs}")
    pipeline = (
        f"load_stdin | split_bytes {chunk_size} | read_lines {', '.join(args)}"
        " | schema = @name | write_ndjson"
    )
    result = subprocess.run(
        [*BINARY, "--bare-mode", pipeline],
        input=payload,
        capture_output=True,
        timeout=10,
    )
    assert result.returncode == 0, (pipeline, result.stderr)
    rows = [json.loads(line) for line in result.stdout.splitlines()]
    assert all(row["schema"] == "tenzir.line" for row in rows), rows
    return [row["line"] for row in rows], result.stderr


def main():
    cases = [
        (b"", []),
        (b"one", ["one"]),
        (b"one\n", ["one"]),
        (b"\r\n\n\r", ["", "", ""]),
        ("alpha\r\n\r\nβ\rlast".encode(), ["alpha", "", "β", "last"]),
    ]
    for jobs in (0, 1, 3):
        for size in (1, 2, 7, 4096):
            for payload, expected in cases:
                lines, stderr = read(payload, size, jobs)
                assert not stderr, stderr
                if jobs:
                    assert sorted(lines) == sorted(expected), (jobs, size, lines)
                else:
                    assert lines == expected, (size, lines)
            lines, stderr = read(b"\nfirst\r\n\rsecond", size, jobs, "skip_empty=true")
            assert not stderr, stderr
            assert sorted(lines) == ["first", "second"], lines
            lines, stderr = read(b"\xff\x00\r\n\x80", size, jobs, "binary=true")
            expected = [base64.b64encode(x).decode() for x in (b"\xff\x00", b"\x80")]
            assert not stderr, stderr
            assert sorted(lines) == sorted(expected), lines
            lines, stderr = read(b"\xff\nok\n", size, jobs)
            assert lines == ["ok"], lines
            assert stderr.count(b"got invalid UTF-8") == 1, stderr
    # A complete line must flush while stdin remains open. The incomplete
    # trailing line must wait for EOF.
    process = subprocess.Popen(
        [*BINARY, "--bare-mode", "from_stdin { read_lines } | write_ndjson"],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        assert process.stdin is not None
        assert process.stdout is not None
        process.stdin.write(b"ready\npartial")
        process.stdin.flush()
        ready, _, _ = select.select([process.stdout], [], [], 10)
        assert ready, "complete line did not flush before EOF"
        assert json.loads(process.stdout.readline()) == {"line": "ready"}
        stdout, stderr = process.communicate(timeout=10)
        assert process.returncode == 0, stderr
        assert not stderr, stderr
        assert [json.loads(line) for line in stdout.splitlines()] == [
            {"line": "partial"}
        ], stdout
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
    print("framing, binary, workers, and streaming flush: ok")


if __name__ == "__main__":
    main()
