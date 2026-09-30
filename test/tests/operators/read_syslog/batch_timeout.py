#!/usr/bin/env python3
# runner: python
# timeout: 20

import os
import select
import subprocess


pipeline = """from_stdin { read_syslog _batch_timeout=10ms }
select message
write_lines"""
with subprocess.Popen(
    [os.environ["TENZIR_BINARY"], "--bare-mode", "--nova=true", pipeline],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
) as process:
    try:
        process.stdin.write(
            b"<165>1 2023-01-01T00:00:00Z host app 123 - - before-eof\n"
        )
        process.stdin.flush()
        readable, _, _ = select.select([process.stdout], [], [], 10)
        assert readable, "the pending message did not flush before end-of-input"
        assert process.stdout.readline() == b"before-eof\n"
        process.stdin.close()
        assert process.wait(timeout=10) == 0
        assert process.stdout.read() == b""
        assert process.stderr.read() == b""
    finally:
        if process.poll() is None:
            process.kill()
            process.wait()
print("flushed before end-of-input")
