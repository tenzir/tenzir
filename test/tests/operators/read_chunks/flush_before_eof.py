import json
import os
import select
import shlex
import subprocess


process = subprocess.Popen(
    [
        *shlex.split(os.environ["TENZIR_BINARY"]),
        "--bare-mode",
        "--nova=true",
        "load_stdin | split_bytes 1 | read_chunks | write_ndjson",
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
)
try:
    process.stdin.write(b"x")
    process.stdin.flush()
    ready, _, _ = select.select([process.stdout], [], [], 10)
    assert ready, "read_chunks did not flush before EOF"
    assert json.loads(process.stdout.readline()) == {"data": "eA=="}
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0 and not stderr, stderr
    assert not stdout, stdout
finally:
    if process.poll() is None:
        process.kill()
        process.wait()
