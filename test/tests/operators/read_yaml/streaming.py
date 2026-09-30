# runner: python

import json
import os
import select
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
process = subprocess.Popen(
    [
        *binary,
        "load_stdin | read_yaml _batch_timeout=100ms | write_ndjson",
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
)
try:
    # Only the completed document may flush while the next document is open.
    process.stdin.write(b"value: 42\n...\nvalue: last")
    process.stdin.flush()
    ready, _, _ = select.select([process.stdout], [], [], 10)
    assert ready, "read_yaml did not flush a complete document before EOF"
    assert json.loads(process.stdout.readline()) == {"value": 42}
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0 and not stderr, stderr
    assert [json.loads(line) for line in stdout.splitlines()] == [{"value": "last"}]
finally:
    if process.poll() is None:
        process.kill()
        process.wait()

print("complete documents flush before EOF; partial documents wait: ok")
