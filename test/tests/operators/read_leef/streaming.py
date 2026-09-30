# runner: python

import json
import os
import select
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
header = b"LEEF:1.0|V|P|1|42|"

# Batch timeouts must flush complete events while input remains open.
process = subprocess.Popen(
    [
        *binary,
        "load_stdin | read_leef _batch_timeout=100ms "
        "| select value=attributes.value | write_ndjson",
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
)
try:
    process.stdin.write(header + b"value=42\n")
    process.stdin.flush()
    ready, _, _ = select.select([process.stdout], [], [], 10)
    assert ready, "read_leef did not flush before EOF"
    assert json.loads(process.stdout.readline()) == {"value": 42}
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0 and not stderr, stderr
    assert not stdout, stdout
finally:
    if process.poll() is None:
        process.kill()
        process.wait()

for batch_size in (1, 2, 7):
    source = b"".join(header + f"value={i}\n".encode() for i in range(19))
    result = subprocess.run(
        [
            *binary,
            f"load_stdin | read_leef _batch_size={batch_size} "
            "| select value=attributes.value | write_ndjson",
        ],
        input=source,
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 0 and not result.stderr, result.stderr
    assert [json.loads(line)["value"] for line in result.stdout.splitlines()] == list(
        range(19)
    )

print("timeouts and batch boundaries: ok")
