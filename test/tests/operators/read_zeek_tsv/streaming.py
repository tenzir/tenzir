# runner: python

import json
import os
import select
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
header = b"#path\tstreaming\n#fields\tvalue\n#types\tcount\n"

# An incomplete batch must be delivered while the input remains open.
process = subprocess.Popen(
    [*binary, "load_stdin | read_zeek_tsv | write_ndjson"],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
)
try:
    process.stdin.write(header + b"42\n")
    process.stdin.flush()
    ready, _, _ = select.select([process.stdout], [], [], 10)
    assert ready, "read_zeek_tsv did not flush before EOF"
    assert json.loads(process.stdout.readline()) == {"value": 42}
    stdout, stderr = process.communicate(timeout=10)
    assert process.returncode == 0 and not stderr, stderr
    assert not stdout, stdout
finally:
    if process.poll() is None:
        process.kill()
        process.wait()

# Crossing a batch boundary must retain every row, including the final tail.
count = 8193
source = header + b"".join(f"{i}\n".encode() for i in range(count))
result = subprocess.run(
    [*binary, "load_stdin | read_zeek_tsv | write_ndjson"],
    input=source,
    capture_output=True,
    timeout=20,
)
assert result.returncode == 0 and not result.stderr, result.stderr
actual = [json.loads(line)["value"] for line in result.stdout.splitlines()]
assert actual == list(range(count)), actual
