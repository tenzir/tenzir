# runner: python

import json
import os
from pathlib import Path
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
schemas = Path(__file__).parent / "schemas"
header = b"CEF:0|V|P|1|42|message|5|"

# Invalid setup must terminate even while the upstream is waiting for input.
process = subprocess.Popen(
    [
        *binary,
        'load_stdin | read_cef schema="diagnostics.missing", schema_only=true '
        "| write_ndjson",
    ],
    stdin=subprocess.PIPE,
    stdout=subprocess.PIPE,
    stderr=subprocess.PIPE,
)
try:
    process.wait(timeout=20)
    stdout, stderr = process.communicate(timeout=20)
    assert process.returncode == 1 and b"error:" in stderr, stderr
    assert b"= note: line " not in stderr, stderr
    assert not stdout, stdout
finally:
    if process.poll() is None:
        process.kill()
        process.wait()

# Deferred selector diagnostics cannot be assigned to the last parsed line.
source = header + b"kind=first value=42\n" + header + b"kind=second value=two\n"
for batch_size in (1, 128):
    result = subprocess.run(
        [
            *binary,
            'load_stdin | read_cef selector="extension.kind:missing", '
            f"_batch_size={batch_size} "
            "| write_ndjson",
        ],
        input=source,
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr
    assert b"selected schema not found" in result.stderr, result.stderr
    assert b"= note: line " not in result.stderr, result.stderr
    assert len(result.stdout.splitlines()) == 2, result.stdout

# Both parser errors and immediate schema diagnostics retain their source line.
result = subprocess.run(
    [*binary, "load_stdin | read_cef | write_ndjson"],
    input=b"\ninvalid\n" + header + b"value=42\n",
    capture_output=True,
    timeout=20,
)
assert result.returncode == 0, result.stderr
assert b"= note: line 2" in result.stderr, result.stderr
assert len(result.stdout.splitlines()) == 1, result.stdout

result = subprocess.run(
    [
        *binary,
        f"--schema-dirs={schemas}",
        'load_stdin | read_cef schema="cef.first" | write_ndjson',
    ],
    input=header + b"kind=first value=invalid\n" + header + b"kind=first value=42\n",
    capture_output=True,
    timeout=20,
)
assert result.returncode == 0, result.stderr
assert b"warning:" in result.stderr, result.stderr
assert b"= note: line 1" in result.stderr, result.stderr
assert b"= note: line 2" not in result.stderr, result.stderr
rows = [json.loads(line) for line in result.stdout.splitlines()]
assert len(rows) == 2 and rows[1]["extension"]["value"] == 42, rows

print("diagnostics and startup failures: ok")
