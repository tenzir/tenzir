# runner: python
# timeout: 120

import json
import os
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
invalid_documents = [
    (b"[1, 2]\n", b"document is not a map"),
    (b"value: [1\n", b"failed to load YAML document"),
    (
        b"keep: 1\nnested:\n  ? [invalid, key]\n  : value\n",
        b"failed to load YAML document",
    ),
    (b"loop: &loop [*loop]\n", b"document nesting exceeds limit"),
]
for invalid, warning in invalid_documents:
    result = subprocess.run(
        [*binary, "load_stdin | read_yaml _batch_size=1 | write_ndjson"],
        input=b"before: 1\n...\n" + invalid + b"...\nafter: 2\n",
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr
    assert b"warning: yaml parser: " in result.stderr, result.stderr
    assert warning in result.stderr, result.stderr
    actual = [json.loads(line) for line in result.stdout.splitlines()]
    assert actual == [{"before": 1}, {"after": 2}], actual

result = subprocess.run(
    [*binary, "load_stdin | read_yaml merge=true | write_ndjson"],
    input=b"value: 42\n",
    capture_output=True,
    timeout=20,
)
assert result.returncode == 0, result.stderr
assert b"`merge` has no effect with `--nova`" in result.stderr, result.stderr
assert json.loads(result.stdout) == {"value": 42}, result.stdout

print("invalid documents are skipped without partial events: ok")
