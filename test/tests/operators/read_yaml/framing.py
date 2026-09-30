# runner: python
# timeout: 120

import json
import os
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
source = (
    "---\r\n{}\r\n...\r\n"
    "---\r\nvalue: héllo\r\n...\r\n"
    "---\rvalue: [1, two, null]\r"
    "---\nvalue: last"
).encode()
expected = [{}, {"value": "héllo"}, {"value": [1, "two", None]}, {"value": "last"}]

for size in (1, 2, 7, 64):
    for batch_size in (1, 3):
        result = subprocess.run(
            [
                *binary,
                f"load_stdin | split_bytes {size} "
                f"| read_yaml _batch_size={batch_size} | write_ndjson",
            ],
            input=source,
            capture_output=True,
            timeout=20,
        )
        assert result.returncode == 0 and not result.stderr, result.stderr
        actual = [json.loads(line) for line in result.stdout.splitlines()]
        assert actual == expected, (size, batch_size, actual)

for source in (b"", b"---\n...\n"):
    result = subprocess.run(
        [*binary, "load_stdin | read_yaml | write_ndjson"],
        input=source,
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 0 and not result.stderr, result.stderr
    assert not result.stdout, result.stdout

print("document markers, chunk boundaries, CRLF, UTF-8, and EOF: ok")
