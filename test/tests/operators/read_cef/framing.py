# runner: python

import json
import os
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
header = "CEF:0|Vendor|Product|1.0|42|Message|5|"
source = (
    header
    + "src=192.0.2.1 value=1\r\n\r\n"
    + header
    + "value=two\r"
    + header
    + 'value="héllo world"\n'
    + header
    + "value=last"
).encode()
expected = [
    {"src": "192.0.2.1", "value": 1},
    {"value": "two"},
    {"value": "héllo world"},
    {"value": "last"},
]
for reader in ("read_cef", "read_auto"):
    for size in (1, 2, 7, 64):
        result = subprocess.run(
            [
                *binary,
                f"load_stdin | split_bytes {size} | {reader} "
                "| select extension, schema=@name | write_ndjson",
            ],
            input=source,
            capture_output=True,
            timeout=20,
        )
        assert result.returncode == 0 and not result.stderr, result.stderr
        actual = [json.loads(line) for line in result.stdout.splitlines()]
        assert actual == [
            {"extension": value, "schema": "cef.event"} for value in expected
        ], (reader, size, actual)

for source in (b"", b"\r\n\n"):
    result = subprocess.run(
        [*binary, "load_stdin | read_cef | write_ndjson"],
        input=source,
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 0 and not result.stderr, result.stderr
    assert not result.stdout, result.stdout

print("framing and auto-detection: ok")
