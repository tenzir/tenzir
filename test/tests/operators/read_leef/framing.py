# runner: python

import json
import os
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
header = "LEEF:1.0|Vendor|Product|1.0|42|"
source = (
    header
    + "src=192.0.2.1\tvalue=1\r\n\r\n"
    + header
    + "value=two\r"
    + "LEEF:2.0|Vendor|Product|1.0|42|^|"
    + 'value="héllo^world"\n'
    + header
    + "value=last"
).encode()
expected = [
    {"src": "192.0.2.1", "value": 1},
    {"value": "two"},
    {"value": "héllo^world"},
    {"value": "last"},
]
for reader in ("read_leef", "read_auto"):
    for size in (1, 2, 7, 64):
        result = subprocess.run(
            [
                *binary,
                f"load_stdin | split_bytes {size} | {reader} "
                "| select attributes, schema=@name, event_class_id | write_ndjson",
            ],
            input=source,
            capture_output=True,
            timeout=20,
        )
        assert result.returncode == 0 and not result.stderr, result.stderr
        actual = [json.loads(line) for line in result.stdout.splitlines()]
        assert actual == [
            {"attributes": value, "schema": "leef.event", "event_class_id": "42"}
            for value in expected
        ], (reader, size, actual)

for delimiter, separator in (
    ("^", "^"),
    ("x5E", "^"),
    ("0x5e", "^"),
    ("x9", "\t"),
    ("0x09", "\t"),
):
    result = subprocess.run(
        [*binary, "load_stdin | read_leef | select attributes | write_ndjson"],
        input=f"LEEF:2.0|V|P|1|42|{delimiter}|a=42{separator}b=two\n".encode(),
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 0 and not result.stderr, result.stderr
    assert json.loads(result.stdout) == {"attributes": {"a": 42, "b": "two"}}

for source in (b"", b"\r\n\n"):
    result = subprocess.run(
        [*binary, "load_stdin | read_leef | write_ndjson"],
        input=source,
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 0 and not result.stderr, result.stderr
    assert not result.stdout, result.stdout

print("framing, delimiters, and auto-detection: ok")
