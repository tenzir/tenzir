# runner: python
# timeout: 120

import json
import os
import shlex
import subprocess


binary = [*shlex.split(os.environ["TENZIR_BINARY"]), "--bare-mode", "--nova=true"]
source = (
    "first: 42\nsecond: hello\n...\n"
    "second: 7\nfirst: [1, two, null, {nested: true}]\n...\n"
    "key: 42\nkey: hello\nkey: [true, null]\n...\n"
    "outer.inner: 192.0.2.1\n"
).encode()
expected = [
    {"first": 42, "second": "hello"},
    {"second": 7, "first": [1, "two", None, {"nested": True}]},
    {"key": [42, "hello", [True, None]]},
    {"outer": {"inner": "192.0.2.1"}},
]
for batch_size in (1, 7):
    result = subprocess.run(
        [
            *binary,
            f'load_stdin | read_yaml unflatten_separator=".", _batch_size={batch_size} '
            "| write_ndjson",
        ],
        input=source,
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 0 and not result.stderr, result.stderr
    actual = [json.loads(line) for line in result.stdout.splitlines()]
    assert actual == expected, actual
    assert list(actual[1]) == ["second", "first"], actual[1]

for raw in (False, True):
    result = subprocess.run(
        [
            *binary,
            f"load_stdin | read_yaml raw={str(raw).lower()} "
            "| select flag=type_of(flag).kind, integer=type_of(integer).kind, "
            "unsigned=type_of(unsigned).kind, real=type_of(real).kind, "
            "ip=type_of(ip).kind, duration=type_of(duration).kind, "
            "time=type_of(time).kind | write_ndjson",
        ],
        input=(
            b"flag: true\ninteger: -42\nunsigned: 18446744073709551615\n"
            b"real: 1.5\nip: 192.0.2.1\nduration: 1s\n"
            b"time: 2024-01-02T03:04:05Z\n"
        ),
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == 0 and not result.stderr, result.stderr
    actual = json.loads(result.stdout)
    assert actual == {
        "flag": "bool",
        "integer": "int",
        "unsigned": "uint",
        "real": "float",
        "ip": "string" if raw else "ip",
        "duration": "string" if raw else "duration",
        "time": "string" if raw else "time",
    }, actual

# The writer and reader must round-trip heterogeneous records and lists.
result = subprocess.run(
    [*binary, "load_stdin | read_yaml | write_yaml | read_yaml | write_ndjson"],
    input=source,
    capture_output=True,
    timeout=20,
)
assert result.returncode == 0 and not result.stderr, result.stderr
actual = [json.loads(line) for line in result.stdout.splitlines()]
assert actual == [*expected[:-1], {"outer.inner": "192.0.2.1"}], actual

print("heterogeneous values, field order, repeated keys, inference, and round-trip: ok")
